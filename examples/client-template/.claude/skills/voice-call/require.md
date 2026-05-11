# Voice Call Skill — 需求與設定指南

## 概述

此 skill 讓 AI agent 能夠撥打電話給指定號碼並播放 TTS 語音訊息（通知型通話）。
用戶只需提供電話號碼和訊息內容，系統自動完成撥號、播放、掛斷。

## 架構流程

```
用戶：「打電話給 +886912345678 說 提醒開會」
  ↓
Agent 提取電話號碼 + 訊息
  ↓
voice_call.py 啟動
  ├── 啟動本地 Flask server (:9090)
  ├── 啟動 cloudflared tunnel → 取得隨機 HTTPS URL
  ├── 呼叫 ACS create_call() → 撥打電話
  ├── CallConnected → play_media_to_all(TTS)
  ├── PlayCompleted → hang_up()
  └── 關閉 tunnel，回報結果
```

## 所需 Azure 服務

### 1. Azure Communication Services (ACS)

| 項目 | 值 |
|------|---|
| 資源名稱 | `mail-lab` |
| Resource Group | `play_ground` |
| 區域 | Japan (global) |
| Hostname | `mail-lab.japan.communication.azure.com` |
| 電話號碼 | `+18442205653` (US Toll-Free, outbound calling) |
| 月費 | $2 USD/月（號碼租用）+ 通話費 |

**用途**：撥打電話、管理通話生命週期（建立、播放、掛斷）

### 2. Azure AI Services (Cognitive Services)

| 項目 | 值 |
|------|---|
| 資源名稱 | `danie-m77lsz0k-eastus2` |
| Resource Group | `AI-lab` |
| 區域 | East US 2 |
| Endpoint | `https://danie-m77lsz0k-eastus2.cognitiveservices.azure.com/` |
| SKU | S0 |
| Kind | AIServices |

**用途**：文字轉語音 (TTS)，將訊息文字合成為語音播放給受話方

### 3. Cloudflared (Cloudflare Tunnel)

| 項目 | 值 |
|------|---|
| 版本 | 2026.3.0 |
| 安裝路徑 | `/usr/local/bin/cloudflared` |
| 用途 | 為本地 callback server 提供臨時 HTTPS URL |
| 模式 | Quick tunnel（無需帳號，每次通話臨時建立） |

**為什麼需要**：ACS 的 webhook callback 要求 HTTPS endpoint，cloudflared 自動建立隨機 `https://xxx.trycloudflare.com` 隧道。

## 關鍵設定：資源連結

### 問題

ACS 播放 TTS 語音時，需要存取 AI Services 資源。若未正確連結，會收到：

```
(8522) Request not allowed when Cognitive Service Configuration not set during call setup.
```

### 解決方案

需要完成以下兩步：

**步驟 1：啟用 ACS 的 Managed Identity**

```bash
az communication update --name mail-lab --resource-group play_ground --type SystemAssigned
```

結果：
- Principal ID: `2b146767-605c-452d-8692-921b18f37f84`

**步驟 2：授予 AI Services 存取權限**

```bash
az role assignment create \
  --assignee "2b146767-605c-452d-8692-921b18f37f84" \
  --role "Cognitive Services User" \
  --scope "/subscriptions/920c2ea4-4af8-4dfe-812d-b5070befb952/resourceGroups/AI-lab/providers/Microsoft.CognitiveServices/accounts/danie-m77lsz0k-eastus2"
```

> 角色指派需要 1-2 分鐘傳播生效。

## 環境設定

### `.env` 檔案

位置：`.claude/skills/voice-call/.env`（git ignored）

```env
ACS_CONNECTION_STRING=endpoint=https://mail-lab.japan.communication.azure.com/;accesskey=<YOUR_KEY>
ACS_PHONE_NUMBER=+18442205653
COGNITIVE_SERVICES_ENDPOINT=https://danie-m77lsz0k-eastus2.cognitiveservices.azure.com/
CALLBACK_PORT=9090
```

> `CALLBACK_HOST` 不需要設定 — 腳本會自動透過 cloudflared 取得 HTTPS URL。

### NSG 規則

| 規則名稱 | Port | 方向 | 用途 |
|----------|------|------|------|
| `AllowACSCallback9090` | TCP 9090 | Inbound | cloudflared tunnel 反向連線（實際上 tunnel 是 outbound，此規則為備用直連方式） |

## SDK 版本注意事項

安裝版本：`azure-communication-callautomation==1.5.0`

### 與文檔範例的差異

| 文檔範例（舊版 API） | SDK v1.5.0 正確用法 |
|---|---|
| `CallInvite(target, source_caller_id_number=caller)` | 直接傳 target 給 `create_call()` |
| `conn.get_call_media().play_media_to_all(src)` | `conn.play_media_to_all(src)` |
| `client.create_call(call_invite, url, cognitive_services_endpoint=...)` | `client.create_call(target, url, source_caller_id_number=caller, cognitive_services_endpoint=...)` |

## 電話號碼限制

| 項目 | 說明 |
|------|------|
| 號碼類型 | US Toll-Free |
| 撥出範圍 | 可撥打國際（含台灣 +886） |
| 來電顯示 | 對方看到美國號碼 `+18442205653` |
| 台灣號碼 | ACS 列出 TW 但無可購買方案，需透過 Portal 申請 |

## TTS 語音選項

| Voice Name | 語言 |
|------------|------|
| `zh-TW-HsiaoChenNeural` | 繁體中文 (台灣) 女聲 — 預設 |
| `zh-TW-YunJheNeural` | 繁體中文 (台灣) 男聲 |
| `zh-CN-XiaoxiaoNeural` | 簡體中文 女聲 |
| `en-US-JennyNeural` | English female |
| `ja-JP-NanamiNeural` | 日本語 女性 |

## 重要行為規則

- **不可自動重試**：每通電話只執行一次，失敗就回報用戶
- **通話時間**：單次通話 timeout 預設 60 秒
- **Tunnel 生命週期**：通話期間臨時建立，結束即關閉

## 檔案結構

```
.claude/skills/voice-call/
├── SKILL.md        ← Agent 指令（name + description frontmatter）
├── voice_call.py   ← 主腳本（自動載入 .env，啟動 tunnel，撥打電話）
├── .env            ← 實際設定（git ignored）
└── example.env     ← 設定範本（git tracked）
```

## 部署到新環境的步驟

1. 安裝依賴：`pip install azure-communication-callautomation flask`
2. 安裝 cloudflared：`curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared`
3. 建立 ACS 資源 + 購買電話號碼
4. 建立 AI Services 資源
5. 連結：ACS Managed Identity → AI Services `Cognitive Services User` 角色
6. 複製 `example.env` → `.env` 並填入值
7. 測試：`python3 .claude/skills/voice-call/voice_call.py --phone "+1..." --message "test"`
