---
name: voice-call
description: Make an outbound voice call to a phone number and play a TTS message. Use when the user asks to call someone, make a phone call, dial a number, or send a voice notification to a specific phone number.
---

# Voice Call

撥打電話給指定號碼，播放語音訊息後自動掛斷。

## Instructions

1. 從用戶訊息中提取：
   - **電話號碼**（E.164 格式，如 `+886912345678`、`+14155551234`）
   - **訊息內容**（要對對方說的話）
2. 根據訊息語言選擇對應語音
3. 執行腳本：

```bash
python3 .claude/skills/voice-call/voice_call.py \
  --phone "+886912345678" \
  --message "您好，提醒您下午三點有會議。"
```

4. 回報結果給用戶

## IMPORTANT: 不可重試

- 無論成功或失敗，每通電話只執行腳本**一次**
- 如果腳本回報失敗（對方未接、忙線、超時），直接告知用戶結果，**不要自動重撥**
- 只有在用戶明確要求「再打一次」時才重新執行

## Voice selection

根據訊息內容的語言自動選擇：
- 中文 → `zh-TW-HsiaoChenNeural`（預設）
- English → `en-US-JennyNeural`
- 日本語 → `ja-JP-NanamiNeural`

可用 `--voice` 參數覆寫。

## Examples

用戶：「打電話給 +886912345678 提醒他下午三點開會」
```bash
python3 .claude/skills/voice-call/voice_call.py --phone "+886912345678" --message "提醒您下午三點有會議，請準時參加。"
```

用戶：「Call +14155551234 and tell them the deployment is done」
```bash
python3 .claude/skills/voice-call/voice_call.py --phone "+14155551234" --message "The deployment is complete." --voice "en-US-JennyNeural"
```
