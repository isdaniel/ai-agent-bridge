//! Telegram Bot API platform using long-poll `getUpdates`.
//!
//! - Inbound: long-poll loop calling `getUpdates?offset=N&timeout=30`
//! - Outbound: `sendMessage`, `sendDocument`, `sendPhoto`, `sendAudio`
//! - Typing: `sendChatAction` with action "typing"
//! - No webhook/tunnel needed — works behind NAT.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{
    split_text, Attachment, AttachmentKind, Message, MessageHandler, Platform, ReplyCtx, Result,
    SessionKey,
};
use serde::Deserialize;
use tracing::{debug, error, info, warn};

const TELEGRAM_TEXT_MAX: usize = 4096;
const POLL_TIMEOUT_SECS: u64 = 30;

#[derive(Clone, Debug)]
pub struct TelegramConfig {
    pub bot_token: String,
}

pub struct TelegramPlatform {
    cfg: TelegramConfig,
    http: reqwest::Client,
    offset: AtomicI64,
}

impl TelegramPlatform {
    pub fn new(cfg: TelegramConfig) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
            offset: AtomicI64::new(0),
        }
    }

    fn api_url(&self, method: &str) -> String {
        format!(
            "https://api.telegram.org/bot{}/{}",
            self.cfg.bot_token, method
        )
    }

    async fn get_updates(&self) -> Result<Vec<Update>> {
        let offset = self.offset.load(Ordering::Relaxed);
        let params = serde_json::json!({
            "offset": offset,
            "timeout": POLL_TIMEOUT_SECS,
            "allowed_updates": ["message"]
        });
        let resp = self
            .http
            .post(self.api_url("getUpdates"))
            .json(&params)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Telegram getUpdates failed: {status} {text}");
        }
        let body: ApiResponse<Vec<Update>> = resp.json().await?;
        if !body.ok {
            anyhow::bail!(
                "Telegram getUpdates not ok: {}",
                body.description.unwrap_or_default()
            );
        }
        let updates = body.result.unwrap_or_default();
        if let Some(last) = updates.last() {
            self.offset.store(last.update_id + 1, Ordering::Relaxed);
        }
        Ok(updates)
    }
}

#[async_trait]
impl Platform for TelegramPlatform {
    fn name(&self) -> &'static str {
        "telegram"
    }

    async fn start(&self, handler: Arc<dyn MessageHandler>) -> Result<()> {
        info!("Telegram long-poll starting");
        loop {
            let updates = match self.get_updates().await {
                Ok(u) => u,
                Err(e) => {
                    error!(error=%e, "Telegram getUpdates error; retrying in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            for update in updates {
                let Some(msg) = update.message else {
                    continue;
                };
                let text = msg.text.unwrap_or_default();
                if text.is_empty() {
                    continue;
                }
                let chat_id = msg.chat.id;
                let user_id = msg.from.map(|u| u.id).unwrap_or(chat_id);
                let key = SessionKey::new("telegram", format!("{chat_id}/{user_id}"));
                let reply_ctx = ReplyCtx {
                    channel: Some(chat_id.to_string()),
                    user: Some(user_id.to_string()),
                    thread: msg.message_id.map(|id| id.to_string()),
                    extra: serde_json::Value::Null,
                };
                let message = Message {
                    key,
                    text,
                    attachments: vec![],
                    reply_ctx,
                    timestamp_ms: msg.date.unwrap_or(0) as i64 * 1000,
                };
                handler.handle(message).await;
            }
        }
    }

    async fn reply(&self, ctx: &ReplyCtx, text: &str) -> Result<()> {
        let chat_id = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Telegram reply requires chat_id in channel"))?;
        let chunks = split_text(text, TELEGRAM_TEXT_MAX);
        for chunk in chunks {
            let mut body = serde_json::json!({
                "chat_id": chat_id,
                "text": chunk,
            });
            if let Some(reply_to) = &ctx.thread {
                if let Ok(id) = reply_to.parse::<i64>() {
                    body["reply_to_message_id"] = serde_json::Value::Number(id.into());
                }
            }
            let resp = self
                .http
                .post(self.api_url("sendMessage"))
                .json(&body)
                .send()
                .await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                anyhow::bail!("Telegram sendMessage failed: {status} {text}");
            }
        }
        Ok(())
    }

    async fn send_attachment(&self, ctx: &ReplyCtx, att: &Attachment) -> Result<()> {
        let chat_id = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Telegram send_attachment requires chat_id"))?;
        let file_bytes = tokio::fs::read(&att.path).await?;
        let file_name = att.name.clone().unwrap_or_else(|| {
            att.path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into()
        });

        let (method, field) = match att.kind {
            AttachmentKind::Image => ("sendPhoto", "photo"),
            AttachmentKind::Audio => ("sendAudio", "audio"),
            AttachmentKind::File => ("sendDocument", "document"),
        };

        let part = reqwest::multipart::Part::bytes(file_bytes)
            .file_name(file_name)
            .mime_str(&att.mime)?;
        let form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .part(field, part);

        let resp = self
            .http
            .post(self.api_url(method))
            .multipart(form)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!(%method, %status, "Telegram attachment send failed: {text}");
            anyhow::bail!("Telegram {method} failed: {status}");
        }
        Ok(())
    }

    async fn show_typing(&self, ctx: &ReplyCtx) -> Result<()> {
        let chat_id = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Telegram show_typing requires chat_id"))?;
        let body = serde_json::json!({
            "chat_id": chat_id,
            "action": "typing"
        });
        let resp = self
            .http
            .post(self.api_url("sendChatAction"))
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            debug!("Telegram sendChatAction failed: {status} {text}");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<TgMessage>,
}

#[derive(Debug, Deserialize)]
struct TgMessage {
    message_id: Option<i64>,
    from: Option<TgUser>,
    chat: TgChat,
    date: Option<u64>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TgUser {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct TgChat {
    id: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_response_ok_parses() {
        let json = r#"{"ok":true,"result":[{"update_id":1}]}"#;
        let resp: ApiResponse<Vec<Update>> = serde_json::from_str(json).unwrap();
        assert!(resp.ok);
        assert_eq!(resp.result.unwrap().len(), 1);
    }

    #[test]
    fn api_response_error_parses() {
        let json = r#"{"ok":false,"description":"Unauthorized"}"#;
        let resp: ApiResponse<Vec<Update>> = serde_json::from_str(json).unwrap();
        assert!(!resp.ok);
        assert_eq!(resp.description.as_deref(), Some("Unauthorized"));
    }

    #[test]
    fn update_with_text_message_parses() {
        let json = r#"{
            "update_id": 100,
            "message": {
                "message_id": 42,
                "from": {"id": 12345},
                "chat": {"id": 67890},
                "date": 1700000000,
                "text": "hello world"
            }
        }"#;
        let update: Update = serde_json::from_str(json).unwrap();
        assert_eq!(update.update_id, 100);
        let msg = update.message.unwrap();
        assert_eq!(msg.text.as_deref(), Some("hello world"));
        assert_eq!(msg.chat.id, 67890);
        assert_eq!(msg.from.unwrap().id, 12345);
        assert_eq!(msg.date, Some(1700000000));
    }

    #[test]
    fn update_without_message_parses() {
        let json = r#"{"update_id": 101}"#;
        let update: Update = serde_json::from_str(json).unwrap();
        assert!(update.message.is_none());
    }

    #[test]
    fn update_without_text_parses() {
        let json = r#"{
            "update_id": 102,
            "message": {
                "chat": {"id": 1},
                "date": 1700000000
            }
        }"#;
        let update: Update = serde_json::from_str(json).unwrap();
        let msg = update.message.unwrap();
        assert!(msg.text.is_none());
        assert!(msg.from.is_none());
    }

    #[test]
    fn platform_name() {
        let p = TelegramPlatform::new(TelegramConfig {
            bot_token: "test".into(),
        });
        assert_eq!(p.name(), "telegram");
    }

    #[test]
    fn api_url_format() {
        let p = TelegramPlatform::new(TelegramConfig {
            bot_token: "123:ABC".into(),
        });
        assert_eq!(
            p.api_url("sendMessage"),
            "https://api.telegram.org/bot123:ABC/sendMessage"
        );
    }

    #[test]
    fn text_split_at_telegram_limit() {
        let text = "A".repeat(8000);
        let chunks = split_text(&text, TELEGRAM_TEXT_MAX);
        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(chunk.len() <= TELEGRAM_TEXT_MAX);
        }
    }
}
