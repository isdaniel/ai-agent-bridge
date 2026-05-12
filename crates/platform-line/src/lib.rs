//! LINE Messaging API platform.
//!
//! - Inbound: HTTPS webhook (signature-verified HMAC-SHA256), parses message
//!   events, downloads media to a temp file, dispatches to MessageHandler.
//! - Outbound: Reply API first (free, no quota), Push API fallback (counted).
//!   Reply tokens from the webhook are valid ~30 s; if the agent responds
//!   within 25 s the reply is free. Otherwise falls back to Push API.
//!   Attachments require a public HTTPS URL — provided by an injected
//!   [`MediaPublisher`].
//! - Optional allowlist of LINE user IDs (drops everything else silently).
//! - Post-restart timestamp filter so backlogged events from before the
//!   process start aren't replayed.

mod sign;
mod webhook;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use core_traits::split_text;
use core_traits::{Attachment, AttachmentKind, MessageHandler, Platform, ReplyCtx, Result};
use media_publisher::MediaPublisher;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

pub use sign::verify_signature;

const REPLY_TOKEN_TTL_MS: i64 = 25_000;
const LINE_TEXT_MAX: usize = 5000;
const LINE_MESSAGES_PER_CALL: usize = 5;

#[derive(Clone, Debug)]
pub struct LineConfig {
    pub channel_secret: String,
    pub channel_token: String,
    pub bind: String,
    pub allowlist: Vec<String>,
    /// Monthly push message limit. None = unlimited (no rate limiting).
    pub push_limit: Option<u64>,
}

pub struct LinePlatform {
    cfg: LineConfig,
    boot_ms: i64,
    http: reqwest::Client,
    handler: Mutex<Option<Arc<dyn MessageHandler>>>,
    publisher: Option<Arc<dyn MediaPublisher>>,
    push_count: AtomicU64,
}

impl LinePlatform {
    pub fn new(cfg: LineConfig) -> Self {
        Self {
            cfg,
            boot_ms: now_ms(),
            http: reqwest::Client::new(),
            handler: Mutex::new(None),
            publisher: None,
            push_count: AtomicU64::new(0),
        }
    }

    pub fn with_publisher(mut self, publisher: Arc<dyn MediaPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    fn extract_reply_token(extra: &serde_json::Value) -> Option<String> {
        let token = extra.get("reply_token")?.as_str()?;
        if token.is_empty() {
            return None;
        }
        let ts = extra.get("reply_token_ms")?.as_i64()?;
        let elapsed = now_ms() - ts;
        if elapsed > REPLY_TOKEN_TTL_MS {
            return None;
        }
        Some(token.to_string())
    }

    async fn reply_via_token(&self, token: &str, messages: &[serde_json::Value]) -> Result<()> {
        let body = serde_json::json!({
            "replyToken": token,
            "messages": messages
        });
        let resp = self
            .http
            .post("https://api.line.me/v2/bot/message/reply")
            .bearer_auth(&self.cfg.channel_token)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("LINE reply-token failed: {status} {text}");
        }
        Ok(())
    }

    async fn push_message(&self, to: &str, messages: &[serde_json::Value]) -> Result<()> {
        if let Some(limit) = self.cfg.push_limit {
            let count = self.push_count.load(Ordering::Relaxed);
            if count >= limit {
                anyhow::bail!(
                    "LINE push message quota exhausted ({}/{}). Wait for monthly reset.",
                    count,
                    limit
                );
            }
            if count >= limit * 9 / 10 {
                warn!(count, limit, "LINE push quota near exhaustion (>90%)");
            }
        }
        let body = serde_json::json!({"to": to, "messages": messages});
        let resp = self
            .http
            .post("https://api.line.me/v2/bot/message/push")
            .bearer_auth(&self.cfg.channel_token)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("LINE push failed: {status} {text}");
        }
        self.push_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl Platform for LinePlatform {
    fn name(&self) -> &'static str {
        "line"
    }

    async fn start(&self, handler: Arc<dyn MessageHandler>) -> Result<()> {
        *self.handler.lock().await = Some(handler.clone());
        let app = webhook::router(self.cfg.clone(), self.boot_ms, handler);
        let bind = self.cfg.bind.clone();
        info!(%bind, "LINE webhook listening");
        let listener = tokio::net::TcpListener::bind(&bind).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn reply(&self, ctx: &ReplyCtx, text: &str) -> Result<()> {
        let to = ctx
            .user
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("LINE reply requires user id in ReplyCtx"))?;

        let chunks = split_text(text, LINE_TEXT_MAX);

        for batch in chunks.chunks(LINE_MESSAGES_PER_CALL) {
            let messages: Vec<serde_json::Value> = batch
                .iter()
                .map(|t| serde_json::json!({"type": "text", "text": t}))
                .collect();

            if let Some(token) = Self::extract_reply_token(&ctx.extra) {
                match self.reply_via_token(&token, &messages).await {
                    Ok(()) => continue,
                    Err(e) => {
                        debug!(error=%e, "reply token failed, falling back to push");
                    }
                }
            }

            self.push_message(to, &messages).await?;
        }
        Ok(())
    }

    async fn send_attachment(&self, ctx: &ReplyCtx, att: &Attachment) -> Result<()> {
        let to = ctx
            .user
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("LINE reply requires user id in ReplyCtx"))?;
        let publisher = match &self.publisher {
            Some(p) => p.clone(),
            None => {
                warn!("LINE send_attachment without publisher; falling back to text marker");
                return self
                    .reply(
                        ctx,
                        &format!(
                            "[attachment {} — no publisher configured]",
                            att.path.display()
                        ),
                    )
                    .await;
            }
        };
        let url = publisher.publish(&att.path, &att.mime).await?;
        let url_str = url.to_string();
        let line_msg = match att.kind {
            AttachmentKind::Image => serde_json::json!({
                "type": "image",
                "originalContentUrl": url_str,
                "previewImageUrl": url_str,
            }),
            AttachmentKind::Audio => serde_json::json!({
                "type": "audio",
                "originalContentUrl": url_str,
                "duration": 1000,
            }),
            AttachmentKind::File => {
                let label = att.name.as_deref().unwrap_or("file");
                serde_json::json!({
                    "type": "text",
                    "text": format!("📎 {label}\n{url_str}"),
                })
            }
        };
        let messages = [line_msg];

        if let Some(token) = Self::extract_reply_token(&ctx.extra) {
            match self.reply_via_token(&token, &messages).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    debug!(error=%e, "reply token failed for attachment, falling back to push");
                }
            }
        }

        self.push_message(to, &messages).await
    }

    async fn show_typing(&self, ctx: &ReplyCtx) -> Result<()> {
        let chat_id = ctx
            .user
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("LINE show_typing requires user id"))?;
        let body = serde_json::json!({
            "chatId": chat_id,
            "loadingSeconds": 20
        });
        let resp = self
            .http
            .post("https://api.line.me/v2/bot/chat/loading")
            .bearer_auth(&self.cfg.channel_token)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            debug!("LINE loading animation failed: {status} {text}");
        }
        Ok(())
    }
}

pub(crate) fn now_ms() -> i64 {
    core_traits::now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_no_split() {
        let chunks = split_text("hello world", 5000);
        assert_eq!(chunks, vec!["hello world"]);
    }

    #[test]
    fn splits_at_section_divider() {
        let text = format!("{}\n---\n{}", "A".repeat(100), "B".repeat(100));
        let chunks = split_text(&text, 150);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].starts_with('A'));
        assert!(chunks[1].starts_with('B'));
    }

    #[test]
    fn splits_at_double_newline() {
        let text = format!("{}\n\n{}", "A".repeat(100), "B".repeat(100));
        let chunks = split_text(&text, 150);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn splits_at_single_newline_fallback() {
        let text = format!("{}\n{}", "A".repeat(100), "B".repeat(100));
        let chunks = split_text(&text, 150);
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn hard_cut_no_newline() {
        let text = "A".repeat(300);
        let chunks = split_text(&text, 100);
        assert!(chunks.len() >= 3);
        for chunk in &chunks {
            assert!(chunk.len() <= 100);
        }
    }

    #[test]
    fn multibyte_chars_no_panic() {
        let text = "正".repeat(2000);
        let chunks = split_text(&text, 5000);
        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(chunk.len() <= 5000);
        }
    }

    #[test]
    fn real_world_long_message() {
        let sections: Vec<String> = (0..10)
            .map(|i| format!("## Section {}\n{}", i, "content ".repeat(80)))
            .collect();
        let text = sections.join("\n\n");
        let chunks = split_text(&text, 5000);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.len() <= 5000, "chunk too long: {}", chunk.len());
        }
    }
}
