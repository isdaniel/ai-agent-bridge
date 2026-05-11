//! LINE Messaging API platform.
//!
//! - Inbound: HTTPS webhook (signature-verified HMAC-SHA256), parses message
//!   events, downloads media to a temp file, dispatches to MessageHandler.
//! - Outbound: Push API (reply tokens are too short-lived for AI latency).
//!   Attachments require a public HTTPS URL — provided by an injected
//!   [`MediaPublisher`].
//! - Optional allowlist of LINE user IDs (drops everything else silently).
//! - Post-restart timestamp filter so backlogged events from before the
//!   process start aren't replayed.

mod sign;
mod webhook;

use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{Attachment, AttachmentKind, MessageHandler, Platform, ReplyCtx, Result};
use media_publisher::MediaPublisher;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

pub use sign::verify_signature;

#[derive(Clone, Debug)]
pub struct LineConfig {
    pub channel_secret: String,
    pub channel_token: String,
    pub bind: String,
    pub allowlist: Vec<String>,
}

pub struct LinePlatform {
    cfg: LineConfig,
    boot_ms: i64,
    http: reqwest::Client,
    handler: Mutex<Option<Arc<dyn MessageHandler>>>,
    publisher: Option<Arc<dyn MediaPublisher>>,
}

impl LinePlatform {
    pub fn new(cfg: LineConfig) -> Self {
        Self {
            cfg,
            boot_ms: now_ms(),
            http: reqwest::Client::new(),
            handler: Mutex::new(None),
            publisher: None,
        }
    }

    pub fn with_publisher(mut self, publisher: Arc<dyn MediaPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
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
        let body = serde_json::json!({
            "to": to,
            "messages": [{"type": "text", "text": text}]
        });
        let resp = self
            .http
            .post("https://api.line.me/v2/bot/message/push")
            .bearer_auth(&self.cfg.channel_token)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("LINE push failed: {status} {body}");
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
                "duration": 1000, // best-effort placeholder; LINE requires it
            }),
            AttachmentKind::File => {
                // LINE has no generic file message; expose as a text link.
                serde_json::json!({
                    "type": "text",
                    "text": format!("file: {url_str}"),
                })
            }
        };
        let body = serde_json::json!({"to": to, "messages": [line_msg]});
        let resp = self
            .http
            .post("https://api.line.me/v2/bot/message/push")
            .bearer_auth(&self.cfg.channel_token)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("LINE push (attachment) failed: {status} {body}");
        }
        Ok(())
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

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
