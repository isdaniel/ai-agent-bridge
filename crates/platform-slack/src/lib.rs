//! Slack platform: Socket Mode WebSocket receiver + Web API replies/uploads.
//!
//! Connect flow:
//!   1. POST `apps.connections.open` with `app_token` (xapp-…) → `{wss_url}`
//!   2. WebSocket-connect to `wss_url`
//!   3. For each frame: parse [`Envelope`], ack with `{envelope_id}`, then
//!      dispatch `events_api/message` events to the engine.
//!   4. Auto-reconnect on disconnect with exponential backoff.

mod envelope;
mod upload;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use core_traits::{
    safe_filename, split_text, Attachment, AttachmentKind, Message, MessageHandler, Platform,
    ReplyCtx, Result, SessionKey,
};
use dashmap::DashMap;
use envelope::{Envelope, EventsApiPayload, MessageEvent, SlackEvent, SlackFile};
use futures_util::{SinkExt, StreamExt};
use media_publisher::MediaPublisher;
use serde::Deserialize;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug)]
pub struct SlackConfig {
    /// `xapp-...` Socket Mode app-level token.
    pub app_token: String,
    /// `xoxb-...` bot token used for chat.postMessage / files.uploadV2.
    pub bot_token: String,
}

pub struct SlackPlatform {
    cfg: SlackConfig,
    http: reqwest::Client,
    bot_user_id: tokio::sync::OnceCell<String>,
    /// Tracks posted "thinking" indicator messages: `"channel:thread_ts"` → message `ts`.
    /// Cleaned up when `reply` or `send_attachment` is called.
    thinking_messages: DashMap<String, String>,
    publisher: Option<Arc<dyn MediaPublisher>>,
}

impl SlackPlatform {
    pub fn new(cfg: SlackConfig) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
            bot_user_id: tokio::sync::OnceCell::new(),
            thinking_messages: DashMap::new(),
            publisher: None,
        }
    }

    pub fn with_publisher(mut self, publisher: Arc<dyn MediaPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    async fn resolve_bot_user_id(&self) -> Result<&str> {
        self.bot_user_id
            .get_or_try_init(|| async {
                #[derive(Deserialize)]
                struct Resp {
                    ok: bool,
                    user_id: Option<String>,
                    error: Option<String>,
                }
                let r: Resp = self
                    .http
                    .post("https://slack.com/api/auth.test")
                    .bearer_auth(&self.cfg.bot_token)
                    .send()
                    .await?
                    .json()
                    .await?;
                if !r.ok {
                    anyhow::bail!("auth.test failed: {}", r.error.unwrap_or_default());
                }
                r.user_id
                    .ok_or_else(|| anyhow::anyhow!("auth.test returned no user_id"))
            })
            .await
            .map(|s| s.as_str())
    }

    async fn open_socket_url(&self) -> Result<String> {
        #[derive(Deserialize)]
        struct Resp {
            ok: bool,
            url: Option<String>,
            error: Option<String>,
        }
        let r: Resp = self
            .http
            .post("https://slack.com/api/apps.connections.open")
            .bearer_auth(&self.cfg.app_token)
            .send()
            .await?
            .json()
            .await?;
        if !r.ok {
            anyhow::bail!(
                "apps.connections.open failed: {}",
                r.error.unwrap_or_default()
            );
        }
        r.url.ok_or_else(|| anyhow::anyhow!("no wss url returned"))
    }

    async fn download_file(&self, file: &SlackFile) -> Result<Attachment> {
        let url = file
            .url_private_download
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("file missing url_private_download"))?;
        let resp = self
            .http
            .get(url)
            .bearer_auth(&self.cfg.bot_token)
            .send()
            .await?
            .error_for_status()?;
        let bytes = resp.bytes().await?;
        let mime = file
            .mimetype
            .clone()
            .unwrap_or_else(|| "application/octet-stream".into());
        let kind = if mime.starts_with("image/") {
            AttachmentKind::Image
        } else if mime.starts_with("audio/") {
            AttachmentKind::Audio
        } else {
            AttachmentKind::File
        };
        // Sanitise: file.name is uploader-controlled.
        let raw_name = file.name.clone().unwrap_or_else(|| file.id.clone());
        let name = safe_filename(&raw_name);
        let path = core_traits::write_temp_file(&name, &bytes)?;
        Ok(Attachment {
            kind,
            path,
            mime,
            bytes: Some(bytes.len() as u64),
            name: Some(name),
        })
    }

    fn thinking_key(ctx: &ReplyCtx) -> String {
        let ch = ctx.channel.as_deref().unwrap_or("");
        let ts = ctx.thread.as_deref().unwrap_or("");
        format!("{ch}:{ts}")
    }

    async fn clear_thinking(&self, ctx: &ReplyCtx) {
        let key = Self::thinking_key(ctx);
        if let Some((_, ts)) = self.thinking_messages.remove(&key) {
            if let Some(channel) = &ctx.channel {
                let body = serde_json::json!({ "channel": channel, "ts": ts });
                let _ = self
                    .http
                    .post("https://slack.com/api/chat.delete")
                    .bearer_auth(&self.cfg.bot_token)
                    .json(&body)
                    .send()
                    .await;
            }
        }
    }

    async fn dispatch(&self, ev: MessageEvent, handler: &Arc<dyn MessageHandler>) {
        if ev.is_skippable() {
            return;
        }
        let channel = match &ev.channel {
            Some(c) => c.clone(),
            None => return,
        };
        let user = ev.user.clone().unwrap_or_default();

        // In channels (C/G), only respond when @mentioned. DMs (D) always respond.
        let is_dm = channel.starts_with('D');
        let mut text = ev.text.clone();
        if !is_dm {
            let bot_id = match self.resolve_bot_user_id().await {
                Ok(id) => id,
                Err(e) => {
                    warn!(error=%e, "could not resolve bot user id");
                    return;
                }
            };
            let mention_tag = format!("<@{bot_id}>");
            if !text.contains(&mention_tag) {
                return;
            }
            text = text.replace(&mention_tag, "").trim().to_string();
            if text.is_empty() {
                return;
            }
        }

        let scoped = format!("{channel}/{user}");
        let key = SessionKey::new("slack", scoped);

        let mut attachments = Vec::new();
        for f in &ev.files {
            match self.download_file(f).await {
                Ok(a) => attachments.push(a),
                Err(e) => warn!(file_id=%f.id, error=%e, "slack file download failed"),
            }
        }

        let ts_ms = ev
            .ts
            .as_deref()
            .and_then(|s| s.split('.').next())
            .and_then(|s| s.parse::<i64>().ok())
            .map(|s| s * 1000)
            .unwrap_or(0);

        let msg = Message {
            key,
            text,
            attachments,
            reply_ctx: ReplyCtx {
                channel: Some(channel),
                thread: ev.thread_ts.or(ev.ts),
                user: ev.user,
                extra: serde_json::Value::Null,
            },
            timestamp_ms: ts_ms,
        };
        handler.handle(msg).await;
    }

    async fn run_once(&self, handler: Arc<dyn MessageHandler>) -> Result<()> {
        let url = self.open_socket_url().await?;
        debug!(%url, "slack socket mode connecting");
        let (ws, _) = tokio_tungstenite::connect_async(&url).await?;
        let (mut write, mut read) = ws.split();
        info!("slack socket mode connected");
        while let Some(frame) = read.next().await {
            let frame = match frame {
                Ok(f) => f,
                Err(e) => {
                    warn!(error=%e, "slack ws read error");
                    break;
                }
            };
            let text = match frame {
                WsMessage::Text(t) => t,
                WsMessage::Ping(p) => {
                    let _ = write.send(WsMessage::Pong(p)).await;
                    continue;
                }
                WsMessage::Close(_) => break,
                _ => continue,
            };
            let env: Envelope = match serde_json::from_str(&text) {
                Ok(e) => e,
                Err(e) => {
                    warn!(error=%e, "slack envelope parse failed");
                    continue;
                }
            };
            if let Some(eid) = &env.envelope_id {
                let ack = serde_json::json!({ "envelope_id": eid });
                let _ = write.send(WsMessage::text(ack.to_string())).await;
            }
            if env.kind != "events_api" {
                continue;
            }
            let payload: EventsApiPayload = match serde_json::from_value(env.payload) {
                Ok(p) => p,
                Err(e) => {
                    warn!(error=%e, "slack events_api payload parse failed");
                    continue;
                }
            };
            if let SlackEvent::Message(m) = payload.event {
                self.dispatch(m, &handler).await;
            }
        }
        Ok(())
    }

    /// Send a download link using Block Kit with an explicit button.
    async fn send_download_link(&self, ctx: &ReplyCtx, name: &str, url: &str) -> Result<()> {
        let channel = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Slack send_download_link requires channel"))?;
        let mut body = serde_json::json!({
            "channel": channel,
            "text": format!("📎 {name}"),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": format!("📎 <{url}|{name}>")
                    },
                    "accessory": {
                        "type": "button",
                        "text": { "type": "plain_text", "text": "Download" },
                        "url": url,
                        "action_id": "download_file"
                    }
                }
            ]
        });
        if let Some(ts) = &ctx.thread {
            body["thread_ts"] = serde_json::Value::String(ts.clone());
        }
        let resp = self
            .http
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(&self.cfg.bot_token)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("slack postMessage HTTP {}", resp.status());
        }
        Ok(())
    }
}

#[async_trait]
impl Platform for SlackPlatform {
    fn name(&self) -> &'static str {
        "slack"
    }

    async fn start(&self, handler: Arc<dyn MessageHandler>) -> Result<()> {
        match self.resolve_bot_user_id().await {
            Ok(id) => {
                info!(bot_user_id=%id, "slack bot identity resolved; mention-only in channels")
            }
            Err(e) => warn!(error=%e, "could not resolve bot user id; will retry on first message"),
        }
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.run_once(handler.clone()).await {
                Ok(()) => {
                    info!("slack socket disconnected cleanly; reconnecting");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    error!(error=%e, "slack socket loop failed; backing off");
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    async fn reply(&self, ctx: &ReplyCtx, text: &str) -> Result<()> {
        self.clear_thinking(ctx).await;
        let channel = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Slack reply requires channel"))?;
        let chunks = split_text(text, 3900);
        for chunk in chunks {
            let mut body = serde_json::json!({
                "channel": channel,
                "text": chunk,
            });
            if let Some(ts) = &ctx.thread {
                body["thread_ts"] = serde_json::Value::String(ts.clone());
            }
            let resp = self
                .http
                .post("https://slack.com/api/chat.postMessage")
                .bearer_auth(&self.cfg.bot_token)
                .json(&body)
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("slack postMessage HTTP {}", resp.status());
            }
        }
        Ok(())
    }

    async fn send_attachment(&self, ctx: &ReplyCtx, att: &Attachment) -> Result<()> {
        self.clear_thinking(ctx).await;
        if let Some(publisher) = &self.publisher {
            let url = publisher.publish(&att.path, &att.mime).await?;
            let name = att
                .name
                .as_deref()
                .or_else(|| att.path.file_name().and_then(|n| n.to_str()))
                .unwrap_or("file");
            self.send_download_link(ctx, name, url.as_str()).await?;
            return Ok(());
        }
        let channel = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Slack send_attachment requires channel"))?;
        let bytes = tokio::fs::read(&att.path).await?;
        let filename = att
            .name
            .clone()
            .or_else(|| {
                att.path
                    .file_name()
                    .map(|s| s.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| "file".into());
        upload::upload_file(
            &self.http,
            &self.cfg.bot_token,
            channel,
            ctx.thread.as_deref(),
            bytes::Bytes::from(bytes),
            &filename,
        )
        .await?;
        Ok(())
    }

    async fn show_typing(&self, ctx: &ReplyCtx) -> Result<()> {
        let key = Self::thinking_key(ctx);
        if self.thinking_messages.contains_key(&key) {
            return Ok(());
        }
        let channel = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Slack show_typing requires channel"))?;
        let mut body = serde_json::json!({
            "channel": channel,
            "text": "\u{1f4ad}",
        });
        if let Some(ts) = &ctx.thread {
            body["thread_ts"] = serde_json::Value::String(ts.clone());
        }
        #[derive(Deserialize)]
        struct Resp {
            ok: bool,
            ts: Option<String>,
        }
        let resp: Resp = self
            .http
            .post("https://slack.com/api/chat.postMessage")
            .bearer_auth(&self.cfg.bot_token)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        if resp.ok {
            if let Some(ts) = resp.ts {
                self.thinking_messages.insert(key, ts);
            }
        }
        Ok(())
    }
}
