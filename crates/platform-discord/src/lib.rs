//! Discord platform via Gateway WebSocket + REST API.
//!
//! - Inbound: connects to the Discord Gateway (wss://gateway.discord.gg),
//!   sends Identify with MESSAGE_CONTENT + GUILD_MESSAGES + DIRECT_MESSAGES intents,
//!   handles heartbeat/resume, dispatches MESSAGE_CREATE events.
//! - Outbound: REST `POST /channels/{id}/messages` (text + multipart attachments).
//! - Typing: `POST /channels/{id}/typing` (free, 10s TTL).
//! - Mention-only in guilds: only responds when bot is mentioned. DMs always respond.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use core_traits::{
    split_text, Attachment, Message, MessageHandler, Platform, ReplyCtx, Result, SessionKey,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, warn};

const DISCORD_TEXT_MAX: usize = 2000;
const API_BASE: &str = "https://discord.com/api/v10";

#[derive(Clone, Debug)]
pub struct DiscordConfig {
    pub bot_token: String,
}

pub struct DiscordPlatform {
    cfg: DiscordConfig,
    http: reqwest::Client,
    bot_user_id: tokio::sync::OnceCell<String>,
    sequence: AtomicU64,
}

impl DiscordPlatform {
    pub fn new(cfg: DiscordConfig) -> Self {
        Self {
            cfg,
            http: reqwest::Client::new(),
            bot_user_id: tokio::sync::OnceCell::new(),
            sequence: AtomicU64::new(0),
        }
    }

    fn auth_header(&self) -> String {
        format!("Bot {}", self.cfg.bot_token)
    }

    async fn get_gateway_url(&self) -> Result<String> {
        let resp = self
            .http
            .get(format!("{API_BASE}/gateway/bot"))
            .header("Authorization", self.auth_header())
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Discord GET /gateway/bot failed: {status} {text}");
        }
        let body: serde_json::Value = resp.json().await?;
        let url = body["url"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("no url in gateway/bot response"))?;
        Ok(format!("{url}/?v=10&encoding=json"))
    }

    fn intents() -> u64 {
        // GUILDS (1<<0) | GUILD_MESSAGES (1<<9) | DIRECT_MESSAGES (1<<12) | MESSAGE_CONTENT (1<<15)
        (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15)
    }

    fn identify_payload(&self) -> serde_json::Value {
        serde_json::json!({
            "op": 2,
            "d": {
                "token": self.cfg.bot_token,
                "intents": Self::intents(),
                "properties": {
                    "os": "linux",
                    "browser": "ai-agent-bridge",
                    "device": "ai-agent-bridge"
                }
            }
        })
    }
}

#[async_trait]
impl Platform for DiscordPlatform {
    fn name(&self) -> &'static str {
        "discord"
    }

    async fn start(&self, handler: Arc<dyn MessageHandler>) -> Result<()> {
        info!("Discord Gateway connecting");
        loop {
            if let Err(e) = self.run_gateway(&handler).await {
                error!(error=%e, "Discord Gateway disconnected; reconnecting in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    async fn reply(&self, ctx: &ReplyCtx, text: &str) -> Result<()> {
        let channel_id = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Discord reply requires channel_id"))?;
        let chunks = split_text(text, DISCORD_TEXT_MAX);
        for chunk in chunks {
            let body = serde_json::json!({"content": chunk});
            let resp = self
                .http
                .post(format!("{API_BASE}/channels/{channel_id}/messages"))
                .header("Authorization", self.auth_header())
                .json(&body)
                .send()
                .await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                anyhow::bail!("Discord sendMessage failed: {status} {text}");
            }
        }
        Ok(())
    }

    async fn send_attachment(&self, ctx: &ReplyCtx, att: &Attachment) -> Result<()> {
        let channel_id = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Discord send_attachment requires channel_id"))?;
        let file_bytes = tokio::fs::read(&att.path).await?;
        let file_name = att.name.clone().unwrap_or_else(|| {
            att.path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into()
        });

        let part = reqwest::multipart::Part::bytes(file_bytes)
            .file_name(file_name)
            .mime_str(&att.mime)?;
        let form = reqwest::multipart::Form::new().part("files[0]", part);

        let resp = self
            .http
            .post(format!("{API_BASE}/channels/{channel_id}/messages"))
            .header("Authorization", self.auth_header())
            .multipart(form)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!("Discord attachment send failed: {status} {text}");
            anyhow::bail!("Discord file upload failed: {status}");
        }
        Ok(())
    }

    async fn show_typing(&self, ctx: &ReplyCtx) -> Result<()> {
        let channel_id = ctx
            .channel
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Discord show_typing requires channel_id"))?;
        let resp = self
            .http
            .post(format!("{API_BASE}/channels/{channel_id}/typing"))
            .header("Authorization", self.auth_header())
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            debug!("Discord typing indicator failed: {status}");
        }
        Ok(())
    }
}

impl DiscordPlatform {
    async fn run_gateway(&self, handler: &Arc<dyn MessageHandler>) -> Result<()> {
        let url = self.get_gateway_url().await?;
        let (ws, _) = tokio_tungstenite::connect_async(&url).await?;
        let (mut sink, mut stream) = ws.split();

        let mut heartbeat_interval = Duration::from_secs(41);
        let mut identified = false;

        loop {
            tokio::select! {
                msg = stream.next() => {
                    let Some(msg) = msg else {
                        anyhow::bail!("Discord Gateway stream ended");
                    };
                    let msg = msg?;
                    let WsMessage::Text(text) = msg else { continue };
                    let payload: GatewayPayload = match serde_json::from_str(&text) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    if let Some(s) = payload.s {
                        self.sequence.store(s, Ordering::Relaxed);
                    }

                    match payload.op {
                        10 => {
                            // Hello — extract heartbeat_interval, send Identify
                            if let Some(d) = &payload.d {
                                if let Some(ms) = d.get("heartbeat_interval").and_then(|v| v.as_u64()) {
                                    heartbeat_interval = Duration::from_millis(ms);
                                }
                            }
                            if !identified {
                                let ident = self.identify_payload();
                                sink.send(WsMessage::Text(serde_json::to_string(&ident)?.into())).await?;
                                identified = true;
                            }
                        }
                        11 => {
                            // Heartbeat ACK
                            debug!("Discord heartbeat ACK");
                        }
                        1 => {
                            let hb = serde_json::json!({"op": 1, "d": self.sequence.load(Ordering::Relaxed)});
                            sink.send(WsMessage::Text(serde_json::to_string(&hb)?.into())).await?;
                        }
                        0 => {
                            // Dispatch
                            if payload.t.as_deref() == Some("READY") {
                                if let Some(d) = &payload.d {
                                    if let Some(user) = d.get("user") {
                                        if let Some(id) = user.get("id").and_then(|v| v.as_str()) {
                                            let _ = self.bot_user_id.set(id.to_string());
                                            info!(bot_id=%id, "Discord bot identified");
                                        }
                                    }
                                }
                            } else if payload.t.as_deref() == Some("MESSAGE_CREATE") {
                                if let Some(d) = payload.d {
                                    self.handle_message_create(d, handler).await;
                                }
                            }
                        }
                        7 => {
                            // Reconnect requested
                            info!("Discord Gateway requested reconnect");
                            anyhow::bail!("reconnect requested");
                        }
                        9 => {
                            warn!("Discord invalid session; will re-identify");
                            tokio::time::sleep(Duration::from_secs(3)).await;
                            let ident = self.identify_payload();
                            sink.send(WsMessage::Text(serde_json::to_string(&ident)?.into())).await?;
                        }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(heartbeat_interval) => {
                    let hb = serde_json::json!({"op": 1, "d": self.sequence.load(Ordering::Relaxed)});
                    sink.send(WsMessage::Text(serde_json::to_string(&hb)?.into())).await?;
                }
            }
        }
    }

    async fn handle_message_create(&self, d: serde_json::Value, handler: &Arc<dyn MessageHandler>) {
        let Some(author) = d.get("author") else {
            return;
        };
        // Ignore bot messages
        if author.get("bot").and_then(|v| v.as_bool()).unwrap_or(false) {
            return;
        }
        let Some(channel_id) = d.get("channel_id").and_then(|v| v.as_str()) else {
            return;
        };
        let Some(author_id) = author.get("id").and_then(|v| v.as_str()) else {
            return;
        };
        let content = d.get("content").and_then(|v| v.as_str()).unwrap_or("");

        // In guild channels, only respond to mentions
        if d.get("guild_id").is_some() {
            let bot_id = self.bot_user_id.get().map(|s| s.as_str()).unwrap_or("");
            let mention_pattern = format!("<@{bot_id}>");
            if !content.contains(&mention_pattern) {
                return;
            }
        }

        let text = strip_mentions(content);
        if text.is_empty() {
            return;
        }

        let key = SessionKey::new("discord", format!("{channel_id}/{author_id}"));
        let reply_ctx = ReplyCtx {
            channel: Some(channel_id.to_string()),
            user: Some(author_id.to_string()),
            thread: None,
            extra: serde_json::Value::Null,
        };
        let message = Message {
            key,
            text,
            attachments: vec![],
            reply_ctx,
            timestamp_ms: 0,
        };
        handler.handle(message).await;
    }
}

fn strip_mentions(content: &str) -> String {
    let mut result = content.to_string();
    while let Some(start) = result.find("<@") {
        if let Some(end) = result[start..].find('>') {
            result.replace_range(start..start + end + 1, "");
        } else {
            break;
        }
    }
    result.trim().to_string()
}

#[derive(Debug, Deserialize)]
struct GatewayPayload {
    op: u8,
    d: Option<serde_json::Value>,
    s: Option<u64>,
    t: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_mentions_removes_mention() {
        let input = "<@123456> hello world";
        assert_eq!(strip_mentions(input), "hello world");
    }

    #[test]
    fn strip_mentions_multiple() {
        let input = "<@111> hey <@222> there";
        assert_eq!(strip_mentions(input), "hey  there");
    }

    #[test]
    fn strip_mentions_no_mention() {
        let input = "plain text";
        assert_eq!(strip_mentions(input), "plain text");
    }
}
