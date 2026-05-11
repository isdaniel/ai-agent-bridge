//! Axum router for the LINE webhook endpoint.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use core_traits::{
    safe_filename, Attachment, AttachmentKind, Message, MessageHandler, ReplyCtx, SessionKey,
};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::sign::verify_signature;
use crate::LineConfig;

#[derive(Clone)]
struct AppState {
    cfg: Arc<LineConfig>,
    boot_ms: i64,
    handler: Arc<dyn MessageHandler>,
    http: reqwest::Client,
}

pub fn router(cfg: LineConfig, boot_ms: i64, handler: Arc<dyn MessageHandler>) -> Router {
    let state = AppState {
        cfg: Arc::new(cfg),
        boot_ms,
        handler,
        http: reqwest::Client::new(),
    };
    Router::new()
        .route("/webhook", post(handle_webhook))
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .with_state(state)
}

async fn handle_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let sig = headers
        .get("x-line-signature")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !verify_signature(&state.cfg.channel_secret, &body, sig) {
        warn!("LINE signature mismatch");
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let parsed: WebhookPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "LINE body parse failed");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    for event in parsed.events {
        if event.timestamp < state.boot_ms {
            debug!(ts = event.timestamp, "skip stale LINE event");
            continue;
        }
        let user_id = event.source.user_id.clone();
        let Some(uid) = user_id else { continue };
        if !state.cfg.allowlist.is_empty() && !state.cfg.allowlist.contains(&uid) {
            debug!(%uid, "drop: not in allowlist");
            continue;
        }
        let Some(em) = event.message else { continue };
        let state_cloned = state.clone();
        let uid_cloned = uid.clone();
        tokio::spawn(async move {
            let (text, attachments) = build_payload(&state_cloned, &em).await;
            // Empty text + no attachments = nothing to dispatch (e.g. sticker we
            // chose not to handle). Suppress to avoid spurious agent prompts.
            if text.is_empty() && attachments.is_empty() {
                return;
            }
            let msg = Message {
                key: SessionKey::new("line", &uid_cloned),
                text,
                attachments,
                reply_ctx: ReplyCtx {
                    user: Some(uid_cloned),
                    ..Default::default()
                },
                timestamp_ms: event.timestamp,
            };
            state_cloned.handler.handle(msg).await;
        });
    }
    StatusCode::OK.into_response()
}

async fn build_payload(state: &AppState, m: &EventMessage) -> (String, Vec<Attachment>) {
    match m.r#type.as_str() {
        "text" => (m.text.clone().unwrap_or_default(), vec![]),
        "image" | "file" | "audio" | "video" => {
            let kind = match m.r#type.as_str() {
                "image" => AttachmentKind::Image,
                "audio" => AttachmentKind::Audio,
                _ => AttachmentKind::File,
            };
            let provider = m
                .content_provider
                .as_ref()
                .map(|p| p.r#type.as_str())
                .unwrap_or("line");
            // External provider: just pass URL as a textual reference; the
            // agent can fetch it directly. Skip download to avoid an extra hop.
            if provider != "line" {
                let url = m
                    .content_provider
                    .as_ref()
                    .and_then(|p| p.original_content_url.clone())
                    .unwrap_or_default();
                return (
                    format!("[external {} attachment: {}]", m.r#type, url),
                    vec![],
                );
            }
            let Some(id) = m.id.clone() else {
                return (String::new(), vec![]);
            };
            match download_content(&state.http, &state.cfg.channel_token, &id).await {
                Ok((bytes, mime)) => {
                    let raw_name = m.file_name.clone().unwrap_or_else(|| {
                        let ext = ext_from_mime(&mime);
                        format!("{id}{ext}")
                    });
                    // Sanitise: LINE-supplied fileName is sender-controlled.
                    let name = safe_filename(&raw_name);
                    match core_traits::write_temp_file(&name, &bytes) {
                        Ok(path) => (
                            String::new(),
                            vec![Attachment {
                                kind,
                                path,
                                mime,
                                bytes: Some(bytes.len() as u64),
                                name: Some(name),
                            }],
                        ),
                        Err(e) => {
                            warn!(error = %e, "write_temp failed");
                            (String::new(), vec![])
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "LINE content download failed");
                    (String::new(), vec![])
                }
            }
        }
        // sticker / location / unsupported types: ignore for now.
        _ => (String::new(), vec![]),
    }
}

async fn download_content(
    http: &reqwest::Client,
    token: &str,
    message_id: &str,
) -> anyhow::Result<(Vec<u8>, String)> {
    let url = format!("https://api-data.line.me/v2/bot/message/{message_id}/content");
    let resp = http
        .get(&url)
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    let mime = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_string();
    let bytes = resp.bytes().await?.to_vec();
    Ok((bytes, mime))
}

fn ext_from_mime(mime: &str) -> &'static str {
    match mime {
        "image/jpeg" => ".jpg",
        "image/png" => ".png",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "audio/mp4" | "audio/m4a" => ".m4a",
        "audio/mpeg" => ".mp3",
        "video/mp4" => ".mp4",
        _ => "",
    }
}

#[derive(Deserialize, Clone)]
struct WebhookPayload {
    events: Vec<EventEnvelope>,
}

#[derive(Deserialize, Clone)]
struct EventEnvelope {
    #[serde(default)]
    timestamp: i64,
    source: EventSource,
    message: Option<EventMessage>,
}

#[derive(Deserialize, Clone)]
struct EventSource {
    #[serde(rename = "userId")]
    user_id: Option<String>,
}

#[derive(Deserialize, Clone)]
struct EventMessage {
    r#type: String,
    text: Option<String>,
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "fileName", default)]
    file_name: Option<String>,
    #[serde(rename = "contentProvider", default)]
    content_provider: Option<ContentProvider>,
}

#[derive(Deserialize, Clone)]
struct ContentProvider {
    r#type: String,
    #[serde(rename = "originalContentUrl", default)]
    original_content_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_event() {
        let raw = r#"{
            "events":[{
                "timestamp":1700000000000,
                "source":{"userId":"U1"},
                "message":{"type":"text","text":"hi"}
            }]
        }"#;
        let p: WebhookPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(p.events.len(), 1);
        let m = p.events[0].message.as_ref().unwrap();
        assert_eq!(m.r#type, "text");
        assert_eq!(m.text.as_deref(), Some("hi"));
    }

    #[test]
    fn parse_image_event_with_line_provider() {
        let raw = r#"{
            "events":[{
                "timestamp":1700000000000,
                "source":{"userId":"U1"},
                "message":{
                    "type":"image",
                    "id":"M1",
                    "contentProvider":{"type":"line"}
                }
            }]
        }"#;
        let p: WebhookPayload = serde_json::from_str(raw).unwrap();
        let m = p.events[0].message.as_ref().unwrap();
        assert_eq!(m.r#type, "image");
        assert_eq!(m.id.as_deref(), Some("M1"));
        assert_eq!(
            m.content_provider.as_ref().map(|c| c.r#type.as_str()),
            Some("line")
        );
    }

    #[test]
    fn ext_from_mime_known_types() {
        assert_eq!(ext_from_mime("image/png"), ".png");
        assert_eq!(ext_from_mime("audio/mpeg"), ".mp3");
        assert_eq!(ext_from_mime("application/pdf"), "");
    }

    #[test]
    fn write_temp_file_rejects_separators() {
        assert!(core_traits::write_temp_file("a/b", b"x").is_err());
        assert!(core_traits::write_temp_file("a\\b", b"x").is_err());
        assert!(core_traits::write_temp_file("a\0b", b"x").is_err());
        assert!(core_traits::write_temp_file("", b"x").is_err());
    }

    #[test]
    fn write_temp_file_writes_safe_name() {
        let p = core_traits::write_temp_file("ok.png", b"hello").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"hello");
        let _ = std::fs::remove_file(&p);
    }
}
