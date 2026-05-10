//! OpenAI-compatible HTTP/SSE agent.
//!
//! Streams `/v1/chat/completions` with `stream: true`, parsing SSE chunks into
//! incremental [`Event::AssistantText { partial: true }`] frames followed by a
//! single non-partial frame and a [`Event::Done`].
//!
//! Conversation history (alternating user/assistant turns) is kept inside
//! [`HttpSession`] so the next `send` carries prior context. The history is
//! shared via `Arc<Mutex<_>>` because each `send` spawns a detached SSE task.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use core_traits::{Agent, AgentSession, Attachment, Event, Result, SessionKey};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::{mpsc, Mutex};
use tokio::time::sleep;
use tracing::{debug, warn};

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub struct HttpAgentConfig {
    pub base_url: String,
    pub model: String,
    pub api_key: String,
    /// Stable display name used by `Agent::name`. Lets one HTTP backend serve
    /// as "openai" / "copilot" / "groq" with different presets.
    pub agent_name: &'static str,
    /// If false, falls back to non-streaming JSON (some gateways lack SSE).
    pub stream: bool,
}

impl Default for HttpAgentConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.openai.com/v1".into(),
            model: "gpt-4o-mini".into(),
            api_key: String::new(),
            agent_name: "http",
            stream: true,
        }
    }
}

pub struct HttpAgent {
    cfg: Arc<HttpAgentConfig>,
}

impl HttpAgent {
    pub fn new(cfg: HttpAgentConfig) -> Self {
        Self { cfg: Arc::new(cfg) }
    }
}

#[async_trait]
impl Agent for HttpAgent {
    fn name(&self) -> &'static str {
        self.cfg.agent_name
    }
    async fn start_session(
        &self,
        _key: SessionKey,
        _resume: Option<String>,
    ) -> Result<Box<dyn AgentSession>> {
        Ok(Box::new(HttpSession {
            id: uuid::Uuid::new_v4().to_string(),
            cfg: self.cfg.clone(),
            client: reqwest::Client::new(),
            tx: None,
            history: Arc::new(Mutex::new(Vec::new())),
        }))
    }
}

pub struct HttpSession {
    id: String,
    cfg: Arc<HttpAgentConfig>,
    client: reqwest::Client,
    tx: Option<mpsc::Sender<Event>>,
    history: Arc<Mutex<Vec<serde_json::Value>>>,
}

#[async_trait]
impl AgentSession for HttpSession {
    fn id(&self) -> String {
        self.id.clone()
    }

    async fn send(&mut self, prompt: String, _attachments: Vec<Attachment>) -> Result<()> {
        {
            let mut h = self.history.lock().await;
            h.push(serde_json::json!({"role": "user", "content": prompt}));
        }
        let tx = self
            .tx
            .clone()
            .ok_or_else(|| anyhow::anyhow!("events() not yet called"))?;
        let url = format!(
            "{}/chat/completions",
            self.cfg.base_url.trim_end_matches('/')
        );
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let history = self.history.clone();
        let session_id = self.id.clone();

        tokio::spawn(async move {
            let result = if cfg.stream {
                run_streaming(&client, &url, &cfg, &history, &tx).await
            } else {
                run_non_streaming(&client, &url, &cfg, &history, &tx).await
            };
            if let Err(e) = result {
                let _ = tx.send(Event::Error(e.to_string())).await;
            }
            let _ = tx.send(Event::Done { session_id }).await;
        });
        Ok(())
    }

    fn events(&mut self) -> mpsc::Receiver<Event> {
        let (tx, rx) = mpsc::channel(64);
        self.tx = Some(tx);
        rx
    }

    async fn answer_permission(&mut self, _id: String, _allow: bool) -> Result<()> {
        Ok(())
    }
    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

async fn run_streaming(
    client: &reqwest::Client,
    url: &str,
    cfg: &HttpAgentConfig,
    history: &Arc<Mutex<Vec<serde_json::Value>>>,
    tx: &mpsc::Sender<Event>,
) -> anyhow::Result<()> {
    let messages = history.lock().await.clone();
    let body = serde_json::json!({
        "model": cfg.model,
        "messages": messages,
        "stream": true,
    });

    let resp = post_with_retry(client, url, &cfg.api_key, &body).await?;
    let mut stream = resp.bytes_stream().eventsource();
    let mut accumulated = String::new();

    while let Some(event_res) = stream.next().await {
        let event = match event_res {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "SSE read error");
                break;
            }
        };
        let data = event.data;
        if data == "[DONE]" {
            break;
        }
        let chunk: ChatChunk = match serde_json::from_str(&data) {
            Ok(c) => c,
            Err(e) => {
                debug!(error = %e, raw = %data, "skip malformed chunk");
                continue;
            }
        };
        if let Some(choice) = chunk.choices.into_iter().next() {
            if let Some(content) = choice.delta.content {
                if !content.is_empty() {
                    accumulated.push_str(&content);
                    if tx
                        .send(Event::AssistantText {
                            text: content,
                            partial: true,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    }

    if !accumulated.is_empty() {
        history.lock().await.push(serde_json::json!({
            "role": "assistant",
            "content": accumulated.clone(),
        }));
        // Final non-partial frame so the session-actor can flush a "turn end".
        let _ = tx
            .send(Event::AssistantText {
                text: accumulated,
                partial: false,
            })
            .await;
    }
    Ok(())
}

async fn run_non_streaming(
    client: &reqwest::Client,
    url: &str,
    cfg: &HttpAgentConfig,
    history: &Arc<Mutex<Vec<serde_json::Value>>>,
    tx: &mpsc::Sender<Event>,
) -> anyhow::Result<()> {
    let messages = history.lock().await.clone();
    let body = serde_json::json!({
        "model": cfg.model,
        "messages": messages,
        "stream": false,
    });
    let resp = post_with_retry(client, url, &cfg.api_key, &body).await?;
    let value: serde_json::Value = resp.json().await?;
    let text = value
        .pointer("/choices/0/message/content")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    if !text.is_empty() {
        history.lock().await.push(serde_json::json!({
            "role": "assistant",
            "content": text.clone(),
        }));
        let _ = tx
            .send(Event::AssistantText {
                text,
                partial: false,
            })
            .await;
    }
    Ok(())
}

async fn post_with_retry(
    client: &reqwest::Client,
    url: &str,
    api_key: &str,
    body: &serde_json::Value,
) -> anyhow::Result<reqwest::Response> {
    let mut backoff = INITIAL_BACKOFF;
    let mut last_err = None;
    for attempt in 0..=MAX_RETRIES {
        let req = client.post(url).bearer_auth(api_key).json(body);
        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return Ok(resp);
                }
                if status.as_u16() == 429 || status.is_server_error() {
                    let retry_after = resp
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(Duration::from_secs)
                        .unwrap_or(backoff);
                    warn!(%status, attempt, "retrying after {:?}", retry_after);
                    last_err = Some(anyhow::anyhow!("HTTP {status}"));
                    sleep(retry_after).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
                let txt = resp.text().await.unwrap_or_default();
                anyhow::bail!("HTTP {status}: {txt}");
            }
            Err(e) => {
                warn!(error=%e, attempt, "request failed; retrying");
                last_err = Some(anyhow::anyhow!("{e}"));
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("retry budget exhausted")))
}

#[derive(Deserialize, Debug)]
struct ChatChunk {
    #[serde(default)]
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize, Debug)]
struct ChatChoice {
    #[serde(default)]
    delta: ChatDelta,
}

#[derive(Deserialize, Debug, Default)]
struct ChatDelta {
    #[serde(default)]
    content: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_openai_chunk() {
        let raw = r#"{"id":"x","choices":[{"index":0,"delta":{"content":"hello"}}]}"#;
        let c: ChatChunk = serde_json::from_str(raw).unwrap();
        assert_eq!(c.choices[0].delta.content.as_deref(), Some("hello"));
    }

    #[test]
    fn parse_chunk_without_content() {
        let raw = r#"{"choices":[{"index":0,"delta":{}}]}"#;
        let c: ChatChunk = serde_json::from_str(raw).unwrap();
        assert!(c.choices[0].delta.content.is_none());
    }
}
