//! Per-message subprocess agent: spawn a fresh process for every prompt.
//! Useful for tools like `gh copilot suggest` that lack a streaming mode.

use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{Agent, AgentSession, Attachment, Event, Result, SessionKey};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};

#[derive(Clone, Debug)]
pub struct CliAgentConfig {
    pub binary: String,
    pub args: Vec<String>,
    /// If true, append `--resume <last_id>` automatically.
    pub supports_resume: bool,
}

pub struct CliAgent {
    cfg: Arc<CliAgentConfig>,
    name: &'static str,
}

impl CliAgent {
    pub fn new(name: &'static str, cfg: CliAgentConfig) -> Self {
        Self {
            cfg: Arc::new(cfg),
            name,
        }
    }
}

#[async_trait]
impl Agent for CliAgent {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn start_session(
        &self,
        _key: SessionKey,
        resume: Option<String>,
    ) -> Result<Box<dyn AgentSession>> {
        Ok(Box::new(CliSession {
            cfg: self.cfg.clone(),
            session_id: Arc::new(Mutex::new(
                resume.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            )),
            tx: None,
        }))
    }
}

/// Per-message subprocess session.
pub struct CliSession {
    cfg: Arc<CliAgentConfig>,
    session_id: Arc<Mutex<String>>,
    tx: Option<mpsc::Sender<Event>>,
}

#[async_trait]
impl AgentSession for CliSession {
    fn id(&self) -> String {
        // Best-effort sync read.
        self.session_id
            .try_lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    async fn send(&mut self, prompt: String, attachments: Vec<Attachment>) -> Result<()> {
        let cfg = self.cfg.clone();
        let sid = self.session_id.clone();
        let tx = self
            .tx
            .clone()
            .ok_or_else(|| anyhow::anyhow!("events() not yet called"))?;
        tokio::spawn(async move {
            let mut cmd = Command::new(&cfg.binary);
            for a in &cfg.args {
                cmd.arg(a);
            }
            if cfg.supports_resume {
                let id = sid.lock().await.clone();
                cmd.args(["--resume", &id]);
            }
            if !attachments.is_empty() {
                let joined = attachments
                    .iter()
                    .map(|a| a.path.display().to_string())
                    .collect::<Vec<_>>()
                    .join("\n");
                cmd.env("AAB_ATTACHMENTS", joined);
                cmd.env("AAB_ATTACHMENT_COUNT", attachments.len().to_string());
            }
            cmd.arg(&prompt);
            cmd.stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            match cmd.spawn() {
                Ok(mut child) => {
                    let mut buf = String::new();
                    if let Some(mut out) = child.stdout.take() {
                        let _ = out.read_to_string(&mut buf).await;
                    }
                    let _ = child.wait().await;
                    let _ = tx
                        .send(Event::AssistantText {
                            text: buf,
                            partial: false,
                        })
                        .await;
                    let id = sid.lock().await.clone();
                    let _ = tx.send(Event::Done { session_id: id }).await;
                }
                Err(e) => {
                    let _ = tx.send(Event::Error(e.to_string())).await;
                }
            }
        });
        Ok(())
    }

    fn events(&mut self) -> mpsc::Receiver<Event> {
        let (tx, rx) = mpsc::channel(8);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cli_session_id_uses_resume_when_provided() {
        let cfg = CliAgentConfig {
            binary: "echo".into(),
            args: vec![],
            supports_resume: false,
        };
        let agent = CliAgent::new("test", cfg);
        let key = SessionKey::new("test", "u1");
        let session = agent
            .start_session(key, Some("prev-id".into()))
            .await
            .unwrap();
        assert_eq!(session.id(), "prev-id");
    }

    #[tokio::test]
    async fn cli_session_events_returns_receiver() {
        let cfg = CliAgentConfig {
            binary: "echo".into(),
            args: vec![],
            supports_resume: false,
        };
        let agent = CliAgent::new("test", cfg);
        let key = SessionKey::new("test", "u1");
        let mut session = agent.start_session(key, None).await.unwrap();
        let _rx = session.events();
    }

    #[tokio::test]
    async fn cli_session_generates_uuid_without_resume() {
        let cfg = CliAgentConfig {
            binary: "echo".into(),
            args: vec![],
            supports_resume: false,
        };
        let agent = CliAgent::new("test", cfg);
        let key = SessionKey::new("test", "u1");
        let session = agent.start_session(key, None).await.unwrap();
        let id = session.id();
        assert!(!id.is_empty());
        assert!(uuid::Uuid::parse_str(&id).is_ok());
    }
}
