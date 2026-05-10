//! Live `claude` subprocess wrapped as an [`AgentSession`].

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use base64::Engine as _;
use core_traits::{
    AgentSession, Attachment, AttachmentKind, Event, PermissionRequest, Result, SessionKey,
};
use dashmap::DashMap;
use serde_json::json;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::stream_event::{ContentBlock, ControlReq, PartialDelta, PartialEvent, StreamEvent};
use crate::{ClaudeCodeConfig, PermissionMode};

const EVENTS_CAP: usize = 64;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(120);

pub struct ClaudeCodeSession {
    session_id: Arc<std::sync::RwLock<String>>,
    stdin: Arc<Mutex<BufWriter<ChildStdin>>>,
    events_rx: Option<mpsc::Receiver<Event>>,
    pending_perms: Arc<DashMap<String, oneshot::Sender<bool>>>,
    child: Option<Child>,
    cfg: Arc<ClaudeCodeConfig>,
    _reader_task: tokio::task::JoinHandle<()>,
}

impl ClaudeCodeSession {
    pub async fn spawn(
        cfg: Arc<ClaudeCodeConfig>,
        key: SessionKey,
        resume: Option<String>,
    ) -> Result<Self> {
        let mut cmd = Command::new(&cfg.binary);
        cmd.args([
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "-p",
            "--verbose",
        ]);
        if cfg.skip_permissions {
            cmd.arg("--dangerously-skip-permissions");
        }
        if cfg.include_partial_messages {
            cmd.arg("--include-partial-messages");
        }
        match cfg.permission_mode {
            PermissionMode::Ask => {}
            PermissionMode::AcceptEdits => {
                cmd.args(["--permission-mode", "acceptEdits"]);
            }
            PermissionMode::BypassPermissions => {
                cmd.args(["--permission-mode", "bypassPermissions"]);
            }
        }
        if let Some(model) = &cfg.model {
            cmd.args(["--model", model]);
        }
        if let Some(fb) = &cfg.fallback_model {
            cmd.args(["--fallback-model", fb]);
        }
        if let Some(effort) = &cfg.effort {
            cmd.args(["--effort", effort]);
        }
        if let Some(prompt) = &cfg.append_system_prompt {
            cmd.args(["--append-system-prompt", prompt]);
        }
        if let Some(budget) = cfg.max_budget_usd {
            cmd.args(["--max-budget-usd", &budget.to_string()]);
        }
        for d in &cfg.add_dirs {
            cmd.args(["--add-dir", &d.to_string_lossy()]);
        }
        for f in &cfg.mcp_config_files {
            cmd.args(["--mcp-config", &f.to_string_lossy()]);
        }
        if !cfg.allowed_tools.is_empty() {
            cmd.arg("--allowedTools");
            for t in &cfg.allowed_tools {
                cmd.arg(t);
            }
        }
        if !cfg.disallowed_tools.is_empty() {
            cmd.arg("--disallowedTools");
            for t in &cfg.disallowed_tools {
                cmd.arg(t);
            }
        }
        // Decide session id up front so it's stable across the spawn boundary:
        // explicit cfg → resume → fresh UUID we mint and pass via --session-id.
        let chosen_id = cfg
            .session_id
            .clone()
            .or_else(|| resume.clone())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if let Some(id) = &resume {
            cmd.args(["--resume", id]);
        } else {
            cmd.args(["--session-id", &chosen_id]);
        }
        for a in &cfg.extra_args {
            cmd.arg(a);
        }
        if let Some(cwd) = &cfg.cwd {
            cmd.current_dir(cwd);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        debug!(?key, ?resume, sid = %chosen_id, "spawning claude");
        let mut child = cmd.spawn().context("spawn claude")?;
        let stdin = child.stdin.take().ok_or_else(|| anyhow!("no stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;

        let stdin = Arc::new(Mutex::new(BufWriter::new(stdin)));
        let session_id = Arc::new(std::sync::RwLock::new(chosen_id));
        let pending_perms: Arc<DashMap<String, oneshot::Sender<bool>>> = Arc::new(DashMap::new());

        let (events_tx, events_rx) = mpsc::channel(EVENTS_CAP);
        let (raw_tx, mut raw_rx) = mpsc::channel::<StreamEvent>(EVENTS_CAP);

        let reader_handle = core_engine::framing::spawn_ndjson_reader(
            stdout,
            core_engine::framing::DEFAULT_MAX_LINE,
            raw_tx,
        );

        let sid = session_id.clone();
        let perms = pending_perms.clone();
        let stdin_for_perm = stdin.clone();
        let translator = tokio::spawn(async move {
            while let Some(evt) = raw_rx.recv().await {
                if let Some(translated) = translate_event(evt, &sid, &perms, &stdin_for_perm).await
                {
                    for e in translated {
                        if events_tx.send(e).await.is_err() {
                            return;
                        }
                    }
                }
            }
            // Source closed; send Done with current session id.
            let id = sid.read().map(|g| g.clone()).unwrap_or_default();
            let _ = events_tx.send(Event::Done { session_id: id }).await;
        });

        // Detach raw reader; translator owns the lifecycle.
        tokio::spawn(async move {
            if let Err(e) = reader_handle.await {
                warn!(error = %e, "stdout reader join failed");
            }
        });

        Ok(Self {
            session_id,
            stdin,
            events_rx: Some(events_rx),
            pending_perms,
            child: Some(child),
            cfg,
            _reader_task: translator,
        })
    }
}

#[async_trait]
impl AgentSession for ClaudeCodeSession {
    fn id(&self) -> String {
        self.session_id
            .read()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    async fn send(&mut self, prompt: String, attachments: Vec<Attachment>) -> Result<()> {
        let mut content = Vec::<serde_json::Value>::new();
        if !prompt.is_empty() {
            content.push(json!({"type": "text", "text": prompt}));
        }
        for att in attachments {
            content.push(serialize_attachment(&att, self.cfg.inline_image_max_bytes)?);
        }
        let frame = json!({
            "type": "user",
            "message": {"role": "user", "content": content}
        });
        let mut w = self.stdin.lock().await;
        core_engine::framing::write_ndjson(&mut *w, &frame).await?;
        Ok(())
    }

    fn events(&mut self) -> mpsc::Receiver<Event> {
        self.events_rx.take().expect("events() called twice")
    }

    async fn answer_permission(&mut self, id: String, allow: bool) -> Result<()> {
        if let Some((_, tx)) = self.pending_perms.remove(&id) {
            let _ = tx.send(allow);
        }
        // Also write the control_response frame so claude advances.
        let frame = json!({
            "type": "control_response",
            "request_id": id,
            "response": {"approved": allow}
        });
        let mut w = self.stdin.lock().await;
        core_engine::framing::write_ndjson(&mut *w, &frame).await?;
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        // Send interrupt, wait grace, then kill.
        {
            let mut w = self.stdin.lock().await;
            let _ = core_engine::framing::write_ndjson(
                &mut *w,
                &json!({"type":"control_request","request":{"subtype":"interrupt"}}),
            )
            .await;
            let _ = w.shutdown().await;
        }
        if let Some(mut child) = self.child.take() {
            match tokio::time::timeout(SHUTDOWN_GRACE, child.wait()).await {
                Ok(Ok(_)) => info!("claude exited cleanly"),
                _ => {
                    warn!("claude shutdown grace exceeded; killing");
                    let _ = child.start_kill();
                }
            }
        }
        Ok(())
    }
}

fn serialize_attachment(att: &Attachment, inline_max: u64) -> Result<serde_json::Value> {
    match att.kind {
        AttachmentKind::Image => {
            let size = att.bytes.unwrap_or_else(|| {
                std::fs::metadata(&att.path)
                    .map(|m| m.len())
                    .unwrap_or(u64::MAX)
            });
            if size <= inline_max {
                let bytes = std::fs::read(&att.path)?;
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                Ok(json!({
                    "type": "image",
                    "source": {"type": "base64", "media_type": att.mime, "data": b64}
                }))
            } else {
                Ok(json!({
                    "type": "image",
                    "source": {"type": "file", "path": att.path.to_string_lossy(), "media_type": att.mime}
                }))
            }
        }
        AttachmentKind::File | AttachmentKind::Audio => {
            // Pass as text reference; claude can read the path from disk.
            Ok(json!({
                "type": "text",
                "text": format!("[attachment: {}]", att.path.to_string_lossy())
            }))
        }
    }
}

async fn translate_event(
    evt: StreamEvent,
    sid: &Arc<std::sync::RwLock<String>>,
    pending: &Arc<DashMap<String, oneshot::Sender<bool>>>,
    _stdin: &Arc<Mutex<BufWriter<ChildStdin>>>,
) -> Option<Vec<Event>> {
    match evt {
        StreamEvent::System { session_id, .. } => {
            if let Some(id) = session_id {
                if let Ok(mut g) = sid.write() {
                    *g = id;
                }
            }
            None
        }
        StreamEvent::Assistant {
            message,
            session_id,
        } => {
            if let Some(id) = session_id {
                if let Ok(mut g) = sid.write() {
                    *g = id;
                }
            }
            let mut out = Vec::new();
            for block in message.content {
                if let ContentBlock::Text { text } = block {
                    if !text.is_empty() {
                        out.push(Event::AssistantText {
                            text,
                            partial: false,
                        });
                    }
                } else if let ContentBlock::ToolUse { id, name, .. } = block {
                    out.push(Event::ToolStart { name, id });
                }
            }
            (!out.is_empty()).then_some(out)
        }
        StreamEvent::User { session_id, .. } => {
            if let Some(id) = session_id {
                if let Ok(mut g) = sid.write() {
                    *g = id;
                }
            }
            None
        }
        StreamEvent::StreamEvent { session_id, event } => {
            if let Some(id) = session_id {
                if let Ok(mut g) = sid.write() {
                    *g = id;
                }
            }
            match event {
                PartialEvent::ContentBlockDelta { delta, .. } => match delta {
                    PartialDelta::TextDelta { text } if !text.is_empty() => {
                        Some(vec![Event::AssistantText {
                            text,
                            partial: true,
                        }])
                    }
                    _ => None,
                },
                PartialEvent::Other => None,
            }
        }
        StreamEvent::Result { session_id, .. } => {
            if let Some(id) = session_id.clone() {
                if let Ok(mut g) = sid.write() {
                    *g = id;
                }
            }
            let id =
                session_id.unwrap_or_else(|| sid.read().map(|g| g.clone()).unwrap_or_default());
            Some(vec![Event::Done { session_id: id }])
        }
        StreamEvent::ControlRequest {
            request_id,
            request,
        } => match request {
            ControlReq::PermissionRequest {
                tool_name,
                description,
                input,
            } => {
                let (tx, _rx) = oneshot::channel();
                pending.insert(request_id.clone(), tx);
                Some(vec![Event::PermissionRequest(PermissionRequest {
                    id: request_id,
                    tool_name,
                    description,
                    input,
                })])
            }
            ControlReq::Interrupt | ControlReq::Unknown => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_image_under_threshold() {
        // Build a tiny image attachment with bytes==Some(small).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.png");
        std::fs::write(&path, b"FAKE").unwrap();
        let att = Attachment {
            kind: AttachmentKind::Image,
            path,
            mime: "image/png".into(),
            bytes: Some(4),
            name: None,
        };
        let v = serialize_attachment(&att, 256 * 1024).unwrap();
        assert_eq!(v["source"]["type"], "base64");
    }

    #[test]
    fn large_image_uses_path_ref() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.png");
        std::fs::write(&path, b"FAKE").unwrap();
        let att = Attachment {
            kind: AttachmentKind::Image,
            path: path.clone(),
            mime: "image/png".into(),
            bytes: Some(10_000_000),
            name: None,
        };
        let v = serialize_attachment(&att, 256 * 1024).unwrap();
        assert_eq!(v["source"]["type"], "file");
    }
}

// Note: `id()` returns a fresh String snapshot of the rotating session_id.
// Engine persists via Event::Done after each turn.
