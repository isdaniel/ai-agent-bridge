//! Live `claude` subprocess wrapped as an [`AgentSession`].

use std::path::Path;
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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
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
        let stderr = child.stderr.take();

        // Log stderr lines so Claude errors are visible in bridge logs.
        if let Some(stderr) = stderr {
            let key_for_log = key.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    warn!(?key_for_log, stderr = %line, "claude stderr");
                }
            });
        }

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
                Ok(Ok(status)) if status.success() => info!("claude exited cleanly"),
                Ok(Ok(status)) => warn!(code = ?status.code(), "claude exited with error"),
                _ => {
                    warn!("claude shutdown grace exceeded; killing");
                    let _ = child.start_kill();
                }
            }
        }
        Ok(())
    }
}

fn serialize_attachment(att: &Attachment, _inline_max: u64) -> Result<serde_json::Value> {
    match att.kind {
        AttachmentKind::Image => {
            let bytes = std::fs::read(&att.path)
                .with_context(|| format!("reading image {}", att.path.display()))?;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
            Ok(json!({
                "type": "image",
                "source": {"type": "base64", "media_type": att.mime, "data": b64}
            }))
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
                if let ContentBlock::Text { ref text } = block {
                    if !text.is_empty() {
                        let file_atts = extract_file_attachments(text);
                        out.push(Event::AssistantText {
                            text: text.clone(),
                            partial: false,
                        });
                        for att in file_atts {
                            out.push(Event::AssistantAttachment(att));
                        }
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
        StreamEvent::Result {
            session_id,
            is_error,
            errors,
            ..
        } => {
            if let Some(id) = session_id.clone() {
                if let Ok(mut g) = sid.write() {
                    *g = id;
                }
            }
            let id =
                session_id.unwrap_or_else(|| sid.read().map(|g| g.clone()).unwrap_or_default());
            let mut out = Vec::new();
            if is_error == Some(true) {
                let msg = errors
                    .and_then(|e| {
                        if e.is_empty() {
                            None
                        } else {
                            Some(e.join("; "))
                        }
                    })
                    .unwrap_or_else(|| "claude exited with an error".into());
                warn!(error = %msg, "claude result error");
                out.push(Event::Error(msg));
            }
            out.push(Event::Done { session_id: id });
            Some(out)
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

/// Scan text for absolute file paths pointing to existing, downloadable files.
/// Returns `Attachment` events for each detected file.
fn extract_file_attachments(text: &str) -> Vec<Attachment> {
    let mut attachments = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for candidate in find_absolute_paths(text) {
        let path = Path::new(candidate);
        if !path.is_file() {
            continue;
        }
        let ext = match path.extension().and_then(|e| e.to_str()) {
            Some(e) => e.to_ascii_lowercase(),
            None => continue,
        };
        let (kind, mime) = match mime_for_ext(&ext) {
            Some(v) => v,
            None => continue,
        };
        if !seen.insert(candidate.to_string()) {
            continue;
        }
        let bytes = std::fs::metadata(path).ok().map(|m| m.len());
        let name = path.file_name().and_then(|n| n.to_str()).map(String::from);
        attachments.push(Attachment {
            kind,
            path: path.to_path_buf(),
            mime: mime.to_string(),
            bytes,
            name,
        });
    }
    attachments
}

/// Extract substrings that look like absolute file paths from text.
fn find_absolute_paths(text: &str) -> Vec<&str> {
    const PREFIXES: &[&str] = &[
        "/tmp/", "/home/", "/var/", "/opt/", "/srv/", "/root/", "/data/", "/mnt/", "/usr/",
    ];
    let mut results = Vec::new();
    let mut search_from = 0;
    while search_from < text.len() {
        let remaining = &text[search_from..];
        let mut earliest: Option<usize> = None;
        for prefix in PREFIXES {
            if let Some(pos) = remaining.find(prefix) {
                earliest = Some(match earliest {
                    Some(e) => e.min(pos),
                    None => pos,
                });
            }
        }
        let start = match earliest {
            Some(pos) => pos,
            None => break,
        };
        let abs_start = search_from + start;
        let path_bytes = &text.as_bytes()[abs_start..];
        let end = path_bytes
            .iter()
            .position(|&b| {
                b == b' '
                    || b == b'\n'
                    || b == b'\r'
                    || b == b'\t'
                    || b == b'"'
                    || b == b'\''
                    || b == b')'
                    || b == b']'
                    || b == b'}'
                    || b == b'>'
                    || b == b'`'
                    || b == b'\0'
            })
            .unwrap_or(path_bytes.len());
        let candidate = &text[abs_start..abs_start + end];
        // Strip trailing punctuation that's likely not part of the path.
        let candidate = candidate.trim_end_matches(['.', ',', ';']);
        if candidate.len() > 5 && candidate.contains('.') {
            results.push(candidate);
        }
        search_from = abs_start + end;
    }
    results
}

fn mime_for_ext(ext: &str) -> Option<(AttachmentKind, &'static str)> {
    match ext {
        // Documents
        "xlsx" => Some((
            AttachmentKind::File,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )),
        "xls" => Some((AttachmentKind::File, "application/vnd.ms-excel")),
        "csv" => Some((AttachmentKind::File, "text/csv")),
        "pdf" => Some((AttachmentKind::File, "application/pdf")),
        "docx" => Some((
            AttachmentKind::File,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )),
        "doc" => Some((AttachmentKind::File, "application/msword")),
        "pptx" => Some((
            AttachmentKind::File,
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        )),
        "txt" => Some((AttachmentKind::File, "text/plain")),
        "json" => Some((AttachmentKind::File, "application/json")),
        "xml" => Some((AttachmentKind::File, "application/xml")),
        "html" | "htm" => Some((AttachmentKind::File, "text/html")),
        // Archives
        "zip" => Some((AttachmentKind::File, "application/zip")),
        "tar" => Some((AttachmentKind::File, "application/x-tar")),
        "gz" | "tgz" => Some((AttachmentKind::File, "application/gzip")),
        // Images
        "png" => Some((AttachmentKind::Image, "image/png")),
        "jpg" | "jpeg" => Some((AttachmentKind::Image, "image/jpeg")),
        "gif" => Some((AttachmentKind::Image, "image/gif")),
        "svg" => Some((AttachmentKind::Image, "image/svg+xml")),
        "webp" => Some((AttachmentKind::Image, "image/webp")),
        // Data
        "parquet" => Some((AttachmentKind::File, "application/octet-stream")),
        "sqlite" | "db" => Some((AttachmentKind::File, "application/x-sqlite3")),
        _ => None,
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
    fn large_image_still_uses_base64() {
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
        assert_eq!(v["source"]["type"], "base64");
    }

    #[test]
    fn find_paths_in_text() {
        let text =
            "整理好了，檔案在：\n\n/tmp/.tmpXSBOEw/slow_log_analysis.xlsx\n\n包含 5 個工作表";
        let paths = find_absolute_paths(text);
        assert_eq!(paths, vec!["/tmp/.tmpXSBOEw/slow_log_analysis.xlsx"]);
    }

    #[test]
    fn find_multiple_paths() {
        let text = "Report: /home/user/report.pdf and data: /tmp/data.csv done.";
        let paths = find_absolute_paths(text);
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0], "/home/user/report.pdf");
        assert_eq!(paths[1], "/tmp/data.csv");
    }

    #[test]
    fn ignores_non_downloadable_paths() {
        let text = "Edited /home/user/main.rs successfully";
        let paths = find_absolute_paths(text);
        assert_eq!(paths, vec!["/home/user/main.rs"]);
        // But mime_for_ext won't match .rs, so extract_file_attachments returns empty
        assert!(mime_for_ext("rs").is_none());
    }

    #[test]
    fn strips_trailing_punctuation() {
        let text = "File saved to /tmp/output.xlsx.";
        let paths = find_absolute_paths(text);
        assert_eq!(paths, vec!["/tmp/output.xlsx"]);
    }

    #[test]
    fn extract_real_file() {
        let dir = tempfile::tempdir().unwrap();
        // tempdir is typically /tmp/... which matches our prefix list
        let path = dir.path().join("report.xlsx");
        std::fs::write(&path, b"PK\x03\x04").unwrap();
        let text = format!("Done! File at {}", path.display());
        let atts = extract_file_attachments(&text);
        assert_eq!(atts.len(), 1);
        assert_eq!(
            atts[0].mime,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        assert_eq!(atts[0].name.as_deref(), Some("report.xlsx"));
    }

    #[test]
    fn skips_nonexistent_file() {
        let text = "File at /tmp/does_not_exist_12345.xlsx";
        let atts = extract_file_attachments(text);
        assert!(atts.is_empty());
    }

    #[test]
    fn no_false_positives_on_slash_commands() {
        let text = "Try /new or /help for more options.";
        let paths = find_absolute_paths(text);
        assert!(paths.is_empty());
    }
}

// Note: `id()` returns a fresh String snapshot of the rotating session_id.
// Engine persists via Event::Done after each turn.
