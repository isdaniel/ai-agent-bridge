//! Core traits and DTOs for the ai-agent-bridge.
//!
//! Two pluggable interfaces — [`Agent`] (backend like Claude Code, Copilot) and
//! [`Platform`] (chat frontend like LINE, Slack) — joined by a [`MessageHandler`]
//! callback. All other crates depend only on this leaf crate so they can compile
//! in parallel.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub type Result<T> = std::result::Result<T, anyhow::Error>;

/// Namespaced per-user identity, e.g. `"line:U1234"`, `"slack:T1/C1/U1"`,
/// `"stdio:local"`. The string form is what gets serialized to the registry.
#[derive(Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionKey(pub String);

impl SessionKey {
    pub fn new(platform: &str, scoped: impl AsRef<str>) -> Self {
        Self(format!("{platform}:{}", scoped.as_ref()))
    }
    pub fn platform(&self) -> Option<&str> {
        self.0.split_once(':').map(|(p, _)| p)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentKind {
    Image,
    File,
    Audio,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attachment {
    pub kind: AttachmentKind,
    pub path: PathBuf,
    pub mime: String,
    pub bytes: Option<u64>,
    /// Original filename if known (Slack file_share, LINE filename, etc).
    #[serde(default)]
    pub name: Option<String>,
}

/// Opaque per-platform reply context. Lets `Platform::reply` know which
/// channel/thread/conversation to post into.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplyCtx {
    pub channel: Option<String>,
    pub thread: Option<String>,
    pub user: Option<String>,
    /// Free-form per-platform extras.
    #[serde(default)]
    pub extra: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct Message {
    pub key: SessionKey,
    pub text: String,
    pub attachments: Vec<Attachment>,
    pub reply_ctx: ReplyCtx,
    /// Platform-side timestamp (ms since epoch) for post-restart filtering.
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub id: String,
    pub tool_name: String,
    pub description: String,
    /// Optional structured tool input for richer chat-side rendering.
    #[serde(default)]
    pub input: serde_json::Value,
}

/// Streaming events emitted by an [`AgentSession`] back to the engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    AssistantText { text: String, partial: bool },
    AssistantAttachment(Attachment),
    PermissionRequest(PermissionRequest),
    ToolStart { name: String, id: String },
    ToolEnd { id: String, ok: bool },
    Error(String),
    Done { session_id: String },
}

/// AI agent backend (Claude Code, Copilot CLI, ACP server, ...).
#[async_trait]
pub trait Agent: Send + Sync {
    /// Stable identifier used in config / `--agent` flags / SessionRegistry.
    fn name(&self) -> &'static str;

    /// Spawn or attach to a per-user session. `resume` carries the prior
    /// `session_id` if the SessionRegistry has one.
    async fn start_session(
        &self,
        key: SessionKey,
        resume: Option<String>,
    ) -> Result<Box<dyn AgentSession>>;

    /// Apply a free-form `(key, value)` override before the next session
    /// spawn. Implementations MAY ignore unknown keys. Returns `Err` only
    /// if the override is malformed or out-of-range.
    ///
    /// Recognised keys differ per agent. For `agent-claude-code`:
    ///   - `"model"` → e.g. `"sonnet"`, `"opus"`
    ///   - `"fallback_model"`
    ///   - `"effort"` → low/medium/high/xhigh/max
    ///   - `"budget"` → USD amount
    ///   - `"add_dir"` → push a path to add_dirs
    ///   - `"allow_tool"` → push to allowed_tools
    ///   - `"deny_tool"` → push to disallowed_tools
    ///   - `"clear_dirs"`, `"clear_tools"` → reset the lists
    async fn set_override(&self, _key: &SessionKey, _name: &str, _value: &str) -> Result<()> {
        Ok(())
    }

    /// Return the per-client workspace directory for `key`, if per-client
    /// isolation is configured. Used by `/mcp` and `/skills` builtins to
    /// read client-specific config files.
    fn client_dir(&self, _key: &SessionKey) -> Option<PathBuf> {
        None
    }
}

/// One live conversation with a backing agent.
#[async_trait]
pub trait AgentSession: Send {
    /// The agent-internal session identifier (e.g. Claude Code session UUID).
    /// May rotate over the session lifetime; callers should re-fetch when needed.
    fn id(&self) -> String;

    /// Send a user turn. Streaming output arrives via [`AgentSession::events`].
    async fn send(&mut self, prompt: String, attachments: Vec<Attachment>) -> Result<()>;

    /// Take the events receiver. May be called only once per session.
    fn events(&mut self) -> mpsc::Receiver<Event>;

    /// Resolve a previously emitted [`Event::PermissionRequest`].
    async fn answer_permission(&mut self, id: String, allow: bool) -> Result<()>;

    /// Graceful shutdown. Implementations should flush, then kill on timeout.
    async fn close(self: Box<Self>) -> Result<()>;
}

/// Chat platform frontend (LINE, Slack, stdio).
#[async_trait]
pub trait Platform: Send + Sync {
    fn name(&self) -> &'static str;

    /// Start receiving messages. Returns when the platform terminates.
    /// Implementations should be cancellation-safe (`tokio::select!`-friendly).
    async fn start(&self, handler: Arc<dyn MessageHandler>) -> Result<()>;

    async fn reply(&self, ctx: &ReplyCtx, text: &str) -> Result<()>;

    async fn send_attachment(&self, ctx: &ReplyCtx, attachment: &Attachment) -> Result<()>;

    /// Show a typing/loading indicator. Platforms that support it (LINE loading
    /// animation, Slack typing indicator) implement this; others are no-ops.
    /// Does NOT consume message quota.
    async fn show_typing(&self, _ctx: &ReplyCtx) -> Result<()> {
        Ok(())
    }
}

/// Inbound dispatch from a [`Platform`] into the engine.
#[async_trait]
pub trait MessageHandler: Send + Sync {
    async fn handle(&self, message: Message);
}

/// Sanitise a remote-supplied filename so it's safe to `Path::join` onto a
/// trusted base directory.
///
/// LINE webhook payloads carry `fileName` chosen by the sender; Slack file
/// uploads carry `name` chosen by the uploader. Either could include
/// `..`, `/`, `\`, NUL, or be the empty string. This function strips path
/// separators, drops leading dots (so an attacker can't drop a "hidden"
/// `.bashrc`-style file), discards control characters and NULs, caps length,
/// and falls back to `"file"` if nothing legible remains.
pub fn safe_filename(raw: &str) -> String {
    // Take only the basename — split on either separator and keep the last
    // non-empty piece.
    let basename = raw.rsplit(['/', '\\']).next().unwrap_or("").to_string();
    let cleaned: String = basename
        .chars()
        .filter(|c| !c.is_control() && *c != '\0')
        .take(200)
        .collect();
    let trimmed = cleaned.trim_start_matches('.').trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "file".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_namespacing() {
        let k = SessionKey::new("line", "U1234");
        assert_eq!(k.0, "line:U1234");
        assert_eq!(k.platform(), Some("line"));
    }

    #[test]
    fn event_round_trip_serde() {
        let evt = Event::AssistantText {
            text: "hi".into(),
            partial: false,
        };
        let json = serde_json::to_string(&evt).unwrap();
        assert!(json.contains("\"type\":\"assistant_text\""));
        let back: Event = serde_json::from_str(&json).unwrap();
        match back {
            Event::AssistantText { text, partial } => {
                assert_eq!(text, "hi");
                assert!(!partial);
            }
            _ => panic!("variant mismatch"),
        }
    }

    #[test]
    fn permission_request_serde() {
        let pr = PermissionRequest {
            id: "p1".into(),
            tool_name: "Bash".into(),
            description: "rm -rf /".into(),
            input: serde_json::json!({"cmd": "rm"}),
        };
        let s = serde_json::to_string(&pr).unwrap();
        let back: PermissionRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.tool_name, "Bash");
    }

    #[test]
    fn safe_filename_strips_traversal() {
        assert_eq!(safe_filename("../../etc/passwd"), "passwd");
        assert_eq!(safe_filename("/etc/passwd"), "passwd");
        assert_eq!(safe_filename("..\\..\\windows\\evil.exe"), "evil.exe");
        assert_eq!(safe_filename("C:\\Windows\\System32\\cmd.exe"), "cmd.exe");
    }

    #[test]
    fn safe_filename_drops_leading_dots_and_empties() {
        assert_eq!(safe_filename(".."), "file");
        assert_eq!(safe_filename("."), "file");
        assert_eq!(safe_filename(""), "file");
        assert_eq!(safe_filename("/"), "file");
        assert_eq!(safe_filename("...bashrc"), "bashrc");
    }

    #[test]
    fn safe_filename_strips_control_chars_and_nul() {
        assert_eq!(safe_filename("a\0b"), "ab");
        assert_eq!(safe_filename("foo\nbar"), "foobar");
        assert_eq!(safe_filename("ok.png"), "ok.png");
    }

    #[test]
    fn safe_filename_caps_length() {
        let huge = "a".repeat(5000);
        let out = safe_filename(&huge);
        assert!(out.len() <= 200);
        assert!(out.chars().all(|c| c == 'a'));
    }
}
