//! `claude` CLI driver in stream-json mode (bidirectional NDJSON over stdio).
//!
//! Spawn line (with all features wired):
//! ```text
//! claude --input-format stream-json --output-format stream-json -p --verbose \
//!        --dangerously-skip-permissions \
//!        --include-partial-messages \
//!        [--model <model>] [--fallback-model <model>] \
//!        [--add-dir <dir>...]              \
//!        [--allowedTools <tool>...]        \
//!        [--disallowedTools <tool>...]     \
//!        [--max-budget-usd <amount>]       \
//!        [--append-system-prompt <prompt>] \
//!        [--effort <level>]                \
//!        [--mcp-config <file>...]          \
//!        [--session-id <uuid>]             \
//!        [--resume <id>]                   \
//!        [extra_args...]
//! ```
//!
//! `--dangerously-skip-permissions` is enabled by default because the bridge's
//! whole purpose is to forward chat messages straight into the CLI without a
//! human at the keyboard to approve each tool call. Operators who want
//! interactive permission prompts (forwarded to chat as `/yes <id>` /
//! `/no <id>`) can flip [`ClaudeCodeConfig::skip_permissions`] to `false`.

pub mod session;
pub mod stream_event;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{Agent, AgentSession, Result, SessionKey};

#[derive(Clone, Debug)]
pub struct ClaudeCodeConfig {
    pub binary: String,
    /// Free-form extras appended at the end of the spawn line. Use sparingly;
    /// most knobs are already first-class fields below.
    pub extra_args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub permission_mode: PermissionMode,

    // ── Permissions / safety ──────────────────────────────────────────────
    /// Pass `--dangerously-skip-permissions`. Default true (unattended chat).
    pub skip_permissions: bool,
    /// Pass `--allowedTools <tool>...`. Examples: `Read`, `Edit`, `Bash(git *)`.
    pub allowed_tools: Vec<String>,
    /// Pass `--disallowedTools <tool>...`. Examples: `Bash(rm *)`.
    pub disallowed_tools: Vec<String>,
    /// Pass `--max-budget-usd <amount>`. Hard ceiling per session.
    pub max_budget_usd: Option<f64>,

    // ── Streaming ─────────────────────────────────────────────────────────
    /// Pass `--include-partial-messages`. Default true so the SessionActor
    /// receives incremental chunks for chat-friendly streaming.
    pub include_partial_messages: bool,

    // ── Model / effort ────────────────────────────────────────────────────
    /// Pass `--model <model>` (alias `sonnet`/`opus`/`haiku` or full id).
    pub model: Option<String>,
    /// Pass `--fallback-model <model>`. Activated only on overload.
    pub fallback_model: Option<String>,
    /// Pass `--effort <low|medium|high|xhigh|max>`.
    pub effort: Option<String>,

    // ── Filesystem / context ──────────────────────────────────────────────
    /// Pass `--add-dir <dir>` for each entry.
    pub add_dirs: Vec<PathBuf>,
    /// Pass `--append-system-prompt <prompt>` (e.g. "Reply concisely; you're on LINE.").
    pub append_system_prompt: Option<String>,
    /// Pass `--mcp-config <file>` for each path.
    pub mcp_config_files: Vec<PathBuf>,

    // ── Session identity ──────────────────────────────────────────────────
    /// Pass `--session-id <uuid>`. If `None`, claude assigns one and we read
    /// it from the first system event.
    pub session_id: Option<String>,

    // ── Attachment handling ──────────────────────────────────────────────
    /// Inline-vs-path threshold for image attachments (bytes). Default 256 KiB.
    pub inline_image_max_bytes: u64,
}

impl Default for ClaudeCodeConfig {
    fn default() -> Self {
        Self {
            binary: "claude".into(),
            extra_args: vec![],
            cwd: None,
            permission_mode: PermissionMode::Ask,
            skip_permissions: true,
            allowed_tools: vec![],
            disallowed_tools: vec![],
            max_budget_usd: None,
            include_partial_messages: true,
            model: None,
            fallback_model: None,
            effort: None,
            add_dirs: vec![],
            append_system_prompt: None,
            mcp_config_files: vec![],
            session_id: None,
            inline_image_max_bytes: 256 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PermissionMode {
    Ask,
    AcceptEdits,
    BypassPermissions,
}

pub struct ClaudeCodeAgent {
    cfg: Arc<tokio::sync::RwLock<ClaudeCodeConfig>>,
}

impl ClaudeCodeAgent {
    pub fn new(cfg: ClaudeCodeConfig) -> Self {
        Self {
            cfg: Arc::new(tokio::sync::RwLock::new(cfg)),
        }
    }
}

#[async_trait]
impl Agent for ClaudeCodeAgent {
    fn name(&self) -> &'static str {
        "claude"
    }

    async fn start_session(
        &self,
        key: SessionKey,
        resume: Option<String>,
    ) -> Result<Box<dyn AgentSession>> {
        let snapshot = Arc::new(self.cfg.read().await.clone());
        session::ClaudeCodeSession::spawn(snapshot, key, resume)
            .await
            .map(|s| Box::new(s) as Box<dyn AgentSession>)
    }

    async fn set_override(&self, _key: &SessionKey, name: &str, value: &str) -> Result<()> {
        let mut cfg = self.cfg.write().await;
        match name {
            "model" => {
                cfg.model = if value.is_empty() {
                    None
                } else {
                    Some(value.into())
                }
            }
            "fallback_model" => {
                cfg.fallback_model = if value.is_empty() {
                    None
                } else {
                    Some(value.into())
                }
            }
            "effort" => {
                if !matches!(value, "low" | "medium" | "high" | "xhigh" | "max" | "") {
                    anyhow::bail!("effort must be one of low|medium|high|xhigh|max");
                }
                cfg.effort = if value.is_empty() {
                    None
                } else {
                    Some(value.into())
                };
            }
            "budget" => {
                let n: f64 = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("budget must be a number (USD)"))?;
                cfg.max_budget_usd = if n > 0.0 { Some(n) } else { None };
            }
            "append_system_prompt" => {
                cfg.append_system_prompt = if value.is_empty() {
                    None
                } else {
                    Some(value.into())
                };
            }
            "add_dir" => cfg.add_dirs.push(value.into()),
            "clear_dirs" => cfg.add_dirs.clear(),
            "allow_tool" => cfg.allowed_tools.push(value.into()),
            "deny_tool" => cfg.disallowed_tools.push(value.into()),
            "clear_tools" => {
                cfg.allowed_tools.clear();
                cfg.disallowed_tools.clear();
            }
            other => anyhow::bail!("unknown override `{other}`"),
        }
        Ok(())
    }
}
