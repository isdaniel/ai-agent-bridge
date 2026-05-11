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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use core_traits::{Agent, AgentSession, Result, SessionKey};
use tracing::info;

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

    // ── Per-client isolation ─────────────────────────────────────────────
    /// Base directory for per-client workspaces. When set, each `SessionKey`
    /// gets its own subdirectory used as `cwd` for the `claude` child process.
    /// Claude reads project-level `.claude/settings.json`, `CLAUDE.md`, and
    /// `.mcp.json` from this directory, giving each client isolated memory,
    /// skills, and MCP servers while sharing the host's auth credentials.
    pub client_config_base_dir: Option<PathBuf>,
    /// Optional template directory copied into new per-client workspaces on
    /// first use. Put default `CLAUDE.md`, `.claude/settings.json`, `.mcp.json`
    /// here.
    pub client_template_dir: Option<PathBuf>,
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
            client_config_base_dir: None,
            client_template_dir: None,
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
        let mut cfg = self.cfg.read().await.clone();
        if let Some(base) = &cfg.client_config_base_dir {
            let dirname = session_key_to_dirname(&key);
            let client_dir = base.join(&dirname);
            ensure_client_dir(&client_dir, cfg.client_template_dir.as_deref())?;
            info!(?key, dir = %client_dir.display(), "per-client workspace");
            cfg.cwd = Some(client_dir);
        }
        let snapshot = Arc::new(cfg);
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

    fn client_dir(&self, key: &SessionKey) -> Option<PathBuf> {
        let cfg = self.cfg.try_read().ok()?;
        let base = cfg.client_config_base_dir.as_ref()?;
        Some(base.join(session_key_to_dirname(key)))
    }
}

/// Convert a `SessionKey` into a filesystem-safe directory name.
/// `"line:U1234"` → `"line__U1234"`, `"slack:C1/U2"` → `"slack__C1__U2"`.
pub fn session_key_to_dirname(key: &SessionKey) -> String {
    key.0
        .replace([':', '/'], "__")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-' || *c == '.')
        .collect()
}

/// Create the per-client workspace directory if it doesn't exist.
/// If a template directory is provided and the client dir is new, its
/// contents are copied recursively.
fn ensure_client_dir(dir: &Path, template: Option<&Path>) -> Result<()> {
    if dir.exists() {
        return Ok(());
    }
    if let Some(tmpl) = template {
        if tmpl.is_dir() {
            copy_dir_recursive(tmpl, dir)?;
            return Ok(());
        }
    }
    std::fs::create_dir_all(dir.join(".claude"))?;
    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_traits::SessionKey;

    #[test]
    fn dirname_from_line_key() {
        let key = SessionKey::new("line", "U1234");
        assert_eq!(session_key_to_dirname(&key), "line__U1234");
    }

    #[test]
    fn dirname_from_slack_key() {
        let key = SessionKey::new("slack", "C1/U2");
        assert_eq!(session_key_to_dirname(&key), "slack__C1__U2");
    }

    #[test]
    fn dirname_from_stdio_key() {
        let key = SessionKey::new("stdio", "local");
        assert_eq!(session_key_to_dirname(&key), "stdio__local");
    }

    #[test]
    fn dirname_strips_unsafe_chars() {
        let key = SessionKey(r#"line:U<>|"test""#.into());
        let dir = session_key_to_dirname(&key);
        assert!(!dir.contains('<'));
        assert!(!dir.contains('>'));
        assert!(!dir.contains('|'));
        assert!(!dir.contains('"'));
    }

    #[test]
    fn ensure_client_dir_creates_dot_claude() {
        let tmp = tempfile::tempdir().unwrap();
        let client = tmp.path().join("line__U1234");
        ensure_client_dir(&client, None).unwrap();
        assert!(client.join(".claude").is_dir());
    }

    #[test]
    fn ensure_client_dir_noop_if_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let client = tmp.path().join("existing");
        std::fs::create_dir_all(&client).unwrap();
        std::fs::write(client.join("marker.txt"), b"keep").unwrap();
        ensure_client_dir(&client, None).unwrap();
        assert!(client.join("marker.txt").exists());
    }

    #[test]
    fn ensure_client_dir_copies_template() {
        let tmp = tempfile::tempdir().unwrap();
        let tmpl = tmp.path().join("template");
        std::fs::create_dir_all(tmpl.join(".claude")).unwrap();
        std::fs::write(tmpl.join("CLAUDE.md"), b"hello").unwrap();
        std::fs::write(tmpl.join(".claude/settings.json"), b"{}").unwrap();

        let client = tmp.path().join("line__U999");
        ensure_client_dir(&client, Some(&tmpl)).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.join("CLAUDE.md")).unwrap(),
            "hello"
        );
        assert_eq!(
            std::fs::read_to_string(client.join(".claude/settings.json")).unwrap(),
            "{}"
        );
    }

    #[test]
    fn copy_dir_recursive_nested() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(src.join("a/b")).unwrap();
        std::fs::write(src.join("top.txt"), b"top").unwrap();
        std::fs::write(src.join("a/mid.txt"), b"mid").unwrap();
        std::fs::write(src.join("a/b/deep.txt"), b"deep").unwrap();
        let dst = tmp.path().join("dst");
        copy_dir_recursive(&src, &dst).unwrap();
        assert_eq!(std::fs::read_to_string(dst.join("top.txt")).unwrap(), "top");
        assert_eq!(
            std::fs::read_to_string(dst.join("a/mid.txt")).unwrap(),
            "mid"
        );
        assert_eq!(
            std::fs::read_to_string(dst.join("a/b/deep.txt")).unwrap(),
            "deep"
        );
    }

    // ── set_override tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn set_override_model() {
        let agent = ClaudeCodeAgent::new(ClaudeCodeConfig::default());
        let key = SessionKey::new("test", "u1");
        agent.set_override(&key, "model", "opus").await.unwrap();
        let cfg = agent.cfg.read().await;
        assert_eq!(cfg.model.as_deref(), Some("opus"));
    }

    #[tokio::test]
    async fn set_override_model_empty_clears() {
        let agent = ClaudeCodeAgent::new(ClaudeCodeConfig {
            model: Some("sonnet".into()),
            ..Default::default()
        });
        let key = SessionKey::new("test", "u1");
        agent.set_override(&key, "model", "").await.unwrap();
        let cfg = agent.cfg.read().await;
        assert!(cfg.model.is_none());
    }

    #[tokio::test]
    async fn set_override_effort_valid() {
        let agent = ClaudeCodeAgent::new(ClaudeCodeConfig::default());
        let key = SessionKey::new("test", "u1");
        for val in &["low", "medium", "high", "xhigh", "max"] {
            agent.set_override(&key, "effort", val).await.unwrap();
        }
        let cfg = agent.cfg.read().await;
        assert_eq!(cfg.effort.as_deref(), Some("max"));
    }

    #[tokio::test]
    async fn set_override_effort_invalid_rejected() {
        let agent = ClaudeCodeAgent::new(ClaudeCodeConfig::default());
        let key = SessionKey::new("test", "u1");
        let res = agent.set_override(&key, "effort", "turbo").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn set_override_budget() {
        let agent = ClaudeCodeAgent::new(ClaudeCodeConfig::default());
        let key = SessionKey::new("test", "u1");
        agent.set_override(&key, "budget", "5.0").await.unwrap();
        let cfg = agent.cfg.read().await;
        assert_eq!(cfg.max_budget_usd, Some(5.0));
    }

    #[tokio::test]
    async fn set_override_add_dir() {
        let agent = ClaudeCodeAgent::new(ClaudeCodeConfig::default());
        let key = SessionKey::new("test", "u1");
        agent
            .set_override(&key, "add_dir", "/tmp/foo")
            .await
            .unwrap();
        let cfg = agent.cfg.read().await;
        assert_eq!(cfg.add_dirs.len(), 1);
    }

    #[tokio::test]
    async fn set_override_unknown_rejected() {
        let agent = ClaudeCodeAgent::new(ClaudeCodeConfig::default());
        let key = SessionKey::new("test", "u1");
        let res = agent.set_override(&key, "nonexistent", "val").await;
        assert!(res.is_err());
    }
}
