//! Strongly-typed config layered over TOML + env vars.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::Result;
use figment::providers::{Env, Format, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AppConfig {
    pub bridge: BridgeSection,
    #[serde(default)]
    pub agents: HashMap<String, AgentSection>,
    #[serde(default)]
    pub platforms: PlatformsSection,
    #[serde(default)]
    pub providers: HashMap<String, ProviderSection>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeSection {
    #[serde(default = "default_agent")]
    pub default_agent: String,
    #[serde(default = "default_platform")]
    pub default_platform: String,
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    /// Hard cap on concurrent live agent sessions. Default 64.
    /// Set to 0 to disable the cap (not recommended on a shared host).
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
}

fn default_agent() -> String {
    "claude".into()
}
fn default_platform() -> String {
    "stdio".into()
}
fn default_max_sessions() -> usize {
    96
}
fn default_state_dir() -> PathBuf {
    if let Some(home) = dirs_home() {
        home.join(".ai-agent-bridge")
    } else {
        PathBuf::from(".ai-agent-bridge")
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AgentSection {
    pub binary: Option<String>,
    pub extra_args: Option<Vec<String>>,
    pub cwd: Option<PathBuf>,
    pub permission_mode: Option<String>,
    /// Forwarded to `claude --dangerously-skip-permissions`. Defaults to true.
    pub skip_permissions: Option<bool>,

    // ── Claude-specific extras (silently ignored by other agents) ─────────
    pub include_partial_messages: Option<bool>,
    pub model: Option<String>,
    pub fallback_model: Option<String>,
    pub effort: Option<String>,
    pub append_system_prompt: Option<String>,
    pub max_budget_usd: Option<f64>,
    pub add_dirs: Option<Vec<PathBuf>>,
    pub allowed_tools: Option<Vec<String>>,
    pub disallowed_tools: Option<Vec<String>>,
    pub mcp_config_files: Option<Vec<PathBuf>>,

    // ── Per-client isolation ─────────────────────────────────────────────
    /// Base directory for per-client workspaces. Each `SessionKey` gets a
    /// subdirectory used as CWD for the spawned `claude` process, giving
    /// each client its own `CLAUDE.md`, `.claude/settings.json`, and
    /// `.mcp.json` (memory, skills, MCP isolation).
    pub client_config_base_dir: Option<PathBuf>,
    /// Template directory whose contents are copied into new per-client
    /// workspaces on first use.
    pub client_template_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PlatformsSection {
    pub line: Option<LineSection>,
    pub slack: Option<SlackSection>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LineSection {
    pub channel_secret_env: Option<String>,
    pub channel_token_env: Option<String>,
    pub webhook_bind: Option<String>,
    pub allowlist: Option<Vec<String>>,
    pub media: Option<MediaSection>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MediaSection {
    /// "local-http" | "azure-blob"
    pub kind: Option<String>,
    /// Bind address for the in-process server, e.g. "0.0.0.0:8081".
    pub bind: Option<String>,
    /// Externally-reachable base URL the recipient will fetch from,
    /// e.g. "https://media.example.com" or "https://abcd.ngrok.io".
    pub public_base_url: Option<String>,
    /// Env var holding the Azure Storage connection string (azure-blob only).
    pub connection_string_env: Option<String>,
    /// Azure Blob Storage container name (azure-blob only).
    pub container: Option<String>,
    /// SAS URL expiry in seconds (azure-blob only, default 3600).
    pub sas_expiry_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SlackSection {
    pub app_token_env: Option<String>,
    pub bot_token_env: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProviderSection {
    pub base_url: Option<String>,
    pub model: Option<String>,
}

impl Default for BridgeSection {
    fn default() -> Self {
        Self {
            default_agent: default_agent(),
            default_platform: default_platform(),
            state_dir: default_state_dir(),
            max_sessions: default_max_sessions(),
        }
    }
}

impl AppConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let fig = Figment::new().merge(figment::providers::Serialized::defaults(
            AppConfig::default(),
        ));
        let fig = if let Some(p) = path {
            fig.merge(Toml::file(p))
        } else {
            let default = default_state_dir().join("config.toml");
            if default.exists() {
                fig.merge(Toml::file(default))
            } else {
                fig
            }
        };
        let fig = fig.merge(Env::prefixed("AAB_").split("__"));
        Ok(fig.extract()?)
    }
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_load_without_file() {
        let cfg = AppConfig::load(None).unwrap();
        assert!(!cfg.bridge.default_agent.is_empty());
    }

    #[test]
    fn env_var_maps_to_agent_append_system_prompt() {
        std::env::set_var(
            "AAB_AGENTS__CLAUDE__APPEND_SYSTEM_PROMPT",
            "test-prompt-123",
        );
        let cfg = AppConfig::load(None).unwrap();
        let agent = cfg.agents.get("claude");
        assert!(agent.is_some());
        assert_eq!(
            agent.unwrap().append_system_prompt.as_deref(),
            Some("test-prompt-123")
        );
        std::env::remove_var("AAB_AGENTS__CLAUDE__APPEND_SYSTEM_PROMPT");
    }
}
