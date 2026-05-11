//! `aab` — ai-agent-bridge CLI entry point.
//!
//! Subcommands:
//!   aab run [--agent <name>] [--platform <name>] [--config <path>]
//!   aab session list
//!   aab daemon status
//!
//! Environment overrides use the `AAB_` prefix with `__` as the section
//! separator, e.g. `AAB_BRIDGE__DEFAULT_AGENT=claude`.

mod config;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use core_engine::{Engine, SessionRegistry};
use core_traits::{Agent, Platform};
use tokio::sync::Mutex;
use tracing::info;

use config::AppConfig;

fn expand_tilde(p: PathBuf) -> PathBuf {
    if let Some(s) = p.to_str() {
        if let Some(rest) = s.strip_prefix("~/") {
            if let Some(home) = std::env::var_os("HOME") {
                return PathBuf::from(home).join(rest);
            }
        }
    }
    p
}

#[derive(Parser, Debug)]
#[command(name = "aab", version, about = "AI Agent Bridge for chat platforms")]
struct Cli {
    /// Path to TOML config (default: ~/.ai-agent-bridge/config.toml)
    #[arg(long, global = true, env = "AAB_CONFIG")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Start the bridge in the foreground.
    Run {
        #[arg(long, value_enum)]
        agent: Option<AgentChoice>,
        #[arg(long, value_enum)]
        platform: Option<PlatformChoice>,
    },
    /// Print the persistent session registry.
    Session {
        #[command(subcommand)]
        action: SessionAction,
    },
    /// Daemon utilities (status, stop). Install/uninstall is platform-specific.
    Daemon {
        #[command(subcommand)]
        action: DaemonAction,
    },
}

#[derive(Subcommand, Debug)]
enum SessionAction {
    List,
    Reset { key: String },
}

#[derive(Subcommand, Debug)]
enum DaemonAction {
    Status,
    Install,
    Uninstall,
    Start,
    Stop,
    /// Print the path of the rotating log directory.
    LogsPath,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum AgentChoice {
    Claude,
    Copilot,
    Shell,
    Acp,
    Http,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum PlatformChoice {
    Stdio,
    Line,
    Slack,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = AppConfig::load(cli.config.as_deref())?;
    match cli.cmd {
        Cmd::Run { agent, platform } => run(cfg, agent, platform).await,
        Cmd::Session { action } => session_action(cfg, action).await,
        Cmd::Daemon { action } => daemon_action(cfg, action).await,
    }
}

fn init_tracing() {
    use tracing_subscriber::prelude::*;
    let filter = tracing_subscriber::EnvFilter::try_from_env("AAB_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .try_init();
}

async fn run(
    cfg: AppConfig,
    agent_override: Option<AgentChoice>,
    platform_override: Option<PlatformChoice>,
) -> Result<()> {
    let agent_name = match agent_override {
        Some(AgentChoice::Claude) => "claude",
        Some(AgentChoice::Copilot) => "copilot",
        Some(AgentChoice::Shell) => "shell",
        Some(AgentChoice::Acp) => "acp",
        Some(AgentChoice::Http) => "http",
        None => &cfg.bridge.default_agent,
    }
    .to_string();

    let platform_name = match platform_override {
        Some(PlatformChoice::Stdio) => "stdio",
        Some(PlatformChoice::Line) => "line",
        Some(PlatformChoice::Slack) => "slack",
        None => &cfg.bridge.default_platform,
    }
    .to_string();

    let agent = build_agent(&agent_name, &cfg)?;
    let platform = build_platform(&platform_name, &cfg).await?;

    let state_path = cfg.bridge.state_dir.join("state.json");
    let registry = Arc::new(Mutex::new(SessionRegistry::open(state_path)?));

    let engine = Engine::builder()
        .add_agent(agent)
        .default_agent(agent_name.clone())
        .platform(platform.clone())
        .registry(registry)
        .max_sessions(cfg.bridge.max_sessions)
        .build()?;

    info!(%agent_name, %platform_name, "engine starting");
    let handler: Arc<dyn core_traits::MessageHandler> = engine.clone();
    let plat_clone = platform.clone();

    let shutdown = tokio::signal::ctrl_c();
    tokio::select! {
        res = plat_clone.start(handler) => {
            res.context("platform start")?;
        }
        _ = shutdown => {
            info!("ctrl-c received");
        }
    }
    engine.shutdown().await;
    Ok(())
}

#[allow(unused_variables)]
fn build_agent(name: &str, cfg: &AppConfig) -> Result<Arc<dyn Agent>> {
    match name {
        #[cfg(feature = "claude-code")]
        "claude" => {
            use agent_claude_code::{ClaudeCodeAgent, ClaudeCodeConfig, PermissionMode};
            let a = cfg.agents.get("claude").cloned().unwrap_or_default();
            let cc = ClaudeCodeConfig {
                binary: a.binary.unwrap_or_else(|| "claude".into()),
                extra_args: a.extra_args.unwrap_or_default(),
                cwd: a.cwd.map(expand_tilde),
                permission_mode: match a.permission_mode.as_deref() {
                    Some("acceptEdits") => PermissionMode::AcceptEdits,
                    Some("bypassPermissions") => PermissionMode::BypassPermissions,
                    _ => PermissionMode::Ask,
                },
                skip_permissions: a.skip_permissions.unwrap_or(true),
                include_partial_messages: a.include_partial_messages.unwrap_or(true),
                model: a.model,
                fallback_model: a.fallback_model,
                effort: a.effort,
                append_system_prompt: a.append_system_prompt,
                max_budget_usd: a.max_budget_usd,
                add_dirs: a.add_dirs.unwrap_or_default(),
                allowed_tools: a.allowed_tools.unwrap_or_default(),
                disallowed_tools: a.disallowed_tools.unwrap_or_default(),
                mcp_config_files: a.mcp_config_files.unwrap_or_default(),
                session_id: None,
                inline_image_max_bytes: 256 * 1024,
                client_config_base_dir: a.client_config_base_dir.map(expand_tilde),
                client_template_dir: a.client_template_dir.map(expand_tilde),
            };
            Ok(Arc::new(ClaudeCodeAgent::new(cc)))
        }
        #[cfg(feature = "cli-agent")]
        "copilot" => {
            // Forwards prompts to `gh copilot explain` (the only `gh copilot`
            // subcommand that produces non-interactive plain-text output).
            // We deliberately do NOT call the GitHub Models API here — the
            // bridge's purpose is to operate the user's already-logged-in
            // CLI agent, not become an API client.
            use agent_cli::{CliAgent, CliAgentConfig};
            let a = cfg.agents.get("copilot").cloned().unwrap_or_default();
            let cc = CliAgentConfig {
                binary: a.binary.unwrap_or_else(|| "gh".into()),
                args: a
                    .extra_args
                    .unwrap_or_else(|| vec!["copilot".into(), "explain".into()]),
                supports_resume: false,
            };
            Ok(Arc::new(CliAgent::new("copilot", cc)))
        }
        #[cfg(feature = "cli-agent")]
        "shell" => {
            // Generic CLI runner: spawns whatever binary the user configured.
            // Useful for wrapping `aichat`, `mods`, custom scripts, etc.
            use agent_cli::{CliAgent, CliAgentConfig};
            let a = cfg.agents.get("shell").cloned().unwrap_or_default();
            let cc = CliAgentConfig {
                binary: a
                    .binary
                    .ok_or_else(|| anyhow!("agents.shell.binary required for --agent shell"))?,
                args: a.extra_args.unwrap_or_default(),
                supports_resume: false,
            };
            Ok(Arc::new(CliAgent::new("shell", cc)))
        }
        #[cfg(feature = "http-agent")]
        "http" | "openai" => {
            // Escape hatch for OpenAI-compatible API backends. NOT the primary
            // mode — most users should use a CLI agent above.
            use agent_http::{HttpAgent, HttpAgentConfig};
            let p = cfg.providers.get("openai").cloned().unwrap_or_default();
            Ok(Arc::new(HttpAgent::new(HttpAgentConfig {
                base_url: p
                    .base_url
                    .unwrap_or_else(|| "https://api.openai.com/v1".into()),
                model: p.model.unwrap_or_else(|| "gpt-4o-mini".into()),
                api_key: std::env::var("OPENAI_API_KEY").unwrap_or_default(),
                agent_name: "openai",
                stream: true,
            })))
        }
        #[cfg(feature = "acp")]
        "acp" => {
            use agent_acp::{AcpAgent, AcpConfig};
            let a = cfg.agents.get("acp").cloned().unwrap_or_default();
            Ok(Arc::new(AcpAgent::new(AcpConfig {
                binary: a
                    .binary
                    .ok_or_else(|| anyhow!("agents.acp.binary required for --agent acp"))?,
                args: a.extra_args.unwrap_or_default(),
                cwd: a.cwd,
            })))
        }
        other => Err(anyhow!("agent `{other}` not enabled in this build")),
    }
}

#[allow(unused_variables)]
async fn build_platform(name: &str, cfg: &AppConfig) -> Result<Arc<dyn Platform>> {
    match name {
        #[cfg(feature = "stdio")]
        "stdio" => Ok(Arc::new(platform_stdio::StdioPlatform::new())),
        #[cfg(feature = "line")]
        "line" => {
            use platform_line::{LineConfig, LinePlatform};
            let p = cfg.platforms.line.clone().unwrap_or_default();
            let mut platform = LinePlatform::new(LineConfig {
                channel_secret: env_or(p.channel_secret_env.as_deref(), "LINE_CHANNEL_SECRET")?,
                channel_token: env_or(p.channel_token_env.as_deref(), "LINE_CHANNEL_TOKEN")?,
                bind: p
                    .webhook_bind
                    .clone()
                    .unwrap_or_else(|| "0.0.0.0:8080".into()),
                allowlist: p.allowlist.clone().unwrap_or_default(),
            });
            #[cfg(feature = "media-local")]
            if let Some(media) = &p.media {
                if media.kind.as_deref() == Some("local-http") {
                    use media_publisher::local_http::LocalHttpPublisher;
                    let bind: std::net::SocketAddr = media
                        .bind
                        .as_deref()
                        .unwrap_or("0.0.0.0:8081")
                        .parse()
                        .map_err(|e| anyhow!("invalid media bind: {e}"))?;
                    let public = url::Url::parse(
                        media
                            .public_base_url
                            .as_deref()
                            .ok_or_else(|| anyhow!("media.public_base_url required"))?,
                    )?;
                    let publisher = LocalHttpPublisher::spawn(bind, public).await?;
                    platform = platform.with_publisher(publisher);
                }
            }
            #[cfg(feature = "media-azure")]
            if let Some(media) = &p.media {
                if media.kind.as_deref() == Some("azure-blob") {
                    use media_publisher::azure_blob::AzureBlobPublisher;
                    let cs_env = media
                        .connection_string_env
                        .as_deref()
                        .unwrap_or("AZURE_STORAGE_CONNECTION_STRING");
                    let cs = env_or(Some(cs_env), "AZURE_STORAGE_CONNECTION_STRING")?;
                    let container = media.container.as_deref().unwrap_or("aab-media");
                    let expiry =
                        std::time::Duration::from_secs(media.sas_expiry_secs.unwrap_or(3600));
                    let publisher = AzureBlobPublisher::new(&cs, container, expiry)?;
                    platform = platform.with_publisher(std::sync::Arc::new(publisher));
                }
            }
            Ok(Arc::new(platform))
        }
        #[cfg(feature = "slack")]
        "slack" => {
            use platform_slack::{SlackConfig, SlackPlatform};
            let p = cfg.platforms.slack.clone().unwrap_or_default();
            Ok(Arc::new(SlackPlatform::new(SlackConfig {
                app_token: env_or(p.app_token_env.as_deref(), "SLACK_APP_TOKEN")?,
                bot_token: env_or(p.bot_token_env.as_deref(), "SLACK_BOT_TOKEN")?,
            })))
        }
        other => Err(anyhow!("platform `{other}` not enabled in this build")),
    }
}

fn env_or(name: Option<&str>, fallback: &str) -> Result<String> {
    let key = name.unwrap_or(fallback);
    std::env::var(key).with_context(|| format!("env var `{key}` not set"))
}

async fn session_action(cfg: AppConfig, action: SessionAction) -> Result<()> {
    let path = cfg.bridge.state_dir.join("state.json");
    let mut reg = SessionRegistry::open(path)?;
    match action {
        SessionAction::List => {
            for (k, v) in reg.entries() {
                println!(
                    "{}  agent={}  active={}  past={}",
                    k.0,
                    v.agent,
                    v.active_session_id.as_deref().unwrap_or("-"),
                    v.past_agent_session_ids.len()
                );
            }
        }
        SessionAction::Reset { key } => {
            reg.clear_active(&core_traits::SessionKey(key));
            reg.persist().await?;
            println!("ok");
        }
    }
    Ok(())
}

async fn daemon_action(cfg: AppConfig, action: DaemonAction) -> Result<()> {
    let svc = daemon::platform_service("aab");
    match action {
        DaemonAction::Status => {
            let lock_path = cfg.bridge.state_dir.join("daemon.lock");
            let lock_state = match daemon::LockGuard::acquire(&lock_path) {
                Ok(_g) => "no instance holds the file lock",
                Err(_) => "an instance holds the file lock",
            };
            let svc_state = svc
                .status()
                .map(|s| format!("{s:?}"))
                .unwrap_or_else(|e| format!("error: {e}"));
            println!("file lock: {lock_state} ({})", lock_path.display());
            println!("service:   {svc_state}");
        }
        DaemonAction::Install => {
            let exe = std::env::current_exe()?;
            let args = vec!["run".to_string()];
            svc.install(&exe, &args)?;
            println!("installed; use `aab daemon start` to launch");
        }
        DaemonAction::Uninstall => {
            svc.uninstall()?;
            println!("uninstalled");
        }
        DaemonAction::Start => {
            svc.start()?;
            println!("started");
        }
        DaemonAction::Stop => {
            svc.stop()?;
            println!("stopped");
        }
        DaemonAction::LogsPath => {
            println!("{}", cfg.bridge.state_dir.join("logs").display());
        }
    }
    Ok(())
}
