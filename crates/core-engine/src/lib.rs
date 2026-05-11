//! Engine: session manager actor + persistent registry + framing helpers + slash commands.

pub mod framing;
pub mod registry;
pub mod session;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use core_commands::{parse_command_line, CommandRegistry, CommandSpec, Source};
use core_traits::{Agent, Message, MessageHandler, Platform, ReplyCtx, Result, SessionKey};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{error, info, warn};

pub use registry::{RegistryEntry, SessionRegistry};
pub use session::{Cmd, SessionActor, SessionHandle};

/// The orchestrator. Holds the agent backends, the active platform, the
/// session registry, and the slash-command registry. Implements
/// [`MessageHandler`] so platforms dispatch into it.
pub struct Engine {
    agents: HashMap<String, Arc<dyn Agent>>,
    default_agent: String,
    platform: Arc<dyn Platform>,
    registry: Arc<Mutex<SessionRegistry>>,
    sessions: dashmap::DashMap<SessionKey, SessionHandle>,
    commands: Arc<RwLock<CommandRegistry>>,
    boot_time_ms: i64,
    /// Hard cap on concurrent live sessions. `0` means unlimited (legacy).
    /// When the cap is hit, new SessionKeys are rejected with a chat reply
    /// — defends against fork-bomb scenarios where many users spawn agents.
    max_sessions: usize,
    /// When true, buffer all partial text and send a single reply per assistant
    /// turn. Avoids fragmented messages on chat platforms (LINE, Slack).
    batch_replies: bool,
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::new()
    }

    pub fn commands(&self) -> Arc<RwLock<CommandRegistry>> {
        self.commands.clone()
    }

    /// Dispatch one inbound chat message: route slash command, claim or spawn
    /// the per-user session actor, forward the prompt.
    pub async fn dispatch(&self, msg: Message) {
        if msg.timestamp_ms != 0 && msg.timestamp_ms < self.boot_time_ms {
            // Stale event from before this process started; ignore.
            return;
        }
        if let Err(e) = self.dispatch_inner(msg).await {
            error!(error = %e, "dispatch failed");
        }
    }

    async fn dispatch_inner(&self, msg: Message) -> Result<()> {
        let Message {
            key,
            text,
            attachments,
            reply_ctx,
            ..
        } = msg;

        let trimmed = text.trim().to_string();
        if let Some((name, args)) = parse_command_line(&trimmed) {
            // Resolve via registry first so users may override descriptions
            // but never built-in semantics.
            let spec_source = {
                let reg = self.commands.read().await;
                reg.get(&name).map(|s| (s.source, s.template.clone()))
            };
            match spec_source {
                Some((Source::Builtin, _)) => {
                    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                    return self
                        .handle_builtin(&name, &arg_refs, &key, &reply_ctx)
                        .await;
                }
                Some((_, template)) => {
                    // User/agent-defined command: expand template, fall through
                    // to agent dispatch.
                    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                    let expanded = expand_simple(&template, &arg_refs);
                    return self
                        .send_to_session(&key, &reply_ctx, expanded, attachments)
                        .await;
                }
                None => {
                    let _ = self
                        .platform
                        .reply(
                            &reply_ctx,
                            &format!("unknown command `/{name}` — try /help"),
                        )
                        .await;
                    return Ok(());
                }
            }
        }

        self.send_to_session(&key, &reply_ctx, trimmed, attachments)
            .await
    }

    async fn send_to_session(
        &self,
        key: &SessionKey,
        reply_ctx: &ReplyCtx,
        prompt: String,
        attachments: Vec<core_traits::Attachment>,
    ) -> Result<()> {
        let handle = self.get_or_spawn_session(key, reply_ctx).await?;
        let cmd = Cmd::Send {
            prompt,
            attachments,
            reply_ctx: reply_ctx.clone(),
        };
        match handle.tx.send(cmd).await {
            Ok(()) => Ok(()),
            Err(mpsc::error::SendError(cmd)) => {
                // Session actor died (e.g. Claude exited on bad --resume).
                // Remove the dead handle and retry once with a fresh session.
                warn!(?key, "session actor dropped; retrying with fresh session");
                self.sessions.remove(key);
                {
                    let mut reg = self.registry.lock().await;
                    reg.clear_active(key);
                    reg.persist().await.ok();
                }
                let handle = self.get_or_spawn_session(key, reply_ctx).await?;
                handle
                    .tx
                    .send(cmd)
                    .await
                    .map_err(|_| anyhow!("session actor dropped on retry"))?;
                Ok(())
            }
        }
    }

    async fn handle_builtin(
        &self,
        name: &str,
        args: &[&str],
        key: &SessionKey,
        reply_ctx: &ReplyCtx,
    ) -> Result<()> {
        match name {
            "help" => {
                let reg = self.commands.read().await;
                let mut lines = vec!["Available commands:".to_string()];
                let mut entries: Vec<_> = reg.list().collect();
                entries.sort_by(|a, b| a.name.cmp(&b.name));
                for spec in entries {
                    let tag = match spec.source {
                        Source::Builtin => "builtin",
                        Source::Config => "config",
                        Source::Agent => "agent",
                    };
                    lines.push(format!(
                        "  /{}  ({})  — {}",
                        spec.name, tag, spec.description
                    ));
                }
                self.platform.reply(reply_ctx, &lines.join("\n")).await?;
            }
            "new" => {
                self.reset_session(key).await?;
                self.platform
                    .reply(reply_ctx, "session reset — next message starts fresh.")
                    .await
                    .ok();
            }
            "clear" => {
                self.clear_session(key).await?;
                self.platform
                    .reply(reply_ctx, "session cleared — all history wiped.")
                    .await
                    .ok();
            }
            "agents" => {
                let mut names: Vec<&str> = self.agents.keys().map(|s| s.as_str()).collect();
                names.sort();
                let body = format!(
                    "agents available: {}\ndefault: {}",
                    names.join(", "),
                    self.default_agent
                );
                self.platform.reply(reply_ctx, &body).await?;
            }
            "resume" => {
                self.handle_resume(args, key, reply_ctx).await?;
            }
            "mcp" => {
                self.handle_mcp(key, reply_ctx).await?;
            }
            "skills" => {
                self.handle_skills(key, reply_ctx).await?;
            }
            other => {
                self.platform
                    .reply(reply_ctx, &format!("/{other} not implemented"))
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_resume(
        &self,
        args: &[&str],
        key: &SessionKey,
        reply_ctx: &ReplyCtx,
    ) -> Result<()> {
        let target_id = args.first().copied().unwrap_or("");
        if target_id.is_empty() {
            // List sessions for this key.
            let reg = self.registry.lock().await;
            let Some(entry) = reg.entries().get(key) else {
                self.platform.reply(reply_ctx, "no sessions found.").await?;
                return Ok(());
            };
            let mut lines = Vec::new();
            if let Some(id) = &entry.active_session_id {
                lines.push(format!("▶ {} (active, agent={})", id, entry.agent));
            }
            let past = &entry.past_agent_session_ids;
            let show = past.len().min(20);
            for (agent, sid) in past.iter().rev().take(show) {
                lines.push(format!("  {} (agent={})", sid, agent));
            }
            if past.len() > show {
                lines.push(format!("  … and {} more", past.len() - show));
            }
            if lines.is_empty() {
                self.platform.reply(reply_ctx, "no sessions found.").await?;
            } else {
                self.platform.reply(reply_ctx, &lines.join("\n")).await?;
            }
        } else {
            // Resume a specific session id.
            if let Some((_, h)) = self.sessions.remove(key) {
                let _ = h.tx.send(Cmd::Close).await;
            }
            let agent_name = {
                let reg = self.registry.lock().await;
                reg.agent_for(key)
                    .unwrap_or_else(|| self.default_agent.clone())
            };
            {
                let mut reg = self.registry.lock().await;
                reg.record_session(key.clone(), agent_name, target_id.to_string());
                reg.persist().await.ok();
            }
            self.platform
                .reply(
                    reply_ctx,
                    &format!("will resume session `{target_id}` on next message."),
                )
                .await?;
        }
        Ok(())
    }

    fn agent_client_dir(&self, key: &SessionKey) -> Option<PathBuf> {
        let agent_name = {
            // Can't hold the lock across the agent call, but we only need the name.
            // Use try_lock to avoid deadlock if called from within a locked context.
            let reg = self.registry.try_lock().ok()?;
            reg.agent_for(key)
                .unwrap_or_else(|| self.default_agent.clone())
        };
        let agent = self.agents.get(&agent_name)?;
        agent.client_dir(key)
    }

    async fn handle_mcp(&self, key: &SessionKey, reply_ctx: &ReplyCtx) -> Result<()> {
        let Some(dir) = self.agent_client_dir(key) else {
            self.platform
                .reply(reply_ctx, "per-client isolation not configured.")
                .await?;
            return Ok(());
        };
        let mcp_path = dir.join(".mcp.json");
        if !mcp_path.exists() {
            self.platform
                .reply(
                    reply_ctx,
                    &format!("no .mcp.json found.\nconfig path: {}", mcp_path.display()),
                )
                .await?;
            return Ok(());
        }
        let raw = std::fs::read_to_string(&mcp_path)?;
        let parsed: serde_json::Value = serde_json::from_str(&raw)?;
        let mut lines = vec!["MCP servers:".to_string()];
        if let Some(servers) = parsed.get("mcpServers").and_then(|v| v.as_object()) {
            for name in servers.keys() {
                lines.push(format!("  - {name}"));
            }
        }
        if lines.len() == 1 {
            lines.push("  (none)".to_string());
        }
        lines.push(format!("config: {}", mcp_path.display()));
        self.platform.reply(reply_ctx, &lines.join("\n")).await?;
        Ok(())
    }

    async fn handle_skills(&self, key: &SessionKey, reply_ctx: &ReplyCtx) -> Result<()> {
        let Some(dir) = self.agent_client_dir(key) else {
            self.platform
                .reply(reply_ctx, "per-client isolation not configured.")
                .await?;
            return Ok(());
        };
        let mut lines = Vec::new();

        // Scan .claude/skills/*/SKILL.md
        let skills_dir = dir.join(".claude").join("skills");
        if skills_dir.is_dir() {
            let mut names: Vec<String> = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&skills_dir) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() && entry.path().join("SKILL.md").exists() {
                        names.push(entry.file_name().to_string_lossy().into_owned());
                    }
                }
            }
            names.sort();
            if names.is_empty() {
                lines.push("Skills: (none)".to_string());
            } else {
                lines.push(format!("Skills ({}):", names.len()));
                for name in &names {
                    lines.push(format!("  /{name}"));
                }
            }
            lines.push(format!("dir: {}", skills_dir.display()));
        } else {
            lines.push("Skills: (none)".to_string());
            lines.push(format!("dir: {} (not found)", skills_dir.display()));
        }

        // Also show settings.json summary if present
        let settings_path = dir.join(".claude").join("settings.json");
        if settings_path.exists() {
            if let Ok(raw) = std::fs::read_to_string(&settings_path) {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) {
                    lines.push(String::new());
                    lines.push("Settings:".to_string());
                    if let Some(obj) = parsed.as_object() {
                        for (k, v) in obj {
                            match v {
                                serde_json::Value::Array(arr) => {
                                    lines.push(format!("  {k}: [{} items]", arr.len()));
                                }
                                serde_json::Value::Object(map) => {
                                    lines.push(format!("  {k}: {{{} keys}}", map.len()));
                                }
                                _ => {
                                    lines.push(format!("  {k}: {v}"));
                                }
                            }
                        }
                    }
                }
            }
        }

        self.platform.reply(reply_ctx, &lines.join("\n")).await?;
        Ok(())
    }

    async fn get_or_spawn_session(
        &self,
        key: &SessionKey,
        reply_ctx: &ReplyCtx,
    ) -> Result<SessionHandle> {
        if let Some(h) = self.sessions.get(key) {
            return Ok(h.clone());
        }
        // F10 cap: never let one chat platform spin up unbounded agent
        // processes. The check is racy (two concurrent first-messages from
        // different keys could both pass it then both insert), but the worst
        // case is O(thread_count) overshoot, not unbounded.
        if self.max_sessions > 0 && self.sessions.len() >= self.max_sessions {
            tracing::warn!(
                ?key,
                cap = self.max_sessions,
                "rejecting new session: max_sessions reached"
            );
            let _ = self
                .platform
                .reply(
                    reply_ctx,
                    "⚠️ bridge is at capacity (too many active sessions). \
                     Try /new on an idle conversation or wait a moment.",
                )
                .await;
            return Err(anyhow!("max_sessions ({}) reached", self.max_sessions));
        }
        let agent_name = {
            let reg = self.registry.lock().await;
            reg.agent_for(key)
                .unwrap_or_else(|| self.default_agent.clone())
        };
        let agent = self
            .agents
            .get(&agent_name)
            .ok_or_else(|| anyhow!("unknown agent `{agent_name}`"))?
            .clone();
        let resume = {
            let reg = self.registry.lock().await;
            reg.last_session_id(key)
        };

        let session = agent
            .start_session(key.clone(), resume)
            .await
            .with_context(|| format!("starting agent `{agent_name}` for {:?}", key))?;
        let id = session.id();

        let actor = SessionActor::spawn(
            session,
            self.platform.clone(),
            reply_ctx.clone(),
            self.registry.clone(),
            key.clone(),
            agent_name.clone(),
            self.batch_replies,
        );

        {
            let mut reg = self.registry.lock().await;
            reg.record_session(key.clone(), agent_name, id.clone());
            reg.persist().await.ok();
        }

        let handle = SessionHandle {
            id,
            tx: actor.tx,
            key: key.clone(),
        };
        self.sessions.insert(key.clone(), handle.clone());
        Ok(handle)
    }

    pub async fn reset_session(&self, key: &SessionKey) -> Result<()> {
        if let Some((_, h)) = self.sessions.remove(key) {
            let _ = h.tx.send(Cmd::Close).await;
        }
        let mut reg = self.registry.lock().await;
        reg.clear_active(key);
        reg.persist().await.ok();
        Ok(())
    }

    /// Hard clear: close the session actor AND wipe all registry history for
    /// this key. The next inbound message starts a brand-new session with no
    /// `--resume`, as if this user had never talked to the bridge.
    pub async fn clear_session(&self, key: &SessionKey) -> Result<()> {
        if let Some((_, h)) = self.sessions.remove(key) {
            let _ = h.tx.send(Cmd::Close).await;
        }
        let mut reg = self.registry.lock().await;
        reg.clear_all(key);
        reg.persist().await.ok();
        Ok(())
    }

    /// Switch the active agent for `key`. Closes any live session and updates
    /// the registry; the next inbound message spawns the new agent.
    pub async fn switch_agent(&self, key: &SessionKey, new_agent: &str) -> Result<()> {
        if !self.agents.contains_key(new_agent) {
            return Err(anyhow!(
                "unknown agent `{new_agent}` (available: {})",
                self.agents.keys().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
        if let Some((_, h)) = self.sessions.remove(key) {
            let _ = h.tx.send(Cmd::Close).await;
        }
        let mut reg = self.registry.lock().await;
        reg.clear_active(key);
        reg.set_agent(key.clone(), new_agent.to_string());
        reg.persist().await.ok();
        Ok(())
    }

    /// Ordered shutdown: signal every session actor to close, then persist.
    pub async fn shutdown(&self) {
        info!("engine shutdown starting");
        let mut handles = Vec::new();
        for entry in self.sessions.iter() {
            handles.push(entry.value().clone());
        }
        self.sessions.clear();
        for h in handles {
            let _ = h.tx.send(Cmd::Close).await;
        }
        let reg = self.registry.lock().await;
        if let Err(e) = reg.persist().await {
            warn!(error = %e, "registry persist on shutdown failed");
        }
    }
}

#[async_trait]
impl MessageHandler for Engine {
    async fn handle(&self, message: Message) {
        self.dispatch(message).await;
    }
}

#[derive(Default)]
pub struct EngineBuilder {
    agents: HashMap<String, Arc<dyn Agent>>,
    default_agent: Option<String>,
    platform: Option<Arc<dyn Platform>>,
    registry: Option<Arc<Mutex<SessionRegistry>>>,
    extra_commands: Vec<CommandSpec>,
    max_sessions: Option<usize>,
    /// Defaults to true via `Self::default()` override below.
    batch_replies: bool,
}

impl EngineBuilder {
    pub fn new() -> Self {
        Self {
            batch_replies: true,
            ..Default::default()
        }
    }
}

impl EngineBuilder {
    pub fn add_agent(mut self, agent: Arc<dyn Agent>) -> Self {
        self.agents.insert(agent.name().to_string(), agent);
        self
    }
    pub fn default_agent(mut self, name: impl Into<String>) -> Self {
        self.default_agent = Some(name.into());
        self
    }
    pub fn platform(mut self, p: Arc<dyn Platform>) -> Self {
        self.platform = Some(p);
        self
    }
    pub fn registry(mut self, r: Arc<Mutex<SessionRegistry>>) -> Self {
        self.registry = Some(r);
        self
    }
    pub fn add_command(mut self, spec: CommandSpec) -> Self {
        self.extra_commands.push(spec);
        self
    }
    /// Cap the number of concurrent live agent sessions. `0` = unlimited.
    /// New SessionKeys arriving when the cap is hit are rejected with a
    /// chat reply and never spawn a backing agent process. Default 96.
    pub fn max_sessions(mut self, n: usize) -> Self {
        self.max_sessions = Some(n);
        self
    }
    /// Buffer all partial text and send a single reply per assistant turn.
    /// Default true — avoids fragmented messages on chat platforms.
    pub fn batch_replies(mut self, b: bool) -> Self {
        self.batch_replies = b;
        self
    }
    pub fn build(self) -> Result<Arc<Engine>> {
        let default_agent = self
            .default_agent
            .ok_or_else(|| anyhow!("default_agent required"))?;
        if !self.agents.contains_key(&default_agent) {
            return Err(anyhow!("default_agent `{default_agent}` not registered"));
        }
        let platform = self.platform.ok_or_else(|| anyhow!("platform required"))?;
        let registry = self
            .registry
            .unwrap_or_else(|| Arc::new(Mutex::new(SessionRegistry::in_memory())));

        let mut commands = CommandRegistry::new();
        for spec in builtin_commands() {
            commands
                .register(spec)
                .expect("builtin commands must register cleanly");
        }
        for spec in self.extra_commands {
            if let Err(e) = commands.register(spec) {
                tracing::warn!(error = %e, "extra command registration rejected");
            }
        }

        Ok(Arc::new(Engine {
            agents: self.agents,
            default_agent,
            platform,
            registry,
            sessions: dashmap::DashMap::new(),
            commands: Arc::new(RwLock::new(commands)),
            boot_time_ms: now_ms(),
            max_sessions: self.max_sessions.unwrap_or(96),
            batch_replies: self.batch_replies,
        }))
    }
}

fn builtin_commands() -> Vec<CommandSpec> {
    fn b(name: &str, desc: &str) -> CommandSpec {
        CommandSpec {
            name: name.to_string(),
            source: Source::Builtin,
            template: String::new(),
            description: desc.to_string(),
        }
    }
    vec![
        b("help", "list available commands"),
        b("new", "start a fresh session on next message"),
        b(
            "clear",
            "wipe all session history and start completely fresh",
        ),
        b("agents", "list registered agents"),
        b(
            "resume",
            "list sessions or resume one: /resume [session_id]",
        ),
        b("mcp", "show MCP servers configured for this client"),
        b("skills", "show skills/settings configured for this client"),
    ]
}

/// Tiny templating used for non-builtin commands. Mirrors the subset of
/// `core_commands::CommandRegistry::expand` semantics: `{{1}}`, `{{2*}}`, `{{args}}`.
fn expand_simple(template: &str, args: &[&str]) -> String {
    let mut reg = CommandRegistry::new();
    let _ = reg.register(CommandSpec {
        name: "_".into(),
        source: Source::Builtin,
        template: template.to_string(),
        description: String::new(),
    });
    reg.expand("_", args).unwrap_or_default()
}

fn now_ms() -> i64 {
    core_traits::now_ms()
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_traits::{Message, SessionKey};
    use std::sync::Arc;
    use test_support::{EchoAgent, MockPlatform};

    #[test]
    fn expand_simple_basic_template() {
        let result = expand_simple("say {{1}}", &["hello"]);
        assert_eq!(result, "say hello");
    }

    #[test]
    fn expand_simple_with_star_and_args() {
        let result = expand_simple("{{1}}: {{2*}}", &["TODO", "fix", "the", "bug"]);
        assert_eq!(result, "TODO: fix the bug");
    }

    #[test]
    fn expand_simple_args_alias() {
        let result = expand_simple("→ {{args}}", &["a", "b"]);
        assert_eq!(result, "→ a b");
    }

    #[tokio::test]
    async fn stale_event_ignored() {
        let platform = Arc::new(MockPlatform::new("t"));
        let engine = Engine::builder()
            .add_agent(Arc::new(EchoAgent))
            .default_agent("echo")
            .platform(platform.clone())
            .build()
            .unwrap();
        let msg = Message {
            key: SessionKey::new("t", "u1"),
            text: "stale".into(),
            attachments: vec![],
            reply_ctx: ReplyCtx::default(),
            timestamp_ms: 1, // way before boot_time_ms
        };
        engine.dispatch(msg).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let replies = platform.replies().await;
        assert!(replies.is_empty(), "stale event should be silently dropped");
    }

    #[tokio::test]
    async fn switch_agent_success() {
        let platform = Arc::new(MockPlatform::new("t"));
        let engine = Engine::builder()
            .add_agent(Arc::new(EchoAgent))
            .add_agent(Arc::new(test_support::EchoAgent)) // re-registering won't work with same name
            .default_agent("echo")
            .platform(platform.clone())
            .build()
            .unwrap();
        let key = SessionKey::new("t", "u1");
        // Switch to echo (same agent) should succeed since it's registered.
        let res = engine.switch_agent(&key, "echo").await;
        assert!(res.is_ok());
    }
}
