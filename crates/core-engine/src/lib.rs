//! Engine: session manager actor + persistent registry + framing helpers + slash commands.

pub mod framing;
pub mod registry;
pub mod session;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use core_commands::{parse_command_line, CommandRegistry, CommandSpec, Source};
use core_traits::{Agent, Message, MessageHandler, Platform, ReplyCtx, Result, SessionKey};
use tokio::sync::{Mutex, RwLock};
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
}

impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
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
        handle
            .tx
            .send(Cmd::Send {
                prompt,
                attachments,
            })
            .await
            .map_err(|_| anyhow!("session actor dropped"))?;
        Ok(())
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
            "reset" | "new" => {
                self.reset_session(key).await?;
                self.platform.reply(reply_ctx, "session reset.").await.ok();
            }
            "agent" => {
                let target = args
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("usage: /agent <name>"))?;
                self.switch_agent(key, target).await?;
                self.platform
                    .reply(reply_ctx, &format!("switched agent → `{target}`"))
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
            "yes" | "no" => {
                let id = args
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("usage: /{name} <permission_id>"))?;
                let allow = name == "yes";
                if let Some(handle) = self.sessions.get(key) {
                    handle
                        .tx
                        .send(Cmd::Permission {
                            id: id.to_string(),
                            allow,
                        })
                        .await
                        .map_err(|_| anyhow!("session actor dropped"))?;
                } else {
                    self.platform
                        .reply(reply_ctx, "no active session for this user")
                        .await
                        .ok();
                }
            }
            "model" => {
                let v = args.first().copied().unwrap_or("");
                self.apply_override(key, "model", v).await?;
                self.reset_session(key).await?;
                self.platform
                    .reply(
                        reply_ctx,
                        &format!("model → `{v}`; new session will start on next message."),
                    )
                    .await?;
            }
            "dir" => {
                let v = args
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("usage: /dir <path>"))?;
                self.apply_override(key, "add_dir", v).await?;
                self.reset_session(key).await?;
                self.platform
                    .reply(reply_ctx, &format!("added directory: `{v}`"))
                    .await?;
            }
            "effort" => {
                let v = args
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("usage: /effort <low|medium|high|xhigh|max>"))?;
                self.apply_override(key, "effort", v).await?;
                self.reset_session(key).await?;
                self.platform
                    .reply(reply_ctx, &format!("effort → `{v}`"))
                    .await?;
            }
            "budget" => {
                let v = args
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("usage: /budget <usd amount>"))?;
                self.apply_override(key, "budget", v).await?;
                self.reset_session(key).await?;
                self.platform
                    .reply(reply_ctx, &format!("max budget → ${v}"))
                    .await?;
            }
            "tools" => {
                // /tools allow Bash(git *)
                // /tools deny  Bash(rm *)
                // /tools clear
                let action = args
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow!("usage: /tools allow|deny|clear [tool]"))?;
                match action {
                    "allow" => {
                        let t = args
                            .get(1)
                            .copied()
                            .ok_or_else(|| anyhow!("missing tool"))?;
                        self.apply_override(key, "allow_tool", t).await?;
                    }
                    "deny" => {
                        let t = args
                            .get(1)
                            .copied()
                            .ok_or_else(|| anyhow!("missing tool"))?;
                        self.apply_override(key, "deny_tool", t).await?;
                    }
                    "clear" => self.apply_override(key, "clear_tools", "").await?,
                    other => anyhow::bail!("unknown action `{other}`; use allow|deny|clear"),
                }
                self.reset_session(key).await?;
                self.platform.reply(reply_ctx, "tools updated.").await?;
            }
            "system" => {
                // /system <prompt...> — append system prompt
                let joined = args.join(" ");
                self.apply_override(key, "append_system_prompt", &joined)
                    .await?;
                self.reset_session(key).await?;
                self.platform
                    .reply(reply_ctx, "system prompt updated.")
                    .await?;
            }
            other => {
                self.platform
                    .reply(reply_ctx, &format!("/{other} not implemented"))
                    .await?;
            }
        }
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
                     Try /reset on an idle conversation or wait a moment.",
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

    /// Forward a `(name, value)` override to whichever agent is currently
    /// active for `key`. Unknown keys / unsupported agents return `Ok(())`
    /// per the trait default.
    async fn apply_override(&self, key: &SessionKey, name: &str, value: &str) -> Result<()> {
        let agent_name = {
            let reg = self.registry.lock().await;
            reg.agent_for(key)
                .unwrap_or_else(|| self.default_agent.clone())
        };
        let Some(agent) = self.agents.get(&agent_name) else {
            return Ok(());
        };
        agent.set_override(key, name, value).await
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
        b(
            "reset",
            "end current session and start a fresh one on next message",
        ),
        b("new", "alias for /reset"),
        b("agent", "switch backing agent: /agent <name>"),
        b("agents", "list registered agents"),
        b("yes", "approve a pending permission: /yes <id>"),
        b("no", "deny a pending permission: /no <id>"),
        b("model", "request model change (requires /reset)"),
        b("dir", "request cwd change (requires /reset)"),
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
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
