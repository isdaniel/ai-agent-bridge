# AGENT.md — Maintainer's guide for ai-agent-bridge

A condensed, code-first orientation for any AI coding agent or human
maintainer touching this repo. Read this before changing anything.

## 1. Project in one paragraph

`ai-agent-bridge` (binary `aab`) is a Rust workspace that bridges chat
platforms (LINE, Slack, Telegram, Discord, local stdio) to AI agent **CLIs**
that are already installed and authenticated on the host (Claude Code, GitHub
Copilot CLI, ACP servers). Chat messages are forwarded to the CLI's stdin
verbatim; CLI stdout is forwarded back to the chat thread. **The bridge holds
no API keys for the core path** — auth/billing/tool execution all live inside
the CLI. An OpenAI-compatible HTTP escape hatch (`agent-http`) exists but is
not the primary mode and is not in `default` features.

Linux-only target. The codebase compiles on macOS/Windows but daemon
helpers and `start-bridge.sh` assume systemd-user.

## 2. Workspace map

```
crates/
├── core-traits/         Leaf crate. Agent / AgentSession / Platform /
│                        MessageHandler traits + DTOs (SessionKey,
│                        Attachment, Event, Message, ReplyCtx, …).
│                        Every other crate depends on this; it depends on
│                        nothing internal. Don't add deps here.
│
├── core-commands/       Slash command registry. Name normalisation
│                        (`-` ≡ `_`, case-insensitive), template expansion
│                        (`{{1}} {{2*}} {{args}}`), Source enum
│                        (Builtin / Config / Agent). Pure logic, no IO.
│
├── core-engine/         The orchestrator.
│   ├── lib.rs           Engine + EngineBuilder + builtin slash dispatcher
│   │                    (/help /reset /agent /agents /yes /no /model /dir
│   │                     /effort /budget /tools /system).
│   ├── session.rs       SessionActor — one tokio task per SessionKey,
│   │                    select! over inbox + agent events, partial-chunk
│   │                    buffer with 1.2 s / 240 byte flush.
│   │                    Batch mode (default): buffers all partial text,
│   │                    sends single message per turn. Fires
│   │                    Platform::show_typing() every 15 s while
│   │                    processing (free, no message quota).
│   │                    reply_ctx updates per-message so thread-aware
│   │                    replies track the latest conversation thread.
│   ├── registry.rs      SessionRegistry — thin wrapper around StateDb.
│   │                    Maps SessionKey → {agent, active_session_id,
│   │                    past_history}.
│   ├── store.rs         StateDb — SQLite (WAL mode) persistence for
│   │                    sessions + scheduled tasks.
│   ├── scheduler.rs     Scheduled actions — one-shot and recurring
│   │                    prompts persisted in SQLite, fired by a
│   │                    30 s tick loop.
│   └── framing.rs       NDJSON helpers: spawn_ndjson_reader,
│                        write_ndjson, DEFAULT_MAX_LINE = 8 MiB
│                        (used by agent-claude-code AND agent-acp).
│
├── agent-claude-code/   PRIMARY agent. Long-lived `claude` subprocess in
│   ├── lib.rs           bidirectional stream-json mode. Wires every useful
│   │                    flag: --dangerously-skip-permissions (default on),
│   │                    --include-partial-messages, --model,
│   │                    --fallback-model, --effort, --add-dir,
│   │                    --allowedTools, --disallowedTools,
│   │                    --max-budget-usd, --append-system-prompt,
│   │                    --mcp-config, --session-id (we mint UUID),
│   │                    --resume.
│   ├── session.rs       spawn() + StreamState lifecycle + permission
│   │                    round-trip (DashMap<request_id, oneshot>).
│   └── stream_event.rs  Tagged enum StreamEvent (System | Assistant |
│                        User | Result | ControlRequest | StreamEvent for
│                        partial chunks). Forward-compatible via
│                        #[serde(other)] / #[serde(flatten)].
│
├── agent-acp/           ACP (Agent Client Protocol) JSON-RPC over stdio.
│   ├── lib.rs           Spec: github.com/zed-industries/agent-client-protocol
│   ├── jsonrpc.rs       Generic JSON-RPC 2.0 client: PendingMap
│   │                    (DashMap<i64, oneshot>) + notification routing
│   │                    (DashMap<method, mpsc::Sender>). Reusable.
│   ├── protocol.rs      ACP message types (subset we use). Liberal serde
│   │                    defaults so spec additions don't break parsing.
│   └── session.rs       initialize → client/initialized → session/new
│                        (or session/load) handshake. Translates
│                        session/update + session/request_permission
│                        notifications into Event::*.
│
├── agent-cli/           Per-prompt subprocess fallback. Used by
│                        --agent copilot (gh copilot explain) and
│                        --agent shell (any binary). Attachments exposed
│                        via AAB_ATTACHMENTS env to the child.
│
├── agent-http/          ESCAPE HATCH (not in default features). OpenAI-
│                        compatible /v1/chat/completions with SSE via
│                        eventsource-stream. 429/5xx retry with
│                        Retry-After. History maintained in
│                        Arc<Mutex<Vec<Value>>>. Only use when you really
│                        cannot drive a CLI.
│
├── platform-line/       LINE Messaging API.
│   ├── lib.rs           Reply-first outbound: tries Reply API (free,
│   │                    token valid ~25 s), falls back to Push API
│   │                    (counted) on expiry. Optional MediaPublisher
│   │                    injection for image/audio (LINE requires public
│   │                    HTTPS URLs — bridge cannot upload binaries).
│   ├── sign.rs          HMAC-SHA256 verification with subtle constant-
│   │                    time comparison. Has unit tests.
│   └── webhook.rs       axum router; allowlist gate; post-restart
│                        timestamp filter; captures replyToken into
│                        ReplyCtx.extra; downloads image/file/audio/
│                        video via api-data.line.me to tempfile.
│
├── platform-slack/      Slack Socket Mode (no public IP needed).
│   ├── lib.rs           apps.connections.open → tokio_tungstenite WS.
│   │                    Auto-reconnect with exponential backoff (1→60s).
│   │                    chat.postMessage thread-aware; envelope ack
│   │                    within 3 s. Mention-only in channels: resolves
│   │                    bot user ID via auth.test, ignores messages
│   │                    without @mention, strips mention tag before
│   │                    forwarding. DMs always respond.
│   ├── envelope.rs      Envelope + EventsApiPayload + MessageEvent +
│   │                    SlackFile parsing. is_skippable() for
│   │                    bot/edit/join filtering.
│   └── upload.rs        files.uploadV2 three-step uploader.
│
├── platform-stdio/      Local terminal frontend. Read stdin lines,
│                        print replies. For dev / smoke test / CI.
│
├── platform-telegram/   Telegram Bot API. Long-poll via getUpdates
│                        (no webhook/tunnel needed). sendMessage,
│                        sendDocument, sendPhoto, sendAudio. Typing
│                        via sendChatAction (free, 5 s TTL).
│
├── platform-discord/    Discord Gateway WebSocket + REST API.
│                        Identifies with GUILD_MESSAGES + DIRECT_MESSAGES
│                        + MESSAGE_CONTENT intents. Mention-only in guilds,
│                        always responds in DMs. Auto heartbeat/reconnect.
│
├── admin-api/           Lightweight axum server for observability:
│                        GET /healthz, /api/metrics, /api/sessions.
│                        Reads Engine::stats() on demand.
│
├── media-publisher/     MediaPublisher trait + LocalHttpPublisher (in-
│                        process axum file server keyed by UUID). For
│                        platforms that need a public URL (LINE outbound).
│                        R2/S3 impls are intentionally NOT included.
│
├── daemon/              fd-lock single-instance + tracing-appender daily
│   ├── lib.rs           rotation. LockGuard Box::leak's the RwLock so the
│   │                    fd-lock guard's lifetime can be 'static.
│   └── service.rs       SystemdUser ServiceManager — writes
│                        ~/.config/systemd/user/aab.service and shells out
│                        to `systemctl --user`. Linux only.
│
├── cli/                 The `aab` binary.
│   ├── main.rs          clap subcommands: run / session list|reset /
│   │                    daemon status|install|start|stop|logs-path.
│   │                    build_agent / build_platform map names → impls.
│   │                    Cargo features gate every agent and platform.
│   └── config.rs        figment chain: defaults → TOML → env (AAB_*
│                        prefix, __ section separator).
│
└── test-support/        EchoAgent + SlowAgent + StreamingAgent +
                         MockPlatform fixtures used by core-engine
                         integration tests.
```

## 3. Crate dependency invariants

- **`core-traits` is a leaf.** Nothing internal. If you need a type used by
  both an agent and a platform, it goes here.
- **`core-engine` depends on `core-traits` + `core-commands`.** It uses
  `framing` for NDJSON, which other crates (`agent-claude-code`,
  `agent-acp`) reuse. Don't duplicate framing logic.
- **Per-agent and per-platform crates are siblings.** They never depend on
  each other. They depend on `core-traits` and may depend on `core-engine`
  for `framing`.
- **`cli` is the only crate that wires everything.** Cargo features there
  gate which agents/platforms are compiled in. `default = ["line", "slack",
  "claude-code", "stdio", "cli-agent", "media-local", "acp"]`. `http-agent`
  is OFF by default (escape-hatch ethos).

## 4. Key invariants & contracts

| Where | What | Why |
|---|---|---|
| `core-traits::Agent::set_override` | Trait method with default `Ok(())` | Lets builtin slash commands (`/model` etc) reconfigure agents without polluting `start_session` signature. Only `agent-claude-code` actually implements it. |
| `SessionActor` (`core-engine/session.rs`) | Buffers `partial:true` text chunks; flushes every 1.2 s OR 240 bytes; force-flushes on `partial:false` / `Done` / `PermissionRequest` / `AssistantAttachment` / `Error`. **Batch mode** (default `batch_replies=true`): never flushes partials — waits for non-partial/Done and sends a single message per turn. Fires `Platform::show_typing()` every 15 s while processing. | LINE / Slack rate-limit + UX. Without throttle, streaming agents flood the chat. Batch mode prevents fragmented replies. Typing indicator uses free platform APIs (LINE Loading Animation / Slack typing) to avoid consuming message quota. |
| `SessionActor` reply_ctx | `Cmd::Send` carries a fresh `ReplyCtx` per message; the actor updates its reply target on each user turn. | Slack thread-aware replies: bot replies in the thread where the user asked, not the thread where the session was first created. |
| `SlackPlatform` mention filter | In channels (C/G prefix), only responds when `<@BOT_USER_ID>` appears in the message text; strips the mention tag before forwarding. DMs (D prefix) always respond. Bot user ID resolved once via `auth.test` on startup. | Prevents the bot from responding to every message in busy channels. |
| `SessionRegistry` | Stores **metadata only** in SQLite (WAL mode). Live agent processes are NEVER serialised. | Process state isn't portable. Next inbound message after restart spawns fresh agent with `--resume <id>` (Claude) or `session/load` (ACP). |
| `state.db` | SQLite database in `db_data/` subdirectory (under `client_config_base_dir` or `state_dir`). | O(1) per-row writes vs O(N) full-file serialize; WAL mode for concurrent reads. |
| `agent-claude-code::session::spawn` | Mints `--session-id <uuid>` for fresh sessions, `--resume <id>` for resumes. | We must know the session id immediately (for the registry) without waiting for the first system event. |
| `Attachment` for Claude inbound | Always base64-encoded inline in the NDJSON stream. | The `"type":"file"` source format is not supported by the Anthropic API; base64 is the universal format. |
| `LinePlatform::reply` | Reply API first (`/v2/bot/message/reply`, free — no quota), Push API fallback (`/v2/bot/message/push`, counted) if reply token expired (>25 s) or already consumed. | Reply tokens are valid ~30 s from webhook receipt. In batch mode most responses finish within the window and cost zero quota. |
| `LinePlatform::send_attachment` | Requires injected `MediaPublisher`. | LINE Messaging API only accepts public HTTPS URLs, not binary upload. |
| `SlackPlatform::run_once` | Always ack the envelope within 3 s of receipt. | Slack disconnects sockets that don't ack. |
| `daemon::LockGuard` | `Box::leak`s the inner `RwLock`. | `fd_lock`'s write-guard borrows from the lock; leaking lets us hold for process lifetime with `'static`. |
| `claude --dangerously-skip-permissions` | Default on (`ClaudeCodeConfig::skip_permissions = true`). | Bridge has no human at the keyboard. Operators wanting interactive permission flow set it false and route via `/yes <id>` / `/no <id>` chat replies. |
| `ClaudeCodeConfig::client_config_base_dir` | When set, each `SessionKey` gets its own subdirectory used as `cwd`. | Per-client isolation: Claude reads project-level `CLAUDE.md`, `.claude/settings.json`, `.mcp.json` from each client's dir, giving isolated memory / skills / MCP while sharing the host's auth. `cwd` is overridden when this is set. |

## 5. How to run / verify locally

```bash
# Build
cargo build --release -p cli

# Local smoke test (no bot setup needed):
./target/release/aab run --agent claude --platform stdio

# Full LINE flow with one command:
cp .env.example .env  # fill LINE_CHANNEL_SECRET / LINE_CHANNEL_TOKEN
chmod +x start-bridge.sh
./start-bridge.sh

# Linting
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI runs all three on every PR (Linux only). Don't merge red.

## 6. How to extend

### Add a new agent backend

1. `crates/agent-<name>/` — implement `core_traits::Agent` and
   `core_traits::AgentSession`. For stream-json-style, model after
   `agent-claude-code`. For request/response RPC, model after
   `agent-acp`. For one-shot subprocess, model after `agent-cli`.
2. Reuse `core_engine::framing::{spawn_ndjson_reader, write_ndjson}` for
   NDJSON or `agent_acp::jsonrpc::JsonRpcClient` for JSON-RPC.
3. Add to workspace `members` in root `Cargo.toml`.
4. Add a feature flag in `crates/cli/Cargo.toml`.
5. Add a match arm in `build_agent` in `crates/cli/src/main.rs`.
6. Add the choice to the `AgentChoice` enum (clap `ValueEnum`).
7. (Optional) Implement `Agent::set_override` for slash-command tunables.
8. Add config fields to `cli/src/config.rs::AgentSection` if needed.
9. Tests: golden-fixture round-trip for any wire format.

### Add a new platform frontend

1. `crates/platform-<name>/` — implement `core_traits::Platform`.
2. Inbound: parse → build `Message { key: SessionKey::new("plat", scoped),
   text, attachments, reply_ctx, timestamp_ms }` → call
   `MessageHandler::handle`.
3. Outbound: `reply()` for text, `send_attachment()` for media. Honour
   thread/channel info from `ReplyCtx`.
4. Same workspace + feature + cli wiring as for agents.
5. Tests: signature verification (if applicable), stale-event filtering,
   payload parsing.

### Add a new builtin slash command

1. `crates/core-engine/src/lib.rs::handle_builtin` — add a match arm.
2. `builtin_commands()` — add a `b("name", "description")` entry so
   `/help` lists it.
3. If it tunes an agent: call `self.apply_override(key, name, value)`,
   then `self.reset_session(key)` so the new spawn picks up the change.
4. Add an integration test in
   `crates/core-engine/tests/engine_round_trip.rs`.

## 7. Testing strategy

- **Unit tests** live `#[cfg(test)] mod tests` per source file. Total
  ~80 across the workspace.
- **Integration tests** live in `crates/<name>/tests/`. Notable:
  `core-engine/tests/engine_round_trip.rs` exercises the full
  Engine → SessionActor → MockPlatform → reply path with `EchoAgent`.
- **Mocking**: `crates/test-support` provides `EchoAgent` and
  `MockPlatform`. `mockall` is in workspace deps but currently unused by
  default — auto-mocks of `Agent`/`Platform` are easy to add when needed.
- **Golden fixtures**: `crates/agent-claude-code/tests/fixtures/` is set
  up; populate with real captured stream-json sessions when expanding
  parser coverage.

## 8. Common pitfalls

- **Cargo.toml workspace deps**: pinned versions are managed centrally
  in root `[workspace.dependencies]`. Per-crate `Cargo.toml` references
  them with `dep.workspace = true`. Don't add a different version in a
  child crate — it breaks the dedup story.
- **`async_trait` everywhere**: trait methods are `async`, so Rust can't
  use `&'static dyn Trait`. We use `Arc<dyn Trait>` for sharing.
- **`Agent::id()` returns `String`, not `&str`**: Claude's session id
  rotates; `&str` would force unsafe leaks. Don't try to "optimise" it
  back to `&str`.
- **`Engine::dispatch` swallows errors via `error!` log**: this is
  intentional — a single broken message must not take down the bridge.
  But tests that assert "error path" must check log output (via
  `tracing-test`) rather than `Result`.
- **LINE webhook signature uses raw body bytes** before any framework
  parsing. Don't pre-parse JSON before HMAC verification — that breaks
  the signature.
- **Slack Socket Mode envelopes must be acked within 3 s**. Don't `await`
  expensive work between `read.next()` and the `write.send(ack)` in
  `run_once` — spawn the dispatch into a `tokio::spawn` if needed.
- **`fd-lock` holds a real OS file lock**. Two `aab` processes against
  the same `state_dir` will visibly conflict; that's by design.
  Tests must use `tempfile::tempdir()` to avoid colliding.
- **`figment` env separator is `__` (double underscore).** Single
  underscore stays inside one section. So `AAB_BRIDGE__DEFAULT_AGENT` not
  `AAB_BRIDGE_DEFAULT_AGENT`.
- **`cwd` is overridden when `client_config_base_dir` is set.** The
  per-client isolation feature derives a per-`SessionKey` subdirectory
  and sets it as `cwd`, so any explicit `cwd` value in config is
  silently ignored.
- **Example template**: `examples/client-template/` ships an office-assistant
  template with skills for Excel/Word generation, CSV analysis, translation,
  and web research (via MCP fetch server). Point `client_template_dir` at it
  for a ready-to-use setup.

## 9. What lives outside the codebase

- **Secrets**: in `.env` (gitignored) or env vars. `config.toml` only
  names which env var to read — never values.
- **Persistent state**: `<state_dir>/db_data/state.db` (SQLite + WAL files).
  When `client_config_base_dir` is set, the database lives under that path
  instead.
  Also: `<state_dir>/daemon.lock`, `<state_dir>/logs/aab.log.YYYY-MM-DD`.
- **Service unit**: `~/.config/systemd/user/aab.service` (managed by
  `aab daemon install/uninstall`).
- **Cloudflare tunnel**: external process. `start-bridge.sh` spawns it;
  for production use a named tunnel (`cloudflared tunnel create`) with
  a stable hostname so the LINE webhook URL doesn't rotate.

## 10. Known follow-ups

(See `docs/architecture.md` "Known follow-ups" for the canonical list.)

- R2 / S3 `MediaPublisher` impl
- LINE imageSet (multi-shot) grouping
- ACP `set_session_mode` / `fs/*` server hooks (spec still in flux)
- `AgentSession::set_model` for hot model switch without `/reset`
- Telegram webhook mode (alternative to long-poll for high-traffic bots)
- Discord voice channel integration

## 11. Where to look when something breaks

| Symptom | First file to read |
|---|---|
| Chat message ignored | `crates/core-engine/src/lib.rs::dispatch_inner` |
| Slash command does nothing | `core-engine/src/lib.rs::handle_builtin`; `core-commands/src/lib.rs::parse_command_line` |
| Streaming chunks arrive in one big lump | `core-engine/src/session.rs::StreamState` (flush thresholds) |
| Claude exits unexpectedly | check `agent-claude-code` spawn args; `AAB_LOG=trace` to see stdin/stdout |
| LINE 401 on push | token wrong, or signature failed — check `platform-line/src/sign.rs` |
| LINE webhook silent | (a) `Use webhook` toggle off (b) `Auto-reply` interferes (c) tunnel URL changed |
| Slack bot loops | `MessageEvent::is_skippable` not catching the bot id — print `subtype` |
| Slack bot responds to every message | `resolve_bot_user_id` failed — check `auth.test` response and `SLACK_APP_TOKEN` / `SLACK_BOT_TOKEN` |
| Slack replies in wrong thread | Check that `Cmd::Send` carries fresh `reply_ctx` — see `session.rs` |
| LINE 429 monthly limit | Push message quota exhausted; `show_typing()` uses free Loading Animation API and should not contribute. Wait for monthly reset or upgrade plan. |
| `another instance holds the daemon lock` | leftover `aab` process or stale lock; `pkill aab` then retry |

## 12. Style

- Prefer `anyhow::Result` at trait boundaries; `thiserror` enums inside
  crates when callers need to discriminate.
- `tracing` everywhere. Targets are crate names by default — use
  `AAB_LOG=info,platform_line=debug` to drill in.
- Avoid `unwrap()` / `expect()` outside tests except for documented
  invariants ("called once" guarded by `Option::take`).
- Format with `cargo fmt --all`; clippy must be clean with `-D warnings`.
- One concept per commit; PR titles describe behaviour change, not file
  list.
