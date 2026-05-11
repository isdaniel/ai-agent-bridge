# Architecture

## Design philosophy: drive the CLI, don't replace it

The bridge's job is to be a **thin transport** between a chat platform and an
AI agent CLI that's already installed and authenticated on the host machine.
Concretely:

- The user's message text is forwarded **verbatim** into the agent's stdin.
- Whatever the agent emits on stdout is forwarded back to the chat thread.
- Authentication / billing / tool execution all happen inside the CLI as if a
  human were typing — the bridge has no API key of its own for the core path.

This is the opposite of an SDK-style integration where the bridge would build
chat-completion requests, manage history, choose models, etc. By staying
out of that loop, we get for free:

- Whatever auth the CLI uses (Claude Pro/Max subscription, Copilot
  subscription, `gh auth login` token, ...).
- Real tool use — `claude` actually runs `Bash`, `Edit`, `Read` against the
  user's working tree.
- Zero need to track model rev / pricing / context-window changes — the CLI
  handles those.

`agent-http` exists as an escape hatch for OpenAI-compatible APIs but is
**not** the primary mode and is not enabled in `default` features.

`--dangerously-skip-permissions` is on by default for `claude` because the
chat user can't react fast enough to per-tool prompts. See README "Security
note" for sandboxing recommendations.

## Component diagram

```
                ┌──────────────────────────────────────────────┐
inbound msg →   │  Platform trait                              │  LINE / Slack / stdio
                │   Platform::start(MessageHandler)            │
                │   Platform::reply / send_attachment          │
                └────────────────────┬─────────────────────────┘
                                     │ MessageHandler::handle(Message)
                                     ▼
                ┌──────────────────────────────────────────────┐
                │  Engine                                      │  core-engine
                │   - HashMap<name, Arc<dyn Agent>>            │
                │   - DashMap<SessionKey, SessionHandle>       │
                │   - SessionRegistry (persisted JSON)         │
                │   - CommandRegistry (slash builtins)         │
                │                                              │
                │   dispatch_inner(msg):                       │
                │     1. drop stale events                     │
                │     2. parse `/slash` → builtin or template  │
                │     3. else send_to_session()                │
                └────────────────────┬─────────────────────────┘
                                     │ SessionHandle.tx → Cmd
                                     ▼
                ┌──────────────────────────────────────────────┐
                │  SessionActor (one tokio task per SessionKey)│  core-engine::session
                │   loop {                                     │
                │     select! {                                │
                │       cmd = inbox.recv() => Send/Permission/Close
                │       evt = session.events().recv() =>       │
                │         buffer partial chunks, flush every   │
                │         1.2 s OR 240 bytes,                  │
                │         platform.reply / send_attachment     │
                │       _ = sleep_until(flush_deadline) => flush
                │     }                                        │
                │   }                                          │
                └────────────────────┬─────────────────────────┘
                                     │ AgentSession::send / events / answer_permission
                                     ▼
   ┌─────────────────┬─────────────────┬─────────────────┬────────────────────┐
   │ agent-claude-   │ agent-acp       │ agent-cli       │ agent-http         │
   │ code            │                 │                 │ (escape hatch)     │
   │ stream-json     │ JSON-RPC stdio  │ per-prompt      │ OpenAI /v1 SSE,    │
   │ NDJSON in&out   │ + ACP protocol  │ subprocess,     │ requires API key — │
   │ +--dangerously- │ enums + sessions│ AAB_ATTACHMENTS │ NOT the primary    │
   │  skip-perms     │                 │ env to child    │ path               │
   └─────────────────┴─────────────────┴─────────────────┴────────────────────┘
```

## Crate dependency graph

```
                  core-traits  ←─────────────────── (every other crate)
                       ▲
                       │
       core-commands ──┘
                       ▲
                       │
                  core-engine ── used by every agent / platform crate that
                       ▲           wants framing or registry helpers
                       │
            ┌──────────┼──────────┬─────────────┬──────────────┐
            │          │          │             │              │
   agent-claude-code  agent-acp  agent-cli  agent-http   platform-{line,slack,stdio}
                                                           │
                                              media-publisher (only platform-line uses it)
                                                           │
                                                          daemon ── cli (binary `aab`)
```

`core-traits` is deliberately a leaf crate so all backends compile in parallel
and there's nowhere to accidentally introduce a cycle.

## Key invariants

- **`core-traits` is leaf-only.** Agent and platform crates compile in parallel.
- **One actor per live session.** Concurrency is structural; no `Mutex` + busy
  flag is needed inside session state. The actor owns the `AgentSession` and
  serialises both inbound `Cmd`s and outbound `Event`s.
- **Streaming throttle in the actor.** `Event::AssistantText { partial: true }`
  chunks are accumulated in a `String` buffer and flushed every 1.2 s OR
  240 bytes, whichever fires first. Non-partial frames + `Event::Done` always
  force-flush. **Batch mode** (default, `batch_replies=true`): partial text is
  never flushed mid-stream — the actor waits for the complete non-partial text
  and sends it as a single message per turn. While processing in batch mode,
  `Platform::show_typing()` fires every 15 s to show the user the bot is still
  working (LINE Loading Animation API / Slack typing indicator — free, no
  message quota consumed). Keeps LINE / Slack rate-limits happy without losing
  the "streaming feel".
- **Reply context updates per message.** `Cmd::Send` carries a fresh `ReplyCtx`
  per user turn. The session actor updates its reply target on each new message,
  so Slack replies always land in the thread where the user asked — not the
  thread where the session was first created.
- **Persistence stores metadata only.** `state.json` records
  `SessionKey → {agent, active_session_id, past_agent_session_ids}`. Live
  agent processes are never serialised — on the next inbound message after a
  restart the engine spawns a fresh agent and passes `--resume <id>` (Claude
  Code) or `session/load` (ACP).
- **Slash commands** live in `core-commands` and are normalized
  (`-` ≡ `_`, case-insensitive). Built-ins win over user/agent-defined names.
- **Bidirectional NDJSON** is the shared transport for stream-json (Claude
  Code) and JSON-RPC (ACP). Both go through
  `core_engine::framing::{spawn_ndjson_reader, write_ndjson}` with an 8 MiB
  per-line cap so inline base64 images don't blow up the parser.

## Core types (in `crates/core-traits/src/lib.rs`)

```rust
pub struct SessionKey(pub String);          // "line:U123" | "slack:C1/U2" | "stdio:local"
pub enum AttachmentKind { Image, File, Audio }
pub struct Attachment   { kind, path, mime, bytes, name }
pub struct Message      { key, text, attachments, reply_ctx, timestamp_ms }
pub struct ReplyCtx     { channel, thread, user, extra }
pub struct PermissionRequest { id, tool_name, description, input }

#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    AssistantText { text, partial },
    AssistantAttachment(Attachment),
    PermissionRequest(PermissionRequest),
    ToolStart { name, id },
    ToolEnd   { id, ok },
    Error(String),
    Done { session_id },
}

#[async_trait] pub trait Agent        { fn name(); async fn start_session(); }
#[async_trait] pub trait AgentSession { fn id(); async fn send(); fn events(); async fn answer_permission(); async fn close(); }
#[async_trait] pub trait Platform     { fn name(); async fn start(); async fn reply(); async fn send_attachment(); async fn show_typing(); }
#[async_trait] pub trait MessageHandler { async fn handle(Message); }
```

## SessionActor lifecycle

`crates/core-engine/src/session.rs` — one tokio task per `(SessionKey, agent)`.

```text
spawn ─► info!("session actor up")
         │
         ▼
         loop {
           select! {
             Cmd::Send{reply_ctx} → update reply_ctx; state.discard_buffer();
                                    session.send(prompt, atts)
             Cmd::Permission      → session.answer_permission(id, allow)
             Cmd::Close | None    → state.flush(); break
             Event from agent     →
                AssistantText{partial:true}  → state.append; flush if ≥240B
                                               (streaming mode only)
                AssistantText{partial:false} → flush, then send full text
                AssistantAttachment          → flush, then platform.send_attachment
                PermissionRequest            → flush, then formatted prompt to platform
                Error                        → flush, then "⚠️ msg"
                Done{session_id}             → flush, registry.record_session, persist
                ToolStart/End                → log only
             sleep_until(deadline)           →
                batch mode + processing: platform.show_typing() every 15s
                streaming mode: flush partial buffer
           }
         }
         session.close().await
         info!("session actor down")
```

The buffer logic lives entirely in `StreamState` so platform code never has to
think about chunk coalescing.

## Session persistence

`crates/core-engine/src/registry.rs`. Schema (`schema_version: 1`):

```json
{
  "schema_version": 1,
  "entries": {
    "line:U123": {
      "agent": "claude",
      "active_session_id": "0a2f-...-uuid",
      "past_agent_session_ids": [["copilot", "older-uuid"]]
    }
  }
}
```

Writes are atomic: `tempfile::NamedTempFile::new_in(parent)` → `.persist(path)`
on a `spawn_blocking` thread. Schema mismatch on load renames the existing
file to `.json.bak` and starts fresh, so one bad upgrade doesn't lose the
ability to boot.

`/agent <name>` calls `Engine::switch_agent`:
1. Drop the live actor (`Cmd::Close`).
2. `registry.clear_active(key)` — pushes the active session id into
   `past_agent_session_ids`.
3. `registry.set_agent(key, new_name)`.
4. `persist().await`.

The next inbound message hits `get_or_spawn_session`, which reads the new
`agent` and (if it had any) the matching `last_session_id` to resume.

## NDJSON framing (shared)

`crates/core-engine/src/framing.rs`:

```rust
pub fn spawn_ndjson_reader<R, T: DeserializeOwned>(
    reader: R, max_line: usize, tx: mpsc::Sender<T>,
) -> JoinHandle<()>;
pub async fn write_ndjson<W, T: Serialize>(
    writer: &mut W, value: &T,
) -> Result<()>;
```

Used by both `agent-claude-code` (stream-json) and `agent-acp` (JSON-RPC).
`DEFAULT_MAX_LINE = 8 MiB` so inline base64 image payloads don't trip the
default `LinesCodec` cap.

## Per-agent details

### `agent-claude-code` (`crates/agent-claude-code/src/session.rs`)

```text
spawn  claude --input-format stream-json --output-format stream-json -p --verbose
       --dangerously-skip-permissions          ← on by default; toggle via
                                                 ClaudeCodeConfig.skip_permissions
       --include-partial-messages              ← on by default; powers incremental
                                                 streaming back to chat
       [--model <model>] [--fallback-model <model>]
       [--effort <low|medium|high|xhigh|max>]
       [--append-system-prompt <prompt>]
       [--max-budget-usd <amount>]
       [--add-dir <dir>...]
       [--mcp-config <file>...]
       [--allowedTools <tool>...] [--disallowedTools <tool>...]
       --session-id <uuid>                     ← we mint this so we know the id
                                                 immediately without waiting
                                                 for the first system event;
                                                 omitted in favour of --resume
                                                 when continuing a session
       [--resume <id>]
       [user-supplied extra args]

       The agent runs as the same user as `aab` and inherits its env, so
       whatever auth `claude login` configured locally (Console, Pro/Max
       subscription, etc.) is what gets used. The bridge holds no API key.

dynamic reconfiguration:
       /model /dir /effort /budget /tools /system slash commands call
       Agent::set_override(key, name, value), which mutates the shared
       Arc<RwLock<ClaudeCodeConfig>>. The next start_session() snapshots the
       config; the previous session is closed via Engine::reset_session.

stdout NDJSON lines → StreamEvent enum (#[serde(tag="type")])
                       System | Assistant | User | Result | ControlRequest |
                       StreamEvent (partial chunks)
                                                              │
                                              translates to:  ▼
                                       Event::AssistantText / ToolStart / Done /
                                       PermissionRequest (with oneshot stored in
                                       DashMap<request_id, oneshot::Sender<bool>>)

       PartialEvent::ContentBlockDelta { delta: TextDelta { text } }
         → Event::AssistantText { text, partial: true }   (buffered+flushed by
                                                            SessionActor)

       PermissionRequest is only emitted when skip_permissions = false.
       Otherwise `claude` resolves all tool prompts internally.

stdin  user turn → {"type":"user","message":{"role":"user","content":[
                       {"type":"text","text": ...},
                       {"type":"image","source":{"type":"base64","media_type":"...","data":"..."}}
                   ]}}

       Images are always base64-encoded inline regardless of size.

close  send {"type":"control_request","request":{"subtype":"interrupt"}}
       wait 120 s for child.wait(); else child.start_kill()
```

#### Per-client config isolation

When `ClaudeCodeConfig::client_config_base_dir` is set, `start_session`
derives a per-`SessionKey` subdirectory and overrides `cwd` before spawning:

```text
SessionKey("line:U1234")
  → dirname "line__U1234"
  → cwd = {client_config_base_dir}/line__U1234/

If the directory doesn't exist:
  1. Copy client_template_dir/ recursively (if set)
  2. Otherwise create an empty .claude/ subdirectory

The spawned claude sees:
  CLAUDE.md             ← per-client instructions / static memory
  .claude/settings.json ← per-client skills & project settings
  .mcp.json             ← per-client MCP servers
  auto-memory           ← stored under ~/.claude/projects/<hash>/memory/
                          (hash derived from cwd, so per-client)
  auth                  ← shared from ~/.claude/ (unchanged)
```

Note: when `client_config_base_dir` is set, any explicit `cwd` value
in the config is silently overridden.

### `agent-cli` (`crates/agent-cli/src/lib.rs`)

Per-prompt `tokio::process::Command` spawn — stdin closed, stdout read to EOF,
emit one `AssistantText{partial:false}` + `Done`. Used by:

- `--agent copilot` → spawns `gh copilot explain <prompt>`. Inherits the
  `gh auth login` token; no API key needed.
- `--agent shell` → spawns whatever `[agents.shell].binary` says. Useful for
  wrapping `aichat`, `mods`, custom scripts.

Attachments are exposed to the child via `AAB_ATTACHMENTS` (newline-joined
paths) + `AAB_ATTACHMENT_COUNT` env vars, so wrapper scripts can pick them up.
If `cfg.supports_resume`, prepends `--resume <last_id>` (no use for `gh
copilot`; reserved for future CLIs).

### `agent-acp` (`crates/agent-acp/src/{lib,jsonrpc,protocol,session}.rs`)

```text
spawn cfg.binary cfg.args
└─► JsonRpcClient { next_id: AtomicI64, pending: DashMap<i64, oneshot>,
                    notify_routes: DashMap<method, mpsc::Sender<Value>> }

handshake:
  → request("initialize", { protocolVersion: 1, clientCapabilities: { fs, clientInfo } })
  ← InitializeResult
  → notify("client/initialized", {})
  → request("session/load" if resume else "session/new", { cwd, mcpServers: [] })
  ← { sessionId }

streaming:
  register_route("session/update", update_tx)
  register_route("session/request_permission", perm_tx)
  background task drains channels, filters by sessionId, emits Event::*

prompt:
  → request("session/prompt", { sessionId, prompt: [PromptBlock::Text|Image] })

permission:
  → notify("session/respond_to_permission_request", { sessionId, requestId, outcome })

close:
  → notify("session/cancel", { sessionId })
  → child.wait timeout 120 s; else start_kill
```

### `agent-http` (escape hatch — not the primary path)

```text
POST {base_url}/chat/completions
  body: { model, messages: history, stream: true }
  Authorization: Bearer {api_key}

response: text/event-stream
  reqwest::Response::bytes_stream().eventsource()
  ├─ data: {"choices":[{"delta":{"content":"..."}}]} → AssistantText{partial:true}
  └─ data: [DONE] → break loop, push assistant turn into history,
                   send AssistantText{partial:false} (full accumulated text),
                   then Done

retry: 429 / 5xx → exponential backoff (500ms→30s), respects Retry-After,
       up to 3 attempts before bubbling Event::Error
```

**This path is not enabled by default** (`http-agent` is no longer in the
`default` Cargo features list) and requires the user to provide an API key.
Most operators should prefer one of the CLI-backed agents above.

## Per-platform details

### `platform-line` (`crates/platform-line/src/{lib,sign,webhook}.rs`)

```text
inbound:
  axum POST /webhook
    └─ verify HMAC-SHA256(secret, body) == X-Line-Signature (constant-time)
    └─ for each event in payload:
         - drop if timestamp < boot_ms (post-restart filter)
         - drop if allowlist non-empty and userId ∉ allowlist
         - if message.type ∈ { image, file, audio, video } and contentProvider == "line":
             GET https://api-data.line.me/v2/bot/message/{id}/content
                 Authorization: Bearer {channel_token}
             write to tempfile → Attachment
         - dispatch Message to handler

outbound text:
  POST https://api.line.me/v2/bot/message/push
    body: { to: user, messages: [{type:"text", text}] }
  Always Push API — reply tokens expire ~1 min, too short for AI latency.

outbound typing indicator:
  POST https://api.line.me/v2/bot/chat/loading
    body: { chatId: user, loadingSeconds: 20 }
  Free — does not consume monthly push message quota.

outbound attachment:
  via injected MediaPublisher → public HTTPS URL
  then push { type:"image", originalContentUrl: url, previewImageUrl: url }
  (audio: { type:"audio", duration }; file: text fallback with link)
```

### `platform-slack` (`crates/platform-slack/src/{lib,envelope,upload}.rs`)

```text
startup:
  POST /api/auth.test → { user_id: BOT_USER_ID }
  (cached in OnceCell; used for mention-only filtering)

connect:
  POST /api/apps.connections.open  Authorization: Bearer {app_token}
  → { url: wss-primary.slack.com/... }
  tokio_tungstenite::connect_async(url)

per envelope:
  ack { envelope_id }
  if type == "events_api" and event.type == "message":
    skip if bot_id is set OR subtype not in {None, "file_share"}
    mention filter:
      - DMs (channel starts with 'D'): always respond
      - channels (C/G): only if text contains <@BOT_USER_ID>;
        strip mention tag before forwarding
    download files via url_private_download (Authorization: Bearer {bot_token})
    SessionKey = "slack:{channel}/{user}"
    ReplyCtx { channel, thread: thread_ts.or(ts), user }

reconnect: exponential backoff 1s → 60s

outbound:
  text:        POST /api/chat.postMessage  { channel, text, thread_ts? }
  attachment:  files.uploadV2 three-step
               1. files.getUploadURLExternal → { upload_url, file_id }
               2. POST upload_url with raw bytes
               3. files.completeUploadExternal { files:[{id,title}], channel_id }
```

### `platform-stdio`

Reads lines from stdin, dispatches each as a `Message` with
`SessionKey::new("stdio", "local")`, and prints `reply` / `send_attachment`
output to stdout. For development / CI without bot tokens.

## Daemon

`crates/daemon/src/{lib,service}.rs`:

| Concern | Implementation |
|---|---|
| Single-instance lock | `fd_lock::RwLock` over `<state_dir>/daemon.lock`. The guard internally `Box::leak`s the `RwLock` so the `'static` write-guard lifetime works. |
| Logs | `tracing-appender::rolling::daily` writing to `<state_dir>/logs/aab.log.YYYY-MM-DD`. |
| Service install | `ServiceManager` trait with one impl: `SystemdUser` — writes `~/.config/systemd/user/aab.service` and shells out to `systemctl --user daemon-reload / enable / start / stop / is-active`. |

CLI subcommands: `aab daemon status | install | uninstall | start | stop | logs-path`.

## Configuration loader

`crates/cli/src/config.rs` — `figment::Figment` chain:

```text
Defaults (from `Default for AppConfig`)
  → Toml::file(<--config> or <state_dir>/config.toml)
  → Env::prefixed("AAB_").split("__")
  → AppConfig (typed)
```

Secrets are referenced indirectly:
`channel_secret_env = "LINE_CHANNEL_SECRET"` → CLI does `std::env::var(name)`
at agent/platform construction time, so the TOML never carries the secret.

## Test surface

| Crate | Highlights |
|---|---|
| `core-traits`     | SessionKey serde + namespacing; Event tagged-enum round-trip |
| `core-engine`     | NDJSON framer (round-trip + bad lines); registry persistence + agent-switch history; **integration:** echo round-trip, /reset, /help lists builtins, /agent switches & validates, unknown-slash hint |
| `core-commands`   | Normalize (case + dash), template `{{1}} {{2*}} {{args}}`, builtin collision rejection, quoted-arg parser |
| `agent-claude-code` | StreamEvent variants, ContentBlock fallthrough, image inline vs path threshold |
| `agent-acp`       | JSON-RPC request/response duplex + notification routing; SessionUpdate/ToolCall/Other parsing |
| `platform-line`   | HMAC sign+verify (good/tampered/bad-b64); webhook payload parsing for text and image events; ext-from-mime |
| `platform-slack`  | Envelope parser; bot/edit message skipping; file_share detection |
| `media-publisher` | LocalHttpPublisher publish-then-fetch round-trip |
| `daemon`          | Second `LockGuard::acquire` of same path returns Err |

Run all: `cargo test --workspace`. Run with logs: `AAB_LOG=debug cargo test ...`.

## Phased delivery (status)

| Phase | Scope | Status |
|---|---|---|
| P1 | core-traits + core-engine + agent-claude-code + StdioPlatform | ✅ done |
| P2 | platform-line: webhook, signature, push API, **inbound attachments**, **outbound via MediaPublisher** | ✅ done |
| P3 | platform-slack Socket Mode + file_share + `files.uploadV2` | ✅ done |
| P4 | core-commands + built-in slash + `/agent` switch | ✅ done |
| P5 | SessionRegistry persistence + daemon lock + log rotation + service install/start/stop | ✅ done |
| P6 | agent-acp (full handshake), agent-http (SSE streaming), Copilot via GitHub Models | ✅ done |
| P7 | Per-client config isolation: per-SessionKey CWD with own CLAUDE.md, .claude/settings.json, .mcp.json | ✅ done |

## Known follow-ups (not in scope of P1–P6)

- **R2 / S3 `MediaPublisher`** — trait is in place; only `LocalHttpPublisher`
  ships. Bring your own crate with `aws-sdk-s3` for cloud hosting.
- **LINE imageSet grouping** — currently each photo of a multi-shot upload
  is dispatched as its own `Message`; could be coalesced.
- **ACP `set_session_mode` / `fs/read_text_file` server hooks** — spec is
  still in flux; we pin `protocolVersion: 1` and accept "unknown" updates
  silently.
- **Granular `/model` switching** — currently `/model` only acks; user must
  `/reset` to apply. A future revision would add `AgentSession::set_model`.
