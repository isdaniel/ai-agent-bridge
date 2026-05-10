# ai-agent-bridge (`aab`)

> **What this is**: a Rust bridge that lets you operate the AI coding agents you already have logged into your terminal — Claude Code, GitHub Copilot CLI, Zed/Gemini ACP servers — from your phone via LINE or Slack.
>
> **What this is not**: an API client. The bridge does **not** call Anthropic / OpenAI / GitHub Models APIs as its primary path. Your prompt is forwarded verbatim into the agent CLI you've already authenticated locally; the CLI's reply is forwarded back to chat. No extra API key required for the core flow — the agent CLIs already handle billing / auth.

```
   ┌─────────┐                     ┌──────────────┐                          ┌────────────────────┐
   │ LINE    │ ──── webhook ─────► │  aab daemon  │ ──── stdin (NDJSON) ───► │  claude.exe        │
   │ Slack   │ ◄─── push reply ─── │  on your     │ ◄─── stdout (NDJSON) ─── │  (already logged   │
   │ phone   │                     │  machine     │                          │   in via `claude`) │
   └─────────┘                     └──────────────┘                          └────────────────────┘
```

## Why CLI, not API

| | CLI mode (default) | HTTP API mode (escape hatch) |
|---|---|---|
| Auth | Whatever the CLI already uses (Claude Pro/Max, Copilot subscription, Anthropic Console login) | Requires an API key the user must obtain separately |
| Billing | Goes through the CLI's existing plan | Pay-per-token directly |
| Tool use | Real Claude Code with all tools (Bash, Edit, etc.) | Just chat completion |
| Filesystem | Operates on your real working tree | API can't touch local files |
| Setup | `claude login` once, done | Need to provision and rotate API keys |

The bridge passes `--dangerously-skip-permissions` to `claude` by default because the whole point is unattended operation from chat — there's no human at the keyboard to click "approve". You can flip this off in config and route permission prompts through chat as `/yes <id>` / `/no <id>` instead.

## Features

| Capability | Notes |
|---|---|
| **`--agent claude`** — drives `claude` in stream-json mode with `--dangerously-skip-permissions` | The flagship path |
| **`--agent copilot`** — drives `gh copilot explain` (subprocess per prompt) | Uses your `gh auth login` session |
| **`--agent acp`** — drives any ACP-spec server (Zed, Gemini, etc.) over JSON-RPC stdio | `[agents.acp].binary = "..."` |
| **`--agent shell`** — generic CLI runner for `aichat`, `mods`, custom scripts | `[agents.shell].binary = "..."` |
| **`--agent http` / `openai`** — escape hatch for OpenAI-compatible APIs | Off by default; needs API key |
| **`--platform line`** — LINE Webhook + Push API | Inbound text + image/file/audio download |
| **`--platform slack`** — Slack Socket Mode | Outbound thread-aware reply + `files.uploadV2` |
| **`--platform stdio`** — local terminal | Dev / smoke test without bot setup |
| Per-user / per-channel **session persistence** | Restart-safe via `--resume <id>` |
| **Slash commands** | `/help /reset /new /agent /agents /yes /no /model /dir` + user templates |
| **Live agent switch** via `/agent <name>` | Closes current session, archives id, next message spawns new agent |
| **Streaming throttle** | Partial chunks coalesced and flushed every 1.2 s / 240 bytes (LINE / Slack rate-limit friendly) |
| **Background daemon** | `fd-lock` single-instance + daily-rotating logs |
| **OS-native service install** | `aab daemon install` → systemd-user unit at `~/.config/systemd/user/aab.service` |

## Prerequisites

You need the agent CLI **already installed and logged in** on the machine that runs `aab`.

| Agent | Install + login |
|---|---|
| `claude` | `npm i -g @anthropic-ai/claude-code` then `claude` once interactively to log in |
| `gh copilot` | `winget install GitHub.cli` then `gh auth login` and `gh extension install github/gh-copilot` |
| ACP server | Whatever spec-compliant binary you want to wrap; pin its path in config |

Plus, on the bridge machine itself: Rust ≥ 1.95 + `build-essential` (Debian/Ubuntu) or equivalent.

> **Supported OS**: Linux only. The codebase compiles on macOS / Windows but
> the daemon helpers (`aab daemon install/start/stop`) and the
> `start-bridge.sh` launcher target systemd-user. Use WSL2 if you're on
> Windows.

## Quick start

The fastest path is the bundled `start-bridge.sh` launcher — it builds, opens
a Cloudflare tunnel for the LINE webhook, prints the URL you need to paste
into the LINE Developers Console, and starts `aab`. Ctrl+C tears everything
down cleanly.

```bash
# 0) one-time prep
cp .env.example .env       # then fill LINE_CHANNEL_SECRET / LINE_CHANNEL_TOKEN
chmod +x start-bridge.sh

# 1) launch (line + claude is the default)
./start-bridge.sh

# the script prints something like:
#   ════════════════════════════════════════════════════════════════════
#     LINE webhook URL (paste into Developers Console → Messaging API):
#       https://random-words-here.trycloudflare.com/webhook
#   ════════════════════════════════════════════════════════════════════
# → paste it into the LINE channel's Webhook URL field, click Verify,
#   enable "Use webhook" and disable "Auto-reply messages".

# Other modes:
./start-bridge.sh --platform stdio                  # local terminal smoke test
./start-bridge.sh --platform slack                  # Slack Socket Mode (no tunnel)
./start-bridge.sh --agent copilot                   # use gh copilot instead
./start-bridge.sh --no-tunnel                       # skip cloudflared (your own ingress)
./start-bridge.sh --skip-build --debug              # use existing target/debug/aab
./start-bridge.sh --port 8443                       # bind webhook on a different port
```

If you'd rather wire it up by hand:

```bash
# Build once
cargo build --release -p cli            # produces target/release/aab

# 1) Smoke test: stdio + Claude
./target/release/aab run --agent claude --platform stdio

# 2) LINE bot — start the tunnel separately
cloudflared tunnel --url http://localhost:8080      # in another terminal
LINE_CHANNEL_SECRET=xxx LINE_CHANNEL_TOKEN=yyy \
  ./target/release/aab run --agent claude --platform line

# 3) Slack Socket Mode (no public IP needed)
SLACK_APP_TOKEN=xapp-... SLACK_BOT_TOKEN=xoxb-... \
  ./target/release/aab run --agent claude --platform slack

# 4) Inspect / reset persistent sessions
./target/release/aab session list
./target/release/aab session reset slack:C123/U456

# 5) Daemon lifecycle (systemd-user)
./target/release/aab daemon status
./target/release/aab daemon install     # writes ~/.config/systemd/user/aab.service
./target/release/aab daemon start
./target/release/aab daemon logs-path
```

## Configuration

Loaded from `~/.ai-agent-bridge/config.toml` (override with `--config <path>` or `AAB_CONFIG`). Every key may be overridden by env var: prefix `AAB_`, separator `__`. Examples:
- `AAB_BRIDGE__DEFAULT_AGENT=copilot`
- `AAB_PLATFORMS__LINE__WEBHOOK_BIND=0.0.0.0:8443`
- `AAB_LOG=debug` (tracing filter)

```toml
[bridge]
default_agent = "claude"           # claude | copilot | shell | acp | http
default_platform = "stdio"         # stdio | line | slack
state_dir = "~/.ai-agent-bridge"

# ----- Agents (CLI-first) -----

[agents.claude]
binary = "claude"
extra_args = []                       # rare; most knobs have first-class fields below
permission_mode = "ask"               # ask | acceptEdits | bypassPermissions
skip_permissions = true               # passes --dangerously-skip-permissions; default true

# ── Streaming UX ──
include_partial_messages = true       # incremental chunks via --include-partial-messages

# ── Model / effort ──
model = "sonnet"                      # alias or full id; --model
fallback_model = "haiku"              # auto-fallback when overloaded
effort = "medium"                     # low|medium|high|xhigh|max

# ── Filesystem / context ──
cwd = "/home/me/sandbox"              # claude's working directory
add_dirs = ["/home/me/projects/foo"]  # extra allowed dirs (--add-dir)
append_system_prompt = "You are answering via LINE chat. Keep replies short. Use markdown but no tables."
mcp_config_files = ["/home/me/.config/aab/github-mcp.json"]

# ── Safety ──
max_budget_usd = 5.0                  # per-session USD cap
allowed_tools = ["Read", "Edit", "Bash(git *)", "Bash(npm *)"]
disallowed_tools = ["Bash(rm *)", "Bash(curl *)"]

[agents.copilot]
binary = "gh"
extra_args = ["copilot", "explain"]   # `explain` produces non-interactive output

[agents.shell]                     # generic subprocess runner
binary = "/usr/local/bin/aichat"
extra_args = []

[agents.acp]                       # required only when --agent acp is used
binary = "/path/to/acp-server"
extra_args = []

# ----- Platforms -----

[platforms.line]
channel_secret_env = "LINE_CHANNEL_SECRET"
channel_token_env  = "LINE_CHANNEL_TOKEN"
webhook_bind = "0.0.0.0:8080"
allowlist = ["U1234..."]                     # optional; empty = allow everyone

# Outbound media: LINE requires public HTTPS URLs.
[platforms.line.media]
kind = "local-http"
bind = "0.0.0.0:8081"
public_base_url = "https://media.example.com"

[platforms.slack]
app_token_env = "SLACK_APP_TOKEN"            # xapp-... (Socket Mode)
bot_token_env = "SLACK_BOT_TOKEN"            # xoxb-... (chat / files)

# ----- HTTP escape hatch (only if you really want to skip the CLI) -----
# Uncomment and supply OPENAI_API_KEY env to use --agent http
# [providers.openai]
# base_url = "https://api.openai.com/v1"
# model = "gpt-4o-mini"
```

Secrets always come from env vars (`LINE_CHANNEL_*`, `SLACK_*_TOKEN`, optionally `OPENAI_API_KEY`). The TOML only points at which env var to read.

## Slash commands

Built-ins (always available):

| Command | Effect |
|---|---|
| `/help` | List all registered commands |
| `/reset`, `/new` | End the current session; next message starts fresh |
| `/agent <name>` | Switch active agent for this user (e.g. `/agent copilot`). Archives prior `session_id` to history. |
| `/agents` | List registered agents |
| `/yes <id>`, `/no <id>` | Approve / deny a pending permission request (only relevant when `skip_permissions = false`) |
| `/model <name>` | Switch model and reset session: `/model sonnet`, `/model opus`, `/model haiku`, or full id |
| `/dir <path>` | Add a directory to the agent's allowed paths (`--add-dir`) and reset |
| `/effort <level>` | Set effort: `/effort low\|medium\|high\|xhigh\|max` |
| `/budget <usd>` | Hard USD ceiling (`--max-budget-usd`) and reset |
| `/tools allow <tool>` | Add to `--allowedTools`, e.g. `/tools allow Bash(git *)` |
| `/tools deny <tool>` | Add to `--disallowedTools`, e.g. `/tools deny Bash(rm *)` |
| `/tools clear` | Clear all tool allow/deny lists |
| `/system <text>` | Append a system prompt (`--append-system-prompt`) and reset |

The `/model`, `/dir`, `/effort`, `/budget`, `/tools`, `/system` commands rewrite the agent's spawn config and reset the active session — the new flag set takes effect on the next message.

User-defined templated commands can be added through the `core-commands::CommandRegistry` API; templates support `{{1}} {{2}} ...` positional, `{{2*}}` (rest from arg N), and `{{args}}` (everything joined by space). Built-in names always win over user names.

## Sample chat flow

```
user (LINE):  /help
bot:          Available commands:
                /agent  (builtin)  — switch backing agent: /agent <name>
                /reset  (builtin)  — end current session and start a fresh one ...
                ...
user (LINE):  list files in ~/projects/foo
bot:          [streamed Claude Code output, actually running ls / Bash tools]
user (LINE):  /agent copilot
bot:          switched agent → `copilot`
user (LINE):  rebase --onto
bot:          [output from `gh copilot explain "rebase --onto"`]
user (LINE):  /reset
bot:          session reset.
```

## Adding a new agent or platform

Implement the trait in `crates/core-traits/src/lib.rs`:

```rust
#[async_trait]
pub trait Agent: Send + Sync {
    fn name(&self) -> &'static str;
    async fn start_session(&self, key: SessionKey, resume: Option<String>) -> Result<Box<dyn AgentSession>>;
}

#[async_trait]
pub trait AgentSession: Send {
    fn id(&self) -> String;
    async fn send(&mut self, prompt: String, attachments: Vec<Attachment>) -> Result<()>;
    fn events(&mut self) -> mpsc::Receiver<Event>;
    async fn answer_permission(&mut self, id: String, allow: bool) -> Result<()>;
    async fn close(self: Box<Self>) -> Result<()>;
}

#[async_trait]
pub trait Platform: Send + Sync {
    fn name(&self) -> &'static str;
    async fn start(&self, handler: Arc<dyn MessageHandler>) -> Result<()>;
    async fn reply(&self, ctx: &ReplyCtx, text: &str) -> Result<()>;
    async fn send_attachment(&self, ctx: &ReplyCtx, att: &Attachment) -> Result<()>;
}
```

Steps:
1. Create `crates/agent-<name>/` (or `crates/platform-<name>/`) and add it to the workspace `members`.
2. Implement the trait. For a CLI-style agent, model after `agent-claude-code` (long-lived stream-json) or `agent-cli` (per-prompt subprocess). Reuse `core_engine::framing::{spawn_ndjson_reader, write_ndjson}` for stdio JSON, or `agent-acp::jsonrpc::JsonRpcClient` for JSON-RPC.
3. Add a Cargo feature in `crates/cli/Cargo.toml` and a match arm in `build_agent` / `build_platform` in `crates/cli/src/main.rs`.

## Workspace layout

| Crate | Purpose |
|---|---|
| `core-traits`       | `Agent`, `AgentSession`, `Platform`, `MessageHandler` traits + DTOs (leaf, no deps on the rest) |
| `core-engine`       | Session manager actor, registry persistence, partial-chunk flushing, NDJSON framing helpers, slash dispatcher |
| `core-commands`     | Slash command parser, normalized name registry, template expansion |
| `agent-claude-code` | Long-lived `claude --input-format stream-json` driver, `--dangerously-skip-permissions` by default |
| `agent-acp`         | JSON-RPC over stdio + ACP `initialize` / `session/*` handshake |
| `agent-cli`         | Per-prompt subprocess runner — used by `--agent copilot` (`gh copilot explain`) and `--agent shell` |
| `agent-http`        | Escape hatch: OpenAI-compatible HTTP/SSE client (off by default) |
| `platform-line`     | LINE Webhook (HMAC-verified) + Push API + content download |
| `platform-slack`    | Slack Socket Mode (WebSocket) + `chat.postMessage` + `files.uploadV2` |
| `platform-stdio`    | Local terminal frontend (dev / demo) |
| `media-publisher`   | `MediaPublisher` trait + in-process `LocalHttpPublisher` for hosting outbound files |
| `daemon`            | Single-instance fd-lock, rotating logs, systemd-user service install |
| `cli`               | `aab` binary; clap subcommands; reads config; wires platforms ↔ engine ↔ agents |
| `test-support`      | Shared `EchoAgent` / `MockPlatform` fixtures |

See [`docs/architecture.md`](docs/architecture.md) for design details (actor model, persistence, streaming, permission round-trip).

## License

Apache-2.0.
