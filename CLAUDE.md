# CLAUDE.md

Claude Code: read [`AGENT.md`](AGENT.md) for the canonical maintainer's
guide. The two files are intentionally identical in content — `CLAUDE.md`
is the convention Claude Code looks for, `AGENT.md` is the convention used
by other coding agents.

## Quick orientation

- **Project**: `ai-agent-bridge` — Rust workspace bridging chat platforms
  (LINE, Slack, stdio) to AI agent **CLIs** (Claude Code, gh copilot, ACP).
  CLI-first: prompts forwarded verbatim to the agent's stdin; no API key
  on the core path.
- **Read order before changing code**:
  1. [`AGENT.md`](AGENT.md) — invariants, contracts, extension guide
  2. [`docs/architecture.md`](docs/architecture.md) — design details
  3. [`README.md`](README.md) — user-facing usage
- **Verify before claiming done**:
  ```bash
  cargo fmt --all
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  ```
- **Linux-only** target. Codebase compiles on macOS/Windows but daemon
  helpers and `start-bridge.sh` assume systemd-user.
- **`claude` is invoked with `--dangerously-skip-permissions` by default**
  because the bridge runs unattended. Don't quietly remove this flag.

See [`AGENT.md`](AGENT.md) for everything else.
