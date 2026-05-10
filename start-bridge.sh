#!/usr/bin/env bash
# start-bridge.sh — one-shot launcher for ai-agent-bridge on Linux.
#
# Usage:
#   ./start-bridge.sh                       # line + claude (default)
#   ./start-bridge.sh --platform stdio      # local terminal smoke test
#   ./start-bridge.sh --platform slack      # Slack Socket Mode (no tunnel)
#   ./start-bridge.sh --agent copilot       # any agent supported by `aab`
#   ./start-bridge.sh --no-tunnel           # skip cloudflared (you have your own ingress)
#   ./start-bridge.sh --release|--debug     # build profile (default: release)
#   ./start-bridge.sh --skip-build          # don't run cargo before launching
#
# Env file:
#   Reads ./.env if present. Recommended keys:
#     LINE_CHANNEL_SECRET=...
#     LINE_CHANNEL_TOKEN=...
#     SLACK_APP_TOKEN=xapp-...
#     SLACK_BOT_TOKEN=xoxb-...
#     AAB_LOG=info,platform_line=debug
#
# Behaviour:
#   - Spawns cloudflared in the background (for --platform line) and prints
#     the public webhook URL — you must paste it into the LINE Developers
#     Console webhook field once per tunnel restart (quick tunnels rotate).
#   - On Ctrl+C, kills cloudflared, then aab.
#
# Requires: cargo, cloudflared (only when --platform line and tunnel enabled).

set -euo pipefail

PLATFORM="line"
AGENT="claude"
PROFILE="release"
USE_TUNNEL=1
SKIP_BUILD=0
WEBHOOK_PORT="${AAB_WEBHOOK_PORT:-8080}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --platform) PLATFORM="$2"; shift 2 ;;
    --agent)    AGENT="$2";    shift 2 ;;
    --release)  PROFILE="release"; shift ;;
    --debug)    PROFILE="debug"; shift ;;
    --no-tunnel) USE_TUNNEL=0; shift ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --port) WEBHOOK_PORT="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

# ── Load .env ────────────────────────────────────────────────────────────
if [[ -f .env ]]; then
  echo "→ loading .env"
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

# ── Build ─────────────────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "→ building aab ($PROFILE)"
  if [[ "$PROFILE" == "release" ]]; then
    cargo build --release -p cli
    AAB_BIN="target/release/aab"
  else
    cargo build -p cli
    AAB_BIN="target/debug/aab"
  fi
else
  AAB_BIN="target/$PROFILE/aab"
fi
[[ -x "$AAB_BIN" ]] || { echo "aab binary not found at $AAB_BIN" >&2; exit 1; }

# ── Pre-flight checks ─────────────────────────────────────────────────────
if [[ "$AGENT" == "claude" ]] && ! command -v claude >/dev/null 2>&1; then
  echo "✗ 'claude' CLI not found in PATH. Install with:"
  echo "    npm i -g @anthropic-ai/claude-code"
  echo "  Then run \`claude\` once interactively to log in."
  exit 1
fi

case "$PLATFORM" in
  line)
    : "${LINE_CHANNEL_SECRET:?LINE_CHANNEL_SECRET not set (put it in .env)}"
    : "${LINE_CHANNEL_TOKEN:?LINE_CHANNEL_TOKEN not set (put it in .env)}"
    ;;
  slack)
    : "${SLACK_APP_TOKEN:?SLACK_APP_TOKEN not set (put it in .env)}"
    : "${SLACK_BOT_TOKEN:?SLACK_BOT_TOKEN not set (put it in .env)}"
    ;;
  stdio) ;;
  *) echo "unknown platform: $PLATFORM" >&2; exit 2 ;;
esac

# ── Cleanup ───────────────────────────────────────────────────────────────
PIDS=()
cleanup() {
  echo
  echo "→ shutting down…"
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

# ── Tunnel helper ─────────────────────────────────────────────────────────
# start_tunnel <port> → sets REPLY to the public URL, appends PID to PIDS.
start_tunnel() {
  local port="$1"
  local log
  log="$(mktemp)"
  echo "→ starting cloudflared tunnel → http://localhost:$port"
  if command -v cloudflared >/dev/null 2>&1; then
    cloudflared tunnel --url "http://localhost:$port" \
      --no-autoupdate >"$log" 2>&1 &
  elif command -v docker >/dev/null 2>&1; then
    echo "  (using Docker: cloudflare/cloudflared)"
    docker run --rm --network host \
      cloudflare/cloudflared:latest tunnel --url "http://localhost:$port" \
      --no-autoupdate >"$log" 2>&1 &
  else
    echo "✗ Neither cloudflared nor docker found. Install one of:"
    echo "    https://github.com/cloudflare/cloudflared#installing-cloudflared"
    echo "    https://docs.docker.com/engine/install/"
    echo "  Or rerun with --no-tunnel if you already have public ingress."
    exit 1
  fi
  local pid=$!
  PIDS+=("$pid")

  # Wait up to 30 s for the public URL.
  REPLY=""
  for _ in {1..60}; do
    local url
    url=$(grep -Eo 'https://[a-z0-9-]+\.trycloudflare\.com' "$log" \
            | head -n1 || true)
    if [[ -n "$url" ]]; then
      REPLY="$url"
      return 0
    fi
    sleep 0.5
  done
  echo "✗ failed to detect cloudflared URL within 30 s. Tunnel log:"
  sed 's/^/  /' "$log" | tail -n 30
  exit 1
}

# ── Tunnels (line only) ──────────────────────────────────────────────────
if [[ "$PLATFORM" == "line" && "$USE_TUNNEL" -eq 1 ]]; then
  MEDIA_PORT="${AAB_MEDIA_PORT:-8081}"

  start_tunnel "$WEBHOOK_PORT"
  TUNNEL_URL="$REPLY"

  start_tunnel "$MEDIA_PORT"
  MEDIA_TUNNEL_URL="$REPLY"

  export AAB_PLATFORMS__LINE__MEDIA__KIND="local-http"
  export AAB_PLATFORMS__LINE__MEDIA__BIND="0.0.0.0:${MEDIA_PORT}"
  export AAB_PLATFORMS__LINE__MEDIA__PUBLIC_BASE_URL="$MEDIA_TUNNEL_URL"

  echo
  echo "════════════════════════════════════════════════════════════════════"
  echo "  LINE webhook URL (paste into Developers Console → Messaging API):"
  echo "    $TUNNEL_URL/webhook"
  echo
  echo "  Media download URL (files served here):"
  echo "    $MEDIA_TUNNEL_URL"
  echo "════════════════════════════════════════════════════════════════════"
  echo
fi

# ── System prompt for Claude agent ────────────────────────────────────────
if [[ "$AGENT" == "claude" && -z "${AAB_AGENTS__CLAUDE__APPEND_SYSTEM_PROMPT:-}" ]]; then
  PROMPT_FILE="$REPO_ROOT/prompts/claude-system.md"
  if [[ -f "$PROMPT_FILE" ]]; then
    export AAB_AGENTS__CLAUDE__APPEND_SYSTEM_PROMPT
    AAB_AGENTS__CLAUDE__APPEND_SYSTEM_PROMPT="$(cat "$PROMPT_FILE")"
  fi
fi

# ── aab ───────────────────────────────────────────────────────────────────
echo "→ launching aab run --agent $AGENT --platform $PLATFORM"
"$AAB_BIN" run --agent "$AGENT" --platform "$PLATFORM" &
AAB_PID=$!
PIDS+=("$AAB_PID")

wait "$AAB_PID"
