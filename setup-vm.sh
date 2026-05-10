#!/usr/bin/env bash
# setup-vm.sh — One-shot environment setup for ai-agent-bridge on a clean Ubuntu/Debian VM.
#
# Usage:
#   curl -sSf <url>/setup-vm.sh | bash        # or
#   chmod +x setup-vm.sh && ./setup-vm.sh
#
# What it installs:
#   1. System packages (build-essential, pkg-config, git, …)
#   2. Rust toolchain via rustup (stable, with clippy + rustfmt)
#   3. Cargo tools (cargo-audit, cargo-deny)
#   4. Node.js 22 LTS via NodeSource (for Claude Code CLI)
#   5. Claude Code CLI (npm -g)
#   6. cloudflared (for LINE webhook tunneling)
#
# Assumes: Ubuntu 22.04+ / Debian 12+ on amd64 or arm64.
# Run as a regular user — sudo is invoked where needed.

set -euo pipefail

# ── Helpers ──────────────────────────────────────────────────────────────
info()  { printf '\033[1;34m→ %s\033[0m\n' "$*"; }
ok()    { printf '\033[1;32m✓ %s\033[0m\n' "$*"; }
warn()  { printf '\033[1;33m⚠ %s\033[0m\n' "$*"; }
fail()  { printf '\033[1;31m✗ %s\033[0m\n' "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1
}

ARCH=$(uname -m)
case "$ARCH" in
  x86_64)  DEB_ARCH="amd64" ;;
  aarch64) DEB_ARCH="arm64" ;;
  *)       fail "Unsupported architecture: $ARCH" ;;
esac

# ── 1. System packages ──────────────────────────────────────────────────
info "Installing system packages"
sudo apt-get update -qq
sudo apt-get install -y -qq \
  build-essential \
  pkg-config \
  git \
  curl \
  wget \
  unzip \
  jq \
  ca-certificates \
  gnupg \
  lsb-release
ok "System packages installed"

# ── 2. Rust toolchain ────────────────────────────────────────────────────
if need_cmd rustup; then
  info "Rust already installed — updating"
  rustup update stable
else
  info "Installing Rust via rustup"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  # shellcheck disable=SC1091
  source "$HOME/.cargo/env"
fi

rustup component add clippy rustfmt
ok "Rust $(rustc --version | awk '{print $2}') ready (clippy + rustfmt)"

# Ensure cargo is in PATH for the rest of this script
export PATH="$HOME/.cargo/bin:$PATH"

# ── 3. Cargo tools ──────────────────────────────────────────────────────
info "Installing cargo tools"

if ! need_cmd cargo-audit; then
  cargo install cargo-audit --locked
fi
ok "cargo-audit $(cargo audit --version 2>/dev/null | head -1)"

if ! need_cmd cargo-deny; then
  cargo install cargo-deny --locked
fi
ok "cargo-deny $(cargo deny --version 2>/dev/null | head -1)"

# ── 4. Node.js 22 LTS ───────────────────────────────────────────────────
if need_cmd node; then
  NODE_VER=$(node --version)
  NODE_MAJOR=${NODE_VER%%.*}
  NODE_MAJOR=${NODE_MAJOR#v}
  if [[ "$NODE_MAJOR" -ge 20 ]]; then
    info "Node.js $NODE_VER already installed — skipping"
  else
    warn "Node.js $NODE_VER is too old (need ≥20), upgrading"
    INSTALL_NODE=1
  fi
else
  INSTALL_NODE=1
fi

if [[ "${INSTALL_NODE:-0}" -eq 1 ]]; then
  info "Installing Node.js 22 LTS via NodeSource"
  sudo mkdir -p /etc/apt/keyrings
  curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | sudo gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg --yes
  echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_22.x nodistro main" \
    | sudo tee /etc/apt/sources.list.d/nodesource.list >/dev/null
  sudo apt-get update -qq
  sudo apt-get install -y -qq nodejs
fi
ok "Node.js $(node --version), npm $(npm --version)"

# ── 5. Claude Code CLI ──────────────────────────────────────────────────
if need_cmd claude; then
  info "Claude Code CLI already installed — upgrading"
  npm update -g @anthropic-ai/claude-code || npm install -g @anthropic-ai/claude-code
else
  info "Installing Claude Code CLI"
  npm install -g @anthropic-ai/claude-code
fi
ok "Claude Code CLI installed ($(claude --version 2>/dev/null || echo 'run claude to login'))"

if need_cmd copilot-proxy; then
  info "copilot-proxy already installed — upgrading"
  npm update -g @jer-y/copilot-proxy || npm install -g @jer-y/copilot-proxy
else
  info "Installing copilot-proxy"
  npm install -g @jer-y/copilot-proxy
fi
ok "copilot-proxy installed"

# ── 6. cloudflared ───────────────────────────────────────────────────────
if need_cmd cloudflared; then
  info "cloudflared already installed"
else
  info "Installing cloudflared"
  CLOUDFLARED_URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${DEB_ARCH}.deb"
  TMP_DEB=$(mktemp --suffix=.deb)
  curl -fsSL -o "$TMP_DEB" "$CLOUDFLARED_URL"
  sudo dpkg -i "$TMP_DEB"
  rm -f "$TMP_DEB"
fi
ok "cloudflared $(cloudflared --version 2>/dev/null | head -1)"

# ── 7. Verify project build ─────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/Cargo.toml" ]]; then
  info "Running verification build from $SCRIPT_DIR"
  cd "$SCRIPT_DIR"
  cargo check --workspace
  ok "Workspace compiles"
else
  info "Not inside the project directory — skipping build check"
fi

# ── Done ─────────────────────────────────────────────────────────────────
echo
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Environment ready!                                            ║"
echo "║                                                                ║"
echo "║  Next steps:                                                   ║"
echo "║    1. cd ai-agent-bridge                                       ║"
echo "║    2. cp .env.example .env   # fill in your secrets            ║"
echo "║    3. claude                 # login interactively once        ║"
echo "║    4. ./start-bridge.sh      # launch the bridge               ║"
echo "║                                                                ║"
echo "║  Verify toolchain:                                             ║"
echo "║    cargo fmt --all                                             ║"
echo "║    cargo clippy --workspace --all-targets -- -D warnings       ║"
echo "║    cargo test --workspace                                      ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
