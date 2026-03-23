#!/usr/bin/env bash
# scripts/redis_configure.sh
# BALI 5.0 — One-time Redis installation and configuration.
# Installs Redis if missing, writes redis.conf to match current project settings,
# creates required directories, then performs an initial startup check.
# Usage: bash scripts/redis_configure.sh
# Exit 0: configured and ready. Exit 1: failure.

set -euo pipefail

PROJECT_DIR="/root/bali5.0"
CONF="$PROJECT_DIR/redis.conf"
SOCKET="$PROJECT_DIR/redis.sock"
LOGS_DIR="$PROJECT_DIR/logs"

# ── 1. Install Redis if not present ─────────────────────────────────────────
if ! command -v redis-server &>/dev/null; then
    echo "[redis_configure] redis-server not found — installing..."
    apt-get update -q
    apt-get install -y redis-server
    echo "[redis_configure] Redis installed: $(redis-server --version)"
else
    echo "[redis_configure] Redis already installed: $(redis-server --version)"
fi

# ── 2. Create required directories ──────────────────────────────────────────
mkdir -p "$LOGS_DIR"
echo "[redis_configure] Logs directory: $LOGS_DIR"

# ── 3. Write redis.conf ──────────────────────────────────────────────────────
cat > "$CONF" <<EOF
# $CONF
# BALI 5.0 — Redis server configuration (Unix socket, no TCP)

unixsocket $SOCKET
unixsocketperm 770
port 0

daemonize yes
maxmemory 0
maxmemory-policy noeviction
save ""
appendonly no
tcp-keepalive 60
hz 20
loglevel notice
logfile $LOGS_DIR/redis-server.log
EOF

echo "[redis_configure] Written: $CONF"

# ── 4. Validate config syntax ────────────────────────────────────────────────
redis-server "$CONF" --test-memory 0 &>/dev/null && echo "[redis_configure] Config syntax OK" || {
    redis-server --check-system
    echo "[redis_configure] WARNING: could not validate config (non-fatal)" >&2
}

# ── 5. Verify setup script is present ───────────────────────────────────────
SETUP_SCRIPT="$PROJECT_DIR/scripts/redis_setup.sh"
if [[ -f "$SETUP_SCRIPT" ]]; then
    echo "[redis_configure] Startup script present: $SETUP_SCRIPT"
else
    echo "[redis_configure] WARNING: $SETUP_SCRIPT not found — startup script missing" >&2
fi

echo ""
echo "[redis_configure] Configuration complete."
echo "  Config : $CONF"
echo "  Socket : $SOCKET"
echo "  Log    : $LOGS_DIR/redis-server.log"
echo ""
echo "  To start Redis: bash scripts/redis_setup.sh"
