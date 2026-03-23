#!/usr/bin/env bash
# scripts/redis_setup.sh
# BALI 5.0 — Idempotent Redis startup via Unix socket.
# Usage: bash scripts/redis_setup.sh
# Exit 0: Redis is ready. Exit 1: startup failed.

set -euo pipefail

SOCKET="/root/bali5.0/redis.sock"
CONF="/root/bali5.0/redis.conf"
LOGS_DIR="/root/bali5.0/logs"

# 1. Ensure log directory exists before Redis tries to open its log file
mkdir -p "$LOGS_DIR"

# 2. Already running?
if redis-cli -s "$SOCKET" ping 2>/dev/null | grep -q PONG; then
    echo "[redis_setup] Redis already running on $SOCKET"
    exit 0
fi

# 3. Remove stale socket file if it exists (left by a crashed Redis process)
rm -f "$SOCKET"

# 4. Start Redis with our config (daemonize yes is set in redis.conf)
echo "[redis_setup] Starting Redis: redis-server $CONF"
redis-server "$CONF"

# 4. Wait up to 10 seconds for the socket to respond
for i in $(seq 1 10); do
    sleep 1
    if redis-cli -s "$SOCKET" ping 2>/dev/null | grep -q PONG; then
        echo "[redis_setup] Redis ready on $SOCKET (attempt $i)"
        exit 0
    fi
done

echo "[redis_setup] ERROR: Redis did not start after 10 seconds" >&2
exit 1
