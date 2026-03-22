#!/usr/bin/env python3
# launcher.py
"""
BALI 5.0 — Process launcher and health monitor.

Starts all collectors and monitors as subprocesses.
Health check every 30 seconds — restarts dead processes.
Handles SIGTERM gracefully.

Usage:
    python launcher.py          # trade branch (no snapshot_monitor)
    python launcher.py --data   # data branch (includes snapshot_monitor)

Or let config.HISTORY_ENABLED drive the decision automatically.
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import config
from logger_setup import setup_logger

log = setup_logger("launcher")

# ── Process definitions ─────────────────────────────────────────────────────

PROCESSES: dict[str, str] = {
    "collector_binance": "collectors/collector_binance.py",
    "collector_bybit":   "collectors/collector_bybit.py",
    "collector_okx":     "collectors/collector_okx.py",
    "collector_gate":    "collectors/collector_gate.py",
    "collector_bitget":  "collectors/collector_bitget.py",
    "redis_monitor":     "monitors/redis_monitor.py",
    "stale_monitor":     "monitors/stale_monitor.py",
    "spread_monitor":    "monitors/spread_monitor.py",
}

# snapshot_monitor only in data branch
if config.HISTORY_ENABLED:
    PROCESSES["snapshot_monitor"] = "monitors/snapshot_monitor.py"

HEALTH_CHECK_INTERVAL = 30  # seconds
COLLECTOR_WARMUP      = 10  # seconds to wait before starting monitors

# ── Global process registry (for SIGTERM handler) ──────────────────────────
_procs: dict[str, subprocess.Popen] = {}


# ── Helpers ────────────────────────────────────────────────────────────────

def check_redis():
    """Block until Redis is reachable or give up after 10 retries."""
    import redis as sync_redis
    for attempt in range(1, 11):
        try:
            r = sync_redis.Redis.from_url(config.REDIS_URL)
            r.ping()
            r.close()
            log.info(f"Redis OK at {config.REDIS_URL}")
            return
        except Exception as exc:
            log.warning(f"Redis not ready (attempt {attempt}/10): {exc}")
            time.sleep(2)
    log.error("Redis unreachable after 10 attempts — aborting")
    sys.exit(1)


def check_subscribe_files():
    exchanges = ["binance", "bybit", "okx", "gate", "bitget"]
    for exch in exchanges:
        for market in ["spot", "futures"]:
            path = Path(f"{config.SUBSCRIBE_DIR}/{exch}/{exch}_{market}.txt")
            if not path.exists():
                log.warning(f"Subscribe file missing: {path}")
            else:
                count = sum(1 for l in path.read_text().splitlines() if l.strip())
                log.info(f"Subscribe file OK: {path} ({count} symbols)")


def start_process(name: str, script: str) -> subprocess.Popen:
    log.info(f"Starting {name} ({script})...")
    p = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log.info(f"Started {name} PID={p.pid}")
    return p


def handle_sigterm(signum, frame):
    log.info(f"Signal {signum} received — shutting down all processes...")
    for name, p in _procs.items():
        try:
            p.terminate()
            log.info(f"Terminated {name} PID={p.pid}")
        except Exception as exc:
            log.warning(f"Failed to terminate {name}: {exc}")
    # Give processes time to clean up
    time.sleep(3)
    for name, p in _procs.items():
        if p.poll() is None:
            p.kill()
            log.info(f"Killed {name} PID={p.pid}")
    log.info("Shutdown complete")
    sys.exit(0)


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    log.info(
        f"BALI 5.0 launcher starting | "
        f"HISTORY_ENABLED={config.HISTORY_ENABLED} "
        f"processes={list(PROCESSES.keys())}"
    )

    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT,  handle_sigterm)

    # Pre-flight checks
    check_redis()
    os.makedirs(config.LOGS_DIR,    exist_ok=True)
    os.makedirs(config.SIGNAL_DIR,  exist_ok=True)
    os.makedirs(config.SNAPSHOT_DIR, exist_ok=True)
    check_subscribe_files()

    # Start collectors first
    collectors = {
        name: script for name, script in PROCESSES.items()
        if name.startswith("collector_")
    }
    monitors = {
        name: script for name, script in PROCESSES.items()
        if not name.startswith("collector_")
    }

    for name, script in collectors.items():
        _procs[name] = start_process(name, script)
        time.sleep(0.5)

    log.info(f"Waiting {COLLECTOR_WARMUP}s for collectors to populate Redis...")
    time.sleep(COLLECTOR_WARMUP)

    for name, script in monitors.items():
        _procs[name] = start_process(name, script)
        time.sleep(0.5)

    log.info("All processes started. Health check every 30s.")

    # Health check loop
    restart_counts: dict[str, int] = {name: 0 for name in PROCESSES}

    while True:
        time.sleep(HEALTH_CHECK_INTERVAL)

        for name, p in list(_procs.items()):
            if p.poll() is not None:   # process exited
                restart_counts[name] = restart_counts.get(name, 0) + 1
                log.error(
                    f"Process {name} (PID={p.pid}) died "
                    f"(exit={p.returncode}) — "
                    f"restarting (attempt #{restart_counts[name]})..."
                )
                script = PROCESSES[name]
                new_p  = start_process(name, script)
                _procs[name] = new_p


if __name__ == "__main__":
    main()
