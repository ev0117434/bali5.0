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

_COMPONENT = "launcher"

def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}

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
            log.info(_evt("redis_ready", url=config.REDIS_URL, attempt=attempt))
            return
        except Exception as exc:
            log.warning(_evt("redis_unavailable", url=config.REDIS_URL,
                             attempt=attempt, error_msg=str(exc)))
            time.sleep(2)
    log.error(_evt("redis_not_ready", url=config.REDIS_URL, attempts=10))
    sys.exit(1)


def flush_redis():
    """Flush all keys from Redis DB."""
    import redis as sync_redis
    r = sync_redis.Redis.from_url(config.REDIS_URL)
    count = r.dbsize()
    r.flushdb()
    r.close()
    log.info(_evt("redis_flushed", keys_removed=count))


def check_subscribe_files():
    exchanges = ["binance", "bybit", "okx", "gate", "bitget"]
    for exch in exchanges:
        for market in ["spot", "futures"]:
            path = Path(f"{config.SUBSCRIBE_DIR}/{exch}/{exch}_{market}.txt")
            if not path.exists():
                log.warning(_evt("subscribe_file_missing", path=str(path)))
            else:
                count = sum(1 for l in path.read_text().splitlines() if l.strip())
                log.info(_evt("subscribe_file_ok", path=str(path), count=count))


def ensure_redis():
    """Start Redis via setup script if not already running."""
    script = Path(__file__).parent / "scripts" / "redis_setup.sh"
    log.info(_evt("redis_setup_start", script=str(script)))
    result = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    if result.stdout:
        log.info(_evt("redis_setup_stdout", output=result.stdout.strip()))
    if result.stderr:
        log.debug(_evt("redis_setup_stderr", output=result.stderr.strip()))
    if result.returncode != 0:
        log.error(_evt("redis_setup_failed", script=str(script), stderr=result.stderr))
        sys.exit(1)


def start_process(name: str, script: str) -> subprocess.Popen:
    p = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log.info(_evt("process_started", name=name, pid=p.pid, script=script))
    return p


def handle_sigterm(signum, frame):
    log.info(_evt("shutdown", signal=str(signum)))
    for name, p in _procs.items():
        try:
            p.terminate()
            log.info(_evt("process_terminated", name=name, pid=p.pid))
        except Exception as exc:
            log.warning(_evt("terminate_failed", name=name, error_msg=str(exc)))
    # Give processes time to clean up
    time.sleep(3)
    for name, p in _procs.items():
        if p.poll() is None:
            p.kill()
            log.info(_evt("process_killed", name=name, pid=p.pid))
    log.info(_evt("shutdown_complete"))
    sys.exit(0)


# ── Main ───────────────────────────────────────────────────────────────────

def ask_spread_delay() -> int:
    """Prompt user for spread_monitor start delay in seconds. 0 = start with others."""
    print("\nSpread monitor start delay (seconds, 0 = with other monitors): ", end="", flush=True)
    try:
        val = input().strip()
        delay = int(val) if val else 0
        if delay < 0:
            delay = 0
    except (ValueError, EOFError):
        delay = 0
    return delay


def main():
    log.info(_evt("launcher_start",
                  history_enabled=config.HISTORY_ENABLED,
                  processes=list(PROCESSES.keys())))

    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT,  handle_sigterm)

    # Interactive prompt before startup
    spread_delay = ask_spread_delay()
    if spread_delay:
        log.info(_evt("spread_monitor_delay", seconds=spread_delay))

    # Pre-flight checks
    ensure_redis()
    check_redis()
    flush_redis()
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

    log.info(_evt("warmup_wait", seconds=COLLECTOR_WARMUP))
    time.sleep(COLLECTOR_WARMUP)

    monitors_without_spread = {
        name: script for name, script in monitors.items()
        if name != "spread_monitor"
    }
    for name, script in monitors_without_spread.items():
        _procs[name] = start_process(name, script)
        time.sleep(0.5)

    if "spread_monitor" in monitors:
        if spread_delay > 0:
            log.info(_evt("spread_monitor_waiting", seconds=spread_delay))
            time.sleep(spread_delay)
        _procs["spread_monitor"] = start_process("spread_monitor", monitors["spread_monitor"])

    log.info(_evt("all_started", health_check_interval_s=30))

    # Health check loop
    restart_counts: dict[str, int] = {name: 0 for name in PROCESSES}

    while True:
        time.sleep(HEALTH_CHECK_INTERVAL)

        for name, p in list(_procs.items()):
            if p.poll() is not None:   # process exited
                restart_counts[name] = restart_counts.get(name, 0) + 1
                log.error(_evt("process_died", name=name, pid=p.pid,
                               exit_code=p.returncode, restart_attempt=restart_counts[name]))
                script = PROCESSES[name]
                new_p  = start_process(name, script)
                _procs[name] = new_p
                log.info(_evt("process_restarted", name=name, pid=new_p.pid,
                              restart_attempt=restart_counts[name]))


if __name__ == "__main__":
    main()
