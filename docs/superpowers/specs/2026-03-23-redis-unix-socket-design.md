# BALI 5.0 — Redis Unix Socket Migration Design

**Date:** 2026-03-23
**Status:** Approved
**Scope:** Replace Redis TCP (`localhost:6379`) with Unix socket (`/root/bali5.0/redis.sock`)

---

## Motivation

BALI 5.0 writes Redis in high-frequency batches (100 commands / 250ms) across 5 collectors and
reads continuously in 4 monitors. All processes run on the same host. Unix socket eliminates
TCP stack overhead, reduces per-pipeline latency ~30–50%, and removes port/firewall concerns.

---

## Decision

**Pure Unix socket — TCP disabled (`port 0`).**
No fallback TCP. All application code and tooling uses the socket exclusively.

Socket path: `/root/bali5.0/redis.sock`

---

## Changes

### 1. `config.py`

Remove `REDIS_HOST`, `REDIS_PORT` (audit confirmed: used only in `config.py` itself, no other
file imports them by name). Add `REDIS_SOCKET_PATH`. Update `REDIS_URL`.

```python
# Before
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB   = 0
REDIS_URL  = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# After
REDIS_SOCKET_PATH = "/root/bali5.0/redis.sock"
REDIS_DB          = 0
REDIS_URL         = f"unix://{REDIS_SOCKET_PATH}?db={REDIS_DB}"
```

No other application files change — all use `config.REDIS_URL` already.

### 2. `redis.conf` (new file, project root)

```conf
unixsocket /root/bali5.0/redis.sock
unixsocketperm 770          # owner + group access (supports multi-process same-user deployments)
port 0                      # TCP disabled
daemonize yes               # consistent with startup script
maxmemory 0                 # no cap — OS manages memory; noeviction prevents accidental eviction
maxmemory-policy noeviction
save ""
appendonly no
tcp-keepalive 60
hz 20
loglevel notice
logfile /root/bali5.0/logs/redis-server.log
```

Note on `maxmemory 0`: system docs estimate ~82 GB history. Setting an explicit cap risks
`OOM_POLICY=noeviction` hard-failing writes if the cap is wrong. `maxmemory 0` (unlimited)
is correct here since hardware resources are not constrained. The `noeviction` policy is kept
so there is no silent data loss if memory ever becomes unexpectedly limited.

### 3. `scripts/redis_setup.sh` (new file)

Idempotent startup script. Creates `logs/` before starting Redis (Redis writes its log there
on startup — if the directory is missing, the server exits immediately).

```bash
#!/usr/bin/env bash
set -euo pipefail

SOCKET="/root/bali5.0/redis.sock"
CONF="/root/bali5.0/redis.conf"
LOGS_DIR="/root/bali5.0/logs"

# Ensure log directory exists before Redis tries to open its log file
mkdir -p "$LOGS_DIR"

# 1. Already running?
if redis-cli -s "$SOCKET" ping 2>/dev/null | grep -q PONG; then
    echo "Redis already running on $SOCKET"
    exit 0
fi

# 2. Start Redis
echo "Starting Redis with $CONF ..."
redis-server "$CONF"

# 3. Wait for socket to appear and respond (up to 10s)
for i in $(seq 1 10); do
    sleep 1
    if redis-cli -s "$SOCKET" ping 2>/dev/null | grep -q PONG; then
        echo "Redis ready on $SOCKET (attempt $i)"
        exit 0
    fi
done

echo "ERROR: Redis did not start after 10 seconds" >&2
exit 1
```

### 4. `launcher.py`

Add `ensure_redis()` before `check_redis()`. Uses `Path(__file__).parent` for an absolute
path — consistent with the rest of `launcher.py`.

```python
def ensure_redis():
    """Start Redis via setup script if not already running."""
    script = Path(__file__).parent / "scripts" / "redis_setup.sh"
    result = subprocess.run(["bash", str(script)])
    if result.returncode != 0:
        log.error("Redis setup script failed — aborting")
        sys.exit(1)
```

Call order in `main()`:
```
ensure_redis()    ← new (starts Redis if needed, creates logs/)
check_redis()     ← existing (verifies via config.REDIS_URL)
os.makedirs(...)  ← existing
...
```

### 5. Tests

| File | Line | Change |
|------|------|--------|
| `tests/test_infra.py` | 16 | `REDIS_URL.startswith("redis://")` → `REDIS_URL.startswith("unix://")` |
| `tests/test_infra.py` | `test_redis_connection` | Replace hardcoded URL with `config.REDIS_URL`; add skip guard if socket unreachable |
| `tests/test_binance_smoke.py` | 24 | Replace hardcoded `"redis://localhost:6379"` with `config.REDIS_URL` |

`test_redis_connection` after fix:
```python
def test_redis_connection():
    import redis
    import config
    try:
        r = redis.Redis.from_url(config.REDIS_URL)
        assert r.ping(), "Redis не отвечает"
        r.close()
    except Exception:
        pytest.skip("Redis not available — запустите scripts/redis_setup.sh")
```

### 6. Documentation

| File | Section | Change |
|------|---------|--------|
| `docs/01_redis_schema.md` | §10 Redis Configuration | Replace TCP config with unix socket config block |
| `docs/06_config.md` | Redis section | Remove `REDIS_HOST/PORT`, document `REDIS_SOCKET_PATH` + `REDIS_URL` |
| `docs/09_launch.md` | §1 порядок запуска | Add `scripts/redis_setup.sh` as Step 0 (before dictionaries) |
| `docs/09_launch.md` | §7 диагностика | Update `redis-cli` commands: add `-s /root/bali5.0/redis.sock` flag |

---

## Out of Scope

- No changes to Redis key schema, TTLs, or pub/sub channels
- No changes to collector or monitor business logic
- No env-var TCP fallback

---

## Test Run After Migration

```bash
# Unit + integration tests (no network required)
pytest tests/ -v --ignore=tests/test_binance_smoke.py

# Smoke test (requires Redis running + internet)
pytest tests/test_binance_smoke.py -v -s --timeout=60
```

---

## Rollback

To fully revert to TCP:

1. `config.py` — restore `REDIS_HOST`, `REDIS_PORT`, original `REDIS_URL`
2. `launcher.py` — remove `ensure_redis()` call
3. `tests/test_infra.py` — restore original assertions and hardcoded URL
4. `tests/test_binance_smoke.py` — restore hardcoded URL
5. Delete `redis.conf` and `scripts/redis_setup.sh`
6. `docs/` — revert §10 of `01_redis_schema.md`, `06_config.md`, `09_launch.md`
7. Start Redis: `redis-server --port 6379 --daemonize yes`
