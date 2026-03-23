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

Remove `REDIS_HOST`, `REDIS_PORT`. Add `REDIS_SOCKET_PATH`. Update `REDIS_URL`.

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
unixsocketperm 700
port 0

maxmemory 90gb
maxmemory-policy noeviction
save ""
appendonly no
tcp-keepalive 60
hz 20
loglevel notice
logfile /root/bali5.0/logs/redis-server.log
```

### 3. `scripts/redis_setup.sh` (new file)

Idempotent startup script:

1. `redis-cli -s /root/bali5.0/redis.sock ping` → if `PONG`, already running → exit 0
2. Else: `redis-server /root/bali5.0/redis.conf --daemonize yes`
3. Retry ping up to 10 times (1s each)
4. If still no response → exit 1 (hard failure)

### 4. `launcher.py`

Call `scripts/redis_setup.sh` before `check_redis()`:

```python
def ensure_redis():
    result = subprocess.run(["bash", "scripts/redis_setup.sh"])
    if result.returncode != 0:
        log.error("Redis setup failed — aborting")
        sys.exit(1)
```

`check_redis()` remains unchanged (already reads `config.REDIS_URL`).

### 5. Tests

| File | Line | Change |
|------|------|--------|
| `tests/test_infra.py` | 16 | `startswith("redis://")` → `"redis.sock" in REDIS_URL` |
| `tests/test_infra.py` | 78 | hardcoded `"redis://localhost:6379"` → `config.REDIS_URL` |
| `tests/test_binance_smoke.py` | 24 | hardcoded `"redis://localhost:6379"` → `config.REDIS_URL` |

### 6. Documentation

| File | Section | Change |
|------|---------|--------|
| `docs/01_redis_schema.md` | §10 Redis Configuration | Replace TCP recommendations with unix socket config block |
| `docs/06_config.md` | Redis section | Remove `REDIS_HOST/PORT`, document `REDIS_SOCKET_PATH` + `REDIS_URL` |
| `docs/09_launch.md` | §1 порядок запуска | Add `scripts/redis_setup.sh` as Step 0 |
| `docs/09_launch.md` | §7 диагностика | Update `redis-cli` commands to use `-s redis.sock` |

---

## Out of Scope

- No changes to Redis key schema, TTLs, or pub/sub channels
- No changes to collector or monitor logic
- No env-var TCP fallback

---

## Test Run After Migration

Full test suite must pass:

```bash
pytest tests/ -v --ignore=tests/test_binance_smoke.py
```

Smoke test (requires network):

```bash
pytest tests/test_binance_smoke.py -v -s --timeout=60
```

---

## Rollback

To revert to TCP: restore `config.py` REDIS section, remove `redis.conf` and `scripts/redis_setup.sh`,
start Redis without config file.
