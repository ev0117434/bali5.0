# Redis Unix Socket Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all Redis TCP connections (`localhost:6379`) with a Unix socket (`/root/bali5.0/redis.sock`), add a Redis setup/startup script, and update all tests and documentation.

**Architecture:** Single config change (`REDIS_URL`) propagates automatically to all 5 collectors and 4 monitors (they all read `config.REDIS_URL`). New `redis.conf` disables TCP (`port 0`) and enables the Unix socket. New `scripts/redis_setup.sh` idempotently starts Redis before the launcher runs.

**Tech Stack:** Python 3, `redis-py` / `redis.asyncio` (both support `unix://` URLs natively), `bash`, Redis 6+

**Spec:** `docs/superpowers/specs/2026-03-23-redis-unix-socket-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `config.py` | Replace TCP constants with `REDIS_SOCKET_PATH` + `unix://` URL |
| Create | `redis.conf` | Redis server config: unix socket, no TCP, memory policy |
| Create | `scripts/redis_setup.sh` | Idempotent: check → start → retry; creates `logs/` first |
| Modify | `launcher.py` | Add `ensure_redis()` before `check_redis()` |
| Modify | `tests/test_infra.py` | Fix URL assertion + fix hardcoded URL in `test_redis_connection` |
| Modify | `tests/test_binance_smoke.py` | Fix hardcoded URL in fixture |
| Modify | `tests/test_launcher.py` | Add tests for `ensure_redis()` success and failure paths |
| Modify | `.gitignore` | Add `redis.sock` runtime socket file |
| Modify | `docs/01_redis_schema.md` | Update §10 Redis Configuration |
| Modify | `docs/06_config.md` | Update Redis section (remove HOST/PORT, add SOCKET_PATH) |
| Modify | `docs/09_launch.md` | Update §1 launch order and §7 diagnostics |

---

## Task 1: Fix tests to reflect unix socket (RED)

These tests currently pass with TCP. Fix them now so they drive the config change in Task 2.

**Files:**
- Modify: `tests/test_infra.py`
- Modify: `tests/test_binance_smoke.py`

- [ ] **Step 1: Fix `test_config_imports` URL assertion**

In `tests/test_infra.py` line 16, change:

```python
# Before
assert REDIS_URL.startswith("redis://")

# After
assert REDIS_URL.startswith("unix://")
```

- [ ] **Step 2: Fix `test_redis_connection` — hardcoded URL + add skip guard**

Replace the entire `test_redis_connection` function (lines 76–80):

```python
def test_redis_connection():
    import redis       # redis is NOT imported at module level in this file
    import config
    try:
        r = redis.Redis.from_url(config.REDIS_URL)
        assert r.ping(), "Redis не отвечает"
        r.close()
    except Exception:
        pytest.skip("Redis not available — запустите scripts/redis_setup.sh")
```

Note: `pytest` is available as a pytest global without an explicit import. `redis` must be imported inside the function — it is not imported at module level in `test_infra.py`.

- [ ] **Step 3: Fix `test_binance_smoke.py` — hardcoded URL in fixture**

Replace the `redis_client` fixture (lines 21–30) — no top-level `import config` needed since it's imported inside the fixture:

```python
@pytest.fixture(scope="module")
def redis_client():
    import config
    import redis
    try:
        r = redis.Redis.from_url(config.REDIS_URL)
        r.ping()
    except Exception:
        pytest.skip("Redis not available")
    yield r
    r.close()
```

- [ ] **Step 4: Run the tests — verify they fail on the URL assertion**

```bash
cd /root/bali5.0
pytest tests/test_infra.py::test_config_imports -v
```

Expected: `FAILED` — `AssertionError` because `REDIS_URL` still starts with `redis://`

- [ ] **Step 5: Commit the test changes**

```bash
git add tests/test_infra.py tests/test_binance_smoke.py
git commit -m "test: update redis connection tests for unix socket"
```

---

## Task 2: Update `config.py`

**Files:**
- Modify: `config.py` lines 15–19

- [ ] **Step 1: Replace the Redis section**

In `config.py`, replace lines 15–19:

```python
# ── Redis ──────────────────────────────────────────────────────────────────
REDIS_SOCKET_PATH = "/root/bali5.0/redis.sock"
REDIS_DB          = 0
REDIS_URL         = f"unix://{REDIS_SOCKET_PATH}?db={REDIS_DB}"
```

The three lines `REDIS_HOST`, `REDIS_PORT`, and the old `REDIS_URL` are deleted.
`REDIS_SOCKET_PATH` starts with `/`, so the URL expands to `unix:///root/bali5.0/redis.sock?db=0`
which is the correct three-slash form for `redis-py`.

- [ ] **Step 2: Run the config tests — verify they pass**

```bash
cd /root/bali5.0
pytest tests/test_infra.py::test_config_imports \
       tests/test_infra.py::test_config_chunk_values \
       tests/test_infra.py::test_config_batch_values \
       tests/test_infra.py::test_config_spread_values -v
```

Expected: all 4 `PASSED`

- [ ] **Step 3: Commit**

```bash
git add config.py
git commit -m "feat: switch redis to unix socket (REDIS_SOCKET_PATH + unix:// URL)"
```

---

## Task 3: Create `redis.conf`

**Files:**
- Create: `redis.conf` (project root `/root/bali5.0/redis.conf`)

- [ ] **Step 1: Create the file**

```conf
# /root/bali5.0/redis.conf
# BALI 5.0 — Redis server configuration (Unix socket, no TCP)

unixsocket /root/bali5.0/redis.sock
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
logfile /root/bali5.0/logs/redis-server.log
```

- [ ] **Step 2: Verify Redis can start with this config**

Start Redis, ping it, then stop it:

```bash
mkdir -p /root/bali5.0/logs
redis-server /root/bali5.0/redis.conf
sleep 2
redis-cli -s /root/bali5.0/redis.sock ping
redis-cli -s /root/bali5.0/redis.sock shutdown nosave 2>/dev/null || true
sleep 1
rm -f /root/bali5.0/redis.sock
```

Expected from `ping`: `PONG`

- [ ] **Step 3: Commit**

```bash
git add redis.conf
git commit -m "feat: add redis.conf for unix socket server config"
```

---

## Task 4: Create `scripts/redis_setup.sh`

**Files:**
- Create: `scripts/redis_setup.sh`

- [ ] **Step 1: Create the `scripts/` directory and the file**

```bash
mkdir -p /root/bali5.0/scripts
```

File contents at `/root/bali5.0/scripts/redis_setup.sh`:

```bash
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

# 3. Start Redis with our config (daemonize yes is set in redis.conf)
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
```

- [ ] **Step 2: Make it executable**

```bash
chmod +x /root/bali5.0/scripts/redis_setup.sh
```

- [ ] **Step 3: Run the script — verify Redis starts**

```bash
bash /root/bali5.0/scripts/redis_setup.sh
```

Expected output:
```
[redis_setup] Starting Redis: redis-server /root/bali5.0/redis.conf
[redis_setup] Redis ready on /root/bali5.0/redis.sock (attempt N)
```

Verify socket responds and TCP is disabled:

```bash
redis-cli -s /root/bali5.0/redis.sock ping
redis-cli -s /root/bali5.0/redis.sock info server | grep -E "tcp_port|unixsocket"
```

Expected:
```
PONG
tcp_port:0
unixsocket:/root/bali5.0/redis.sock
```

- [ ] **Step 4: Run script again — verify idempotency**

```bash
bash /root/bali5.0/scripts/redis_setup.sh
```

Expected: `[redis_setup] Redis already running on /root/bali5.0/redis.sock` and exit 0

- [ ] **Step 5: Run `test_redis_connection` — verify it now passes**

```bash
cd /root/bali5.0
pytest tests/test_infra.py::test_redis_connection -v
```

Expected: `PASSED`

- [ ] **Step 6: Commit**

```bash
git add scripts/redis_setup.sh
git commit -m "feat: add scripts/redis_setup.sh — idempotent redis unix socket startup"
```

---

## Task 5: Update `launcher.py`

**Files:**
- Modify: `launcher.py`

- [ ] **Step 1: Add `ensure_redis()` function**

Insert after line 83 (end of `check_subscribe_files()`), before line 86 (`def start_process`):

```python
def ensure_redis():
    """Start Redis via setup script if not already running."""
    script = Path(__file__).parent / "scripts" / "redis_setup.sh"
    log.info(f"Running Redis setup: {script}")
    result = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    if result.stdout:
        log.info(result.stdout.strip())
    if result.returncode != 0:
        log.error(f"Redis setup script failed:\n{result.stderr}")
        sys.exit(1)
```

- [ ] **Step 2: Call `ensure_redis()` before `check_redis()` in `main()`**

In `main()`, find `check_redis()` (line 129) and add `ensure_redis()` above it:

```python
# Before
check_redis()

# After
ensure_redis()
check_redis()
```

- [ ] **Step 3: Commit**

```bash
git add launcher.py
git commit -m "feat: launcher calls ensure_redis() before check_redis()"
```

---

## Task 6: Add tests for `ensure_redis()`

**Files:**
- Modify: `tests/test_launcher.py`

- [ ] **Step 1: Add a new test class for `ensure_redis`**

Append to `tests/test_launcher.py`:

```python
class TestEnsureRedis:
    def test_ensure_redis_success(self, monkeypatch):
        """ensure_redis() should not exit when script returns 0."""
        import launcher
        from unittest.mock import patch, MagicMock

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "[redis_setup] Redis already running"
        mock_result.stderr = ""

        with patch("launcher.subprocess.run", return_value=mock_result):
            # Should complete without raising SystemExit
            launcher.ensure_redis()

    def test_ensure_redis_failure_exits(self, monkeypatch):
        """ensure_redis() should call sys.exit(1) when script returns non-zero."""
        import launcher
        from unittest.mock import patch, MagicMock
        import pytest

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "ERROR: Redis did not start"

        with patch("launcher.subprocess.run", return_value=mock_result):
            with pytest.raises(SystemExit) as exc_info:
                launcher.ensure_redis()
            assert exc_info.value.code == 1
```

- [ ] **Step 2: Run the new tests**

```bash
cd /root/bali5.0
pytest tests/test_launcher.py::TestEnsureRedis -v
```

Expected: both tests `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_launcher.py
git commit -m "test: add ensure_redis() success and failure tests"
```

---

## Task 7: Add `redis.sock` to `.gitignore`

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Add runtime socket file to `.gitignore`**

Under the `# Runtime output` section, add:

```
# Redis unix socket (runtime file — not committed)
redis.sock
```

- [ ] **Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: ignore redis.sock runtime socket file"
```

---

## Task 8: Run the full test suite

- [ ] **Step 1: Confirm Redis is running**

```bash
redis-cli -s /root/bali5.0/redis.sock ping
```

Expected: `PONG`. If not, run `bash /root/bali5.0/scripts/redis_setup.sh` first.

- [ ] **Step 2: Run all tests except smoke**

```bash
cd /root/bali5.0
pytest tests/ -v --ignore=tests/test_binance_smoke.py
```

Expected: all `PASSED`. If any fail, fix before continuing.

- [ ] **Step 3: Commit any fixes if needed**

```bash
git add -p
git commit -m "fix: <describe what broke>"
```

---

## Task 9: Update documentation

**Files:**
- Modify: `docs/01_redis_schema.md`
- Modify: `docs/06_config.md`
- Modify: `docs/09_launch.md`

- [ ] **Step 1: Update `docs/01_redis_schema.md` — replace §10 Redis Configuration**

Find `## 10. Redis Configuration Recommendations` and replace the entire code block and any text below it in that section with:

````markdown
```conf
# /root/bali5.0/redis.conf
# Unix socket — TCP disabled (port 0)

unixsocket /root/bali5.0/redis.sock
unixsocketperm 770
port 0

daemonize yes
maxmemory 0                 # unlimited — ~82 GB история, ресурс не ограничен
maxmemory-policy noeviction # отказать в записи если вдруг закончится память
save ""                     # RDB snapshots отключены — ускоряет запись
appendonly no               # AOF отключён — ускоряет запись
tcp-keepalive 60
hz 20
loglevel notice
logfile /root/bali5.0/logs/redis-server.log
```

Запуск: `bash scripts/redis_setup.sh` (идемпотентно — запускает только если не работает)
Диагностика: `redis-cli -s /root/bali5.0/redis.sock ping`
````

- [ ] **Step 2: Update `docs/06_config.md` — Redis section**

In the full config.py block inside the doc, replace the Redis section:

```python
# ── Redis ──────────────────────────────────────────────────────────────────
REDIS_SOCKET_PATH = "/root/bali5.0/redis.sock"
REDIS_DB          = 0
REDIS_URL         = f"unix://{REDIS_SOCKET_PATH}?db={REDIS_DB}"
```

In the "Обоснование значений" table, add a row for `REDIS_SOCKET_PATH`:

```
| `REDIS_SOCKET_PATH` | `/root/bali5.0/redis.sock` | Unix socket путь; все коллекторы и мониторы используют `REDIS_URL` |
```

- [ ] **Step 3: Update `docs/09_launch.md` — §1 порядок запуска**

Replace the existing "Шаг 0: Убедиться что Redis запущен" with:

```
Шаг 0: Запустить Redis (однократно при старте; launcher делает это автоматически)
    bash scripts/redis_setup.sh

Шаг 1: Запустить dictionaries/main.py (однократно, ~70 сек)
Шаг 2: python launcher.py [--data]
        └── вызывает ensure_redis() → scripts/redis_setup.sh автоматически
```

- [ ] **Step 4: Update `docs/09_launch.md` — §7 Диагностика**

Replace TCP-based Redis check lines with socket-based equivalents:

```bash
# Проверить Redis
redis-cli -s /root/bali5.0/redis.sock ping
redis-cli -s /root/bali5.0/redis.sock info memory
redis-cli -s /root/bali5.0/redis.sock info stats

# Количество заполненных MD ключей
redis-cli -s /root/bali5.0/redis.sock keys "md:*" | wc -l

# Проверить конкретный символ
redis-cli -s /root/bali5.0/redis.sock hgetall "md:binance:spot:BTCUSDT"

# Все активные cooldown
redis-cli -s /root/bali5.0/redis.sock keys "spread:cooldown:*" | wc -l

# Остановить Redis
redis-cli -s /root/bali5.0/redis.sock shutdown nosave
```

Also add to §2 "Использование":

```bash
# Запустить Redis (если не запущен)
bash scripts/redis_setup.sh
```

- [ ] **Step 5: Commit all doc changes**

```bash
git add docs/01_redis_schema.md docs/06_config.md docs/09_launch.md
git commit -m "docs: update redis config, launch, diagnostics for unix socket"
```

---

## Task 10: Final verification

- [ ] **Step 1: Run the full test suite**

```bash
cd /root/bali5.0
pytest tests/ -v --ignore=tests/test_binance_smoke.py
```

Expected: all `PASSED`

- [ ] **Step 2: Verify TCP is truly disabled**

```bash
redis-cli -p 6379 ping 2>&1
```

Expected: `Could not connect to Redis at 127.0.0.1:6379: Connection refused`

- [ ] **Step 3: Verify socket info**

```bash
redis-cli -s /root/bali5.0/redis.sock info server | grep -E "tcp_port|unixsocket"
```

Expected:
```
tcp_port:0
unixsocket:/root/bali5.0/redis.sock
```

- [ ] **Step 4: Confirm clean git status**

```bash
git status
```

Expected: `nothing to commit, working tree clean`
