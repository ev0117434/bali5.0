# JSON Metrics (NDJSON Logging) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all text log lines across BALI 5.0 with machine-readable NDJSON — one JSON object per line — covering all pipeline stages, latencies, and lifecycle events.

**Architecture:** Add `JsonFormatter` to existing `logger_setup.py` (file handler → JSON, console handler stays text for readability). Each script defines a local `_evt()` factory that stamps `ts`/`component`/`event` fields. New stats fields track parse errors, parse latency, buffer age, e2e latency, and slow-flush counts.

**Tech Stack:** Python 3.11+, stdlib `logging`/`json`/`time`/`statistics`, pytest. No new dependencies.

> **Task dependency:** Task 1 (JsonFormatter) MUST complete before Tasks 2–9. All other tasks are independent of each other and can be executed sequentially in any order after Task 1.

**Spec:** `docs/superpowers/specs/2026-03-23-json-metrics-schema-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| **Modify** | `logger_setup.py` | Add `JsonFormatter`; file handler uses JSON, console keeps text |
| **Create** | `tests/test_json_logger.py` | Tests for `JsonFormatter` |
| **Modify** | `collectors/collector_binance.py` | New stats fields + parse tracking + JSON events |
| **Modify** | `collectors/collector_bybit.py` | Same pattern as binance |
| **Modify** | `collectors/collector_okx.py` | Same pattern as binance |
| **Modify** | `collectors/collector_gate.py` | Same pattern as binance |
| **Modify** | `collectors/collector_bitget.py` | Same pattern as binance |
| **Modify** | `monitors/spread_monitor.py` | JSON events + pipeline/calc timing split |
| **Modify** | `monitors/redis_monitor.py` | JSON events |
| **Modify** | `monitors/stale_monitor.py` | JSON events |
| **Modify** | `monitors/snapshot_monitor.py` | JSON events + stream_lag + history breakdown |
| **Modify** | `launcher.py` | JSON events |

---

## Task 1: JsonFormatter in logger_setup.py

**Files:**
- Modify: `logger_setup.py`
- Create: `tests/test_json_logger.py`

### Step 1.1 — Write the failing tests

- [ ] Create `tests/test_json_logger.py`:

```python
# tests/test_json_logger.py
"""Tests for JsonFormatter and setup_logger JSON output."""
import json
import logging
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from logger_setup import JsonFormatter, setup_logger


class TestJsonFormatter:
    def _make_logger(self):
        fmt = JsonFormatter()
        handler = logging.StreamHandler()
        handler.setFormatter(fmt)
        logger = logging.getLogger(f"test_{id(self)}")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger, handler

    def test_dict_msg_is_serialized_as_is(self, caplog):
        """Dict message passes through as JSON with ts/component/event preserved."""
        import io
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_dict")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.info({"ts": 1741234567890, "component": "test", "event": "hello", "value": 42})
        line = stream.getvalue().strip()
        obj = json.loads(line)
        assert obj["component"] == "test"
        assert obj["event"] == "hello"
        assert obj["value"] == 42
        assert obj["ts"] == 1741234567890

    def test_string_msg_wrapped_in_envelope(self):
        """Plain string messages get wrapped with ts/component/event=log/msg fields."""
        import io
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_str")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.warning("something went wrong")
        line = stream.getvalue().strip()
        obj = json.loads(line)
        assert obj["event"] == "log"
        assert obj["msg"] == "something went wrong"
        assert obj["level"] == "WARNING"
        assert "ts" in obj

    def test_output_is_valid_json(self):
        """Every emitted line must be parseable as JSON."""
        import io
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_valid_json")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.info({"ts": 1, "component": "x", "event": "y"})
        logger.warning("plain text")
        lines = [l for l in stream.getvalue().strip().splitlines() if l]
        for line in lines:
            json.loads(line)  # must not raise

    def test_ts_is_unix_milliseconds(self):
        """ts field in wrapped plain-string messages is current Unix ms."""
        import io
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_ts")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        before = int(time.time() * 1000)
        logger.info("ts test")
        after = int(time.time() * 1000)
        obj = json.loads(stream.getvalue().strip())
        assert before <= obj["ts"] <= after

    def test_dict_msg_not_mutated(self):
        """Original dict passed as msg is not modified."""
        import io
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_nomutate")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        original = {"ts": 1, "component": "c", "event": "e", "x": 99}
        copy = dict(original)
        logger.info(original)
        assert original == copy  # not mutated

    def test_setup_logger_file_handler_uses_json(self, tmp_path, monkeypatch):
        """setup_logger writes JSON to log file."""
        import config
        monkeypatch.setattr(config, "LOGS_DIR", str(tmp_path))
        # Clear existing handlers
        log = logging.getLogger("setup_test_unique_123")
        log.handlers.clear()

        from logger_setup import setup_logger
        logger = setup_logger("setup_test_unique_123")
        logger.info({"ts": 999, "component": "setup_test_unique_123", "event": "test_event"})

        log_file = tmp_path / "setup_test_unique_123.log"
        lines = [l for l in log_file.read_text().strip().splitlines() if l]
        assert len(lines) == 1
        obj = json.loads(lines[0])
        assert obj["event"] == "test_event"
```

- [ ] Run to confirm FAIL:

```bash
cd /root/bali5.0 && python -m pytest tests/test_json_logger.py -v 2>&1 | head -40
```

Expected: `ImportError: cannot import name 'JsonFormatter' from 'logger_setup'`

---

### Step 1.2 — Implement JsonFormatter in logger_setup.py

- [ ] Edit `logger_setup.py` — add `JsonFormatter` class and update file handler:

```python
# logger_setup.py
"""
BALI 5.0 — Настройка логирования.
Единственная функция: setup_logger(name) → logging.Logger

File handler: NDJSON (one JSON object per line, machine-readable).
Console handler: human-readable text (WARNING+ only).
"""

import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler

import config


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON (NDJSON).

    - If record.msg is a dict: serialize it directly (must contain ts/component/event).
    - If record.msg is a str/other: wrap in envelope with ts, component, event='log', level, msg.
    """

    def format(self, record: logging.LogRecord) -> str:
        msg = record.msg
        if isinstance(msg, dict):
            payload = dict(msg)  # shallow copy — never mutate caller's dict
        else:
            payload = {
                "ts":        int(record.created * 1000),
                "component": record.name,
                "event":     "log",
                "level":     record.levelname,
                "msg":       record.getMessage(),
            }
        return json.dumps(payload, ensure_ascii=False)


def setup_logger(name: str) -> logging.Logger:
    """
    Создаёт и возвращает логгер с именем `name`.

    - Файл: logs/{name}.log  — NDJSON, RotatingFileHandler 10MB × 5, уровень DEBUG
    - Консоль: StreamHandler — текст, уровень WARNING
    """
    os.makedirs(config.LOGS_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured, don't duplicate

    logger.setLevel(logging.DEBUG)

    # ── File handler: NDJSON ──────────────────────────────────────────────
    fh = RotatingFileHandler(
        filename=os.path.join(config.LOGS_DIR, f"{name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)

    # ── Console handler: human-readable text (WARNING+) ──────────────────
    text_fmt = logging.Formatter(
        fmt="[%(asctime)s.%(msecs)03d] [%(levelname)-8s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(text_fmt)
    logger.addHandler(ch)

    return logger
```

- [ ] Run tests:

```bash
cd /root/bali5.0 && python -m pytest tests/test_json_logger.py -v
```

Expected: all 6 tests PASS.

- [ ] Run existing tests to verify no regression:

```bash
cd /root/bali5.0 && python -m pytest tests/ -v --ignore=tests/test_json_logger.py 2>&1 | tail -20
```

Expected: same pass/fail as before.

- [ ] Commit:

```bash
cd /root/bali5.0
git add logger_setup.py tests/test_json_logger.py
git commit -m "feat: add JsonFormatter to logger_setup — NDJSON file output"
```

---

## Task 2: New Stats Fields + Parse Tracking in collector_binance.py

**Files:**
- Modify: `collectors/collector_binance.py`
- Modify: `tests/test_binance_parsers.py`

### Step 2.1 — Write failing tests for new stats fields

- [ ] Add to `tests/test_binance_parsers.py`:

```python
# ── New stats fields ───────────────────────────────────────────────────────

class TestCollectorBinanceStats:
    def test_stats_dict_has_new_fields(self):
        """stats dict must contain all NEW fields required for JSON metrics."""
        from collectors.collector_binance import stats
        required_new = [
            "parse_errors",
            "parse_lat_sum", "parse_lat_max",
            "flush_slow_count", "hist_flush_slow_count",
            "buffer_age_sum", "buffer_age_max",
            "e2e_lat_sum", "e2e_lat_max", "e2e_lat_count",
        ]
        for field in required_new:
            assert field in stats, f"Missing stats field: {field}"

    def test_parse_md_increments_parse_errors_on_bad_input(self):
        """parse_md with invalid JSON must increment stats['parse_errors']."""
        from collectors import collector_binance as cb
        before = cb.stats["parse_errors"]
        cb.parse_md("not-valid-json")
        assert cb.stats["parse_errors"] == before + 1

    def test_parse_md_increments_parse_errors_on_missing_fields(self):
        """parse_md with missing fields must increment stats['parse_errors']."""
        import json
        from collectors import collector_binance as cb
        before = cb.stats["parse_errors"]
        cb.parse_md(json.dumps({"stream": "btcusdt@bookTicker", "data": {}}))
        assert cb.stats["parse_errors"] == before + 1
```

- [ ] Run to confirm FAIL:

```bash
cd /root/bali5.0 && python -m pytest tests/test_binance_parsers.py::TestCollectorBinanceStats -v
```

Expected: FAIL — missing stats fields.

---

### Step 2.2 — Add new stats fields and parse error tracking

- [ ] In `collectors/collector_binance.py`, extend the `stats` dict (find the existing dict and add new fields):

```python
stats: dict = {
    # ── existing ──────────────────────────────────────────────────────────
    "md_msgs":             0,
    "ob_msgs":             0,
    "fr_msgs":             0,
    "flushes":             0,
    "flush_lat_sum":       0.0,
    "flush_lat_max":       0.0,
    "batch_sum":           0,
    "hist_flushes":        0,
    "hist_flush_lat_sum":  0.0,
    "hist_flush_lat_max":  0.0,
    "hist_cmds":           0,
    "ob_hist_skipped":     0,
    "reconnects":          0,
    # ── new ───────────────────────────────────────────────────────────────
    "parse_errors":        0,
    "parse_lat_sum":       0.0,   # microseconds
    "parse_lat_max":       0.0,
    "flush_slow_count":    0,
    "hist_flush_slow_count": 0,
    "buffer_age_sum":      0.0,   # ms; accumulated at each flush
    "buffer_age_max":      0.0,
    "e2e_lat_sum":         0.0,   # ms; exchange_ts → local_ts delta
    "e2e_lat_max":         0.0,
    "e2e_lat_count":       0,
}
```

- [ ] Wrap each `parse_*` function body in try/except to count errors. Example for `parse_md`:

```python
def parse_md(raw: str):
    _t = time.monotonic()
    try:
        msg = json.loads(raw)
        data = msg.get("data", msg)
        sym = data.get("s")
        bid = data.get("b")
        ask = data.get("a")
        if not sym or bid is None or ask is None:
            stats["parse_errors"] += 1
            return None
        ts_ms = int(time.time() * 1000)
        return sym, bid, ask, ts_ms
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us
```

Apply the same `try/except/finally` pattern to `parse_ob`, `parse_ob_fut`, and `parse_fr`.

- [ ] Run tests:

```bash
cd /root/bali5.0 && python -m pytest tests/test_binance_parsers.py -v
```

Expected: all tests PASS (including new `TestCollectorBinanceStats` tests).

- [ ] Commit:

```bash
cd /root/bali5.0
git add collectors/collector_binance.py tests/test_binance_parsers.py
git commit -m "feat: add parse error tracking and new stats fields to collector_binance"
```

---

## Task 3: JSON Events in collector_binance.py

**Files:**
- Modify: `collectors/collector_binance.py`

This task converts every `log.info(f"...")` / `log.warning(f"...")` / `log.error(f"...")` call in collector_binance to emit dict events.

### Step 3.1 — Add `_evt()` factory at top of module

- [ ] After the `stats` dict, add:

```python
_COMPONENT = "collector_binance"

def _evt(event: str, **kwargs) -> dict:
    """Build a base metrics event envelope."""
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}
```

### Step 3.2 — Replace log calls with JSON events

Replace each log call as follows. Find the exact string in the file and replace with the JSON form below.

**Startup:**
```python
# BEFORE:
log.info(f"Starting collector_binance | spot={len(spot_symbols)} fut={len(fut_symbols)} | HISTORY_ENABLED={config.HISTORY_ENABLED}")

# AFTER:
log.info(_evt("collector_start", exchange="binance",
              spot_symbols=len(spot_symbols), fut_symbols=len(fut_symbols),
              history_enabled=config.HISTORY_ENABLED))
```

**Symbol file not found:**
```python
# BEFORE:
log.error(f"Symbol file not found: {filepath}")

# AFTER:
log.error(_evt("symbol_file_missing", exchange="binance", path=str(filepath)))
```

**WS connect:**
```python
# BEFORE:
log.info(f"[{label}] Connecting: {url[:80]}...")
log.info(f"[{label}] Connected")

# AFTER:
log.info(_evt("ws_connect", exchange="binance", stream=label,
              url_prefix=url[:80], symbols=len(symbols)))
log.info(_evt("ws_connected", exchange="binance", stream=label))
```

**WS disconnect/error:**
```python
# BEFORE:
log.warning(f"[{label}] WS error: {type(exc).__name__}: {exc}. Reconnecting in {backoff}s...")

# AFTER:
log.warning(_evt("ws_disconnect", exchange="binance", stream=label,
                 error_type=type(exc).__name__, error_msg=str(exc),
                 reconnect_backoff_s=backoff,
                 reconnects_total=stats["reconnects"]))
```

**Redis pipeline error:**
```python
# BEFORE:
log.error(f"Redis pipeline error: {exc}")
log.error(f"Redis hist pipeline error: {exc}")

# AFTER:
log.error(_evt("redis_error", exchange="binance", pipeline_type="primary",
               error_type=type(exc).__name__, error_msg=str(exc)))
log.error(_evt("redis_error", exchange="binance", pipeline_type="history",
               error_type=type(exc).__name__, error_msg=str(exc)))
```

**Slow flush — primary:**
```python
# BEFORE:
log.warning(f"Slow primary flush: {flush_lat_ms:.1f}ms keys={len(current_batch)}")

# AFTER:
stats["flush_slow_count"] += 1
log.warning(_evt("slow_flush", exchange="binance", flush_type="primary",
                 lat_ms=round(flush_lat_ms, 1), threshold_ms=50.0,
                 cmds=len(current_batch)))
```

**Slow flush — history:**
```python
# BEFORE:
log.warning(f"Slow hist flush: {flush_lat_ms:.1f}ms cmds={len(current_hist)} expire_new={expire_cmds}")

# AFTER:
stats["hist_flush_slow_count"] += 1
log.warning(_evt("slow_flush", exchange="binance", flush_type="history",
                 lat_ms=round(flush_lat_ms, 1), threshold_ms=100.0,
                 cmds=len(current_hist), expire_cmds=expire_cmds))
```

**Chunk rotate:**
```python
# BEFORE:
log.info(f"Chunk changed: {last_chunk_id} → {new_chunk_id}")

# AFTER:
log.info(_evt("chunk_rotate", exchange="binance",
              chunk_id_prev=last_chunk_id, chunk_id_new=new_chunk_id,
              expire_keys_reset=len(expire_set)))
```

**No symbols:**
```python
# BEFORE:
log.error("No symbols loaded — exiting")

# AFTER:
log.error(_evt("no_symbols", exchange="binance"))
```

**Metrics interval — replace the full `task_metrics` log line:**
```python
# BEFORE (entire formatted string):
log.info(f"METRICS | md={md_msgs/interval:.0f}msg/s ...")

# AFTER:
n  = stats["flushes"]        or 1
nh = stats["hist_flushes"]   or 1
nm = (stats["md_msgs"] + stats["ob_msgs"] + stats["fr_msgs"]) or 1

log.info(_evt("metrics_interval", exchange="binance",
    interval_s=round(interval, 1),
    ingestion={
        "md_msgs":           stats["md_msgs"],
        "md_msgs_per_s":     round(stats["md_msgs"] / interval, 1),
        "ob_msgs":           stats["ob_msgs"],
        "ob_msgs_per_s":     round(stats["ob_msgs"] / interval, 1),
        "fr_msgs":           stats["fr_msgs"],
        "fr_msgs_per_s":     round(stats["fr_msgs"] / interval, 1),
        "parse_errors":      stats["parse_errors"],
        "parse_errors_per_s": round(stats["parse_errors"] / interval, 1),
    },
    primary_flush={
        "count":       stats["flushes"],
        "count_per_s": round(stats["flushes"] / interval, 1),
        "batch_avg":   round(stats["batch_sum"] / n, 1),
        "lat_avg_ms":  round(stats["flush_lat_sum"] / n, 1),
        "lat_max_ms":  round(stats["flush_lat_max"], 1),
        "slow_count":  stats["flush_slow_count"],
    },
    history_flush={
        "count":       stats["hist_flushes"],
        "count_per_s": round(stats["hist_flushes"] / interval, 1),
        "cmds_total":  stats["hist_cmds"],
        "cmds_per_s":  round(stats["hist_cmds"] / interval, 1),
        "lat_avg_ms":  round(stats["hist_flush_lat_sum"] / nh, 1),
        "lat_max_ms":  round(stats["hist_flush_lat_max"], 1),
        "slow_count":  stats["hist_flush_slow_count"],
        "ob_skipped":  stats["ob_hist_skipped"],
        "ob_skipped_per_s": round(stats["ob_hist_skipped"] / interval, 1),
    },
    latency={
        "parse_avg_us":      round(stats["parse_lat_sum"] / nm, 1),
        "parse_max_us":      round(stats["parse_lat_max"], 1),
        "buffer_age_avg_ms": round(stats["buffer_age_sum"] / n, 1),
        "buffer_age_max_ms": round(stats["buffer_age_max"], 1),
        # e2e_lat only for exchanges that embed a timestamp in the message.
        # Binance bookTicker has NO exchange timestamp — omit these fields
        # entirely for Binance. Include only for bybit/okx/gate/bitget.
        # In collector_binance.py: omit e2e fields from this dict.
        # In collector_bybit/okx/gate/bitget.py: include:
        #   "e2e_avg_ms": round(stats["e2e_lat_sum"] / (stats["e2e_lat_count"] or 1), 1),
        #   "e2e_max_ms": round(stats["e2e_lat_max"], 1),
    },
    state={
        "expire_keys_tracked": len(expire_set),
        "reconnects":          stats["reconnects"],
        # Count live asyncio WS tasks. asyncio.all_tasks() is safe here because
        # task_metrics() is itself an asyncio coroutine running in the same event loop.
        # Requires WS tasks to have names set at creation:
        #   task = asyncio.create_task(coro(), name="ws_md_spot")
        "active_streams":      sum(1 for t in asyncio.all_tasks()
                                   if t.get_name().startswith("ws_")),
    },
))
```

**Then reset ALL stats** (add new fields to the reset block):
```python
stats.update({k: 0 for k in stats})
stats["flush_lat_max"] = 0.0
stats["hist_flush_lat_max"] = 0.0
stats["parse_lat_max"] = 0.0
stats["e2e_lat_max"] = 0.0
stats["buffer_age_max"] = 0.0
```

**Shutdown:**
```python
# BEFORE:
log.info("KeyboardInterrupt — shutting down")
log.info("collector_binance stopped")

# AFTER:
log.info(_evt("collector_stop", exchange="binance", reason="KeyboardInterrupt"))
log.info(_evt("collector_stop", exchange="binance", reason="stopped"))
```

### Step 3.3 — Smoke test: verify output is valid JSON

- [ ] Run the existing smoke test (if any) or a quick check:

```bash
cd /root/bali5.0 && python -m pytest tests/test_binance_smoke.py tests/test_binance_parsers.py -v
```

Expected: all PASS.

- [ ] Verify JSON structure of metrics_interval manually:

```bash
cd /root/bali5.0 && python -c "
import json, sys, time
sys.path.insert(0, '.')
from collectors.collector_binance import _evt, stats
evt = _evt('metrics_interval', exchange='binance', interval_s=5.0,
           ingestion={'md_msgs': 100}, primary_flush={'count': 20})
print(json.dumps(evt, indent=2))
json.loads(json.dumps(evt))  # must not raise
print('OK')
"
```

Expected: valid JSON printed, `OK`.

- [ ] Commit:

```bash
cd /root/bali5.0
git add collectors/collector_binance.py
git commit -m "feat: convert collector_binance all log lines to JSON events"
```

---

## Task 4: Apply Collector Pattern to bybit, okx, gate, bitget

**Files:**
- Modify: `collectors/collector_bybit.py`
- Modify: `collectors/collector_okx.py`
- Modify: `collectors/collector_gate.py`
- Modify: `collectors/collector_bitget.py`
- Modify: `tests/test_other_parsers.py`

Follow **exactly** the same steps as Tasks 2–3 for each collector. Key differences per exchange:

| Exchange | `_COMPONENT` | `exchange` field | `e2e_lat` available | Notes |
|----------|-------------|-----------------|---------------------|-------|
| bybit | `"collector_bybit"` | `"bybit"` | Yes (`msg["ts"]`) | Heartbeat log also converted |
| okx | `"collector_okx"` | `"okx"` | Yes (`data[0]["ts"]`) | Two separate WS loops (MD+OB, FR) |
| gate | `"collector_gate"` | `"gate"` | Yes (`result["t"]`) | Three separate WS loops |
| bitget | `"collector_bitget"` | `"bitget"` | Yes (`data[0]["ts"]`) | Delta OB accumulation state |

### Step 4.1 — Add stats fields + parse tracking (all 4)

- [ ] For each file, apply the same stats dict extension as Task 2.2.
- [ ] Wrap each `parse_*` function in `try/except/finally` as shown in Task 2.2.

### Step 4.2 — Add `_evt()` and replace log calls (all 4)

- [ ] For each file, add `_COMPONENT` constant and `_evt()` factory (Task 3.1).
- [ ] Replace all `log.info/warning/error` calls following Task 3.2 patterns.
  - Heartbeat error (bybit/okx/bitget): `log.debug(_evt("heartbeat_error", exchange=..., error_msg=str(exc)))`

### Step 4.3 — Tests

- [ ] Add `TestCollector{Exchange}Stats` class to `tests/test_other_parsers.py` for each exchange, mirroring `TestCollectorBinanceStats` from Task 2.1.

- [ ] Run:

```bash
cd /root/bali5.0 && python -m pytest tests/test_other_parsers.py tests/test_binance_parsers.py -v
```

Expected: all PASS.

- [ ] Commit per exchange (4 commits):

```bash
git add collectors/collector_bybit.py && git commit -m "feat: JSON metrics in collector_bybit"
git add collectors/collector_okx.py   && git commit -m "feat: JSON metrics in collector_okx"
git add collectors/collector_gate.py  && git commit -m "feat: JSON metrics in collector_gate"
git add collectors/collector_bitget.py tests/test_other_parsers.py && git commit -m "feat: JSON metrics in collector_bitget + stats tests"
```

---

## Task 5: JSON Events in spread_monitor.py

**Files:**
- Modify: `monitors/spread_monitor.py`
- Modify: `tests/test_spread_logic.py`

### Step 5.1 — Add timing split + `_evt()` + p99 helper

- [ ] At top of `monitors/spread_monitor.py`, after imports, add:

```python
_COMPONENT = "spread_monitor"

def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}

def _p99(values: list) -> float:
    """Compute 99th percentile. Uses statistics.quantiles (Python 3.8+)."""
    from statistics import quantiles
    if len(values) < 2:
        return round(values[0], 1) if values else 0.0
    return round(quantiles(values, n=100)[98], 1)  # index 98 = 99th percentile
```

- [ ] Split cycle timing into `pipeline_lat_ms` + `calc_lat_ms` (see spec section 5.4).

### Step 5.2 — Add `spread_summary` state

- [ ] Add a local aggregate dict that resets every 30s:

```python
_summary = {
    "cycles": 0,
    "cycle_lat_sum": 0.0,
    "cycle_lat_max": 0.0,
    "cycle_lats": [],       # list of floats for p99 calculation
    "slow_cycles": 0,
    "signals": 0,
    "no_data_sum": 0,
    "stale_sum": 0,
    "_last_summary_ts": time.monotonic(),
}
SUMMARY_INTERVAL = 30.0
```

- [ ] Each cycle, append to `_summary["cycle_lats"]`:

```python
_summary["cycle_lats"].append(elapsed_ms)
```

### Step 5.3 — Replace log calls

```python
# spread_start
log.info(_evt("spread_start",
              pairs=len(pairs),
              threshold_pct=config.SPREAD_THRESHOLD,
              poll_interval_ms=config.SPREAD_POLL_INTERVAL_MS,
              stale_threshold_ms=config.SPREAD_DATA_STALE_MS))

# signal
log.info(_evt("signal",
              spot_exchange=spot_exch, fut_exchange=fut_exch, symbol=symbol,
              ask_spot=float(ask_spot), bid_fut=float(bid_fut),
              spread_pct=round(spread_pct, 4),
              data_age_spot_ms=int(now_ms - ts_spot),
              data_age_fut_ms=int(now_ms - ts_fut),
              cooldown_applied=bool(not cooldown_ok),
              emit_lat_ms=round(emit_lat_ms, 1)))

# cycle (DEBUG level — every cycle)
log.debug(_evt("cycle",
               pairs_total=len(pairs), pairs_ok=pairs_ok,
               pairs_no_data=no_data_count, pairs_stale=stale_count,
               signals=signals_count,
               cycle_lat_ms=round(elapsed_ms, 1),
               pipeline_lat_ms=round(pipeline_lat_ms, 1),
               calc_lat_ms=round(calc_lat_ms, 1)))

# cycle_slow (WARNING)
log.warning(_evt("cycle_slow",
                 cycle_lat_ms=round(elapsed_ms, 1), threshold_ms=250.0,
                 pipeline_lat_ms=round(pipeline_lat_ms, 1),
                 calc_lat_ms=round(calc_lat_ms, 1)))

# spread_summary (INFO, every 30s)
log.info(_evt("spread_summary",
              interval_s=SUMMARY_INTERVAL,
              cycles=_summary["cycles"],
              cycle_lat_avg_ms=round(_summary["cycle_lat_sum"] / (_summary["cycles"] or 1), 1),
              cycle_lat_max_ms=round(_summary["cycle_lat_max"], 1),
              cycle_lat_p99_ms=_p99(_summary["cycle_lats"]),
              slow_cycles=_summary["slow_cycles"],
              signals_total=_summary["signals"],
              pairs_no_data_avg=round(_summary["no_data_sum"] / (_summary["cycles"] or 1), 1),
              pairs_stale_avg=round(_summary["stale_sum"] / (_summary["cycles"] or 1), 1)))
# reset lats list after emit
_summary["cycle_lats"] = []

# no_pairs
log.warning(_evt("no_pairs",
                 combination_dir=str(config.COMBINATION_DIR),
                 retry_in_s=60))

# redis_error
log.error(_evt("redis_error",
               error_type=type(exc).__name__, error_msg=str(exc)))

# spread_stop
log.info(_evt("spread_stop", reason="KeyboardInterrupt"))
```

### Step 5.4 — Tests

- [ ] Add to `tests/test_spread_logic.py`:

```python
class TestSpreadMonitorEvents:
    def test_signal_event_is_valid_json(self):
        """_evt('signal') produces valid JSON with required fields."""
        import json
        from monitors.spread_monitor import _evt
        evt = _evt("signal", spot_exchange="binance", fut_exchange="bybit",
                   symbol="BTCUSDT", ask_spot=45000.10, bid_fut=45676.35,
                   spread_pct=1.5023, data_age_spot_ms=120, data_age_fut_ms=85,
                   cooldown_applied=False, emit_lat_ms=1.2)
        obj = json.loads(json.dumps(evt))
        assert obj["event"] == "signal"
        assert obj["component"] == "spread_monitor"
        assert "spread_pct" in obj
        assert "ts" in obj
```

- [ ] Run:

```bash
cd /root/bali5.0 && python -m pytest tests/test_spread_logic.py -v
```

Expected: all PASS.

- [ ] Commit:

```bash
cd /root/bali5.0
git add monitors/spread_monitor.py tests/test_spread_logic.py
git commit -m "feat: JSON metrics in spread_monitor with pipeline/calc split and summary"
```

---

## Task 6: JSON Events in redis_monitor.py

**Files:**
- Modify: `monitors/redis_monitor.py`

### Step 6.1 — Add `_evt()` and replace log calls

- [ ] Add `_COMPONENT = "redis_monitor"` and `_evt()` factory.

- [ ] Replace `monitor_loop` log calls:

```python
# redis_health (main OK log)
warnings = []
if mem_mb > config.REDIS_MEMORY_WARN_MB:
    warnings.append("memory_high")
if ops_sec > config.REDIS_OPS_WARN_PER_SEC:
    warnings.append("ops_high")
if frag_ratio > 1.5:
    warnings.append("fragmentation_high")
if blocked > config.REDIS_BLOCKED_WARN_THRESHOLD:
    warnings.append("blocked_clients")

log.info(_evt("redis_health",
    status="warn" if warnings else "ok",
    memory={"used_mb": round(mem_mb, 1), "rss_mb": round(mem_rss_mb, 1),
            "peak_mb": round(mem_peak_mb, 1), "frag_ratio": round(frag_ratio, 2)},
    ops={"per_sec": ops_sec, "clients": clients, "blocked": blocked,
         "keys": total_keys, "hit_rate_pct": round(hit_rate, 1)},
    network={"in_kbps": int(net_in_kbps), "out_kbps": int(net_out_kbps)},
    latency={"ping_ms": round(elapsed_ms, 1), "eventloop_us": el_us,
             "lpush_p99_us": lpush_p99, "hset_p99_us": hset_p99},
    warnings=warnings))

# per-warning events
for w in warnings:
    log.warning(_evt("redis_warn", warning=w,
                     value={"memory_high": mem_mb, "ops_high": ops_sec,
                            "fragmentation_high": frag_ratio,
                            "blocked_clients": blocked}[w],
                     threshold={"memory_high": config.REDIS_MEMORY_WARN_MB,
                                "ops_high": config.REDIS_OPS_WARN_PER_SEC,
                                "fragmentation_high": 1.5,
                                "blocked_clients": config.REDIS_BLOCKED_WARN_THRESHOLD}[w]))

# redis_unreachable
log.error(_evt("redis_unreachable",
               error_type=type(exc).__name__, error_msg=str(exc)))

# redis_monitor_start
log.info(_evt("redis_monitor_start",
              interval_s=config.REDIS_CHECK_INTERVAL,
              memory_warn_mb=config.REDIS_MEMORY_WARN_MB,
              ops_warn_per_sec=config.REDIS_OPS_WARN_PER_SEC,
              frag_warn=1.5))

# redis_monitor_stop
log.info(_evt("redis_monitor_stop", reason="KeyboardInterrupt"))
```

- [ ] Smoke test — import check:

```bash
cd /root/bali5.0 && python -c "from monitors.redis_monitor import _evt; import json; print(json.dumps(_evt('test')))"
```

Expected: valid JSON printed.

- [ ] Commit:

```bash
cd /root/bali5.0
git add monitors/redis_monitor.py
git commit -m "feat: JSON metrics in redis_monitor with per-warning events"
```

---

## Task 7: JSON Events in stale_monitor.py

**Files:**
- Modify: `monitors/stale_monitor.py`

### Step 7.1 — Add `_evt()` and replace log calls

```python
_COMPONENT = "stale_monitor"

def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}
```

Replace log calls:

```python
# stale_monitor_start
log.info(_evt("stale_monitor_start",
              threshold_s=config.STALE_THRESHOLD_SECONDS,
              interval_s=config.STALE_CHECK_INTERVAL))

# stale_key (one per stale key — WARNING)
log.warning(_evt("stale_key",
                 key=key.decode(),
                 age_s=round(age_s, 0),
                 last_ts=ts,
                 threshold_s=config.STALE_THRESHOLD_SECONDS))

# stale_scan (INFO — after each full scan)
log.info(_evt("stale_scan",
              total_keys=total,
              stale_keys=len(stale),
              scan_lat_ms=round(elapsed_ms, 0),
              threshold_s=config.STALE_THRESHOLD_SECONDS))

# stale_monitor_stop
log.info(_evt("stale_monitor_stop", reason="KeyboardInterrupt"))
```

- [ ] Smoke test:

```bash
cd /root/bali5.0 && python -c "from monitors.stale_monitor import _evt; import json; print(json.dumps(_evt('stale_scan', total_keys=100, stale_keys=2, scan_lat_ms=230.0)))"
```

- [ ] Commit:

```bash
cd /root/bali5.0
git add monitors/stale_monitor.py
git commit -m "feat: JSON metrics in stale_monitor"
```

---

## Task 8: JSON Events in snapshot_monitor.py

**Files:**
- Modify: `monitors/snapshot_monitor.py`

### Step 8.1 — Add `_evt()`, `_p99()`, and replace log calls

```python
_COMPONENT = "snapshot_monitor"

def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}

def _p99(values: list) -> float:
    """Compute 99th percentile. Uses statistics.quantiles (Python 3.8+)."""
    from statistics import quantiles
    if len(values) < 2:
        return round(values[0], 1) if values else 0.0
    return round(quantiles(values, n=100)[98], 1)  # index 98 = 99th percentile
```

- [ ] Add row latency tracking list at snapshot scope (inside `run_snapshot()`, before recording loop):

```python
row_lats: list[float] = []
# After each row write: row_lats.append(row_lat_ms)
```

Replace every log call per the spec events. Key replacements:

```python
# snapshot_monitor_start
log.info(_evt("snapshot_monitor_start",
              duration_s=config.SNAPSHOT_DURATION,
              interval_ms=config.SNAPSHOT_INTERVAL_MS,
              lookback_min=config.HISTORY_LOOKBACK_MS // 60000,
              stream_resume_id=last_id))

# signal_received — compute stream_lag_ms
signal_ts = int(signal_str.split(",")[-1])  # last field is ts
stream_lag_ms = int(time.time() * 1000) - signal_ts
log.info(_evt("signal_received",
              stream_id=msg_id_str,
              spot_exchange=spot_ex, fut_exchange=fut_ex, symbol=symbol,
              spread_pct=float(spread_pct_str),
              signal_ts=signal_ts,
              stream_lag_ms=stream_lag_ms))

# snapshot_start
log.info(_evt("snapshot_start",
              symbol=symbol,
              spot_exchange=spot_ex, fut_exchange=fut_ex,
              file=str(fname)))

# history_load_start
log.info(_evt("history_load_start",
              symbol=symbol,
              lookback_ms=config.HISTORY_LOOKBACK_MS,
              chunks_to_load=config.MAX_HISTORY_CHUNKS))

# history_loaded
log.info(_evt("history_loaded",
              symbol=symbol,
              rows_total=len(history_rows),
              rows_md=rows_md, rows_ob=rows_ob, rows_fr=rows_fr,
              chunks_loaded=config.MAX_HISTORY_CHUNKS,
              load_lat_ms=round(load_lat_ms, 1),
              merge_lat_ms=round(merge_lat_ms, 1)))

# snapshot_progress (every 60s during recording)
log.info(_evt("snapshot_progress",
              symbol=symbol,
              elapsed_s=round(elapsed_s, 0),
              rows_written=row_count,
              row_lat_avg_ms=round(row_lat_sum / (row_count or 1), 1),
              row_lat_max_ms=round(row_lat_max, 1),
              row_lat_last_ms=round(row_lat, 1),
              remaining_s=round(config.SNAPSHOT_DURATION - elapsed_s, 0)))

# snapshot_row_slow (new — if row_lat_ms > 100)
if row_lat_ms > 100.0:
    log.warning(_evt("snapshot_row_slow",
                     symbol=symbol,
                     row_lat_ms=round(row_lat_ms, 1),
                     threshold_ms=100.0,
                     row_number=row_count))

# snapshot_complete
log.info(_evt("snapshot_complete",
              symbol=symbol, file=str(fname),
              rows=row_count, elapsed_s=round(elapsed_s, 0),
              row_lat_avg_ms=round(row_lat_sum / (row_count or 1), 1),
              row_lat_max_ms=round(row_lat_max, 1),
              row_lat_p99_ms=_p99(row_lats),
              slow_rows=sum(1 for l in row_lats if l > 100.0)))

# snapshot_error
log.warning(_evt("snapshot_error",
                 symbol=symbol, row_number=row_count,
                 error_type=type(exc).__name__, error_msg=str(exc)))

# stream_read (DEBUG)
log.debug(_evt("stream_read",
               stream=config.STREAM_SIGNALS,
               messages=len(messages),
               block_ms=2000))

# snapshot_monitor_stop
log.info(_evt("snapshot_monitor_stop", reason="KeyboardInterrupt"))
```

- [ ] Run existing snapshot tests:

```bash
cd /root/bali5.0 && python -m pytest tests/test_snapshot_csv.py -v
```

Expected: all PASS.

- [ ] Commit:

```bash
cd /root/bali5.0
git add monitors/snapshot_monitor.py
git commit -m "feat: JSON metrics in snapshot_monitor with stream_lag and history breakdown"
```

---

## Task 9: JSON Events in launcher.py

**Files:**
- Modify: `launcher.py`
- Modify: `tests/test_launcher.py`

### Step 9.1 — Add `_evt()` and replace log calls

```python
_COMPONENT = "launcher"

def _evt(event: str, **kwargs) -> dict:
    import time
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}
```

Key replacements:

```python
# launcher_start
log.info(_evt("launcher_start",
              history_enabled=config.HISTORY_ENABLED,
              processes=list(PROCESSES.keys())))

# redis_ready
log.info(_evt("redis_ready", url=config.REDIS_URL, attempt=attempt,
              elapsed_ms=round(elapsed_ms, 1)))

# redis_unavailable (WARNING → repeated attempts)
log.warning(_evt("redis_unavailable", url=config.REDIS_URL,
                 attempt=attempt, error_msg=str(exc)))

# redis_flushed
log.info(_evt("redis_flushed", keys_removed=count))

# process_started
log.info(_evt("process_started", name=name, pid=p.pid, script=script))

# process_died
log.error(_evt("process_died", name=name, pid=p.pid,
               exit_code=p.returncode, restart_attempt=restart_counts[name]))

# process_restarted
log.info(_evt("process_restarted", name=name, pid=new_p.pid,
              restart_attempt=restart_counts[name]))

# shutdown
log.info(_evt("shutdown", signal=signum,
              processes_terminated=terminated, processes_killed=killed))
```

- [ ] Run existing launcher tests:

```bash
cd /root/bali5.0 && python -m pytest tests/test_launcher.py -v
```

Expected: all PASS.

- [ ] Commit:

```bash
cd /root/bali5.0
git add launcher.py tests/test_launcher.py
git commit -m "feat: JSON metrics in launcher — all lifecycle events"
```

---

## Task 10: End-to-End Validation

**Verify the full pipeline produces valid NDJSON.**

### Step 10.1 — Full test suite

- [ ] Run all tests:

```bash
cd /root/bali5.0 && python -m pytest tests/ -v 2>&1 | tail -30
```

Expected: all tests PASS (or same failures as before this feature).

### Step 10.2 — NDJSON format validation script

- [ ] Create a one-shot validator (temporary, not committed):

```bash
cd /root/bali5.0 && python -c "
import json, sys, time
sys.path.insert(0, '.')

# Simulate all _evt functions across components
components = {
    'collector_binance': 'collectors.collector_binance',
    'spread_monitor':    'monitors.spread_monitor',
    'redis_monitor':     'monitors.redis_monitor',
    'stale_monitor':     'monitors.stale_monitor',
    'snapshot_monitor':  'monitors.snapshot_monitor',
}
for name, mod_path in components.items():
    mod = __import__(mod_path, fromlist=['_evt'])
    evt = mod._evt('test_event', value=42)
    line = json.dumps(evt)
    parsed = json.loads(line)
    assert parsed['component'] == name, f'Wrong component in {name}'
    assert parsed['event'] == 'test_event'
    assert 'ts' in parsed
    print(f'OK: {name}')
print('All components produce valid NDJSON.')
"
```

Expected: `OK: collector_binance`, ..., `All components produce valid NDJSON.`

### Step 10.3 — Verify jq compatibility

```bash
# Check that logs can be queried with jq once populated
cd /root/bali5.0 && python -c "
import json, time
lines = [
    {'ts': int(time.time()*1000), 'component': 'collector_binance', 'event': 'metrics_interval', 'ingestion': {'md_msgs_per_s': 1200.0}},
    {'ts': int(time.time()*1000), 'component': 'spread_monitor', 'event': 'signal', 'spread_pct': 1.5},
]
ndjson = '\n'.join(json.dumps(l) for l in lines)
print(ndjson)
" | python -c "
import json, sys
for line in sys.stdin:
    obj = json.loads(line)
    print(f'  event={obj[\"event\"]} component={obj[\"component\"]}')
"
```

Expected: two lines printed, one per event.

### Step 10.4 — Final commit

```bash
cd /root/bali5.0
git add .
git commit -m "feat: complete NDJSON metrics implementation — all components"
```

---

## Summary

| Task | Files Changed | New Tests | Commits |
|------|---------------|-----------|---------|
| 1 — JsonFormatter | `logger_setup.py` | `test_json_logger.py` (6 tests) | 1 |
| 2 — Binance stats | `collector_binance.py` | `test_binance_parsers.py` (+3 tests) | 1 |
| 3 — Binance events | `collector_binance.py` | — | 1 |
| 4 — Other 4 collectors | 4 collector files | `test_other_parsers.py` (+12 tests) | 4 |
| 5 — spread_monitor | `spread_monitor.py` | `test_spread_logic.py` (+1 test) | 1 |
| 6 — redis_monitor | `redis_monitor.py` | — | 1 |
| 7 — stale_monitor | `stale_monitor.py` | — | 1 |
| 8 — snapshot_monitor | `snapshot_monitor.py` | — | 1 |
| 9 — launcher | `launcher.py` | `test_launcher.py` | 1 |
| 10 — E2E validation | — | — | 1 |
| **Total** | **11 files** | **22+ new tests** | **13 commits** |
