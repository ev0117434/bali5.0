# JSON Metrics Schema — BALI 5.0
**Date:** 2026-03-23
**Status:** DRAFT
**Scope:** Replace all text log lines with NDJSON. One JSON object per line. Every `logs/*.log` becomes a machine-readable NDJSON stream.

---

## 1. Design Principles

| Principle | Rule |
|-----------|------|
| **Format** | NDJSON — one `json.dumps({})` per `logging.info()` call |
| **Envelope** | All events share `ts`, `component`, `event` fields |
| **Timestamps** | `ts` always Unix milliseconds (integer) |
| **Latencies** | Always in milliseconds (float, 1 decimal) |
| **Rates** | Always per-second (float, 1 decimal) |
| **No nulls** | Omit optional fields if absent instead of `null` |
| **Additive** | New fields (marked `[NEW]`) instrument gaps in current code |
| **Levels** | `logging.info` for normal, `logging.warning` for thresholds, `logging.error` for failures — log level preserved alongside JSON body |

---

## 2. Universal Envelope

Every single log line is a JSON object with this base:

```json
{
  "ts":        1741234567890,
  "component": "collector_binance",
  "event":     "metrics_interval"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ts` | `int` | Unix timestamp, milliseconds |
| `component` | `str` | Process name: `collector_binance`, `collector_bybit`, `collector_okx`, `collector_gate`, `collector_bitget`, `spread_monitor`, `redis_monitor`, `stale_monitor`, `snapshot_monitor`, `launcher` |
| `event` | `str` | Event type slug (see catalog below) |

All event-specific fields are merged into the top-level object (flat, no nesting by default except where noted).

---

## 3. Pipeline Latency Map

Full end-to-end pipeline with all measurable latency points:

```
Exchange Server
     │
     │  [L1] WS transport latency (unmeasurable without clock sync)
     ▼
WS recv() call
     │
     │  [L2] parse_lat_ms  ← [NEW] time to parse + normalize one message
     ▼
batch_buffer write
     │
     │  [L3] buffer_age_ms ← [NEW] max time data waits before flush
     ▼
Primary Flush (pipeline.execute)
     │
     │  [L4] flush_lat_ms  ← EXISTING
     ▼
Redis HSET committed
     │
     │  [L5] e2e_lat_ms    ← [NEW] exchange_ts → local write_ts delta
     ▼
Spread Monitor HMGET pipeline
     │
     │  [L6] pipeline_lat_ms ← [NEW] split from cycle_lat_ms
     ▼
Spread calculation
     │
     │  [L7] calc_lat_ms   ← [NEW] split from cycle_lat_ms
     ▼
Signal emitted (XADD + CSV write)
     │
     │  [L8] emit_lat_ms   ← [NEW] time from threshold detection to XADD
     ▼
snapshot_monitor XREAD receives signal
     │
     │  [L9] stream_lag_ms ← [NEW] signal_ts → xread_receive delta
     ▼
History load
     │
     │  [L10] history_load_lat_ms ← EXISTING (only in text)
     ▼
Snapshot row write
     │
     │  [L11] row_lat_ms ← EXISTING
```

---

## 4. Event Catalog

### 4.1 LAUNCHER EVENTS

---

#### `launcher_start`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "launcher_start",
  "history_enabled": false,
  "processes": ["collector_binance", "collector_bybit", "collector_okx", "collector_gate", "collector_bitget", "redis_monitor", "stale_monitor", "spread_monitor"]
}
```

---

#### `redis_ready`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "redis_ready",
  "url": "redis://localhost:6379",
  "attempt": 1,
  "elapsed_ms": 12.3
}
```

---

#### `redis_unavailable`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "redis_unavailable",
  "url": "redis://localhost:6379",
  "attempt": 10,
  "error": "ConnectionRefusedError: [Errno 111] Connection refused"
}
```

---

#### `redis_flushed`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "redis_flushed",
  "keys_removed": 84312
}
```

---

#### `process_started`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "process_started",
  "name": "collector_binance",
  "pid": 12345,
  "script": "collectors/collector_binance.py"
}
```

---

#### `process_died`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "process_died",
  "name": "collector_binance",
  "pid": 12345,
  "exit_code": 1,
  "restart_attempt": 3
}
```

---

#### `process_restarted`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "process_restarted",
  "name": "collector_binance",
  "pid": 12346,
  "restart_attempt": 3
}
```

---

#### `shutdown`
```json
{
  "ts": 1741234567890,
  "component": "launcher",
  "event": "shutdown",
  "signal": "SIGTERM",
  "processes_terminated": ["collector_binance", "collector_bybit"],
  "processes_killed": []
}
```

---

### 4.2 COLLECTOR EVENTS (all 5 exchanges, identical schema)

---

#### `collector_start`
```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "collector_start",
  "exchange": "binance",
  "spot_symbols": 1200,
  "fut_symbols": 380,
  "history_enabled": false
}
```

---

#### `ws_connect`
```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "ws_connect",
  "exchange": "binance",
  "stream": "md_spot",
  "symbols": 1200,
  "url_prefix": "wss://stream.binance.com"
}
```

`stream` values: `md_spot`, `md_fut`, `ob_spot`, `ob_fut`, `fr`

---

#### `ws_disconnect`
```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "ws_disconnect",
  "exchange": "binance",
  "stream": "md_spot",
  "error_type": "ConnectionClosedError",
  "error_msg": "no close frame received or sent",
  "reconnect_backoff_s": 2.0,
  "reconnects_total": 3
}
```

---

#### `redis_error`
```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "redis_error",
  "exchange": "binance",
  "pipeline_type": "primary",
  "error_type": "ConnectionError",
  "error_msg": "Error 111 connecting to localhost:6379"
}
```

`pipeline_type`: `primary` | `history`

---

#### `slow_flush`
```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "slow_flush",
  "exchange": "binance",
  "flush_type": "primary",
  "lat_ms": 67.3,
  "threshold_ms": 50.0,
  "cmds": 97
}
```

`flush_type`: `primary` | `history`

---

#### `chunk_rotate`
```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "chunk_rotate",
  "exchange": "binance",
  "chunk_id_prev": 1450752,
  "chunk_id_new": 1450753,
  "expire_keys_reset": 4241
}
```

---

#### `parse_error` [NEW]
Fires when a message is silently dropped due to parse failure.

```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "parse_error",
  "exchange": "binance",
  "stream": "ob_spot",
  "error_type": "KeyError",
  "error_msg": "'bids'",
  "raw_preview": "{\"e\":\"depthUpdate\",\"s\":\"BTCUSDT\"..."
}
```

---

#### `metrics_interval`

Emitted every `METRICS_LOG_INTERVAL` seconds (default 5s). All rate fields are per-second averages over the interval.

```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "metrics_interval",
  "exchange": "binance",
  "interval_s": 5.0,

  "ingestion": {
    "md_msgs":         6024,
    "md_msgs_per_s":   1204.8,
    "ob_msgs":         3987,
    "ob_msgs_per_s":   797.4,
    "fr_msgs":         1012,
    "fr_msgs_per_s":   202.4,
    "parse_errors":    0,
    "parse_errors_per_s": 0.0
  },

  "primary_flush": {
    "count":           20,
    "count_per_s":     4.0,
    "batch_avg":       85.3,
    "lat_avg_ms":      12.5,
    "lat_max_ms":      45.2,
    "slow_count":      0
  },

  "history_flush": {
    "count":           17,
    "count_per_s":     3.4,
    "cmds_total":      142000,
    "cmds_per_s":      28400.0,
    "lat_avg_ms":      22.3,
    "lat_max_ms":      98.1,
    "slow_count":      1,
    "ob_skipped":      251,
    "ob_skipped_per_s": 50.2
  },

  "latency": {
    "parse_avg_us":    145.0,
    "parse_max_us":    890.0,
    "buffer_age_avg_ms": 87.3,
    "buffer_age_max_ms": 248.1,
    "e2e_avg_ms":      23.4,
    "e2e_max_ms":      112.7
  },

  "state": {
    "expire_keys_tracked": 4200,
    "reconnects":      2,
    "active_streams":  5
  }
}
```

**Field glossary:**

| Field | Source | Status |
|-------|--------|--------|
| `ingestion.md_msgs` | `stats["md_msgs"]` | EXISTING |
| `ingestion.ob_msgs` | `stats["ob_msgs"]` | EXISTING |
| `ingestion.fr_msgs` | `stats["fr_msgs"]` | EXISTING |
| `ingestion.parse_errors` | `stats["parse_errors"]` | **[NEW]** |
| `primary_flush.count` | `stats["flushes"]` | EXISTING |
| `primary_flush.batch_avg` | `stats["batch_sum"] / count` | EXISTING |
| `primary_flush.lat_avg_ms` | `stats["flush_lat_sum"] / count` | EXISTING |
| `primary_flush.lat_max_ms` | `stats["flush_lat_max"]` | EXISTING |
| `primary_flush.slow_count` | `stats["flush_slow_count"]` | **[NEW]** |
| `history_flush.count` | `stats["hist_flushes"]` | EXISTING |
| `history_flush.cmds_total` | `stats["hist_cmds"]` | EXISTING |
| `history_flush.lat_avg_ms` | `stats["hist_flush_lat_sum"] / count` | EXISTING |
| `history_flush.lat_max_ms` | `stats["hist_flush_lat_max"]` | EXISTING |
| `history_flush.slow_count` | `stats["hist_flush_slow_count"]` | **[NEW]** |
| `history_flush.ob_skipped` | `stats["ob_hist_skipped"]` | EXISTING |
| `latency.parse_avg_us` | `stats["parse_lat_sum"] / msgs` | **[NEW]** |
| `latency.parse_max_us` | `stats["parse_lat_max"]` | **[NEW]** |
| `latency.buffer_age_avg_ms` | `stats["buffer_age_sum"] / flushes` | **[NEW]** |
| `latency.buffer_age_max_ms` | `stats["buffer_age_max"]` | **[NEW]** |
| `latency.e2e_avg_ms` | `stats["e2e_lat_sum"] / msgs` | **[NEW]** (requires exchange ts) |
| `latency.e2e_max_ms` | `stats["e2e_lat_max"]` | **[NEW]** |
| `state.expire_keys_tracked` | `len(expire_set)` | EXISTING |
| `state.reconnects` | `stats["reconnects"]` | EXISTING |
| `state.active_streams` | count of live WS tasks | **[NEW]** |

> **Note on `e2e_avg_ms`:** Only available for exchanges that provide message timestamps (Bybit, OKX, Bitget, Gate.io). Binance bookTicker has no timestamp — use `None` for that stream.

---

#### `collector_stop`
```json
{
  "ts": 1741234567890,
  "component": "collector_binance",
  "event": "collector_stop",
  "exchange": "binance",
  "reason": "KeyboardInterrupt"
}
```

---

### 4.3 SPREAD MONITOR EVENTS

---

#### `spread_start`
```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "spread_start",
  "pairs": 4012,
  "threshold_pct": 1.0,
  "poll_interval_ms": 300,
  "stale_threshold_ms": 5000
}
```

---

#### `signal`
Fires on each signal crossing threshold.

```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "signal",
  "spot_exchange": "binance",
  "fut_exchange": "bybit",
  "symbol": "BTCUSDT",
  "ask_spot": 45000.10,
  "bid_fut":  45676.35,
  "spread_pct": 1.5023,
  "data_age_spot_ms": 120,
  "data_age_fut_ms":  85,
  "cooldown_applied": false,
  "emit_lat_ms": 1.2
}
```

| Field | Description | Status |
|-------|-------------|--------|
| `spread_pct` | `(bid_fut - ask_spot) / ask_spot * 100` | EXISTING |
| `data_age_spot_ms` | `now - ts_spot` | **[NEW]** |
| `data_age_fut_ms` | `now - ts_fut` | **[NEW]** |
| `cooldown_applied` | True if SET NX returned 0 (cooldown active, signal suppressed) | **[NEW]** |
| `emit_lat_ms` | Time from threshold detection to XADD completion | **[NEW]** |

---

#### `cycle` [NEW — replaces DEBUG cycle text]
One record per 300ms spread monitor cycle. Emitted only if `elapsed_ms > 0` to avoid log flood; log at DEBUG level.

```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "cycle",
  "pairs_total":    4012,
  "pairs_ok":       3940,
  "pairs_no_data":  52,
  "pairs_stale":    20,
  "signals":        3,
  "cycle_lat_ms":   87.3,
  "pipeline_lat_ms": 65.1,
  "calc_lat_ms":    22.2
}
```

| Field | Description | Status |
|-------|-------------|--------|
| `cycle_lat_ms` | Full cycle wall time | EXISTING (text only) |
| `pipeline_lat_ms` | Redis pipeline portion only | **[NEW]** |
| `calc_lat_ms` | Spread calculation portion only | **[NEW]** |

---

#### `cycle_slow`
Fires when `cycle_lat_ms > 250ms`.

```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "cycle_slow",
  "cycle_lat_ms":    267.4,
  "threshold_ms":    250.0,
  "pipeline_lat_ms": 240.1,
  "calc_lat_ms":     27.3
}
```

---

#### `spread_summary` [NEW]
Periodic aggregate over last 30s. Replaces the gap where spread monitor had no periodic health log.

```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "spread_summary",
  "interval_s":       30.0,
  "cycles":           100,
  "cycle_lat_avg_ms": 87.3,
  "cycle_lat_max_ms": 234.1,
  "cycle_lat_p99_ms": 198.4,
  "slow_cycles":      2,
  "signals_total":    7,
  "pairs_no_data_avg": 50.2,
  "pairs_stale_avg":  18.7
}
```

---

#### `no_pairs`
```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "no_pairs",
  "combination_dir": "dictionaries/combination",
  "retry_in_s": 60
}
```

---

#### `redis_error`
```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "redis_error",
  "error_type": "ConnectionError",
  "error_msg": "Error 111 connecting to localhost:6379"
}
```

---

#### `spread_stop`
```json
{
  "ts": 1741234567890,
  "component": "spread_monitor",
  "event": "spread_stop",
  "reason": "KeyboardInterrupt"
}
```

---

### 4.4 REDIS MONITOR EVENTS

---

#### `redis_health`
Fires every 30s. Replaces the current `REDIS OK | ...` text line.

```json
{
  "ts": 1741234567890,
  "component": "redis_monitor",
  "event": "redis_health",
  "status": "ok",

  "memory": {
    "used_mb":    1234.5,
    "rss_mb":     1456.7,
    "peak_mb":    1800.0,
    "frag_ratio": 1.18
  },

  "ops": {
    "per_sec":    145000,
    "clients":    8,
    "blocked":    1,
    "keys":       84200,
    "hit_rate_pct": 99.7
  },

  "network": {
    "in_kbps":    450,
    "out_kbps":   3200
  },

  "latency": {
    "ping_ms":         0.4,
    "eventloop_us":    125,
    "lpush_p99_us":    45,
    "hset_p99_us":     38
  },

  "warnings": []
}
```

`status`: `"ok"` | `"warn"` | `"error"`
`warnings`: array of strings from: `"memory_high"`, `"ops_high"`, `"fragmentation_high"`, `"blocked_clients"`

---

#### `redis_warn`
One event per threshold violation (instead of one REDIS OK line with flags buried inside).

```json
{
  "ts": 1741234567890,
  "component": "redis_monitor",
  "event": "redis_warn",
  "warning": "memory_high",
  "value": 3241.0,
  "threshold": 3000.0,
  "unit": "MB"
}
```

`warning` values: `memory_high` | `ops_high` | `fragmentation_high` | `blocked_clients`

---

#### `redis_unreachable`
```json
{
  "ts": 1741234567890,
  "component": "redis_monitor",
  "event": "redis_unreachable",
  "error_type": "ConnectionError",
  "error_msg": "Error 111 connecting to localhost:6379"
}
```

---

### 4.5 STALE MONITOR EVENTS

---

#### `stale_scan`
Fires after each full SCAN. Replaces `Stale check done: total_keys=X stale=X elapsed=Xms`.

```json
{
  "ts": 1741234567890,
  "component": "stale_monitor",
  "event": "stale_scan",
  "total_keys":    12450,
  "stale_keys":    3,
  "scan_lat_ms":   234.0,
  "threshold_s":   360
}
```

---

#### `stale_key`
One event per stale key found. Replaces `STALE | key=... age=Xs` WARNING lines.

```json
{
  "ts": 1741234567890,
  "component": "stale_monitor",
  "event": "stale_key",
  "key": "md:gate:spot:XYZUSDT",
  "age_s": 412.0,
  "last_ts": 1741234155000,
  "threshold_s": 360
}
```

---

### 4.6 SNAPSHOT MONITOR EVENTS

---

#### `snapshot_monitor_start`
```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "snapshot_monitor_start",
  "duration_s":     3500,
  "interval_ms":    300,
  "lookback_min":   60,
  "stream_resume_id": "1741230000000-0"
}
```

---

#### `stream_read` [NEW]
Fires each time XREAD returns (including empty reads). Log at DEBUG level.

```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "stream_read",
  "stream": "stream:signals",
  "messages": 1,
  "block_ms": 2000
}
```

---

#### `signal_received`
```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "signal_received",
  "stream_id": "1741234567890-0",
  "spot_exchange": "binance",
  "fut_exchange": "bybit",
  "symbol": "BTCUSDT",
  "spread_pct": 1.5023,
  "signal_ts": 1741234567000,
  "stream_lag_ms": 890
}
```

| Field | Description | Status |
|-------|-------------|--------|
| `stream_lag_ms` | `now - signal_ts` — how stale the signal is when received | **[NEW]** |

---

#### `snapshot_start`
```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "snapshot_start",
  "symbol": "BTCUSDT",
  "spot_exchange": "binance",
  "fut_exchange": "bybit",
  "file": "snapshots/binance_bybit_BTCUSDT_1741234567890.csv"
}
```

---

#### `history_load_start` [NEW]
```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "history_load_start",
  "symbol": "BTCUSDT",
  "lookback_ms": 3600000,
  "chunks_to_load": 4
}
```

---

#### `history_loaded`
```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "history_loaded",
  "symbol": "BTCUSDT",
  "rows_total": 72000,
  "rows_md": 36000,
  "rows_ob": 28000,
  "rows_fr": 8000,
  "chunks_loaded": 4,
  "load_lat_ms": 1234.5,
  "merge_lat_ms": 234.1,
  "lookback_actual_ms": 3594210
}
```

| Field | Description | Status |
|-------|-------------|--------|
| `rows_md/ob/fr` | Breakdown by data type | **[NEW]** |
| `merge_lat_ms` | Time to merge+ffill after loading | **[NEW]** |
| `lookback_actual_ms` | Actual history coverage vs requested | **[NEW]** |

---

#### `snapshot_progress` [NEW — replaces periodic print in current code]
Fires every 60s during recording. Replaces `Snapshot SYMBOL: elapsed=Xs rows=N row_lat=Xms`.

```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "snapshot_progress",
  "symbol": "BTCUSDT",
  "elapsed_s": 300.0,
  "rows_written": 1000,
  "row_lat_avg_ms": 11.8,
  "row_lat_max_ms": 45.2,
  "row_lat_last_ms": 12.3,
  "remaining_s": 3200.0
}
```

---

#### `snapshot_row_slow` [NEW]
Fires when a single row write exceeds 100ms.

```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "snapshot_row_slow",
  "symbol": "BTCUSDT",
  "row_lat_ms": 143.2,
  "threshold_ms": 100.0,
  "row_number": 1047
}
```

---

#### `snapshot_complete`
```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "snapshot_complete",
  "symbol": "BTCUSDT",
  "file": "snapshots/binance_bybit_BTCUSDT_1741234567890.csv",
  "rows": 11667,
  "elapsed_s": 3500.0,
  "row_lat_avg_ms": 11.8,
  "row_lat_max_ms": 45.2,
  "row_lat_p99_ms": 28.7,
  "slow_rows": 3
}
```

---

#### `snapshot_error`
```json
{
  "ts": 1741234567890,
  "component": "snapshot_monitor",
  "event": "snapshot_error",
  "symbol": "BTCUSDT",
  "row_number": 524,
  "error_type": "OSError",
  "error_msg": "No space left on device"
}
```

---

## 5. Implementation Guide

### 5.1 Logger Setup (one-time per process)

```python
import logging
import json

class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON.
    The log record's 'msg' must already be a dict or json string."""
    def format(self, record):
        if isinstance(record.msg, dict):
            payload = record.msg
        else:
            # fallback for plain text (e.g. third-party libraries)
            payload = {"msg": record.getMessage()}
        payload.setdefault("level", record.levelname)
        return json.dumps(payload, ensure_ascii=False)

def setup_logger(name: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger
```

### 5.2 Emitting an Event

```python
# Instead of:
logger.info(f"METRICS | md={md_msgs}msg/s ...")

# Write:
logger.info({
    "ts": int(time.time() * 1000),
    "component": "collector_binance",
    "event": "metrics_interval",
    "exchange": "binance",
    "interval_s": 5.0,
    "ingestion": {"md_msgs": md_msgs, ...},
    ...
})
```

### 5.3 New Stats Fields Required in Collectors

Add these to the existing `stats` dict:

```python
stats = {
    # ... existing fields ...

    # [NEW] Parse error tracking
    "parse_errors": 0,

    # [NEW] Parse latency (microseconds for precision)
    "parse_lat_sum": 0.0,
    "parse_lat_max": 0.0,

    # [NEW] Slow flush counters
    "flush_slow_count": 0,
    "hist_flush_slow_count": 0,

    # [NEW] Buffer age tracking
    # Set buffer_write_ts[key] = time.monotonic() when writing to buffer
    # At flush time: age = flush_time - min(buffer_write_ts.values())
    "buffer_age_sum": 0.0,
    "buffer_age_max": 0.0,

    # [NEW] E2E latency (exchange timestamp vs local write time)
    # Only for exchanges that provide ts in message (Bybit, OKX, Bitget, Gate)
    "e2e_lat_sum": 0.0,
    "e2e_lat_max": 0.0,
    "e2e_lat_count": 0,
}
```

### 5.4 Spread Monitor Split Timing

```python
# Instead of timing the whole cycle:
t_cycle = time.monotonic()
results = await pipe.execute()         # pipeline
# ... calc ...                         # calculation
cycle_lat_ms = (time.monotonic() - t_cycle) * 1000

# Split into:
t_cycle = time.monotonic()
t_pipe = time.monotonic()
results = await pipe.execute()
pipeline_lat_ms = (time.monotonic() - t_pipe) * 1000
t_calc = time.monotonic()
# ... calc ...
calc_lat_ms = (time.monotonic() - t_calc) * 1000
cycle_lat_ms = (time.monotonic() - t_cycle) * 1000
```

---

## 6. Gaps Found in Current Instrumentation

| Gap | Impact | Fix |
|-----|--------|-----|
| No parse error counter | Silent message drops are invisible | Add `stats["parse_errors"]` in `except` blocks |
| No parse latency | Can't detect CPU bottleneck in hot path | Wrap parser calls with `time.monotonic()` |
| No buffer age metric | Can't detect data getting stale in buffer | Track `buffer_write_ts[key]` |
| No E2E latency | Can't measure actual pipeline delay | Use exchange `ts` field where available |
| No pipeline/calc split in spread monitor | Can't diagnose if Redis or Python is the bottleneck | Split timing (see 5.4) |
| No `cooldown_applied` flag on signals | Can't distinguish detected vs emitted signals | Check SET NX result |
| No stream lag metric | Can't detect snapshot_monitor falling behind | `now - signal_ts` after XREAD |
| No history breakdown by type | Can't see if MD/OB/FR load differently | Count `rows_md`, `rows_ob`, `rows_fr` |
| No periodic spread_summary | Spread monitor has no health heartbeat | Add 30s aggregate event |
| No snapshot row slow alerts | Silent degradation in snapshot quality | Threshold event at >100ms |
| No `active_streams` gauge | Can't detect if a WS task silently died | Count live asyncio tasks |

---

## 7. Improved Original Prompt

**Original (user's prompt):**
> "Дай мне все метрики всех скриптов, всё что только может попасть в лог"

**Transformed:**
> "Проанализируй архитектуру BALI 5.0 end-to-end: WebSocket ingestion → Redis buffering (primary + history) → spread detection → signal emission → snapshot recording. Для каждого логического этапа пайплайна: (1) идентифицируй все операции и состояния, (2) определи все точки измерения задержек — включая те, что сейчас не инструментированы, (3) спроектируй machine-readable JSON-схему метрик (NDJSON, один объект на строку) с единым envelope {ts, component, event}, покрывающую: periodic aggregate метрики (интервальные агрегаты по счётчикам и латентностям), event-driven метрики (per-event: reconnect, signal, slow_flush, parse_error), и lifecycle события (start, stop, restart). Укажи статус каждого поля: EXISTING (есть в коде) vs NEW (требует добавления). Выяви пробелы в текущем инструментировании."

---

## 8. Log File Format

After implementation, each log file is valid NDJSON:

```
{"ts":1741234567890,"component":"collector_binance","event":"collector_start","exchange":"binance","spot_symbols":1200,...}
{"ts":1741234567891,"component":"collector_binance","event":"ws_connect","exchange":"binance","stream":"md_spot",...}
{"ts":1741234567892,"component":"collector_binance","event":"ws_connect","exchange":"binance","stream":"md_fut",...}
...
{"ts":1741234572890,"component":"collector_binance","event":"metrics_interval","exchange":"binance","interval_s":5.0,...}
```

Parse with:
```bash
cat logs/collector_binance.log | jq 'select(.event == "metrics_interval") | .ingestion.md_msgs_per_s'
cat logs/spread_monitor.log    | jq 'select(.event == "signal")'
cat logs/redis_monitor.log     | jq 'select(.event == "redis_warn")'
```

---

*End of spec. Next step: invoke writing-plans to create implementation plan.*
