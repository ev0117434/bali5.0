# BALI 5.0 — Terminal Dashboard Design Spec

**Date:** 2026-03-23
**Branch:** data
**Status:** Approved (v2 — post-review fixes)

---

## Overview

A standalone TUI dashboard (`dashboard.py`) that reads BALI 5.0 log files in real-time and displays system health in a single-screen Mission Control layout. Runs independently of `launcher.py` — the user starts it in a separate terminal or tmux pane.

---

## Architecture

```
LogWatcher   →   StateParser   →   Textual App
(tail logs)      (parse into        (render every
                  state dicts)       2 seconds)
```

### Components

| Component | Responsibility |
|-----------|---------------|
| `LogWatcher` | Asyncio background task. Tails each log file from the end, feeds new lines to StateParser |
| `StateParser` | Regex-based parser. Maintains current state dicts for each exchange and monitor |
| `DashboardApp` | Textual `App` subclass. Auto-refreshes widgets every 2s |

### Log files consumed

| File | Data extracted |
|------|---------------|
| `logs/collector_binance.log` | md/ob/fr msg/s (aggregated), flush avg/max, hist avg/max, batch_avg, reconnects, WS error labels |
| `logs/collector_bybit.log` | same |
| `logs/collector_okx.log` | same |
| `logs/collector_gate.log` | same |
| `logs/collector_bitget.log` | same |
| `logs/redis_monitor.log` | mem, ops/s, ping, lpush_p99, hset_p99, frag, keys, hit_rate, net_in, net_out |
| `logs/stale_monitor.log` | stale key count, total keys, last scan time |
| `logs/spread_monitor.log` | signal lines: timestamp, symbol, spot_exch, fut_exch, spread_pct |
| `logs/snapshot_monitor.log` | monitor liveness (last write time) |

---

## Log Timestamp Format

All log files use the format from `logger_setup.py`:
```
[YYYY-MM-DD HH:MM:SS.mmm] [LEVEL   ] [name] message
```
Example: `[2026-03-23 12:44:38.123] [INFO    ] [collector_binance] METRICS | ...`

"Today's signals" count is derived by counting signal lines where the date prefix matches the current date.

---

## Stream State Logic

Each collector manages 5 WS streams. The label used in log reconnect lines is as follows:

| Stream | Log label | Notes |
|--------|-----------|-------|
| MD spot | `md_spot` | |
| MD futures | `md_fut` | |
| OB spot | `ob_spot` | |
| OB futures | `ob_fut` | |
| FR futures | `fr` | Label is `fr`, not `fr_fut` |

**State determination** (per stream, per exchange):

| State | Symbol | Color | Condition |
|-------|--------|-------|-----------|
| connected | `●` | green | latest METRICS line written within last 60s (msg/s may be 0 between intervals) AND no recent reconnect |
| reconnecting | `↻` | yellow | log line matching `\[<label>\] WS error.*Reconnecting in` seen within last 30s |
| dead | `✗` | red | log file not written for >60s, or process not running |
| unknown | `○` | grey | log file missing or not yet created |

**Reconnect counter:** cumulative count of `Reconnecting in` lines per stream label since parser started.

### Important: msg/s aggregation

The METRICS line aggregates across both spot and futures tasks:
```
METRICS | md=<N>msg/s ob=<N>msg/s fr=<N>msg/s | ...
```
`md=` is the **combined** total of `md_spot` + `md_fut`. `ob=` is the combined total of `ob_spot` + `ob_fut`.

**Consequence for layout:** the stream table shows `●` / `↻` / `✗` per stream row (derived from reconnect log lines), but `msg/s` is only available as a combined value per type. The `msg/s` column is therefore shown **per exchange** in the flush row, not per individual stream row.

---

## Log Parsing Patterns

All patterns are applied with `re.search()` (partial line match), not `re.fullmatch`. Log lines may have trailing content beyond the matched portion.


### Collector METRICS line
```
METRICS \| md=(\d+)msg/s ob=(\d+)msg/s fr=(\d+)msg/s \| flush_lat avg=([\d.]+)ms max=([\d.]+)ms batch_avg=([\d.]+) \| hist_writes=([\d.]+)/s hist_flush_lat avg=([\d.]+)ms max=([\d.]+)ms .*reconnects=(\d+)
```

### Collector reconnect line (per stream)
```
\[(\w+)\] WS error: .* Reconnecting in ([\d.]+)s
```
First capture group is the stream label (`md_spot`, `md_fut`, `ob_spot`, `ob_fut`, `fr`).

### Redis METRICS line
```
REDIS OK \| mem=([\d.]+)MB rss=([\d.]+)MB frag=([\d.]+) peak=([\d.]+)MB \| ops/s=(\d+) clients=(\d+) blocked=(\d+) \| keys=(\d+) hit_rate=([\d.]+)% \| net_in=([\d.]+)kbps net_out=([\d.]+)kbps \| eventloop=(\d+)us lpush_p99=(\d+)us hset_p99=(\d+)us \| ping=([\d.]+)ms
```
Note: latency values in the log use `us` (ASCII), displayed as `µs` in the UI.

### Stale monitor summary line
```
Stale check done: total_keys=(\d+) stale=(\d+) elapsed=[\d.]+ms
```

### Spread signal line
```
SIGNAL \| (\w+)→(\w+) (\w+) ask_spot=([\d.]+) bid_fut=([\d.]+) spread=([\d.]+)%
```
Groups: `spot_exch`, `fut_exch`, `symbol`, `ask_spot`, `bid_fut`, `spread_pct`
Display: prefix with `+` sign, round to 2 decimal places.
Exchange names are logged without market suffix (e.g., `binance`, not `binance_spot`). Do NOT append `_spot`/`_fut` in the signals display.

---

## Layout

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  BALI 5.0  ·  <timestamp>  ·  uptime <HH:MM:SS>  ·  branch: <data|trade>  ║
╠═══════════════╦═══════════╦═══════════╦═══════════╦═══════════╦═════════════╣
║  STREAM       ║  BINANCE  ║   BYBIT   ║    OKX    ║   GATE    ║   BITGET    ║
╠═══════════════╬═══════════╬═══════════╬═══════════╬═══════════╬═════════════╣
║ MD  spot      ║     ●  0↻ ║     ●  0↻ ║     ↻  3↻ ║     ●  0↻ ║      ●  0↻ ║
║ MD  futures   ║     ●  0↻ ║     ●  0↻ ║     ↻  3↻ ║     ●  0↻ ║      ●  0↻ ║
║ OB  spot      ║     ●  0↻ ║     ●  0↻ ║     ✗  3↻ ║     ●  0↻ ║      ●  0↻ ║
║ OB  futures   ║     ●  0↻ ║     ●  0↻ ║     ✗  3↻ ║     ●  0↻ ║      ●  0↻ ║
║ FR  futures   ║     ●  0↻ ║     ●  0↻ ║     ✗  3↻ ║     ●  0↻ ║      ●  0↻ ║
╠═══════════════╬═══════════╬═══════════╬═══════════╬═══════════╬═════════════╣
║ md+ob msg/s   ║  842+420  ║  310+155  ║     —     ║  178+ 88  ║   134+  67  ║
║ flush avg/max ║ 1.2/ 4.8ms║ 0.9/ 3.1ms║     —     ║ 1.1/3.4ms ║  1.0/ 3.8ms ║
║ hist  avg/max ║ 2.1/ 8.3ms║ 1.8/ 6.2ms║     —     ║ 2.0/6.8ms ║  1.9/ 7.1ms ║
╠═══════════════╩═══════════╩═══╦═══════════════════════════════╩═════════════╣
║  REDIS                        ║  MONITORS                                   ║
║  mem  1842MB  ops  84k/s      ║  ✓ redis_monitor   ✓ stale_monitor          ║
║  ping   0.4ms  frag  1.08     ║  ✓ spread_monitor  ✓ snapshot_monitor       ║
║  lpush_p99  3µs  hset_p99 4µs ║  stale keys:  0 / 12450  ✓                 ║
║  keys  12450   hit  98.7%     ║  last scan:  12:44:55                       ║
╠═══════════════════════════════╩═════════════════════════════════════════════╣
║  SIGNALS ─────────────────────────────────────────────────────── today: 3  ║
║  ▸ 12:44:38  BTCUSDT  binance_spot → bybit_fut   +1.23%                    ║
║  ▸ 12:31:12  ETHUSDT  okx_spot → binance_fut     +0.87%                    ║
║  ▸ 11:58:44  SOLUSDT  gate_spot → bybit_fut      +1.05%                    ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

**Minimum terminal size:** 80 columns × 30 rows

---

## Sections

### Section 1 — Stream Table

- Rows: `MD spot`, `MD futures`, `OB spot`, `OB futures`, `FR futures`
- Columns: binance, bybit, okx, gate, bitget
- Each cell: `<status_symbol>  <reconnect_count>↻`
- Status per stream derived from reconnect log lines (see Stream State Logic)
- Three metric rows below stream rows per exchange:
  - `md+ob msg/s` — combined md and ob message rates from METRICS line
  - `flush avg/max` — primary flush latency from METRICS line
  - `hist avg/max` — hist flush latency from METRICS line
- Dead exchange shows `—` in all metric rows

### Section 2 — Redis Health

Parsed from latest `REDIS OK` line in `redis_monitor.log`:
- Displayed: `mem`, `ops/s`, `ping`, `lpush_p99`, `hset_p99` (shown as µs), `frag`, `keys`, `hit_rate`
- Not displayed (parsed but reserved): `net_in`, `net_out`, `rss`, `blocked`, `eventloop`
- `fr` msg/s is captured by the METRICS regex but not displayed (no display location in layout)
- Warning highlight (yellow) if `mem > config.REDIS_MEMORY_WARN_MB` or `ops/s > config.REDIS_OPS_WARN_PER_SEC`

### Section 3 — Monitors

Status determined by last log write time:
- Green `✓` if log file written within last 60s
- Red `✗` if silent for >60s or file missing

Includes stale key count from latest summary line in `stale_monitor.log`.

### Section 4 — Signals Strip

- Last 3 signal lines from `spread_monitor.log`, newest first
- Format: `▸ HH:MM:SS  SYMBOL  spot_exch → fut_exch  +X.XX%`
- `today: N` count = lines with today's date prefix in `spread_monitor.log` matching SIGNAL pattern

---

## Key Bindings

| Key | Action |
|-----|--------|
| `q` | Quit dashboard |
| `r` | Refresh immediately |

---

## Dependencies

Add to `requirements.txt`:
```
textual>=0.60
```

---

## File

- **Script:** `dashboard.py` at project root
- **Usage:** `python dashboard.py`
- **No arguments required** — reads `config.py` for log paths and branch flag
