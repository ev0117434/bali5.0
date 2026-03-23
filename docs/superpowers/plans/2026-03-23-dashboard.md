# Dashboard TUI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `dashboard.py` — a standalone Textual TUI that tails BALI 5.0 log files and displays a live Mission Control screen with stream status, Redis health, and spread signals.

**Architecture:** A `LogWatcher` asyncio background task tails each log file and feeds new lines into a `StateParser` (regex-based) that maintains a shared `DashboardState` dataclass. A `Textual` App renders this state every 2 seconds using Rich renderables passed to `Static.update()`.

**Tech Stack:** Python 3.11+, `textual>=0.60`, `asyncio`, `re`, `rich`, project's own `config.py`

**Spec:** `docs/superpowers/specs/2026-03-23-dashboard-design.md`

> **Run from project root:** `python dashboard.py` must be run from `/root/bali5.0` because `config.LOGS_DIR = "logs"` is a relative path.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `requirements.txt` | Modify | Add `textual>=0.60` |
| `dashboard/__init__.py` | Create | Package marker |
| `dashboard/state.py` | Create | Dataclasses: `StreamState`, `CollectorState`, `RedisState`, `MonitorLiveness`, `StaleState`, `SignalRecord`, `DashboardState`; helper `resolve_stream_status()` |
| `dashboard/parser.py` | Create | `StateParser` — regex patterns + `parse_line(source, line)` |
| `dashboard/watcher.py` | Create | `LogWatcher` — async tail of log files, feeds lines to parser |
| `dashboard.py` | Create | `DashboardApp(App)` — entry point, layout, all render methods inline |
| `tests/test_dashboard_parser.py` | Create | Unit tests for all regex patterns in `StateParser` |
| `tests/test_dashboard_watcher.py` | Create | Unit tests for `LogWatcher` file-tail behaviour |

---

## Task 1: Add dependency and create package skeleton

**Files:**
- Modify: `requirements.txt`
- Create: `dashboard/__init__.py`

- [ ] **Step 1: Add textual to requirements**

Append to `requirements.txt`:
```
textual>=0.60
```

- [ ] **Step 2: Install it**

```bash
pip install "textual>=0.60"
```
Expected: installs without error.

- [ ] **Step 3: Create package**

```bash
mkdir dashboard && touch dashboard/__init__.py
```

- [ ] **Step 4: Commit**

```bash
git add requirements.txt dashboard/__init__.py
git commit -m "chore: add textual dependency, scaffold dashboard package"
```

---

## Task 2: State dataclasses

**Files:**
- Create: `dashboard/state.py`

No tests needed — pure data, no logic.

- [ ] **Step 1: Create `dashboard/state.py`**

```python
# dashboard/state.py
"""
BALI 5.0 Dashboard — shared state dataclasses.
All fields have safe defaults so the UI can render before log data arrives.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal
import time

StreamStatus = Literal["connected", "reconnecting", "dead", "unknown"]

STREAM_LABELS = ["md_spot", "md_fut", "ob_spot", "ob_fut", "fr"]
EXCHANGES     = ["binance", "bybit", "okx", "gate", "bitget"]

METRICS_STALE_S    = 60.0   # collector is dead if no METRICS line for this long
RECONNECT_WINDOW_S = 30.0  # stream stays "reconnecting" for this many seconds


@dataclass
class StreamState:
    status: StreamStatus = "unknown"
    reconnects: int = 0
    last_reconnect_ts: float = 0.0   # monotonic, time of last "Reconnecting" line


@dataclass
class CollectorState:
    md_msgs_s: int   = 0
    ob_msgs_s: int   = 0
    flush_avg_ms: float = 0.0
    flush_max_ms: float = 0.0
    hist_avg_ms: float  = 0.0
    hist_max_ms: float  = 0.0
    last_metrics_ts: float = 0.0     # monotonic, time of last METRICS line
    streams: dict[str, StreamState] = field(
        default_factory=lambda: {s: StreamState() for s in STREAM_LABELS}
    )


@dataclass
class RedisState:
    mem_mb: float   = 0.0
    ops_s: int      = 0
    ping_ms: float  = 0.0
    lpush_p99_us: int = 0
    hset_p99_us: int  = 0
    frag: float     = 0.0
    keys: int       = 0
    hit_rate: float = 0.0
    last_update_ts: float = 0.0      # monotonic


@dataclass
class StaleState:
    stale_count: int  = 0
    total_keys: int   = 0
    last_scan_time: str = "—"        # "HH:MM:SS" from log timestamp


@dataclass
class MonitorLiveness:
    last_write_ts: float = 0.0       # monotonic, time of last log line from this monitor


@dataclass
class SignalRecord:
    time_str: str    # "HH:MM:SS"
    symbol: str
    spot_exch: str
    fut_exch: str
    spread_pct: float


@dataclass
class DashboardState:
    started_at: float = field(default_factory=time.monotonic)
    collectors: dict[str, CollectorState] = field(
        default_factory=lambda: {e: CollectorState() for e in EXCHANGES}
    )
    redis: RedisState     = field(default_factory=RedisState)
    stale: StaleState     = field(default_factory=StaleState)
    monitors: dict[str, MonitorLiveness] = field(
        default_factory=lambda: {
            m: MonitorLiveness()
            for m in ["redis_monitor", "stale_monitor", "spread_monitor", "snapshot_monitor"]
        }
    )
    signals: list[SignalRecord] = field(default_factory=list)  # newest first, max 3
    signals_today: int = 0
    today_date: str = ""   # "YYYY-MM-DD" from first parsed signal line


def resolve_stream_status(stream: StreamState, collector_metrics_ts: float) -> StreamStatus:
    """
    Derive stream status from timing:
    - dead        → collector's last METRICS line is >60s old (or never seen)
    - reconnecting → a Reconnecting log line was seen within the last 30s
    - connected   → metrics are fresh and no recent reconnect
    """
    now = time.monotonic()
    if collector_metrics_ts == 0.0 or (now - collector_metrics_ts) > METRICS_STALE_S:
        return "dead"
    if stream.last_reconnect_ts > 0 and (now - stream.last_reconnect_ts) < RECONNECT_WINDOW_S:
        return "reconnecting"
    return "connected"
```

- [ ] **Step 2: Verify import works**

```bash
python -c "from dashboard.state import DashboardState; s = DashboardState(); print('ok', list(s.collectors))"
```
Expected: `ok ['binance', 'bybit', 'okx', 'gate', 'bitget']`

- [ ] **Step 3: Commit**

```bash
git add dashboard/state.py
git commit -m "feat: dashboard state dataclasses"
```

---

## Task 3: StateParser — tests first

**Files:**
- Create: `tests/test_dashboard_parser.py`
- Create: `dashboard/parser.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_dashboard_parser.py
"""Unit tests for dashboard StateParser — no file I/O, no Textual."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.state import DashboardState, resolve_stream_status
from dashboard.parser import StateParser


def make_log_line(body: str, level: str = "INFO", name: str = "collector_binance") -> str:
    return f"[2026-03-23 12:44:38.123] [{level:<8}] [{name}] {body}"


class TestMetricsParsing:
    def setup_method(self):
        self.state = DashboardState()
        self.parser = StateParser(self.state)

    def test_parses_md_ob_msgs(self):
        line = make_log_line(
            "METRICS | md=842msg/s ob=420msg/s fr=12msg/s | "
            "flush_lat avg=1.2ms max=4.8ms batch_avg=84 | "
            "hist_writes=145/s hist_flush_lat avg=2.1ms max=8.3ms "
            "ob_skip=5/s | expire_set=12 reconnects=0"
        )
        self.parser.parse_line("binance", line)
        c = self.state.collectors["binance"]
        assert c.md_msgs_s == 842
        assert c.ob_msgs_s == 420

    def test_parses_flush_latency(self):
        line = make_log_line(
            "METRICS | md=300msg/s ob=150msg/s fr=10msg/s | "
            "flush_lat avg=0.9ms max=3.1ms batch_avg=31 | "
            "hist_writes=80/s hist_flush_lat avg=1.8ms max=6.2ms "
            "ob_skip=2/s | expire_set=5 reconnects=0"
        )
        self.parser.parse_line("bybit", line)
        c = self.state.collectors["bybit"]
        assert c.flush_avg_ms == 0.9
        assert c.flush_max_ms == 3.1
        assert c.hist_avg_ms  == 1.8
        assert c.hist_max_ms  == 6.2

    def test_updates_last_metrics_ts(self):
        before = time.monotonic()
        line = make_log_line(
            "METRICS | md=100msg/s ob=50msg/s fr=5msg/s | "
            "flush_lat avg=1.0ms max=3.0ms batch_avg=50 | "
            "hist_writes=60/s hist_flush_lat avg=1.5ms max=5.0ms "
            "ob_skip=1/s | expire_set=3 reconnects=0"
        )
        self.parser.parse_line("gate", line)
        assert self.state.collectors["gate"].last_metrics_ts >= before

    def test_non_metrics_line_does_not_update_metrics_ts(self):
        """A random log line must NOT update last_metrics_ts."""
        line = make_log_line("some debug line")
        before_ts = self.state.collectors["binance"].last_metrics_ts
        self.parser.parse_line("binance", line)
        assert self.state.collectors["binance"].last_metrics_ts == before_ts


class TestReconnectParsing:
    def setup_method(self):
        self.state = DashboardState()
        self.parser = StateParser(self.state)

    def test_increments_stream_reconnect_count(self):
        line = make_log_line(
            "[md_spot] WS error: ConnectionClosedError: no close frame. "
            "Reconnecting in 1s..."
        )
        self.parser.parse_line("binance", line)
        assert self.state.collectors["binance"].streams["md_spot"].reconnects == 1
        assert self.state.collectors["binance"].streams["md_spot"].status == "reconnecting"

    def test_increments_fr_stream(self):
        line = make_log_line("[fr] WS error: TimeoutError: timed out. Reconnecting in 2s...")
        self.parser.parse_line("okx", line)
        assert self.state.collectors["okx"].streams["fr"].reconnects == 1
        assert self.state.collectors["okx"].streams["fr"].status == "reconnecting"

    def test_cumulative_reconnects(self):
        line = make_log_line("[ob_fut] WS error: OSError: broken. Reconnecting in 1s...")
        self.parser.parse_line("gate", line)
        self.parser.parse_line("gate", line)
        assert self.state.collectors["gate"].streams["ob_fut"].reconnects == 2


class TestRedisParsing:
    def setup_method(self):
        self.state = DashboardState()
        self.parser = StateParser(self.state)

    def _redis_line(self):
        return make_log_line(
            "REDIS OK | mem=1842.3MB rss=2010.5MB frag=1.08 peak=1900.0MB | "
            "ops/s=84230 clients=8 blocked=0 | keys=12450 hit_rate=98.7% | "
            "net_in=1240kbps net_out=3810kbps | "
            "eventloop=42us lpush_p99=3us hset_p99=4us | ping=0.4ms",
            name="redis_monitor"
        )

    def test_parses_mem(self):
        self.parser.parse_line("redis_monitor", self._redis_line())
        assert self.state.redis.mem_mb == 1842.3

    def test_parses_ops_ping_latency(self):
        self.parser.parse_line("redis_monitor", self._redis_line())
        r = self.state.redis
        assert r.ops_s        == 84230
        assert r.ping_ms      == 0.4
        assert r.lpush_p99_us == 3
        assert r.hset_p99_us  == 4

    def test_parses_keys_hit_rate_frag(self):
        self.parser.parse_line("redis_monitor", self._redis_line())
        r = self.state.redis
        assert r.keys     == 12450
        assert r.hit_rate == 98.7
        assert r.frag     == 1.08

    def test_updates_monitor_liveness(self):
        before = time.monotonic()
        self.parser.parse_line("redis_monitor", self._redis_line())
        assert self.state.monitors["redis_monitor"].last_write_ts >= before


class TestStaleParsing:
    def setup_method(self):
        self.state = DashboardState()
        self.parser = StateParser(self.state)

    def test_parses_stale_count(self):
        line = make_log_line(
            "Stale check done: total_keys=12450 stale=7 elapsed=230ms",
            name="stale_monitor"
        )
        self.parser.parse_line("stale_monitor", line)
        assert self.state.stale.stale_count == 7
        assert self.state.stale.total_keys  == 12450

    def test_zero_stale(self):
        line = make_log_line(
            "Stale check done: total_keys=9800 stale=0 elapsed=180ms",
            name="stale_monitor"
        )
        self.parser.parse_line("stale_monitor", line)
        assert self.state.stale.stale_count == 0

    def test_extracts_time_from_timestamp(self):
        line = make_log_line(
            "Stale check done: total_keys=9800 stale=0 elapsed=180ms",
            name="stale_monitor"
        )
        self.parser.parse_line("stale_monitor", line)
        assert self.state.stale.last_scan_time == "12:44:38"


class TestSignalParsing:
    def setup_method(self):
        self.state = DashboardState()
        self.parser = StateParser(self.state)

    def test_parses_signal(self):
        line = make_log_line(
            "SIGNAL | binance→bybit BTCUSDT ask_spot=84000.1234 bid_fut=85032.5678 spread=1.2300%",
            name="spread_monitor"
        )
        self.parser.parse_line("spread_monitor", line)
        assert len(self.state.signals) == 1
        s = self.state.signals[0]
        assert s.symbol    == "BTCUSDT"
        assert s.spot_exch == "binance"
        assert s.fut_exch  == "bybit"
        assert abs(s.spread_pct - 1.23) < 0.001
        assert s.time_str  == "12:44:38"

    def test_signals_newest_first_max3(self):
        body = "SIGNAL | binance→bybit BTCUSDT ask_spot=1.0 bid_fut=1.01 spread=1.0000%"
        for _ in range(5):
            self.parser.parse_line("spread_monitor", make_log_line(body, name="spread_monitor"))
        assert len(self.state.signals) == 3

    def test_increments_today_count(self):
        body = "SIGNAL | okx→binance ETHUSDT ask_spot=3000.0 bid_fut=3030.0 spread=1.0000%"
        self.parser.parse_line("spread_monitor", make_log_line(body, name="spread_monitor"))
        self.parser.parse_line("spread_monitor", make_log_line(body, name="spread_monitor"))
        assert self.state.signals_today == 2


class TestMonitorLiveness:
    def setup_method(self):
        self.state = DashboardState()
        self.parser = StateParser(self.state)

    def test_any_line_updates_monitor_ts(self):
        before = time.monotonic()
        line = make_log_line("some info line", name="stale_monitor")
        self.parser.parse_line("stale_monitor", line)
        assert self.state.monitors["stale_monitor"].last_write_ts >= before


class TestStreamStatusResolution:
    def test_connected_when_metrics_fresh_no_reconnect(self):
        stream = StreamState()
        stream.last_reconnect_ts = 0.0
        metrics_ts = time.monotonic()
        assert resolve_stream_status(stream, metrics_ts) == "connected"

    def test_reconnecting_within_30s(self):
        stream = StreamState()
        stream.last_reconnect_ts = time.monotonic()
        metrics_ts = time.monotonic()
        assert resolve_stream_status(stream, metrics_ts) == "reconnecting"

    def test_dead_when_metrics_never_seen(self):
        stream = StreamState()
        stream.last_reconnect_ts = 0.0
        assert resolve_stream_status(stream, 0.0) == "dead"

    def test_dead_when_metrics_stale(self):
        stream = StreamState()
        stream.last_reconnect_ts = 0.0
        stale_ts = time.monotonic() - 120   # 120s ago
        assert resolve_stream_status(stream, stale_ts) == "dead"


# Import StreamState for TestStreamStatusResolution
from dashboard.state import StreamState
```

- [ ] **Step 2: Run tests — expect failures**

```bash
cd /root/bali5.0 && python -m pytest tests/test_dashboard_parser.py -v 2>&1 | head -15
```
Expected: `ModuleNotFoundError: No module named 'dashboard.parser'`

- [ ] **Step 3: Implement `dashboard/parser.py`**

```python
# dashboard/parser.py
"""
BALI 5.0 Dashboard — log line parser.
All patterns use re.search() (partial match). Log lines may have trailing content.
"""
from __future__ import annotations
import re
import time

from dashboard.state import DashboardState, SignalRecord, EXCHANGES

# ── Timestamp ─────────────────────────────────────────────────────────────────
RE_TS = re.compile(r"\[(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\.\d+\]")

# ── Collector: METRICS ────────────────────────────────────────────────────────
RE_METRICS = re.compile(
    r"METRICS \| md=(\d+)msg/s ob=(\d+)msg/s fr=\d+msg/s \| "
    r"flush_lat avg=([\d.]+)ms max=([\d.]+)ms batch_avg=[\d.]+ \| "
    r"hist_writes=[\d.]+/s hist_flush_lat avg=([\d.]+)ms max=([\d.]+)ms"
)

# ── Collector: WS reconnect ───────────────────────────────────────────────────
RE_RECONNECT = re.compile(r"\[(\w+)\] WS error:.*Reconnecting in [\d.]+s")

# ── Redis ─────────────────────────────────────────────────────────────────────
RE_REDIS_MEM   = re.compile(r"mem=([\d.]+)MB")
RE_REDIS_OPS   = re.compile(r"ops/s=(\d+)")
RE_REDIS_PING  = re.compile(r"ping=([\d.]+)ms")
RE_REDIS_LPUSH = re.compile(r"lpush_p99=(\d+)us")
RE_REDIS_HSET  = re.compile(r"hset_p99=(\d+)us")
RE_REDIS_FRAG  = re.compile(r"frag=([\d.]+)")
RE_REDIS_KEYS  = re.compile(r"keys=(\d+)")
RE_REDIS_HIT   = re.compile(r"hit_rate=([\d.]+)%")

# ── Stale ─────────────────────────────────────────────────────────────────────
RE_STALE = re.compile(r"Stale check done: total_keys=(\d+) stale=(\d+)")

# ── Signal ────────────────────────────────────────────────────────────────────
RE_SIGNAL = re.compile(
    r"SIGNAL \| (\w+)→(\w+) (\w+) ask_spot=[\d.]+ bid_fut=[\d.]+ spread=([\d.]+)%"
)

MONITOR_NAMES = {"redis_monitor", "stale_monitor", "spread_monitor", "snapshot_monitor"}


def _extract_ts(line: str) -> tuple[str, str] | None:
    m = RE_TS.search(line)
    return (m.group(1), m.group(2)) if m else None


class StateParser:
    """
    Call parse_line(source, line) for every new log line.
    `source`: exchange name (e.g. 'binance') or monitor name (e.g. 'redis_monitor').
    Mutates shared DashboardState in-place. Thread-safe for single-threaded asyncio use.
    """

    def __init__(self, state: DashboardState) -> None:
        self._state = state

    def parse_line(self, source: str, line: str) -> None:
        ts = _extract_ts(line)

        # Monitor liveness: any line counts
        if source in MONITOR_NAMES:
            self._state.monitors[source].last_write_ts = time.monotonic()

        if source in EXCHANGES:
            self._parse_collector(source, line)
        elif source == "redis_monitor" and "REDIS OK" in line:
            self._parse_redis(line)
        elif source == "stale_monitor" and "Stale check done" in line:
            self._parse_stale(line, ts)
        elif source == "spread_monitor" and "SIGNAL |" in line:
            self._parse_signal(line, ts)

    def _parse_collector(self, exchange: str, line: str) -> None:
        c = self._state.collectors[exchange]

        m = RE_METRICS.search(line)
        if m:
            c.md_msgs_s    = int(m.group(1))
            c.ob_msgs_s    = int(m.group(2))
            c.flush_avg_ms = float(m.group(3))
            c.flush_max_ms = float(m.group(4))
            c.hist_avg_ms  = float(m.group(5))
            c.hist_max_ms  = float(m.group(6))
            c.last_metrics_ts = time.monotonic()
            return

        m = RE_RECONNECT.search(line)
        if m:
            label = m.group(1)
            if label in c.streams:
                c.streams[label].reconnects       += 1
                c.streams[label].status            = "reconnecting"
                c.streams[label].last_reconnect_ts = time.monotonic()

    def _parse_redis(self, line: str) -> None:
        r = self._state.redis
        def _f(pat): m = pat.search(line); return float(m.group(1)) if m else 0.0
        def _i(pat): m = pat.search(line); return int(m.group(1)) if m else 0
        r.mem_mb       = _f(RE_REDIS_MEM)
        r.ops_s        = _i(RE_REDIS_OPS)
        r.ping_ms      = _f(RE_REDIS_PING)
        r.lpush_p99_us = _i(RE_REDIS_LPUSH)
        r.hset_p99_us  = _i(RE_REDIS_HSET)
        r.frag         = _f(RE_REDIS_FRAG)
        r.keys         = _i(RE_REDIS_KEYS)
        r.hit_rate     = _f(RE_REDIS_HIT)
        r.last_update_ts = time.monotonic()

    def _parse_stale(self, line: str, ts: tuple[str, str] | None) -> None:
        m = RE_STALE.search(line)
        if not m:
            return
        self._state.stale.total_keys  = int(m.group(1))
        self._state.stale.stale_count = int(m.group(2))
        self._state.stale.last_scan_time = ts[1] if ts else "—"

    def _parse_signal(self, line: str, ts: tuple[str, str] | None) -> None:
        m = RE_SIGNAL.search(line)
        if not m:
            return
        rec = SignalRecord(
            time_str  = ts[1] if ts else "—",
            spot_exch = m.group(1),
            fut_exch  = m.group(2),
            symbol    = m.group(3),
            spread_pct = float(m.group(4)),
        )
        self._state.signals.insert(0, rec)
        self._state.signals = self._state.signals[:3]
        today = ts[0] if ts else ""
        if not self._state.today_date:
            self._state.today_date = today
        if today == self._state.today_date:
            self._state.signals_today += 1
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /root/bali5.0 && python -m pytest tests/test_dashboard_parser.py -v
```
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add dashboard/parser.py tests/test_dashboard_parser.py
git commit -m "feat: dashboard StateParser with full test coverage"
```

---

## Task 4: LogWatcher — tests first

**Files:**
- Create: `tests/test_dashboard_watcher.py`
- Create: `dashboard/watcher.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_dashboard_watcher.py
"""Unit tests for LogWatcher — uses temp files, no real logs needed."""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from dashboard.state import DashboardState
from dashboard.watcher import LogWatcher


@pytest.mark.asyncio
async def test_reads_existing_tail_on_startup(tmp_path):
    """Watcher reads last N lines from an existing file at startup."""
    log_file = tmp_path / "collector_binance.log"
    metrics = (
        "[2026-03-23 12:44:38.000] [INFO    ] [collector_binance] "
        "METRICS | md=842msg/s ob=420msg/s fr=12msg/s | "
        "flush_lat avg=1.2ms max=4.8ms batch_avg=84 | "
        "hist_writes=145/s hist_flush_lat avg=2.1ms max=8.3ms "
        "ob_skip=5/s | expire_set=12 reconnects=0\n"
    )
    log_file.write_text(metrics * 10)  # 10 lines, all within tail window

    state   = DashboardState()
    watcher = LogWatcher(state, log_dir=tmp_path)
    task    = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert state.collectors["binance"].md_msgs_s == 842


@pytest.mark.asyncio
async def test_picks_up_new_appended_lines(tmp_path):
    """Watcher detects lines appended after startup."""
    log_file = tmp_path / "collector_bybit.log"
    log_file.write_text("")

    state   = DashboardState()
    watcher = LogWatcher(state, log_dir=tmp_path)
    task    = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.05)

    metrics = (
        "[2026-03-23 12:44:38.000] [INFO    ] [collector_bybit] "
        "METRICS | md=310msg/s ob=155msg/s fr=10msg/s | "
        "flush_lat avg=0.9ms max=3.1ms batch_avg=31 | "
        "hist_writes=80/s hist_flush_lat avg=1.8ms max=6.2ms "
        "ob_skip=2/s | expire_set=5 reconnects=0\n"
    )
    with open(log_file, "a") as f:
        f.write(metrics)

    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert state.collectors["bybit"].md_msgs_s == 310


@pytest.mark.asyncio
async def test_missing_log_file_does_not_crash(tmp_path):
    """Watcher must not crash if a log file does not yet exist."""
    state   = DashboardState()
    watcher = LogWatcher(state, log_dir=tmp_path)
    task    = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # reaching here without exception = pass
```

- [ ] **Step 2: Run tests — expect failures**

```bash
cd /root/bali5.0 && python -m pytest tests/test_dashboard_watcher.py -v 2>&1 | head -10
```
Expected: `ModuleNotFoundError: No module named 'dashboard.watcher'`

- [ ] **Step 3: Implement `dashboard/watcher.py`**

```python
# dashboard/watcher.py
"""
BALI 5.0 Dashboard — async log file watcher.
Tails each collector and monitor log file; feeds new lines into StateParser.
"""
from __future__ import annotations
import asyncio
from pathlib import Path

from dashboard.state import DashboardState, EXCHANGES
from dashboard.parser import StateParser

LOG_SOURCES: dict[str, str] = {
    **{f"collector_{e}": e for e in EXCHANGES},
    "redis_monitor":    "redis_monitor",
    "stale_monitor":    "stale_monitor",
    "spread_monitor":   "spread_monitor",
    "snapshot_monitor": "snapshot_monitor",
}

TAIL_LINES    = 100    # lines to read from end of file on startup
POLL_INTERVAL = 0.5   # seconds between size checks


class LogWatcher:
    """
    Tails all BALI log files asynchronously.
    Cancels cleanly on asyncio.CancelledError.
    Must be run from the project root (log_dir defaults to Path("logs")).
    """

    def __init__(self, state: DashboardState, log_dir: Path | str | None = None) -> None:
        self._state   = state
        self._parser  = StateParser(state)
        self._log_dir = Path(log_dir) if log_dir else Path("logs")

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._tail(stem, source))
            for stem, source in LOG_SOURCES.items()
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _tail(self, stem: str, source: str) -> None:
        log_path = self._log_dir / f"{stem}.log"

        while not log_path.exists():
            await asyncio.sleep(POLL_INTERVAL)

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f.readlines()[-TAIL_LINES:]:
                self._parser.parse_line(source, line.rstrip())
            pos = f.tell()

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            try:
                size = log_path.stat().st_size
            except FileNotFoundError:
                await asyncio.sleep(POLL_INTERVAL)
                continue
            if size < pos:
                pos = 0   # log was rotated
            if size > pos:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(pos)
                    for line in f:
                        self._parser.parse_line(source, line.rstrip())
                    pos = f.tell()
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cd /root/bali5.0 && python -m pytest tests/test_dashboard_watcher.py -v
```
Expected: all 3 tests PASS.

- [ ] **Step 5: Run all dashboard tests together**

```bash
cd /root/bali5.0 && python -m pytest tests/test_dashboard_parser.py tests/test_dashboard_watcher.py -v
```
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add dashboard/watcher.py tests/test_dashboard_watcher.py
git commit -m "feat: dashboard LogWatcher with tail-and-follow"
```

---

## Task 5: DashboardApp — wire everything together

**Files:**
- Create: `dashboard.py`

Render methods return Rich renderables (`Table`, `Panel`) directly to `Static.update()` — no `.plain` conversion (which would strip colour).

- [ ] **Step 1: Create `dashboard.py`**

```python
#!/usr/bin/env python3
# dashboard.py
"""
BALI 5.0 — Terminal dashboard.

Run from project root:
    python dashboard.py

Reads logs/ in real-time. Press q to quit, r to force refresh.
"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static
from textual.containers import Horizontal
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.console import Group

import config
from dashboard.state import (
    DashboardState, EXCHANGES, STREAM_LABELS,
    resolve_stream_status, METRICS_STALE_S,
)
from dashboard.watcher import LogWatcher

_STREAM_LABEL = {
    "md_spot": "MD  spot",
    "md_fut":  "MD  fut ",
    "ob_spot": "OB  spot",
    "ob_fut":  "OB  fut ",
    "fr":      "FR  fut ",
}


def _status_sym(stream, metrics_ts: float) -> Text:
    s = resolve_stream_status(stream, metrics_ts)
    return {
        "connected":   Text("●", style="bold green"),
        "reconnecting":Text("↻", style="bold yellow"),
        "dead":        Text("✗", style="bold red"),
        "unknown":     Text("○", style="dim"),
    }[s]


def _rc_text(n: int) -> Text:
    if n == 0:
        return Text("0↻", style="dim")
    return Text(f"{n}↻", style="bold yellow" if n < 5 else "bold red")


class DashboardApp(App):
    BINDINGS = [("q", "quit", "Quit"), ("r", "refresh_now", "Refresh")]

    def __init__(self) -> None:
        super().__init__()
        self._state   = DashboardState()
        self._watcher = LogWatcher(self._state, log_dir=Path(config.LOGS_DIR))

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="streams")
        with Horizontal():
            yield Static(id="redis",    classes="half")
            yield Static(id="monitors", classes="half")
        yield Static(id="signals")
        yield Footer()

    DEFAULT_CSS = """
    Screen { background: #0d1117; }
    #streams  { border: solid #21262d; padding: 0 1; height: auto; }
    Horizontal { height: auto; }
    .half { width: 50%; border: solid #21262d; padding: 0 1; height: auto; }
    #signals  { border: solid #f0a500; padding: 0 1; height: auto; }
    """

    def on_mount(self) -> None:
        self._watch_task = asyncio.create_task(self._watcher.run())
        self.set_interval(2.0, self._refresh)
        self._refresh()

    def on_unmount(self) -> None:
        self._watch_task.cancel()

    def action_refresh_now(self) -> None:
        self._refresh()

    def _refresh(self) -> None:
        self.query_one("#streams",  Static).update(self._build_streams())
        self.query_one("#redis",    Static).update(self._build_redis())
        self.query_one("#monitors", Static).update(self._build_monitors())
        self.query_one("#signals",  Static).update(self._build_signals())

    # ── Renderers (return Rich renderables) ──────────────────────────────

    def _build_streams(self) -> Table:
        state = self._state
        now   = time.monotonic()

        t = Table(show_header=True, header_style="bold cyan",
                  box=None, padding=(0, 1), expand=True)
        t.add_column("STREAM", style="dim", width=10)
        for e in EXCHANGES:
            t.add_column(e[:6].upper(), justify="center", min_width=12)

        for label in STREAM_LABELS:
            row = [Text(_STREAM_LABEL[label], style="cyan")]
            for e in EXCHANGES:
                c  = state.collectors[e]
                st = c.streams[label]
                row.append(Text.assemble(_status_sym(st, c.last_metrics_ts), " ", _rc_text(st.reconnects)))
            t.add_row(*row)

        t.add_row("")  # spacer

        def _dead(e):
            c = state.collectors[e]
            return c.last_metrics_ts == 0.0 or (now - c.last_metrics_ts) > METRICS_STALE_S

        # md+ob msg/s
        r1 = [Text("md+ob /s", style="dim")]
        for e in EXCHANGES:
            c = state.collectors[e]
            r1.append(Text("—", style="dim") if _dead(e)
                      else Text(f"{c.md_msgs_s}+{c.ob_msgs_s}", style="green"))
        t.add_row(*r1)

        for lbl, avg_a, max_a in [
            ("flush a/m", "flush_avg_ms", "flush_max_ms"),
            ("hist  a/m", "hist_avg_ms",  "hist_max_ms"),
        ]:
            rx = [Text(lbl, style="dim")]
            for e in EXCHANGES:
                if _dead(e):
                    rx.append(Text("—", style="dim"))
                else:
                    c   = state.collectors[e]
                    avg = getattr(c, avg_a)
                    mx  = getattr(c, max_a)
                    col = "red" if mx > 50 else ("yellow" if mx > 10 else "green")
                    rx.append(Text(f"{avg:.1f}/{mx:.1f}", style=col))
            t.add_row(*rx)

        return t

    def _build_redis(self) -> Panel:
        r   = self._state.redis
        ok  = r.last_update_ts > 0 and (time.monotonic() - r.last_update_ts) < 90

        def v(val, fmt, warn=None) -> Text:
            s = fmt.format(val)
            if not ok:               return Text(s, style="dim")
            if warn and val > warn:  return Text(s, style="bold yellow")
            return Text(s, style="green")

        rows = [
            Text.assemble("mem    ", v(r.mem_mb,       "{:.0f}MB",  config.REDIS_MEMORY_WARN_MB)),
            Text.assemble("ops/s  ", v(r.ops_s,        "{}",        config.REDIS_OPS_WARN_PER_SEC)),
            Text.assemble("ping   ", v(r.ping_ms,       "{:.1f}ms")),
            Text.assemble("lpush  ", v(r.lpush_p99_us,  "{}µs")),
            Text.assemble("hset   ", v(r.hset_p99_us,   "{}µs")),
            Text.assemble("frag   ", v(r.frag,          "{:.2f}",   1.5)),
            Text.assemble("keys   ", v(r.keys,          "{}")),
            Text.assemble("hit    ", v(r.hit_rate,      "{:.1f}%")),
        ]
        return Panel(Group(*rows), title="REDIS", border_style="cyan")

    def _build_monitors(self) -> Panel:
        now   = time.monotonic()
        state = self._state
        rows  = []
        for name, lv in state.monitors.items():
            alive = lv.last_write_ts > 0 and (now - lv.last_write_ts) < 90
            sym   = Text("✓ ", style="bold green") if alive else Text("✗ ", style="bold red")
            rows.append(Text.assemble(sym, name))

        rows.append(Text(""))
        st  = state.stale
        col = "bold red" if st.stale_count > 0 else "green"
        rows.append(Text(f"stale {st.stale_count}/{st.total_keys}  {st.last_scan_time}", style=col))
        return Panel(Group(*rows), title="MONITORS", border_style="cyan")

    def _build_signals(self) -> Panel:
        state = self._state
        rows  = []
        for s in state.signals:
            rows.append(Text.assemble(
                Text("▸ ", style="bold yellow"),
                Text(f"{s.time_str}  ", style="dim"),
                Text(f"{s.symbol:<10}", style="bold white"),
                Text(f"{s.spot_exch}→{s.fut_exch}  ", style="cyan"),
                Text(f"+{s.spread_pct:.2f}%", style="bold green"),
            ))
        if not rows:
            rows.append(Text("no signals yet", style="dim"))
        return Panel(
            Group(*rows),
            title=f"SIGNALS  today: {state.signals_today}",
            border_style="yellow",
        )


if __name__ == "__main__":
    DashboardApp().run()
```

- [ ] **Step 2: Verify imports**

```bash
cd /root/bali5.0 && python -c "
from dashboard.state import DashboardState, STREAM_LABELS, EXCHANGES, resolve_stream_status, METRICS_STALE_S
from dashboard.parser import StateParser
from dashboard.watcher import LogWatcher
print('all imports ok')
"
```
Expected: `all imports ok`

- [ ] **Step 3: Run all tests**

```bash
cd /root/bali5.0 && python -m pytest tests/test_dashboard_parser.py tests/test_dashboard_watcher.py -v
```
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add dashboard.py
git commit -m "feat: DashboardApp Textual TUI with Rich renderables"
```

---

## Task 6: Smoke test

**No new code — visual verification.**

- [ ] **Step 1: Seed fake logs (if system is not running)**

```bash
cd /root/bali5.0
mkdir -p logs

echo "[2026-03-23 12:44:38.000] [INFO    ] [collector_binance] METRICS | md=842msg/s ob=420msg/s fr=12msg/s | flush_lat avg=1.2ms max=4.8ms batch_avg=84 | hist_writes=145/s hist_flush_lat avg=2.1ms max=8.3ms ob_skip=5/s | expire_set=12 reconnects=0" >> logs/collector_binance.log

echo "[2026-03-23 12:44:38.000] [INFO    ] [redis_monitor] REDIS OK | mem=1842.3MB rss=2010.5MB frag=1.08 peak=1900.0MB | ops/s=84230 clients=8 blocked=0 | keys=12450 hit_rate=98.7% | net_in=1240kbps net_out=3810kbps | eventloop=42us lpush_p99=3us hset_p99=4us | ping=0.4ms" >> logs/redis_monitor.log

echo "[2026-03-23 12:44:38.000] [INFO    ] [stale_monitor] Stale check done: total_keys=12450 stale=0 elapsed=180ms" >> logs/stale_monitor.log

echo "[2026-03-23 12:44:38.000] [INFO    ] [spread_monitor] SIGNAL | binance→bybit BTCUSDT ask_spot=84000.1234 bid_fut=85032.5678 spread=1.2300%" >> logs/spread_monitor.log
```

- [ ] **Step 2: Launch dashboard**

```bash
cd /root/bali5.0 && python dashboard.py
```
Expected:
- Textual TUI opens
- Stream table shows binance row with green `●` and `0↻`
- Redis panel shows `mem 1842MB`, `ops/s 84230`, `ping 0.4ms`
- Monitors panel shows stale `0/12450`
- Signals strip shows `▸ 12:44:38  BTCUSDT  binance→bybit  +1.23%`
- `q` exits cleanly, `r` refreshes immediately

- [ ] **Step 3: Fix any visual issues and commit**

```bash
git add -p
git commit -m "fix: dashboard smoke test corrections"
```

---

## Final state

After all tasks:
- `python dashboard.py` (from project root) opens a live TUI
- `pytest tests/test_dashboard_parser.py tests/test_dashboard_watcher.py -v` → all pass
- No changes to any existing collector, monitor, or launcher code
