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
