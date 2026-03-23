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
