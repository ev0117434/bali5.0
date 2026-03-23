# tests/test_dashboard_parser.py
"""Unit tests for dashboard StateParser — no file I/O, no Textual."""
import sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.state import DashboardState, StreamState, resolve_stream_status
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
