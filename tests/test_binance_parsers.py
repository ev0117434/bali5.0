# tests/test_binance_parsers.py
"""Unit tests for Binance collector parsers (no network, no Redis)."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from collectors.collector_binance import parse_md, parse_ob, parse_ob_fut, parse_fr


# ── parse_md ───────────────────────────────────────────────────────────────

class TestParseMd:
    def test_combined_stream_format(self):
        raw = json.dumps({
            "stream": "btcusdt@bookTicker",
            "data": {"u": 400900217, "s": "BTCUSDT", "b": "34000.10", "B": "10.5", "a": "34001.50", "A": "5.2"}
        })
        result = parse_md(raw)
        assert result is not None
        symbol, bid, ask, ts_ms = result
        assert symbol == "BTCUSDT"
        assert bid == "34000.10"
        assert ask == "34001.50"
        assert isinstance(ts_ms, int)
        assert ts_ms > 0

    def test_ts_is_current_time(self):
        raw = json.dumps({
            "stream": "ethusdt@bookTicker",
            "data": {"s": "ETHUSDT", "b": "2000.00", "B": "1.0", "a": "2001.00", "A": "2.0"}
        })
        before = int(time.time() * 1000)
        result = parse_md(raw)
        after  = int(time.time() * 1000)
        assert result is not None
        ts_ms = result[3]
        assert before <= ts_ms <= after

    def test_missing_required_fields_returns_none(self):
        raw = json.dumps({"stream": "btcusdt@bookTicker", "data": {"u": 123}})
        assert parse_md(raw) is None

    def test_invalid_json_returns_none(self):
        assert parse_md("not-json") is None

    def test_flat_message_format(self):
        """Some Binance messages arrive without outer stream wrapper."""
        raw = json.dumps({"s": "BNBUSDT", "b": "300.00", "a": "300.10"})
        result = parse_md(raw)
        assert result is not None
        assert result[0] == "BNBUSDT"


# ── parse_ob ───────────────────────────────────────────────────────────────

class TestParseOb:
    def test_combined_stream_format(self):
        bids = [["34000.10", "10.5"], ["33999.00", "5.2"]]
        asks = [["34001.50", "8.3"], ["34002.00", "12.1"]]
        raw = json.dumps({
            "stream": "btcusdt@depth10@100ms",
            "data": {"lastUpdateId": 1027024, "bids": bids, "asks": asks}
        })
        result = parse_ob(raw)
        assert result is not None
        symbol, got_bids, got_asks, ts_ms = result
        assert symbol == "BTCUSDT"
        assert got_bids == bids
        assert got_asks == asks
        assert isinstance(ts_ms, int)

    def test_truncates_to_10_levels(self):
        bids = [[str(34000 - i), "1.0"] for i in range(15)]
        asks = [[str(34001 + i), "1.0"] for i in range(15)]
        raw = json.dumps({
            "stream": "ethusdt@depth10@100ms",
            "data": {"bids": bids, "asks": asks}
        })
        result = parse_ob(raw)
        assert result is not None
        _, got_bids, got_asks, _ = result
        assert len(got_bids) == 10
        assert len(got_asks) == 10

    def test_symbol_from_stream_name(self):
        raw = json.dumps({
            "stream": "solusdt@depth10@100ms",
            "data": {"bids": [["100.0", "5.0"]], "asks": [["100.1", "3.0"]]}
        })
        result = parse_ob(raw)
        assert result is not None
        assert result[0] == "SOLUSDT"

    def test_missing_bids_returns_none(self):
        raw = json.dumps({"stream": "btcusdt@depth10@100ms", "data": {}})
        assert parse_ob(raw) is None

    def test_invalid_json_returns_none(self):
        assert parse_ob("{bad json") is None


# ── parse_ob_fut ───────────────────────────────────────────────────────────

class TestParseObFut:
    def test_futures_depth_update_format(self):
        """Binance futures uses depthUpdate with b/a keys, NOT bids/asks."""
        bids = [["34000.10", "10.5"], ["33999.00", "5.2"]]
        asks = [["34001.50", "8.3"], ["34002.00", "12.1"]]
        raw = json.dumps({
            "stream": "aiausdt@depth10@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1700000000000,
                "T": 1700000000000,
                "s": "AIAUSDT",
                "U": 100,
                "u": 110,
                "pu": 99,
                "b": bids,
                "a": asks,
            }
        })
        result = parse_ob_fut(raw)
        assert result is not None
        symbol, got_bids, got_asks, ts_ms = result
        assert symbol == "AIAUSDT"
        assert got_bids == bids
        assert got_asks == asks
        assert isinstance(ts_ms, int)

    def test_truncates_to_10_levels(self):
        bids = [[str(34000 - i), "1.0"] for i in range(15)]
        asks = [[str(34001 + i), "1.0"] for i in range(15)]
        raw = json.dumps({
            "stream": "btcusdt@depth10@100ms",
            "data": {"e": "depthUpdate", "s": "BTCUSDT", "b": bids, "a": asks}
        })
        result = parse_ob_fut(raw)
        assert result is not None
        _, got_bids, got_asks, _ = result
        assert len(got_bids) == 10
        assert len(got_asks) == 10

    def test_spot_format_returns_none(self):
        """Spot format (bids/asks) must be rejected by parse_ob_fut."""
        raw = json.dumps({
            "stream": "btcusdt@depth10@100ms",
            "data": {"lastUpdateId": 1027024, "bids": [["100", "1"]], "asks": [["101", "1"]]}
        })
        assert parse_ob_fut(raw) is None

    def test_non_depth_update_returns_none(self):
        raw = json.dumps({
            "stream": "btcusdt@bookTicker",
            "data": {"e": "bookTicker", "s": "BTCUSDT", "b": "100", "a": "101"}
        })
        assert parse_ob_fut(raw) is None

    def test_invalid_json_returns_none(self):
        assert parse_ob_fut("{bad json") is None


# ── parse_fr ───────────────────────────────────────────────────────────────

class TestParseFr:
    def test_single_markprice_update(self):
        raw = json.dumps({
            "stream": "btcusdt@markPrice",
            "data": {
                "e": "markPriceUpdate", "E": 1705312345000,
                "s": "BTCUSDT", "p": "34005.00",
                "r": "0.00010000", "T": 1705320000000
            }
        })
        results = parse_fr(raw)
        assert len(results) == 1
        symbol, rate, fr_ts = results[0]
        assert symbol == "BTCUSDT"
        assert rate == "0.00010000"
        assert fr_ts == "1705320000000"

    def test_arr_stream_multiple_symbols(self):
        items = [
            {"e": "markPriceUpdate", "s": "BTCUSDT", "r": "0.0001", "T": 1705320000000},
            {"e": "markPriceUpdate", "s": "ETHUSDT", "r": "0.0002", "T": 1705320000000},
            {"e": "markPriceUpdate", "s": "BNBUSDT", "r": "0.0003", "T": 1705320000000},
        ]
        raw = json.dumps({"stream": "!markPrice@arr", "data": items})
        results = parse_fr(raw)
        assert len(results) == 3
        symbols = {r[0] for r in results}
        assert symbols == {"BTCUSDT", "ETHUSDT", "BNBUSDT"}

    def test_arr_stream_skips_wrong_event_type(self):
        items = [
            {"e": "markPriceUpdate", "s": "BTCUSDT", "r": "0.0001", "T": 1705320000000},
            {"e": "otherEvent",      "s": "ETHUSDT", "r": "0.0002", "T": 1705320000000},
        ]
        raw = json.dumps({"stream": "!markPrice@arr", "data": items})
        results = parse_fr(raw)
        assert len(results) == 1
        assert results[0][0] == "BTCUSDT"

    def test_missing_rate_skipped(self):
        raw = json.dumps({
            "stream": "btcusdt@markPrice",
            "data": {"e": "markPriceUpdate", "s": "BTCUSDT", "p": "34005.00", "T": 1705320000000}
            # "r" missing
        })
        results = parse_fr(raw)
        assert results == []

    def test_invalid_json_returns_empty(self):
        assert parse_fr("bad") == []

    def test_empty_arr_returns_empty(self):
        raw = json.dumps({"data": []})
        results = parse_fr(raw)
        assert results == []


# ── Chunk ID logic ─────────────────────────────────────────────────────────

class TestChunkId:
    def test_chunk_id_from_ts(self):
        # ts_ms = 1705312345000 → ts_sec = 1705312345
        # chunk_id = int(1705312345 / 1200) = 1421093
        ts_ms = 1705312345000
        chunk_id = int(ts_ms / 1000 / 1200)
        assert chunk_id == 1421093

    def test_chunk_boundary(self):
        # Two timestamps 20 minutes apart should be in different chunks
        ts1 = 1705312000000
        ts2 = ts1 + 1200 * 1000  # exactly 20 minutes later
        chunk1 = int(ts1 / 1000 / 1200)
        chunk2 = int(ts2 / 1000 / 1200)
        assert chunk2 == chunk1 + 1

    def test_chunk_same_within_period(self):
        ts1 = 1705312000000
        ts2 = ts1 + 100 * 1000  # 100 seconds later — same chunk
        chunk1 = int(ts1 / 1000 / 1200)
        chunk2 = int(ts2 / 1000 / 1200)
        assert chunk1 == chunk2

    def test_four_chunks_cover_80_minutes(self):
        """4 active chunks at 20 min each = 80 minutes of history."""
        assert 4 * 1200 == 4800  # seconds = 80 minutes
        assert 4800 < 6000       # TTL=6000 covers all 4 chunks


# ── Buffer writer unit tests ───────────────────────────────────────────────

class TestBufferWriters:
    _original_history_enabled = None

    def setup_method(self):
        """Reset shared state before each test."""
        import collectors.collector_binance as cb
        import config
        cb.batch_buffer.clear()
        cb.hist_buffer.clear()
        cb.cmd_counter = 0
        # Save original HISTORY_ENABLED to restore after each test
        TestBufferWriters._original_history_enabled = config.HISTORY_ENABLED

    def teardown_method(self):
        import config
        if TestBufferWriters._original_history_enabled is not None:
            config.HISTORY_ENABLED = TestBufferWriters._original_history_enabled

    def test_write_md_primary_key(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = False
        cb.write_md_to_buffer("BTCUSDT", "34000.10", "34001.50", 1705312345000, "spot")
        assert "md:binance:spot:BTCUSDT" in cb.batch_buffer
        fields = cb.batch_buffer["md:binance:spot:BTCUSDT"]
        assert fields["b"] == "34000.10"
        assert fields["a"] == "34001.50"
        assert fields["ts"] == "1705312345000"

    def test_write_md_history_when_enabled(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = True
        ts_ms = 1705312345000
        cb.write_md_to_buffer("BTCUSDT", "34000.10", "34001.50", ts_ms, "spot")
        assert len(cb.hist_buffer) == 1
        hist_key, line = cb.hist_buffer[0]
        assert "md:hist:binance:spot:BTCUSDT:" in hist_key
        assert "34000.10,34001.50,1705312345000" in line

    def test_write_md_no_history_when_disabled(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = False
        cb.write_md_to_buffer("BTCUSDT", "34000.10", "34001.50", 1705312345000, "spot")
        assert len(cb.hist_buffer) == 0

    def test_write_ob_primary_key(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = False
        bids = [["34000.10", "10.5"], ["33999.00", "5.2"]]
        asks = [["34001.50", "8.3"]]
        cb.write_ob_to_buffer("BTCUSDT", bids, asks, 1705312345000, "spot")
        assert "ob:binance:spot:BTCUSDT" in cb.batch_buffer
        fields = cb.batch_buffer["ob:binance:spot:BTCUSDT"]
        assert fields["b1"] == "34000.10"
        assert fields["b1q"] == "10.5"
        assert fields["a1"] == "34001.50"
        assert fields["a1q"] == "8.3"

    def test_write_ob_history_line_format(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = True
        bids = [["34000.10", "10.5"]]
        asks = [["34001.50", "8.3"]]
        ts_ms = 1705312345000
        cb.write_ob_to_buffer("BTCUSDT", bids, asks, ts_ms, "futures")
        assert len(cb.hist_buffer) == 1
        hist_key, line = cb.hist_buffer[0]
        assert "ob:hist:binance:futures:BTCUSDT:" in hist_key
        assert "34000.10,10.5" in line
        assert "34001.50,8.3" in line
        assert str(ts_ms) in line

    def test_write_fr_primary_key(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = False
        cb.write_fr_to_buffer("BTCUSDT", "0.00010000", "1705320000000")
        assert "fr:binance:futures:BTCUSDT" in cb.batch_buffer
        fields = cb.batch_buffer["fr:binance:futures:BTCUSDT"]
        assert fields["fr"] == "0.00010000"
        assert fields["fr_ts"] == "1705320000000"

    def test_cmd_counter_increments(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = True
        cb.write_md_to_buffer("BTCUSDT", "1.0", "2.0", 1705312345000, "spot")
        assert cb.cmd_counter == 1  # только HSET (hist_buffer — отдельная очередь)
        assert len(cb.hist_buffer) == 1  # LPUSH попал в hist_buffer

    def test_cmd_counter_without_history(self):
        import collectors.collector_binance as cb
        import config
        config.HISTORY_ENABLED = False
        cb.write_md_to_buffer("BTCUSDT", "1.0", "2.0", 1705312345000, "spot")
        assert cb.cmd_counter == 1  # HSET only


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
