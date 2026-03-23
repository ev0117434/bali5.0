# tests/test_other_parsers.py
"""Unit tests for Bybit, OKX, Gate.io, Bitget parsers (no network, no Redis)."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


# ──────────────────────────────────────────────────────────────────────────
# Bybit
# ──────────────────────────────────────────────────────────────────────────

class TestBybitParsers:
    from collectors.collector_bybit import parse_md, parse_ob, parse_fr_from_ticker

    def test_parse_md_snapshot(self):
        raw = json.dumps({
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "data": {
                "symbol": "BTCUSDT",
                "bid1Price": "34000.10",
                "bid1Size": "10.50",
                "ask1Price": "34001.50",
                "ask1Size": "5.20",
            },
            "ts": 1705312345123,
        })
        from collectors.collector_bybit import parse_md
        result = parse_md(raw)
        assert result is not None
        symbol, bid, ask, ts_ms = result
        assert symbol == "BTCUSDT"
        assert bid == "34000.10"
        assert ask == "34001.50"
        assert ts_ms == 1705312345123

    def test_parse_md_pong_returns_none(self):
        from collectors.collector_bybit import parse_md
        raw = json.dumps({"op": "pong", "ret_msg": "", "conn_id": "abc"})
        assert parse_md(raw) is None

    def test_parse_md_missing_bid_returns_none(self):
        from collectors.collector_bybit import parse_md
        raw = json.dumps({
            "topic": "tickers.BTCUSDT",
            "data": {"symbol": "BTCUSDT", "ask1Price": "34001.50"},
        })
        assert parse_md(raw) is None

    def test_parse_fr_from_ticker(self):
        from collectors.collector_bybit import parse_fr_from_ticker
        raw = json.dumps({
            "topic": "tickers.BTCUSDT",
            "data": {
                "symbol": "BTCUSDT",
                "bid1Price": "34000.10",
                "ask1Price": "34001.50",
                "fundingRate": "0.00010000",
                "nextFundingTime": "1705320000000",
            },
        })
        result = parse_fr_from_ticker(raw)
        assert result is not None
        symbol, rate, fr_ts = result
        assert symbol == "BTCUSDT"
        assert rate == "0.00010000"
        assert fr_ts == "1705320000000"

    def test_parse_fr_no_funding_returns_none(self):
        from collectors.collector_bybit import parse_fr_from_ticker
        raw = json.dumps({
            "topic": "tickers.BTCUSDT",
            "data": {"symbol": "BTCUSDT", "bid1Price": "34000.10"},
        })
        assert parse_fr_from_ticker(raw) is None

    def test_parse_ob_snapshot(self):
        from collectors.collector_bybit import parse_ob
        raw = json.dumps({
            "topic": "orderbook.10.BTCUSDT",
            "type": "snapshot",
            "data": {
                "s": "BTCUSDT",
                "b": [["34000.10", "10.50"], ["33999.00", "5.20"]],
                "a": [["34001.50", "8.30"], ["34002.00", "12.10"]],
            },
            "ts": 1705312345123,
        })
        result = parse_ob(raw)
        assert result is not None
        symbol, bids, asks, ts_ms = result
        assert symbol == "BTCUSDT"
        assert bids[0] == ["34000.10", "10.50"]
        assert asks[0] == ["34001.50", "8.30"]
        assert ts_ms == 1705312345123

    def test_parse_ob_empty_data_returns_none(self):
        from collectors.collector_bybit import parse_ob
        raw = json.dumps({"topic": "orderbook.10.BTCUSDT", "data": {"s": "", "b": [], "a": []}})
        assert parse_ob(raw) is None


# ──────────────────────────────────────────────────────────────────────────
# OKX
# ──────────────────────────────────────────────────────────────────────────

class TestOkxParsers:
    def test_parse_md_bbo_tbt(self):
        from collectors.collector_okx import parse_md
        raw = json.dumps({
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"},
            "data": [{
                "bids": [["34000.10", "10.50", "0", "1"]],
                "asks": [["34001.50", "8.30", "0", "1"]],
                "ts": "1705312345123",
            }],
        })
        result = parse_md(raw)
        assert result is not None
        symbol, bid, ask, ts_ms = result
        assert symbol == "BTCUSDT"   # BTC-USDT → BTCUSDT
        assert bid == "34000.10"
        assert ask == "34001.50"
        assert ts_ms == 1705312345123

    def test_parse_md_normalizes_swap(self):
        from collectors.collector_okx import parse_md
        raw = json.dumps({
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"},
            "data": [{"bids": [["34000.10", "1"]], "asks": [["34001.50", "1"]], "ts": "1705312345000"}],
        })
        result = parse_md(raw)
        assert result is not None
        assert result[0] == "BTCUSDT"   # BTC-USDT-SWAP → BTCUSDT

    def test_parse_md_wrong_channel_returns_none(self):
        from collectors.collector_okx import parse_md
        raw = json.dumps({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "data": [{"bids": [], "asks": [], "ts": "1705312345000"}],
        })
        assert parse_md(raw) is None

    def test_parse_ob_snapshot(self):
        from collectors.collector_okx import parse_ob
        raw = json.dumps({
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "bids": [["34000.10", "10.50", "0", "1"], ["33999.00", "5.20", "0", "1"]],
                "asks": [["34001.50", "8.30", "0", "1"]],
                "ts": "1705312345123",
            }],
        })
        result = parse_ob(raw)
        assert result is not None
        symbol, bids, asks, ts_ms = result
        assert symbol == "BTCUSDT"
        assert len(bids) == 2
        assert bids[0] == ["34000.10", "10.50"]

    def test_parse_ob_update_accepted(self):
        from collectors.collector_okx import parse_ob
        raw = json.dumps({
            "arg": {"channel": "books", "instId": "ETH-USDT"},
            "action": "update",
            "data": [{"bids": [["2000.00", "5.0", "0", "1"]], "asks": [], "ts": "1705312345000"}],
        })
        result = parse_ob(raw)
        assert result is not None

    def test_parse_fr(self):
        from collectors.collector_okx import parse_fr
        raw = json.dumps({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "instId": "BTC-USDT-SWAP",
                "fundingRate": "0.00010000",
                "nextFundingTime": "1705320000000",
            }],
        })
        result = parse_fr(raw)
        assert result is not None
        symbol, rate, fr_ts = result
        assert symbol == "BTCUSDT"
        assert rate == "0.00010000"
        assert fr_ts == "1705320000000"

    def test_parse_fr_missing_rate_returns_none(self):
        from collectors.collector_okx import parse_fr
        raw = json.dumps({
            "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
            "data": [{"instId": "BTC-USDT-SWAP", "nextFundingTime": "1705320000000"}],
        })
        assert parse_fr(raw) is None

    def test_normalize_spot(self):
        from collectors.collector_okx import _normalize
        assert _normalize("BTC-USDT") == "BTCUSDT"

    def test_normalize_swap(self):
        from collectors.collector_okx import _normalize
        assert _normalize("BTC-USDT-SWAP") == "BTCUSDT"

    def test_normalize_eth(self):
        from collectors.collector_okx import _normalize
        assert _normalize("ETH-USDT") == "ETHUSDT"


# ──────────────────────────────────────────────────────────────────────────
# Gate.io
# ──────────────────────────────────────────────────────────────────────────

class TestGateParsers:
    def test_parse_md_spot(self):
        from collectors.collector_gate import parse_md_spot
        raw = json.dumps({
            "channel": "spot.book_ticker",
            "event": "update",
            "result": {
                "t": 1705312345123,
                "s": "BTC_USDT",
                "b": "34000.10",
                "B": "10.50",
                "a": "34001.50",
                "A": "5.20",
            },
        })
        result = parse_md_spot(raw)
        assert result is not None
        symbol, bid, ask, ts_ms = result
        assert symbol == "BTCUSDT"   # BTC_USDT → BTCUSDT
        assert bid == "34000.10"
        assert ask == "34001.50"
        assert ts_ms == 1705312345123

    def test_parse_md_spot_subscribe_event_returns_none(self):
        from collectors.collector_gate import parse_md_spot
        raw = json.dumps({
            "channel": "spot.book_ticker",
            "event": "subscribe",
            "result": None,
        })
        assert parse_md_spot(raw) is None

    def test_parse_md_fut(self):
        from collectors.collector_gate import parse_md_fut
        raw = json.dumps({
            "channel": "futures.book_ticker",
            "event": "update",
            "result": {
                "t": 1705312345123,
                "s": "BTC_USDT",
                "b": "34000.10",
                "a": "34001.50",
            },
        })
        result = parse_md_fut(raw)
        assert result is not None
        assert result[0] == "BTCUSDT"

    def test_parse_fr_seconds_to_ms(self):
        """Gate.io funding_next_apply is in seconds — must convert to ms."""
        from collectors.collector_gate import parse_fr
        raw = json.dumps({
            "channel": "futures.tickers",
            "event": "update",
            "result": [{
                "contract": "BTC_USDT",
                "funding_rate": "0.000100",
                "funding_next_apply": 1705320000,  # SECONDS
            }],
        })
        result = parse_fr(raw)
        assert result is not None
        symbol, rate, fr_ts_ms = result
        assert symbol == "BTCUSDT"
        assert fr_ts_ms == "1705320000000"   # converted to ms

    def test_parse_fr_subscribe_returns_none(self):
        from collectors.collector_gate import parse_fr
        raw = json.dumps({"channel": "futures.tickers", "event": "subscribe", "result": None})
        assert parse_fr(raw) is None

    def test_parse_ob_spot(self):
        from collectors.collector_gate import parse_ob_spot
        raw = json.dumps({
            "channel": "spot.order_book",
            "event": "update",
            "result": {
                "t": 1705312345123,
                "s": "BTC_USDT",
                "bids": [["34000.10", "10.50"], ["33999.00", "5.20"]],
                "asks": [["34001.50", "8.30"]],
            },
        })
        result = parse_ob_spot(raw)
        assert result is not None
        symbol, bids, asks, ts_ms = result
        assert symbol == "BTCUSDT"
        assert bids[0] == ["34000.10", "10.50"]

    def test_normalize(self):
        from collectors.collector_gate import _normalize
        assert _normalize("BTC_USDT") == "BTCUSDT"
        assert _normalize("ETH_USDT") == "ETHUSDT"


# ──────────────────────────────────────────────────────────────────────────
# Bitget
# ──────────────────────────────────────────────────────────────────────────

class TestBitgetParsers:
    def test_parse_md(self):
        from collectors.collector_bitget import parse_md
        raw = json.dumps({
            "action": "snapshot",
            "arg": {"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [{
                "instId": "BTCUSDT",
                "bidPr": "34000.10",
                "bidSz": "10.50",
                "askPr": "34001.50",
                "askSz": "5.20",
                "ts": "1705312345123",
            }],
        })
        result = parse_md(raw)
        assert result is not None
        symbol, bid, ask, ts_ms = result
        assert symbol == "BTCUSDT"
        assert bid == "34000.10"
        assert ask == "34001.50"
        assert ts_ms == 1705312345123

    def test_parse_md_pong_returns_none(self):
        from collectors.collector_bitget import parse_md
        assert parse_md("pong") is None

    def test_parse_md_subscribe_returns_none(self):
        from collectors.collector_bitget import parse_md
        raw = json.dumps({"event": "subscribe", "arg": {}})
        assert parse_md(raw) is None

    def test_parse_fr_from_ticker(self):
        from collectors.collector_bitget import parse_fr_from_ticker
        raw = json.dumps({
            "data": [{
                "instId": "BTCUSDT",
                "fundingRate": "0.000100",
                "nextSettleTime": "1705320000000",
            }]
        })
        result = parse_fr_from_ticker(raw)
        assert result is not None
        symbol, rate, fr_ts = result
        assert symbol == "BTCUSDT"
        assert rate == "0.000100"
        assert fr_ts == "1705320000000"

    def test_parse_fr_no_funding_returns_none(self):
        from collectors.collector_bitget import parse_fr_from_ticker
        raw = json.dumps({"data": [{"instId": "BTCUSDT", "nextSettleTime": "1705320000000"}]})
        assert parse_fr_from_ticker(raw) is None

    def test_parse_ob(self):
        from collectors.collector_bitget import parse_ob
        raw = json.dumps({
            "action": "snapshot",
            "arg": {"instType": "SPOT", "channel": "books", "instId": "BTCUSDT"},
            "data": [{
                "bids": [["34000.10", "10.50"], ["33999.00", "5.20"]],
                "asks": [["34001.50", "8.30"]],
                "ts": "1705312345123",
            }],
        })
        result = parse_ob(raw)
        assert result is not None
        symbol, bids, asks, ts_ms = result
        assert symbol == "BTCUSDT"
        assert bids[0] == ["34000.10", "10.50"]
        assert ts_ms == 1705312345123

    def test_parse_ob_truncates_to_10_levels(self):
        from collectors.collector_bitget import parse_ob
        bids = [[str(34000 - i), "1.0"] for i in range(15)]
        asks = [[str(34001 + i), "1.0"] for i in range(15)]
        raw = json.dumps({
            "arg": {"instId": "ETHUSDT"},
            "data": [{"bids": bids, "asks": asks, "ts": "1705312345000"}],
        })
        result = parse_ob(raw)
        assert result is not None
        _, got_bids, got_asks, _ = result
        assert len(got_bids) == 10
        assert len(got_asks) == 10


# ──────────────────────────────────────────────────────────────────────────
# Stats dict tests — one per collector
# ──────────────────────────────────────────────────────────────────────────

_REQUIRED_NEW_FIELDS = [
    "parse_errors", "parse_lat_sum", "parse_lat_max",
    "flush_slow_count", "hist_flush_slow_count",
    "buffer_age_sum", "buffer_age_max",
    "e2e_lat_sum", "e2e_lat_max", "e2e_lat_count",
]


class TestCollectorBybitStats:
    def test_stats_dict_has_new_fields(self):
        from collectors.collector_bybit import stats
        for field in _REQUIRED_NEW_FIELDS:
            assert field in stats, f"Missing stats field: {field}"


class TestCollectorOkxStats:
    def test_stats_dict_has_new_fields(self):
        from collectors.collector_okx import stats
        for field in _REQUIRED_NEW_FIELDS:
            assert field in stats, f"Missing stats field: {field}"


class TestCollectorGateStats:
    def test_stats_dict_has_new_fields(self):
        from collectors.collector_gate import stats
        for field in _REQUIRED_NEW_FIELDS:
            assert field in stats, f"Missing stats field: {field}"


class TestCollectorBitgetStats:
    def test_stats_dict_has_new_fields(self):
        from collectors.collector_bitget import stats
        for field in _REQUIRED_NEW_FIELDS:
            assert field in stats, f"Missing stats field: {field}"
