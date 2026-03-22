# tests/test_snapshot_csv.py
"""Unit tests for snapshot monitor CSV building logic."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
# Patch HISTORY_ENABLED to True for import
config.HISTORY_ENABLED = True

from monitors.snapshot_monitor import (
    build_snapshot_row,
    ob_list_to_dict,
    _ob_fields_to_list,
    _CSV_HEADER,
)


# ── OB helpers ─────────────────────────────────────────────────────────────

class TestObHelpers:
    def test_ob_fields_to_list_full_10_levels(self):
        ob = {f"b{i}": str(i * 100) for i in range(1, 11)}
        ob.update({f"b{i}q": str(i * 0.5) for i in range(1, 11)})

        prices_qtys = _ob_fields_to_list(ob, "b")
        assert len(prices_qtys) == 20  # 10 prices + 10 qtys
        assert prices_qtys[0] == "100"   # b1 price
        assert prices_qtys[10] == "0.5"  # b1q

    def test_ob_fields_to_list_empty_ob(self):
        result = _ob_fields_to_list({}, "b")
        assert len(result) == 20
        assert all(v == "" for v in result)

    def test_ob_fields_to_list_none_ob(self):
        result = _ob_fields_to_list(None, "b")
        assert len(result) == 20
        assert all(v == "" for v in result)

    def test_ob_list_to_dict_roundtrip(self):
        # Build OB data
        ob_original = {}
        for i in range(1, 11):
            ob_original[f"b{i}"]  = str(34000 - i)
            ob_original[f"b{i}q"] = str(i * 1.5)
        for i in range(1, 11):
            ob_original[f"a{i}"]  = str(34001 + i)
            ob_original[f"a{i}q"] = str(i * 0.8)

        # Serialize to OB line format (as written by collector)
        bids = [f"{ob_original[f'b{i}']},{ob_original[f'b{i}q']}" for i in range(1, 11)]
        asks = [f"{ob_original[f'a{i}']},{ob_original[f'a{i}q']}" for i in range(1, 11)]
        ts_ms = 1705312345000
        line = ",".join(bids + asks + [str(ts_ms)])

        # Parse back
        parts = line.split(",")
        recovered = ob_list_to_dict(parts[:-1])  # strip ts

        assert recovered["b1"]  == ob_original["b1"]
        assert recovered["b1q"] == ob_original["b1q"]
        assert recovered["a10"]  == ob_original["a10"]
        assert recovered["a10q"] == ob_original["a10q"]


# ── CSV row building ───────────────────────────────────────────────────────

class TestBuildSnapshotRow:
    def _make_ob(self, n_levels=10, prefix_b="34000", prefix_a="34001"):
        ob = {}
        for i in range(1, n_levels + 1):
            ob[f"b{i}"]  = str(float(prefix_b) - i)
            ob[f"b{i}q"] = str(i * 1.5)
            ob[f"a{i}"]  = str(float(prefix_a) + i)
            ob[f"a{i}q"] = str(i * 0.8)
        return ob

    def test_row_has_correct_column_count(self):
        ob_spot = self._make_ob()
        ob_fut  = self._make_ob(prefix_b="45000", prefix_a="45001")

        row = build_snapshot_row(
            "binance", "bybit", "BTCUSDT",
            "34001.50", "45676.35", "1.5023", 1705312345890,
            "0.000100", "1705320000000",
            ob_spot, ob_fut,
        )
        cols = row.split(",")
        # Header column count
        header_cols = _CSV_HEADER.split(",")
        assert len(cols) == len(header_cols), (
            f"Row has {len(cols)} cols, header has {len(header_cols)}"
        )

    def test_row_starts_with_metadata(self):
        row = build_snapshot_row(
            "binance", "bybit", "BTCUSDT",
            "34001.50", "45676.35", "1.5023", 1705312345890,
            "0.000100", "1705320000000",
            {}, {},
        )
        parts = row.split(",")
        assert parts[0] == "binance"
        assert parts[1] == "bybit"
        assert parts[2] == "BTCUSDT"
        assert parts[3] == "34001.50"
        assert parts[4] == "45676.35"
        assert parts[5] == "1.5023"
        assert parts[6] == "1705312345890"
        assert parts[7] == "0.000100"
        assert parts[8] == "1705320000000"

    def test_row_with_empty_ob_still_correct_columns(self):
        row = build_snapshot_row(
            "binance", "bybit", "BTCUSDT",
            "34001.50", "45676.35", "1.5023", 1705312345890,
            "", "",
            {}, {},
        )
        cols = row.split(",")
        header_cols = _CSV_HEADER.split(",")
        assert len(cols) == len(header_cols)

    def test_csv_header_column_count(self):
        """Header must have exactly 89 columns (metadata + OB levels)."""
        expected = (
            9           # spot_exch,fut_exch,symbol,ask_spot,bid_fut,spread_pct,ts,fr,fr_time
            + 10 + 10   # s_b1..10, s_bq1..10
            + 10 + 10   # s_a1..10, s_aq1..10
            + 10 + 10   # f_b1..10, f_bq1..10
            + 10 + 10   # f_a1..10, f_aq1..10
        )
        actual = len(_CSV_HEADER.split(","))
        assert actual == expected, (
            f"Header has {actual} cols, expected {expected}"
        )

    def test_row_preserves_fr_values(self):
        ob = self._make_ob()
        row = build_snapshot_row(
            "binance", "bybit", "BTCUSDT",
            "34001.50", "45676.35", "1.5023", 1705312345890,
            "0.00010000", "1705320000000",
            ob, ob,
        )
        parts = row.split(",")
        assert parts[7] == "0.00010000"
        assert parts[8] == "1705320000000"


# ── History row parsing (OB line format) ──────────────────────────────────

class TestObLineFormat:
    def test_collector_format_parsed_correctly(self):
        """
        OB history line from collector_binance:
        b1p,b1q,b2p,b2q,...,b10p,b10q,a1p,a1q,...,a10p,a10q,ts_ms
        """
        bids = [["34000.10", "10.5"], ["33999.00", "5.2"]] + [["0", "0"]] * 8
        asks = [["34001.50", "8.3"], ["34002.00", "12.1"]] + [["0", "0"]] * 8
        ts_ms = 1705312345000

        bid_parts = [f"{p},{q}" for p, q in bids]
        ask_parts = [f"{p},{q}" for p, q in asks]
        line = ",".join(bid_parts + ask_parts + [str(ts_ms)])

        parts = line.split(",")
        # ts is last
        assert int(parts[-1]) == ts_ms

        # Parse OB (without ts)
        ob = ob_list_to_dict(parts[:-1])
        assert ob["b1"]  == "34000.10"
        assert ob["b1q"] == "10.5"
        assert ob["a1"]  == "34001.50"
        assert ob["a1q"] == "8.3"
