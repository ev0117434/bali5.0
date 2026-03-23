# tests/test_spread_logic.py
"""Unit tests for spread monitor logic — spread formula, pair loading, CSV."""

import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Spread formula ─────────────────────────────────────────────────────────

class TestSpreadFormula:
    """
    Correct formula: spread_pct = (bid_futures - ask_spot) / ask_spot * 100
    Positive spread means futures > spot → arbitrage opportunity.
    """

    def _spread(self, ask_spot: float, bid_fut: float) -> float:
        return (bid_fut - ask_spot) / ask_spot * 100

    def test_positive_spread_example_from_spec(self):
        """
        From docs/10_project_structure.md example:
        ask_spot=45000.10, bid_futures=45676.35 → spread≈1.5023%
        """
        spread = self._spread(45000.10, 45676.35)
        assert abs(spread - 1.5023) < 0.001

    def test_negative_spread_when_futures_below_spot(self):
        """When futures < spot, spread is negative → no signal."""
        spread = self._spread(45000.10, 44000.00)
        assert spread < 0

    def test_zero_spread(self):
        spread = self._spread(34000.0, 34000.0)
        assert spread == 0.0

    def test_above_threshold(self):
        spread = self._spread(34000.0, 34340.0)   # ~ 1.0% spread
        assert spread >= 1.00

    def test_just_at_threshold(self):
        threshold = 1.00
        ask_spot = 34000.0
        bid_fut  = ask_spot * (1 + threshold / 100)  # exactly 1%
        spread   = self._spread(ask_spot, bid_fut)
        assert abs(spread - threshold) < 0.0001

    def test_formula_not_inverted(self):
        """
        Confirms old (wrong) formula (ask_spot - bid_fut) gives NEGATIVE.
        The correct formula should give POSITIVE for this case.
        """
        ask_spot = 45000.10
        bid_fut  = 45676.35
        wrong_spread  = (ask_spot - bid_fut) / ask_spot * 100
        correct_spread = (bid_fut - ask_spot) / ask_spot * 100
        assert wrong_spread < 0, "Wrong formula should give negative"
        assert correct_spread > 0, "Correct formula should give positive"
        assert abs(correct_spread) == abs(wrong_spread)


# ── Combo filename parsing ─────────────────────────────────────────────────

class TestComboFilenameParsing:
    def test_standard_format(self):
        from monitors.spread_monitor import _parse_combo_filename
        spot_exch, fut_exch = _parse_combo_filename(
            "dictionaries/combination/binance_spot_bybit_futures.txt"
        )
        assert spot_exch == "binance"
        assert fut_exch == "bybit"

    def test_all_exchange_combos(self):
        from monitors.spread_monitor import _parse_combo_filename
        combos = [
            ("binance_spot_bybit_futures.txt",  "binance", "bybit"),
            ("binance_spot_okx_futures.txt",    "binance", "okx"),
            ("bybit_spot_gate_futures.txt",     "bybit",   "gate"),
            ("okx_spot_bitget_futures.txt",     "okx",     "bitget"),
        ]
        for fname, expected_spot, expected_fut in combos:
            spot, fut = _parse_combo_filename(f"dictionaries/combination/{fname}")
            assert spot == expected_spot, f"spot mismatch for {fname}"
            assert fut  == expected_fut,  f"fut mismatch for {fname}"


# ── Pair loading ───────────────────────────────────────────────────────────

class TestPairLoading:
    def test_loads_pairs_from_tmp_dir(self, tmp_path, monkeypatch):
        import config
        monkeypatch.setattr(config, "COMBINATION_DIR", str(tmp_path))

        # Create a combo file
        combo_file = tmp_path / "binance_spot_bybit_futures.txt"
        combo_file.write_text("BTCUSDT\nETHUSDT\nSOLUSDT\n")

        from monitors.spread_monitor import load_pairs
        pairs = load_pairs()

        assert len(pairs) == 3
        assert ("binance", "bybit", "BTCUSDT") in pairs
        assert ("binance", "bybit", "ETHUSDT") in pairs

    def test_empty_combo_dir_returns_empty(self, tmp_path, monkeypatch):
        import config
        monkeypatch.setattr(config, "COMBINATION_DIR", str(tmp_path))

        from monitors.spread_monitor import load_pairs
        pairs = load_pairs()
        assert pairs == []

    def test_blank_lines_skipped(self, tmp_path, monkeypatch):
        import config
        monkeypatch.setattr(config, "COMBINATION_DIR", str(tmp_path))

        (tmp_path / "binance_spot_bybit_futures.txt").write_text(
            "BTCUSDT\n\n  \nETHUSDT\n"
        )

        from monitors.spread_monitor import load_pairs
        pairs = load_pairs()
        assert len(pairs) == 2

    def test_symbols_normalized_to_uppercase(self, tmp_path, monkeypatch):
        import config
        monkeypatch.setattr(config, "COMBINATION_DIR", str(tmp_path))

        (tmp_path / "binance_spot_bybit_futures.txt").write_text("btcusdt\n")

        from monitors.spread_monitor import load_pairs
        pairs = load_pairs()
        assert pairs[0][2] == "BTCUSDT"


# ── Signal CSV ─────────────────────────────────────────────────────────────

class TestSignalCsv:
    @pytest.mark.asyncio
    async def test_creates_csv_with_header(self, tmp_path, monkeypatch):
        import config
        monkeypatch.setattr(config, "SIGNAL_DIR", str(tmp_path))
        monkeypatch.setattr(config, "SIGNAL_CSV", str(tmp_path / "signal.csv"))

        from monitors.spread_monitor import ensure_signal_csv
        await ensure_signal_csv()

        csv_path = tmp_path / "signal.csv"
        assert csv_path.exists()
        content = csv_path.read_text()
        assert content.startswith(config.SIGNAL_CSV_HEADER)

    @pytest.mark.asyncio
    async def test_does_not_overwrite_existing_nonempty_csv(self, tmp_path, monkeypatch):
        import config
        monkeypatch.setattr(config, "SIGNAL_DIR", str(tmp_path))
        csv_path = tmp_path / "signal.csv"
        monkeypatch.setattr(config, "SIGNAL_CSV", str(csv_path))

        # Pre-existing content
        csv_path.write_text("existing_header\nexisting_row\n")

        from monitors.spread_monitor import ensure_signal_csv
        await ensure_signal_csv()

        content = csv_path.read_text()
        assert "existing_row" in content


class TestSpreadMonitorEvents:
    def test_evt_produces_valid_json(self):
        import json
        from monitors.spread_monitor import _evt
        evt = _evt("signal", spot_exchange="binance", fut_exchange="bybit",
                   symbol="BTCUSDT", spread_pct=1.5)
        obj = json.loads(json.dumps(evt))
        assert obj["event"] == "signal"
        assert obj["component"] == "spread_monitor"
        assert "ts" in obj

    def test_p99_of_single_value(self):
        from monitors.spread_monitor import _p99
        assert _p99([42.0]) == 42.0

    def test_p99_of_multiple_values(self):
        from monitors.spread_monitor import _p99
        values = list(range(1, 101))  # 1..100
        result = _p99(values)
        assert 98.0 <= result <= 100.0
