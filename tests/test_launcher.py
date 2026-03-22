# tests/test_launcher.py
"""Unit tests for launcher — startup checks, process definitions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestLauncherConfig:
    def test_all_expected_processes_defined(self):
        import config
        config.HISTORY_ENABLED = True  # test data branch
        # Re-import to pick up HISTORY_ENABLED change
        import importlib
        import launcher
        importlib.reload(launcher)

        expected_base = {
            "collector_binance",
            "collector_bybit",
            "collector_okx",
            "collector_gate",
            "collector_bitget",
            "redis_monitor",
            "stale_monitor",
            "spread_monitor",
            "snapshot_monitor",
        }
        assert set(launcher.PROCESSES.keys()) == expected_base

    def test_snapshot_monitor_excluded_without_history(self):
        import config
        config.HISTORY_ENABLED = False
        import importlib
        import launcher
        importlib.reload(launcher)

        assert "snapshot_monitor" not in launcher.PROCESSES
        assert "spread_monitor" in launcher.PROCESSES

    def test_all_scripts_exist(self):
        import config
        config.HISTORY_ENABLED = True
        import importlib
        import launcher
        importlib.reload(launcher)

        for name, script in launcher.PROCESSES.items():
            p = Path(script)
            assert p.exists(), f"Script for {name} not found: {script}"

    def teardown_method(self):
        import config
        config.HISTORY_ENABLED = True  # restore


class TestSubscribeFiles:
    def test_all_subscribe_files_exist(self):
        import config
        exchanges = ["binance", "bybit", "okx", "gate", "bitget"]
        for exch in exchanges:
            for market in ["spot", "futures"]:
                path = Path(f"{config.SUBSCRIBE_DIR}/{exch}/{exch}_{market}.txt")
                assert path.exists(), f"Missing subscribe file: {path}"

    def test_subscribe_files_not_empty(self):
        import config
        exchanges = ["binance", "bybit", "okx", "gate", "bitget"]
        for exch in exchanges:
            for market in ["spot", "futures"]:
                path = Path(f"{config.SUBSCRIBE_DIR}/{exch}/{exch}_{market}.txt")
                if path.exists():
                    lines = [l.strip() for l in path.read_text().splitlines() if l.strip()]
                    assert len(lines) > 0, f"Subscribe file is empty: {path}"
