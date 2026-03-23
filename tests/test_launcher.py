# tests/test_launcher.py
"""Unit tests for launcher — startup checks, process definitions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestLauncherConfig:
    _original_history_enabled = None

    def setup_method(self):
        import config
        TestLauncherConfig._original_history_enabled = config.HISTORY_ENABLED

    def teardown_method(self):
        import config
        import importlib, launcher
        config.HISTORY_ENABLED = TestLauncherConfig._original_history_enabled
        importlib.reload(launcher)

    def test_base_processes_always_defined(self):
        import importlib
        import launcher
        importlib.reload(launcher)

        base = {
            "collector_binance", "collector_bybit", "collector_okx",
            "collector_gate", "collector_bitget",
            "redis_monitor", "stale_monitor", "spread_monitor",
        }
        assert base.issubset(set(launcher.PROCESSES.keys()))

    def test_snapshot_monitor_present_with_history_enabled(self):
        import config
        config.HISTORY_ENABLED = True
        import importlib
        import launcher
        importlib.reload(launcher)
        assert "snapshot_monitor" in launcher.PROCESSES

    def test_snapshot_monitor_excluded_without_history(self):
        import config
        config.HISTORY_ENABLED = False
        import importlib
        import launcher
        importlib.reload(launcher)
        assert "snapshot_monitor" not in launcher.PROCESSES
        assert "spread_monitor" in launcher.PROCESSES

    def test_all_scripts_exist(self):
        """All scripts in PROCESSES for current branch must exist on disk."""
        import importlib
        import launcher
        importlib.reload(launcher)

        for name, script in launcher.PROCESSES.items():
            p = Path(script)
            assert p.exists(), f"Script for {name} not found: {script}"


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


class TestEnsureRedis:
    def test_ensure_redis_success(self, monkeypatch):
        """ensure_redis() should not exit when script returns 0."""
        import launcher
        from unittest.mock import patch, MagicMock

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "[redis_setup] Redis already running"
        mock_result.stderr = ""

        with patch("launcher.subprocess.run", return_value=mock_result):
            # Should complete without raising SystemExit
            launcher.ensure_redis()

    def test_ensure_redis_failure_exits(self, monkeypatch):
        """ensure_redis() should call sys.exit(1) when script returns non-zero."""
        import launcher
        from unittest.mock import patch, MagicMock
        import pytest

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "ERROR: Redis did not start"

        with patch("launcher.subprocess.run", return_value=mock_result):
            with pytest.raises(SystemExit) as exc_info:
                launcher.ensure_redis()
            assert exc_info.value.code == 1
