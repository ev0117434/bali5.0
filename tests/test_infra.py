# tests/test_infra.py
"""Phase 0 smoke tests — config, logger, Redis connection."""

import sys
import os
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_config_imports():
    from config import REDIS_URL, HISTORY_ENABLED, SPREAD_THRESHOLD
    assert SPREAD_THRESHOLD == 1.00
    assert isinstance(HISTORY_ENABLED, bool)  # True in data branch, False in trade
    assert REDIS_URL.startswith("redis://")


def test_config_chunk_values():
    from config import CHUNK_DURATION, CHUNK_TTL, MAX_HISTORY_CHUNKS
    assert CHUNK_DURATION == 1200
    assert CHUNK_TTL == 6000
    assert MAX_HISTORY_CHUNKS == 4
    # TTL должен покрывать все активные чанки
    assert CHUNK_TTL >= CHUNK_DURATION * MAX_HISTORY_CHUNKS


def test_config_batch_values():
    from config import BATCH_FLUSH_INTERVAL_MS, BATCH_MAX_COMMANDS
    assert BATCH_FLUSH_INTERVAL_MS == 250
    assert BATCH_MAX_COMMANDS == 100


def test_config_spread_values():
    from config import SPREAD_THRESHOLD, COOLDOWN_SECONDS, SNAPSHOT_DURATION
    assert SPREAD_THRESHOLD == 1.00
    assert COOLDOWN_SECONDS == 3600
    assert SNAPSHOT_DURATION == 3500


def test_logger_creates_file(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "LOGS_DIR", str(tmp_path))

    # Re-import to pick up patched LOGS_DIR
    import importlib
    import logger_setup
    importlib.reload(logger_setup)

    log = logger_setup.setup_logger("test_infra")
    log.info("test message")

    log_file = tmp_path / "test_infra.log"
    assert log_file.exists(), f"Log file not found at {log_file}"
    content = log_file.read_text()
    assert "test message" in content


def test_logger_format(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "LOGS_DIR", str(tmp_path))

    import importlib
    import logger_setup
    importlib.reload(logger_setup)

    log = logger_setup.setup_logger("test_format")
    log.debug("format_check")

    content = (tmp_path / "test_format.log").read_text()
    assert "[DEBUG   ]" in content
    assert "[test_format]" in content
    assert "format_check" in content


def test_redis_connection():
    import redis
    r = redis.Redis.from_url("redis://localhost:6379")
    assert r.ping(), "Redis не отвечает — убедитесь что redis-server запущен"
    r.close()
