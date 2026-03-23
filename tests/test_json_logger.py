# tests/test_json_logger.py
"""Tests for JsonFormatter and setup_logger JSON output."""
import io
import json
import logging
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from logger_setup import JsonFormatter, setup_logger


class TestJsonFormatter:
    _TEST_LOGGER_NAMES = [
        "test_dict", "test_str", "test_valid_json", "test_ts", "test_nomutate",
        "test_exc", "test_defaults",
    ]

    @pytest.fixture(autouse=True)
    def _clean_loggers(self):
        yield
        for name in self._TEST_LOGGER_NAMES:
            logging.getLogger(name).handlers.clear()

    def test_dict_msg_is_serialized_as_is(self):
        """Dict message passes through as JSON with ts/component/event preserved."""
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_dict")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.info({"ts": 1741234567890, "component": "test", "event": "hello", "value": 42})
        line = stream.getvalue().strip()
        obj = json.loads(line)
        assert obj["component"] == "test"
        assert obj["event"] == "hello"
        assert obj["value"] == 42
        assert obj["ts"] == 1741234567890

    def test_string_msg_wrapped_in_envelope(self):
        """Plain string messages get wrapped with ts/component/event=log/msg fields."""
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_str")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.warning("something went wrong")
        line = stream.getvalue().strip()
        obj = json.loads(line)
        assert obj["event"] == "log"
        assert obj["msg"] == "something went wrong"
        assert obj["level"] == "WARNING"
        assert "ts" in obj

    def test_output_is_valid_json(self):
        """Every emitted line must be parseable as JSON."""
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_valid_json")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.info({"ts": 1, "component": "x", "event": "y"})
        logger.warning("plain text")
        lines = [l for l in stream.getvalue().strip().splitlines() if l]
        for line in lines:
            json.loads(line)  # must not raise

    def test_ts_is_unix_milliseconds(self):
        """ts field in wrapped plain-string messages is current Unix ms."""
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_ts")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        before = int(time.time() * 1000)
        logger.info("ts test")
        after = int(time.time() * 1000)
        obj = json.loads(stream.getvalue().strip())
        assert before <= obj["ts"] <= after

    def test_dict_msg_not_mutated(self):
        """Original dict passed as msg is not modified."""
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_nomutate")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        original = {"ts": 1, "component": "c", "event": "e", "x": 99}
        copy = dict(original)
        logger.info(original)
        assert original == copy  # not mutated

    def test_exc_info_included_in_output(self):
        """Tracebacks from exc_info are included as 'traceback' field."""
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_exc")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            raise ValueError("test error")
        except ValueError:
            logger.exception("something failed")

        obj = json.loads(stream.getvalue().strip())
        assert "traceback" in obj
        assert "ValueError" in obj["traceback"]

    def test_dict_msg_always_has_ts_and_level(self):
        """Dict messages without ts/level get them from the log record."""
        stream = io.StringIO()
        fmt = JsonFormatter()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(fmt)
        logger = logging.getLogger("test_defaults")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        logger.warning({"component": "x", "event": "y"})  # no ts, no level
        obj = json.loads(stream.getvalue().strip())
        assert "ts" in obj
        assert obj["level"] == "WARNING"
        assert obj["event"] == "y"  # caller's value preserved

    def test_setup_logger_file_handler_uses_json(self, tmp_path, monkeypatch):
        """setup_logger writes JSON to log file."""
        import config
        monkeypatch.setattr(config, "LOGS_DIR", str(tmp_path))
        # Clear existing handlers
        log = logging.getLogger("setup_test_unique_123")
        log.handlers.clear()

        from logger_setup import setup_logger
        logger = setup_logger("setup_test_unique_123")
        logger.info({"ts": 999, "component": "setup_test_unique_123", "event": "test_event"})

        log_file = tmp_path / "setup_test_unique_123.log"
        lines = [l for l in log_file.read_text().strip().splitlines() if l]
        assert len(lines) == 1
        obj = json.loads(lines[0])
        assert obj["event"] == "test_event"
