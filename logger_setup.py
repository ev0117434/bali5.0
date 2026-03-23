# logger_setup.py
"""
BALI 5.0 — Настройка логирования.

File handler: NDJSON (one JSON object per line, machine-readable).
Console handler: human-readable text (WARNING+ only).
"""

import json
import logging
import os
from logging.handlers import RotatingFileHandler

import config


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON (NDJSON).

    - If record.msg is a dict: serialize it directly.
    - If record.msg is str/other: wrap in envelope with ts, component, event='log', level, msg.
    """

    def format(self, record: logging.LogRecord) -> str:
        msg = record.msg
        if isinstance(msg, dict):
            payload = dict(msg)  # shallow copy — never mutate caller's dict
            # Guarantee ts and level are always present
            payload.setdefault("ts", int(record.created * 1000))
            payload.setdefault("level", record.levelname)
        else:
            payload = {
                "ts":        int(record.created * 1000),
                "component": record.name,
                "event":     "log",
                "level":     record.levelname,
                "msg":       record.getMessage(),
            }
        # Include traceback if present (works for both dict and string messages)
        if record.exc_info:
            payload["traceback"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logger(name: str) -> logging.Logger:
    """
    Создаёт и возвращает логгер с именем `name`.

    - Файл: logs/{name}.log  — NDJSON, RotatingFileHandler 10MB × 5, уровень DEBUG
    - Консоль: StreamHandler — текст, уровень WARNING
    """
    os.makedirs(config.LOGS_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured, don't duplicate

    logger.setLevel(logging.DEBUG)

    # ── File handler: NDJSON ──────────────────────────────────────────────
    fh = RotatingFileHandler(
        filename=os.path.join(config.LOGS_DIR, f"{name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)

    # ── Console handler: human-readable text (WARNING+) ──────────────────
    text_fmt = logging.Formatter(
        fmt="[%(asctime)s.%(msecs)03d] [%(levelname)-8s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(text_fmt)
    logger.addHandler(ch)

    return logger
