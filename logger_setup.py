# logger_setup.py
"""
BALI 5.0 — Настройка логирования.
Единственная функция: setup_logger(name) → logging.Logger
"""

import logging
import os
from logging.handlers import RotatingFileHandler

import config


def setup_logger(name: str) -> logging.Logger:
    """
    Создаёт и возвращает логгер с именем `name`.

    - Файл: logs/{name}.log, RotatingFileHandler 10MB × 5 файлов, уровень DEBUG
    - Консоль: StreamHandler, уровень WARNING
    - Формат: [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL    ] [name] message
    """
    os.makedirs(config.LOGS_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # уже настроен, не дублировать

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="[%(asctime)s.%(msecs)03d] [%(levelname)-8s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Файловый обработчик
    fh = RotatingFileHandler(
        filename=os.path.join(config.LOGS_DIR, f"{name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Консольный обработчик
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger
