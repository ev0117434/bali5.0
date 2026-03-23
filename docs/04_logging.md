# BALI 5.0 — Logging Design

## Принципы

1. **Один файл лога на процесс** — нет смешения между модулями
2. **Rotating файлы** — max 10MB, 5 ротаций = 50MB на модуль
3. **Консоль только WARNING+** — файл DEBUG+
4. **Единый формат** — timestamp.ms + level + module + message
5. **Структурированные METRICS** — ключевое слово для grep/parsing
6. **Latency в каждом METRICS** — avg и max за период

---

## Формат строки

```
[2024-01-15 10:23:45.123] [INFO ] [collector_binance] <message>
[2024-01-15 10:23:45.456] [WARN ] [spread_monitor] <message>
[2024-01-15 10:23:45.789] [ERROR] [redis_monitor] <message>
```

---

## Файлы логов

```
logs/
├── launcher.log             # Process manager
├── collector_binance.log    # MD+OB+FR Binance
├── collector_bybit.log      # MD+OB+FR Bybit
├── collector_okx.log        # MD+OB+FR OKX
├── collector_gate.log       # MD+OB+FR Gate.io
├── collector_bitget.log     # MD+OB+FR Bitget
├── redis_monitor.log        # Redis health
├── stale_monitor.log        # Stale keys
├── spread_monitor.log       # Spread signals
└── snapshot_monitor.log     # Snapshot recording
```

---

## Общая функция setup_logger

```python
# logger_setup.py
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logger(name: str, level_file=logging.DEBUG, level_console=logging.WARNING) -> logging.Logger:
    Path("logs").mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    fmt = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] [%(levelname)-5s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Rotating file handler
    fh = RotatingFileHandler(
        f"logs/{name}.log",
        maxBytes=10_000_000,   # 10 MB
        backupCount=5,
        encoding="utf-8"
    )
    fh.setLevel(level_file)
    fh.setFormatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level_console)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger
```

---

## Что логировать: таблица

### collector_{exchange}.log

| Событие | Level | Периодичность | Пример сообщения |
|---------|-------|--------------|-----------------|
| Старт | INFO | 1 раз | `Starting collector_binance \| spot=412 fut=389 symbols` |
| WS connect | INFO | при подключении | `[md_spot] Connected to wss://stream.binance... symbols=412` |
| WS disconnect | WARNING | при разрыве | `[md_spot] WS disconnected: code=1006 reason='' reconnecting in 2s` |
| WS reconnect OK | INFO | при восстановлении | `[md_spot] Reconnected after 2s (attempt=1)` |
| WS reconnect fail | ERROR | при ошибке | `[md_spot] Reconnect attempt 5 failed: Connection refused` |
| Metrics | INFO | каждые 5 сек | `METRICS \| md=1523msg/s ob=412msg/s fr=5msg/s flush_lat avg=2.1ms max=8.3ms batch_avg=87 reconnects=0` |
| Flush медленный | WARNING | при flush > 100ms | `Slow flush: 145.3ms (threshold=100ms) batch=97` |
| Смена чанка | INFO | каждые 20 мин | `Chunk changed: 1421093 → 1421094 \| expire old=1421090` |
| Пустой subscribe | WARNING | при старте | `[ob_spot] Empty symbol list, skipping task` |

### spread_monitor.log

| Событие | Level | Периодичность | Пример |
|---------|-------|--------------|--------|
| Старт | INFO | 1 раз | `Starting spread_monitor \| pairs=3842 files=20` |
| Цикл debug | DEBUG | каждые 300мс | `Cycle: 23.4ms \| pairs=3842 no_data=12 stale=3 signals=0` |
| Медленный цикл | WARNING | при > 250ms | `Slow cycle: 287.1ms > 250ms threshold` |
| Сигнал | INFO | при сигнале | `SIGNAL \| binance→bybit BTCUSDT ask=45000.10 bid=45676.35 spread=1.5023%` |
| Cooldown hit | DEBUG | при cooldown | `Cooldown active: binance→bybit BTCUSDT (TTL=3243s)` |

### snapshot_monitor.log

| Событие | Level | Периодичность | Пример |
|---------|-------|--------------|--------|
| Сигнал получен | INFO | при сигнале | `Signal received: binance,bybit,BTCUSDT,...` |
| История загружена | INFO | при старте snap | `History loaded: 10847 rows for BTCUSDT (60min window)` |
| Snapshot старт | INFO | при старте snap | `Snapshot started: signal/snapshot/binance_bybit_BTCUSDT_1705312345890.csv` |
| Прогресс | INFO | каждые 60 сек | `Snapshot BTCUSDT: elapsed=300s rows=1247 row_lat=4.2ms` |
| Snapshot конец | INFO | по окончании | `Snapshot done: rows=11667 elapsed=3500s file=...csv` |
| Нет истории | WARNING | при пустой | `No history for ob:hist:binance:spot:BTCUSDT (chunk=1421090) — new symbol?` |

### stale_monitor.log

| Событие | Level | Периодичность | Пример |
|---------|-------|--------------|--------|
| Цикл OK | INFO | каждые 30 сек | `Stale check: total_keys=4123 stale=0 elapsed=234ms` |
| Stale ключ | WARNING | при stale | `STALE \| md:gate:spot:XYZUSDT age=412s last_ts=1705312000000` |
| Много stale | ERROR | при > 10 stale | `HIGH STALE COUNT: 45 keys stale (>10 threshold)` |

### redis_monitor.log

| Событие | Level | Периодичность | Пример |
|---------|-------|--------------|--------|
| Health OK | INFO | каждые 30 сек | `REDIS OK \| mem=1234.5MB peak=1240.0MB ops/s=8432 clients=12 keys=49832 hit_rate=99.8% ping=0.8ms` |
| Redis недоступен | ERROR | при ошибке | `Redis UNREACHABLE: Connection refused` |
| Память высокая | WARNING | при > 3GB | `Redis memory high: 3127.4MB (threshold=3000MB)` |

### launcher.log

| Событие | Level | Периодичность | Пример |
|---------|-------|--------------|--------|
| Старт | INFO | 1 раз | `BALI 5.0 launcher starting` |
| Процесс запущен | INFO | при старте | `Started collector_binance PID=12345` |
| Процесс умер | ERROR | при смерти | `Process collector_binance (PID=12345) died! Restarting...` |
| Health check OK | DEBUG | каждые 30 сек | `Health check: all 9 processes alive` |
| Shutdown | INFO | при SIGTERM | `Shutting down: terminating 9 processes` |

---

## Пример лог-файла spread_monitor.log (первые минуты)

```
[2024-01-15 10:00:00.001] [INFO ] [spread_monitor] Starting spread_monitor | pairs=3842 files=20
[2024-01-15 10:00:00.045] [INFO ] [spread_monitor] Loaded combinations: binance_spot_bybit_futures=412 pairs, ...
[2024-01-15 10:00:00.046] [INFO ] [spread_monitor] signal/signal.csv: header written
[2024-01-15 10:00:00.047] [INFO ] [spread_monitor] Monitor loop started (threshold=1.00% cooldown=3600s)
[2024-01-15 10:00:00.300] [DEBUG] [spread_monitor] Cycle: 254.2ms | pairs=3842 no_data=3842 stale=0 signals=0
[2024-01-15 10:00:00.601] [DEBUG] [spread_monitor] Cycle: 23.1ms | pairs=3842 no_data=412 stale=0 signals=0
...
[2024-01-15 10:00:15.302] [DEBUG] [spread_monitor] Cycle: 21.8ms | pairs=3842 no_data=0 stale=3 signals=0
[2024-01-15 10:05:23.156] [INFO ] [spread_monitor] SIGNAL | binance→bybit BTCUSDT ask=45000.10 bid=45676.35 spread=1.5023%
[2024-01-15 10:05:23.203] [DEBUG] [spread_monitor] Cooldown set: binance→bybit BTCUSDT (3600s)
```

---

## Grep-команды для диагностики

```bash
# Все сигналы за последний час
grep "SIGNAL" logs/spread_monitor.log | tail -50

# Метрики коллектора (задержки flush)
grep "METRICS" logs/collector_binance.log | tail -20

# Медленные циклы spread monitor
grep "Slow cycle" logs/spread_monitor.log

# Все реконнекты
grep "Reconnect" logs/collector_*.log

# Stale ключи
grep "STALE" logs/stale_monitor.log | tail -20

# Redis проблемы
grep -E "UNREACHABLE|high" logs/redis_monitor.log

# Ошибки по всем логам
grep "ERROR" logs/*.log | tail -50
```
