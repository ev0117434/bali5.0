# BALI 5.0 — Logging Design (NDJSON)

## Принципы

1. **Один файл лога на процесс** — нет смешения между модулями
2. **Rotating файлы** — max 10MB, 5 ротаций = 50MB на модуль
3. **Консоль только WARNING+** — человекочитаемый текст
4. **Файл: NDJSON** — один JSON объект на строку, machine-readable
5. **Единый envelope** — каждая строка содержит `ts`, `component`, `event`
6. **Latency в каждом metrics_interval** — avg, max, p99 за период

---

## Формат файла

Каждый лог-файл — **NDJSON** (Newline-Delimited JSON). Одна строка = один JSON объект:

```
{"ts":1741234567890,"component":"collector_binance","event":"collector_start","exchange":"binance","spot_symbols":1200,"fut_symbols":380}
{"ts":1741234567891,"component":"collector_binance","event":"ws_connect","exchange":"binance","stream":"md_spot","url_prefix":"wss://stream.binance.com","symbols":1200}
{"ts":1741234572890,"component":"collector_binance","event":"metrics_interval","exchange":"binance","interval_s":5.0,...}
```

Консоль (WARNING+ только) — по-прежнему человекочитаемый текст:
```
[2024-01-15 10:23:45.456] [WARNING ] [spread_monitor] {...}
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

## Универсальный envelope

Каждое событие содержит минимум три поля:

| Поле | Тип | Описание |
|------|-----|----------|
| `ts` | `int` | Unix timestamp, миллисекунды |
| `component` | `str` | Имя процесса (`collector_binance`, `spread_monitor`, ...) |
| `event` | `str` | Тип события (см. каталог ниже) |

---

## Каталог событий

### collector_{exchange}.log

| Event | Level | Когда |
|-------|-------|-------|
| `collector_start` | INFO | Старт процесса |
| `symbols_loaded` | INFO | Загрузка символов |
| `symbol_file_missing` | ERROR | Файл символов не найден |
| `no_symbols` | ERROR | Пустой список символов |
| `ws_connect` | INFO | Попытка подключения к WS |
| `ws_connected` | INFO | WS подключён |
| `ws_disconnect` | WARNING | WS разрыв / ошибка → reconnect |
| `heartbeat_error` | DEBUG | Ошибка heartbeat (bybit/okx/bitget) |
| `redis_error` | ERROR | Ошибка Redis pipeline |
| `slow_flush` | WARNING | Primary flush > 50ms или hist flush > 100ms |
| `chunk_rotate` | INFO | Смена history chunk ID |
| `parse_error` | — | Через `parse_errors` счётчик в `metrics_interval` |
| `metrics_interval` | INFO | Каждые 5s — агрегат метрик |
| `collector_stop` | INFO | Остановка процесса |

**metrics_interval** содержит вложенные объекты:

```json
{
  "ts": 1741234572890,
  "component": "collector_binance",
  "event": "metrics_interval",
  "exchange": "binance",
  "interval_s": 5.0,
  "ingestion": {
    "md_msgs": 6024, "md_msgs_per_s": 1204.8,
    "ob_msgs": 3987, "ob_msgs_per_s": 797.4,
    "fr_msgs": 1012, "fr_msgs_per_s": 202.4,
    "parse_errors": 0, "parse_errors_per_s": 0.0
  },
  "primary_flush": {
    "count": 20, "count_per_s": 4.0,
    "batch_avg": 85.3, "lat_avg_ms": 12.5, "lat_max_ms": 45.2, "slow_count": 0
  },
  "history_flush": {
    "count": 17, "cmds_total": 142000, "cmds_per_s": 28400.0,
    "lat_avg_ms": 22.3, "lat_max_ms": 98.1, "slow_count": 1,
    "ob_skipped": 251, "ob_skipped_per_s": 50.2
  },
  "latency": {
    "parse_avg_us": 145.0, "parse_max_us": 890.0,
    "buffer_age_avg_ms": 87.3, "buffer_age_max_ms": 248.1
  },
  "state": {
    "expire_keys_tracked": 4200, "reconnects": 0, "active_streams": 5
  }
}
```

> **Примечание:** `latency.e2e_avg_ms` / `e2e_max_ms` присутствуют у bybit/okx/gate/bitget (есть exchange timestamp). У Binance — отсутствуют (bookTicker не содержит ts).

---

### spread_monitor.log

| Event | Level | Когда |
|-------|-------|-------|
| `spread_monitor_start` | INFO | Старт |
| `signal` | INFO | Спред ≥ threshold |
| `cycle` | DEBUG | Каждые 300ms |
| `cycle_slow` | WARNING | Цикл > 250ms |
| `spread_summary` | INFO | Каждые 30s — агрегат |
| `no_pairs` | WARNING | Нет combination files |
| `redis_error` | ERROR | Ошибка Redis |
| `spread_monitor_stop` | INFO | Остановка |

```json
{
  "ts": 1741234567890, "component": "spread_monitor", "event": "signal",
  "spot_exchange": "binance", "fut_exchange": "bybit", "symbol": "BTCUSDT",
  "ask_spot": 45000.10, "bid_fut": 45676.35, "spread_pct": 1.5023,
  "data_age_spot_ms": 120, "data_age_fut_ms": 85,
  "cooldown_applied": false, "emit_lat_ms": 1.2
}
```

---

### redis_monitor.log

| Event | Level | Когда |
|-------|-------|-------|
| `redis_monitor_start` | INFO | Старт |
| `redis_health` | INFO | Каждые 30s |
| `redis_warn` | WARNING | Одно событие на каждый threshold violation |
| `redis_unreachable` | ERROR | Redis недоступен |
| `redis_monitor_stop` | INFO | Остановка |

`redis_warn.warning` values: `memory_high`, `ops_high`, `fragmentation_high`, `blocked_clients`

---

### stale_monitor.log

| Event | Level | Когда |
|-------|-------|-------|
| `stale_monitor_start` | INFO | Старт |
| `stale_scan` | INFO | Каждые 30s — итог сканирования |
| `stale_key` | WARNING | Одно событие на каждый stale ключ |
| `key_skip` | DEBUG | Ключ пропущен (not_a_hash / no_ts_field / ts_parse_error) |
| `stale_monitor_stop` | INFO | Остановка |

---

### snapshot_monitor.log

| Event | Level | Когда |
|-------|-------|-------|
| `snapshot_monitor_start` | INFO | Старт |
| `stream_first_run` / `stream_resume` | INFO | Позиция в стриме |
| `stream_read` | DEBUG | Каждый XREAD poll |
| `signal_received` | INFO | Получен сигнал из stream:signals |
| `snapshot_start` | INFO | Открыт файл snapshot |
| `history_load_start` | INFO | Начало загрузки истории |
| `history_loaded` | INFO | История загружена |
| `snapshot_progress` | INFO | Каждые 60s во время записи |
| `snapshot_row_slow` | WARNING | Строка записана > 100ms |
| `snapshot_complete` | INFO | Snapshot завершён |
| `snapshot_error` | WARNING | Ошибка при записи строки |
| `xread_error` | ERROR | Ошибка XREAD |
| `snapshot_monitor_stop` | INFO | Остановка |

---

### launcher.log

| Event | Level | Когда |
|-------|-------|-------|
| `launcher_start` | INFO | Старт системы |
| `redis_ready` | INFO | Redis доступен |
| `redis_unavailable` | WARNING | Redis ещё не готов (retry) |
| `redis_not_ready` | ERROR | Redis не доступен после 10 попыток |
| `redis_flushed` | INFO | Redis очищен при старте |
| `subscribe_file_missing` | WARNING | Файл символов отсутствует |
| `subscribe_file_ok` | INFO | Файл символов валиден |
| `process_started` | INFO | Процесс запущен |
| `process_died` | ERROR | Процесс упал |
| `process_restarted` | INFO | Процесс перезапущен |
| `terminate_failed` | WARNING | Не удалось завершить процесс |
| `warmup_wait` | INFO | Ожидание прогрева коллекторов |
| `all_started` | INFO | Все процессы запущены |
| `shutdown` | INFO | SIGTERM получен |

---

## jq-команды для диагностики

```bash
# Все сигналы
jq 'select(.event == "signal")' logs/spread_monitor.log

# Метрики коллектора — msg/s за последние записи
jq 'select(.event == "metrics_interval") | {ts, md: .ingestion.md_msgs_per_s, ob: .ingestion.ob_msgs_per_s, flush_lat: .primary_flush.lat_avg_ms}' logs/collector_binance.log | tail -5

# Медленные циклы spread monitor
jq 'select(.event == "cycle_slow")' logs/spread_monitor.log

# Все WS реконнекты
jq 'select(.event == "ws_disconnect")' logs/collector_*.log

# Stale ключи
jq 'select(.event == "stale_key")' logs/stale_monitor.log

# Redis warnings
jq 'select(.event == "redis_warn")' logs/redis_monitor.log

# Redis health последняя запись
jq 'select(.event == "redis_health")' logs/redis_monitor.log | tail -1 | jq .

# Ошибки Redis pipeline
jq 'select(.event == "redis_error")' logs/collector_*.log

# Parse errors по коллекторам
jq 'select(.event == "metrics_interval" and .ingestion.parse_errors > 0)' logs/collector_*.log

# Медленные snapshot строки
jq 'select(.event == "snapshot_row_slow")' logs/snapshot_monitor.log

# Процессы которые падали
jq 'select(.event == "process_died")' logs/launcher.log

# E2E latency (bybit пример)
jq 'select(.event == "metrics_interval") | .latency.e2e_avg_ms' logs/collector_bybit.log | tail -5

# Все ошибки по всем логам
jq 'select(.level == "ERROR")' logs/*.log 2>/dev/null | jq '{ts, component, event, error_msg}'

# Сигналы за последний час (ts > now-3600000)
NOW=$(python3 -c "import time; print(int(time.time()*1000))"); \
jq --argjson since "$((NOW - 3600000))" 'select(.event == "signal" and .ts > $since)' logs/spread_monitor.log
```

---

## Реализация: JsonFormatter

`logger_setup.py` содержит `JsonFormatter` — форматтер для file handler:

- **Dict message** → shallow copy → JSON (всегда содержит `ts` и `level` через `setdefault`)
- **String message** → envelope с `ts`, `component`, `event="log"`, `level`, `msg`
- **exc_info** → добавляет поле `"traceback"` с трейсбеком

Каждый скрипт определяет локальную фабрику:

```python
_COMPONENT = "collector_binance"

def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}
```

---

## Связанные документы

- `docs/superpowers/specs/2026-03-23-json-metrics-schema-design.md` — полная схема всех событий
- `docs/superpowers/plans/2026-03-23-json-metrics-implementation.md` — план реализации
