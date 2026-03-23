# BALI 5.0 — Project Structure & File Map

---

## Полная структура файлов

```
bali5.0/
│
├── config.py                        ← Все константы (REDIS_URL, BATCH_SIZE, etc.)
├── logger_setup.py                  ← setup_logger(name) → logging.Logger
├── launcher.py                      ← Process manager (запуск + health check)
├── requirements.txt                 ← websockets, redis[hiredis], aiofiles, aiohttp
│
├── collectors/                      ← 5 коллекторов (MD+OB+FR per exchange)
│   ├── collector_binance.py         ← asyncio: 7 tasks
│   ├── collector_bybit.py           ← asyncio: 7 tasks (FR через tickers)
│   ├── collector_okx.py             ← asyncio: 7 tasks + REST FR init
│   ├── collector_gate.py            ← asyncio: 7 tasks
│   └── collector_bitget.py          ← asyncio: 7 tasks (FR через ticker)
│
├── monitors/                        ← 4 монитора
│   ├── redis_monitor.py             ← Redis health → logs/redis_monitor.log
│   ├── stale_monitor.py             ← SCAN md:* → logs/stale_monitor.log
│   ├── spread_monitor.py            ← 300ms loop → signal/signal.csv + ch:signals
│   └── snapshot_monitor.py         ← SUBSCRIBE ch:signals → signal/snapshot/*.csv
│                                      [только ветка data]
│
├── dictionaries/                    ← уже существует ✓
│   ├── main.py
│   ├── binance/
│   ├── bybit/
│   ├── okx/
│   ├── gate/
│   ├── bitget/
│   ├── combination/                 ← создаётся main.py (в .gitignore)
│   ├── subscribe/                   ← создаётся main.py (в .gitignore)
│   └── config/
│       ├── source_ids.json
│       └── symbol_ids.json
│
├── docs/                            ← документация ✓
│   ├── 00_architecture.md           ← C4 + архитектурные решения
│   ├── 01_redis_schema.md           ← Все Redis ключи
│   ├── 02_exchange_adapters.md      ← WS endpoints, форматы, нормализация
│   ├── 03_algorithms.md             ← Псевдокод всех модулей
│   ├── 04_logging.md                ← Структура логов
│   ├── 05_history_chunks.md         ← Chunk система и алгоритм чтения
│   ├── 06_config.md                 ← Полный config.py с обоснованием
│   ├── 07_open_questions.md         ← Решённые вопросы
│   ├── 08_review_iterations.md      ← 7 итераций поиска ошибок
│   ├── 09_launch.md                 ← Запуск и диагностика
│   └── 10_project_structure.md      ← Этот файл
│
├── signal/                          ← создаётся launcher (в .gitignore)
│   ├── signal.csv
│   └── snapshot/
│       └── binance_bybit_BTCUSDT_1705312345890.csv
│
├── logs/                            ← создаётся launcher (в .gitignore)
│   ├── launcher.log
│   ├── collector_binance.log
│   ├── collector_bybit.log
│   ├── collector_okx.log
│   ├── collector_gate.log
│   ├── collector_bitget.log
│   ├── redis_monitor.log
│   ├── stale_monitor.log
│   ├── spread_monitor.log
│   └── snapshot_monitor.log
│
├── README.md                        ← Краткое описание, запуск
└── .gitignore
```

---

## Что в каждой ветке

### main
```
dictionaries/
docs/
README.md
.gitignore
```

### data  ← весь код пишется здесь
```
все из main +
config.py                        ← HISTORY_ENABLED = True
logger_setup.py
launcher.py
requirements.txt
collectors/collector_*.py        ← пишут primary + history
monitors/redis_monitor.py
monitors/stale_monitor.py
monitors/spread_monitor.py
monitors/snapshot_monitor.py
```

### trade  ← создаётся из data в конце (2 изменения)
```
всё из data, кроме:
  - monitors/snapshot_monitor.py  ← удалён
  - config.py: HISTORY_ENABLED = False  ← изменено
```

### Разница между ветками в одной таблице

| Компонент | trade | data |
|-----------|-------|------|
| config.HISTORY_ENABLED | False | True |
| collectors пишут md:hist:* | ❌ | ✅ |
| collectors пишут ob:hist:* | ❌ | ✅ |
| collectors пишут fr:hist:* | ❌ | ✅ |
| snapshot_monitor.py | ❌ | ✅ |
| signal/signal.csv | ✅ | ✅ |
| signal/snapshot/*.csv | ❌ | ✅ |

---

## Зависимости между файлами

```
config.py ← импортируется ВСЕМИ модулями
logger_setup.py ← импортируется ВСЕМИ модулями

collectors/collector_*.py:
    import config
    from logger_setup import setup_logger
    import asyncio, json, time
    import websockets
    import redis.asyncio as aioredis
    # (collector_okx.py дополнительно: import aiohttp)

monitors/spread_monitor.py:
    import config
    from logger_setup import setup_logger
    import asyncio, time, aiofiles
    import redis.asyncio as aioredis
    from pathlib import Path

monitors/snapshot_monitor.py:
    import config
    from logger_setup import setup_logger
    import asyncio, time, aiofiles
    import redis.asyncio as aioredis

monitors/stale_monitor.py:
    import config
    from logger_setup import setup_logger
    import asyncio
    import redis.asyncio as aioredis

monitors/redis_monitor.py:
    import config
    from logger_setup import setup_logger
    import asyncio
    import redis.asyncio as aioredis

launcher.py:
    import config
    from logger_setup import setup_logger
    import subprocess, sys, os, time, signal
    import redis  # sync, только для startup check
```

---

## Порядок реализации (Step-by-Step)

```
Фаза 0: Инфраструктура
  1. requirements.txt
  2. config.py
  3. logger_setup.py
  4. Обновить .gitignore

Фаза 1: Первый коллектор (Binance) — тест всей цепочки
  5. collectors/collector_binance.py
  Тест: python collectors/collector_binance.py
        redis-cli hgetall "md:binance:spot:BTCUSDT"
        redis-cli hgetall "ob:binance:spot:BTCUSDT"

Фаза 2: Остальные коллекторы
  6. collectors/collector_bybit.py
  7. collectors/collector_okx.py
  8. collectors/collector_gate.py
  9. collectors/collector_bitget.py

Фаза 3: Мониторы (ветка trade)
  10. monitors/redis_monitor.py
  11. monitors/stale_monitor.py
  12. monitors/spread_monitor.py

Фаза 4: Launcher
  13. launcher.py

Фаза 5: Git ветки
  14. commit main (dictionaries + docs)
  15. branch trade, commit collectors + monitors (без snapshot)
  16. branch data от trade, добавить snapshot_monitor, commit

Фаза 6: Snapshot (только ветка data)
  17. monitors/snapshot_monitor.py
```

---

## Snapshot CSV пример

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts,fr,fr_time,
s_b1,s_b2,...,s_b10,s_bq1,...,s_bq10,
s_a1,...,s_a10,s_aq1,...,s_aq10,
f_b1,...,f_b10,f_bq1,...,f_bq10,
f_a1,...,f_a10,f_aq1,...,f_aq10

binance,bybit,BTCUSDT,45000.10,45676.35,1.5023,1741234567890,0.000100,1741248000000,
44998.00,44997.50,...,44990.00,5.50,3.20,...,0.80,
45000.10,45001.00,...,45008.00,8.30,6.10,...,1.20,
45675.00,45674.50,...,45668.00,10.20,8.50,...,2.10,
45676.35,45677.00,...,45683.00,7.30,5.80,...,1.50
```
