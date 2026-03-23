# BALI 5.0 — Architecture

## 1. System Context (C4 Level 1)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         External Systems                            │
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌───────┐  │
│  │ Binance  │  │  Bybit   │  │   OKX    │  │Gate.io │  │Bitget │  │
│  │ (WS API) │  │ (WS API) │  │ (WS API) │  │(WS API)│  │(WS API│  │
│  └────▲─────┘  └────▲─────┘  └────▲─────┘  └───▲────┘  └──▲────┘  │
└───────┼─────────────┼─────────────┼─────────────┼───────────┼──────┘
        │ WebSocket (public, no auth)│             │           │
┌───────▼─────────────▼─────────────▼─────────────▼───────────▼──────┐
│                      BALI 5.0 System                                │
│                                                                     │
│  Reads: best bid/ask, order book 10 levels, funding rates           │
│  Writes: spread signals, market snapshots                           │
└─────────────────────────────────────────┬───────────────────────────┘
                                          │
                             ┌────────────▼────────────┐
                             │   Output Files          │
                             │  signal/signal.csv      │  ← ветка trade + data
                             │  signal/snapshot/*.csv  │  ← только ветка data
                             └─────────────────────────┘
```

---

## 2. Container Diagram (C4 Level 2)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           BALI 5.0                                       │
│                                                                          │
│  ┌─────────────────┐                                                     │
│  │  launcher.py    │  supervises все процессы, health-check, restart     │
│  └────────┬────────┘                                                     │
│           │ subprocess                                                   │
│    ┌──────┴──────────────────────────────────────┐                       │
│    │                                             │                       │
│  ┌─▼──────────────────────┐              ┌──────▼──────────────────┐    │
│  │   COLLECTORS (×5)      │              │      MONITORS           │    │
│  │                        │              │                         │    │
│  │  collector_binance.py  │              │  redis_monitor.py       │    │
│  │  collector_bybit.py    │   pipeline   │  stale_monitor.py       │    │
│  │  collector_okx.py      │──────────►  │  spread_monitor.py      │    │
│  │  collector_gate.py     │             │  snapshot_monitor.py *  │    │
│  │  collector_bitget.py   │             │  (* только ветка data)  │    │
│  └──────────┬─────────────┘             └──────────┬──────────────┘    │
│             │ HSET+LPUSH (pipeline)                 │ HMGET (pipeline)  │
│             │                                       │ SUBSCRIBE         │
│             ▼                                       ▼                   │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                        Redis :6379                              │    │
│  │   md:*  ob:*  fr:*  (primary)                                  │    │
│  │   md:hist:*  ob:hist:*  fr:hist:*  (rolling 80 min)            │    │
│  │   hist:config  spread:cooldown:*                               │    │
│  │   stream:signals (Redis Stream)  snapshot:stream_id (cursor)  │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Diagram (C4 Level 3) — Collector

```
collector_binance.py (один asyncio event loop)
│
├── task_md_spot   ──► WsManager(spot, bookTicker) ──► parse ──► BatchBuffer
├── task_md_fut    ──► WsManager(fut,  bookTicker) ──► parse ──► BatchBuffer
├── task_ob_spot   ──► WsManager(spot, depth10)    ──► parse ──► BatchBuffer
├── task_ob_fut    ──► WsManager(fut,  depth10)    ──► parse ──► BatchBuffer
├── task_fr        ──► WsManager(fut,  markPrice)  ──► parse ──► BatchBuffer
│
├── BatchBuffer (shared dict)
│   keys:  "md:binance:spot:BTCUSDT" → {"b": "34000", "a": "34001", "ts": 1705312345000}
│          "ob:binance:spot:BTCUSDT" → {"b1": ..., "b1q": ..., ...}
│          "fr:binance:futures:BTCUSDT" → {"fr": "0.0001", "fr_ts": 1705312345000}
│
├── HistBuffer (list of (hist_key, line_str))
│
├── task_flusher   ──► asyncio timer 250ms OR event on 100 cmds
│   └── Redis pipeline: HSET primary + LPUSH hist + EXPIRE hist
│
├── task_metrics   ──► log every 5s: msgs/s, lat_avg, lat_max, batch_avg, reconnects
│
└── WsManager (shared base class)
    ├── connect()
    ├── subscribe(symbols_chunk)
    ├── recv_loop()
    ├── heartbeat()  ← ping/pong per exchange protocol
    └── reconnect(backoff)  ← exponential: 1s→2s→4s→...→60s
```

---

## 4. Key Architectural Decisions

### ADR-001: Один процесс на биржу (не 15 отдельных)
**Контекст:** Можно запускать MD, OB, FR отдельными процессами.
**Решение:** Один процесс на биржу, три типа потоков через asyncio tasks.
**Обоснование:**
- Общий Redis connection pool → меньше соединений
- Общий event loop → нет overhead на IPC
- Общий BatchBuffer → один pipeline на все типы данных
- Меньше процессов для мониторинга launcher

### ADR-002: История через LPUSH в том же pipeline
**Контекст:** История могла быть отдельным процессом (polling) или через pub/sub.
**Решение:** Коллектор пишет hist-ключи в том же Redis pipeline, что и primary.
**Обоснование:**
- Ноль дополнительных RTT до Redis
- Нет задержки между primary и hist записью
- Нет отдельного процесса-подписчика

### ADR-003: TTL вместо явного удаления чанков
**Контекст:** Нужно удалять чанки старше 80 минут.
**Решение:** При создании hist-ключа ставить EXPIRE = 6000 сек (5 × chunk_duration).
**Обоснование:**
- Redis сам удаляет истёкшие ключи
- Нет необходимости в SCAN + DEL (дорогая операция)
- Нет race condition между writer и deleter

### ADR-004: Spread cooldown в Redis (SETEX)
**Контекст:** Cooldown мог храниться в памяти процесса.
**Решение:** `SETEX spread:cooldown:{spot}:{fut}:{symbol} 3600 1`
**Обоснование:**
- Выживает перезапуск процесса
- TTL автоматически истекает — не нужен cleanup
- Атомарная проверка + установка через SET NX

### ADR-005: Spread → Snapshot через Redis Stream (не Pub/Sub)
**Контекст:** Изначально использовался Redis pub/sub (`PUBLISH ch:signals`). Pub/sub —
fire-and-forget: если subscriber не онлайн в момент публикации, сообщение теряется.
Это приводило к ситуации, когда сигналы записывались в `signal.csv`, но снапшоты
не создавались — при старте системы (race condition) и после краша snapshot_monitor
(до следующего restart за 30 сек).

**Решение:** `XADD stream:signals {data: payload}` из spread_monitor;
`XREAD BLOCK 2000 STREAMS stream:signals {last_id}` в snapshot_monitor.
Курсор последнего обработанного сообщения хранится в `snapshot:stream_id`.

**Обоснование:**
- Stream буферизует сообщения — не теряются при недоступности subscriber
- После краша snapshot_monitor читает `snapshot:stream_id` и догоняет пропущенные сигналы
- При старте системы (flush_redis) курсор сбрасывается, новый run стартует с `$`
- `MAXLEN ~1000` ограничивает размер стрима (с cooldown 1ч дублей не будет)
- Нет disk I/O polling, нет race condition между стартом процессов

### ADR-006: Pipeline для spread_monitor (не N×HGET)
**Контекст:** До 4000 пар нужно проверять каждые 300мс.
**Решение:** Один Redis pipeline с HMGET для всех пар.
**Обоснование:**
- 1 TCP round-trip вместо 8000
- Python разбирает ответы локально
- Цикл укладывается в 300мс даже при 4000+ парах

### ADR-007: Exchange Adapter Pattern
**Контекст:** У каждой биржи свой формат символов и WS-сообщений.
**Решение:** Каждый collector содержит inline адаптер: функции parse_md(), parse_ob(), parse_fr(), native(sym), normalize(sym).
**Обоснование:**
- Нет абстрактного базового класса (лишняя сложность)
- Каждый collector полностью самодостаточен
- Изменение одной биржи не влияет на другие

---

## 5. Technology Stack

| Компонент | Технология | Обоснование |
|-----------|-----------|-------------|
| Язык | Python 3.10+ | Уже используется в dictionaries |
| Async | asyncio | Нативная поддержка WS + Redis |
| WebSocket | websockets 12+ | Уже используется |
| Redis client | redis-py (asyncio mode) | Async + pipeline support |
| Логирование | logging + RotatingFileHandler | Стандартная библиотека |
| Process mgmt | subprocess (launcher) | Простота, без зависимостей |
| CSV запись | aiofiles | Async IO, не блокирует event loop |

---

## 6. Data Flow

```
Биржа WS
    │
    │ bookTicker message
    ▼
WsManager.recv_loop()
    │
    │ parse_md(raw) → (symbol, bid, ask, ts)
    ▼
BatchBuffer["md:binance:spot:BTCUSDT"] = {"b": bid, "a": ask, "ts": ts}
HistBuffer.append(("md:hist:binance:spot:BTCUSDT:{chunk}", f"{bid},{ask},{ts}"))
    │
    │ (каждые 250мс ИЛИ при накоплении 100 команд)
    ▼
Redis Pipeline:
  HSET md:binance:spot:BTCUSDT b 34000 a 34001 ts 1705312345000
  LPUSH md:hist:binance:spot:BTCUSDT:12345 "34000,34001,1705312345000"
  EXPIRE md:hist:binance:spot:BTCUSDT:12345 6000
  ... (до 100 команд)
  EXECUTE
    │
    ▼
Redis (primary + history)
    │
    ├──► Stale Monitor (читает ts из md:*)
    ├──► Spread Monitor (читает b,a из md:* через pipeline)
    └──► Snapshot Monitor (читает ob:*, fr:*, md:hist:*, ob:hist:*, fr:hist:*)
```

---

## 7. Branch Strategy

| Ветка | HISTORY_ENABLED | snapshot_monitor | Назначение |
|-------|----------------|-----------------|-----------|
| `main` | — | — | Только dictionaries + docs |
| `data` | `True` | ✅ есть | Primary + history ключи + сигналы + снапшоты |
| `trade` | `False` | ❌ нет | Только primary ключи + сигналы |

### Ключевое решение: один флаг управляет историей

```python
# config.py
HISTORY_ENABLED = True   # ветка data — пишем md:hist:*, ob:hist:*, fr:hist:*
                         # ветка trade — не пишем историю совсем
```

В каждом коллекторе одна проверка:
```python
if HISTORY_ENABLED:
    hist_buffer.append((hist_key, line))   # md, ob, fr history
```

Нет дублирования кода между ветками — только значение флага отличается.

### Последовательность создания веток

```
main  ──────────────────────────────────────────────────────►
       │
       │ (после фазы 0 — инфраструктура готова)
       └──► data  ──────────────────────────────────────────►
                   (весь код пишется здесь)
                                          │
                                          │ (финальная фаза)
                                          └──► trade
                                               git checkout -b trade data
                                               # 1. config.py: HISTORY_ENABLED = False
                                               # 2. удалить monitors/snapshot_monitor.py
                                               # 3. git commit
```

**Итого для создания `trade` из `data`: 2 изменения, 1 коммит.**
