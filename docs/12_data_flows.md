# Data Flows & Latency Map

> Все потоки данных системы BALI 5.0 с реальными метриками из логов.
> Ветка: `data` (HISTORY_ENABLED=True)
> Логи собраны: 2026-03-23
> Источник: 60 сигналов, 60 history_loaded, ~1000 циклов spread monitor

---

## Поток 1: MD — Best Bid/Ask

```
Exchange WebSocket
    │
    ▼  parse_md()
    │  parse_lat avg: ~3µs  │  max: 1–4ms (Binance спайки)
    ▼
batch_buffer (RAM)
    │  ждёт flush: BATCH_FLUSH_INTERVAL_MS=250ms ИЛИ 100 команд
    │
    │  buffer_age avg по биржам:
    │    Binance:  10.5ms avg  │  16.6ms max
    │    Bybit:    14.1ms avg  │  22.9ms max
    │    OKX:      30.9ms avg  │  45.3ms max  ← самый медленный буфер
    │    Gate:     28.7ms avg  │  45.9ms max
    │    Bitget:   14.1ms avg  │  17.7ms max
    ▼
task_flusher() — Redis pipeline HSET
    │
    │  flush_lat avg / max по биржам:
    │    Binance:  2.8ms / 32.4ms  │  slow_flushes: 81  max: 632ms  ← проблема
    │    Bybit:    1.6ms /  3.4ms  │  slow_flushes:  7
    │    OKX:      5.2ms /  6.5ms  │  slow_flushes:  3
    │    Gate:     2.8ms /  3.6ms  │  slow_flushes:  0
    │    Bitget:   1.5ms /  1.8ms  │  slow_flushes:  0  ← самый быстрый
    ▼
Redis HASH  md:{exch}:{market}:{symbol}
    │  fields: b, a, ts
    │
    └──► hist_buffer (RAM)
             │  flush: HIST_FLUSH_INTERVAL_MS=300ms ИЛИ 5000 команд
             ▼
         task_hist_flusher() — Redis pipeline LPUSH
             │
             │  hist_flush avg / max:
             │    Binance:  24.8ms / 167.9ms  │  cmds/5s: 62 600  ← bottleneck
             │    Bybit:     3.6ms /  34.8ms
             │    OKX:       7.9ms /  39.7ms
             │    Gate:      5.3ms /  20.1ms
             │    Bitget:   10.2ms /  15.9ms
             ▼
         Redis LIST  md:hist:{exch}:{market}:{symbol}:{chunk_id}
             TTL=100мин, чанк=20мин, LPUSH (prepend)
```

---

## Поток 2: OB — Order Book (10 уровней)

```
Exchange WebSocket
    │
    ▼  parse_ob()
    │  parse_lat: аналогично MD (~3µs)
    ▼
write_ob_to_buffer()
    │  GATE 10Hz: ts - last_ts >= 100ms  →  ob_hist_skipped для всего быстрее
    ▼
batch_buffer ──► [flush_lat] ──► Redis HASH  ob:{exch}:{market}:{symbol}
                                     40 fields: b1..b10, b1q..b10q, a1..a10, a1q..a10q

    └──► hist_buffer ──► [hist_flush_lat] ──► Redis LIST  ob:hist:...:{chunk_id}

    OB msgs/s по биржам:
        Bybit:   6 140/s  ← лидер
        Bitget:  3 600/s
        Binance: 2 600/s
        OKX:     2 300/s
        Gate:    1 260/s
```

---

## Поток 3: FR — Funding Rate (только futures)

```
Exchange WebSocket (futures stream)
    │  Обновляется редко: каждые 1–8 часов
    ▼
batch_buffer ──► Redis HASH  fr:{exch}:futures:{symbol}   fields: fr, fr_ts
    └──► hist_buffer ──► Redis LIST  fr:hist:...:{chunk_id}

    Активный поток: Bitget 1 075 FR msgs/s  │  остальные — редкие
```

---

## Поток 4: Spread Detection → Signal

```
Redis HASH  md:spot + md:futures   (poll каждые 300ms)
    │
    ▼  HMGET pipeline — все 6 272 пары за один вызов
    │
    │  pipeline_lat:  avg 48.9ms  │  p99 61.1ms  │  max 73.6ms
    │  [75% цикла — это ожидание Redis]
    ▼
Staleness check  (порог: 5 000ms)
    │
    │  Статистика на цикл:
    │    pairs OK:      avg 4 018 / 6 272  (64%)
    │    pairs stale:   avg 1 939          (31%)
    │    pairs no_data:     315            ( 5%)
    ▼
Spread calculation
    │  calc_lat:  avg 2.7ms  │  max 4.3ms
    ▼
Threshold check  (>= 1.00%)
    │  signals per cycle:  avg 5.5  │  max 10
    ▼
Cooldown check  SET NX EX 3600
    ▼
handle_signal()   emit_lat: avg 0.8ms  │  max 16.4ms
    ├──► aiofiles → signal/signal.csv
    └──► XADD stream:signals  maxlen=1000

Cycle summary:
    cycle_lat:  avg 65.1ms  │  p99 78.5ms  │  max 91.7ms
    slow_cycles (>250ms): 0
```

---

## Поток 5: Snapshot Recording

```
Redis Stream stream:signals
    │  XREAD BLOCK 2000ms
    │  stream_lag:  avg 69ms  │  min 58ms  │  max 99ms
    ▼
Dedup check
    ▼
load_history()
    │  Загружает 4 чанка × 5 типов = 20 Redis команд  │  окно: 1 час
    │
    │  load_lat зависит от количества строк в истории:
    │    0 rows:      n=11  avg  20.9ms  max 105ms
    │    1–10 rows:   n=23  avg  47.2ms  max 192ms
    │    11–100 rows: n=18  avg 215.8ms  max 840ms
    │    101–500 rows:n= 5  avg 192.2ms  max 343ms
    │    >500 rows:   n= 3  avg 779.1ms  max 1467ms
    ▼
Запись заголовка + историч. строк → snapshot CSV
    ▼
Real-time loop  3 500s  │  target 300ms/row
    │  row_lat:  avg 4.7–10ms  │  p99 14–15ms  │  max 177ms
    │  slow rows: 1–2 на snapshot (из ~11 630)
    ▼
snapshot_complete
    rows: ~11 630  │  elapsed: ~3 500s  │  row_lat_p99: 15ms
```

---

## ПОЛНАЯ E2E ЗАДЕРЖКА — С РЕАЛЬНЫМИ ДАННЫМИ ИЗ ЛОГОВ

### Путь до сигнала (Signal E2E)

```
Exchange broadcast
    │  [сетевой RTT — не измеряется]
    ▼
parse           ~3µs  (пренебрежимо)
    ▼
buffer_age      10–31ms avg  │  16–46ms max
    ▼
flush_lat        2–5ms avg   │  до 632ms при Binance slow flush
    ▼
Redis HASH  md:...
    │  [ждёт следующего poll цикла: 0–300ms, avg ~150ms]
    ▼
pipeline_lat    48.9ms avg  │  61ms p99  │  74ms max
    ▼
calc_lat         2.7ms avg
    ▼
emit_lat         0.8ms avg  │  max 16ms
    ▼
SIGNAL EMITTED
```

#### E2E из логов (data_age_ms из реальных сигналов)

> `data_age` = возраст данных в момент детектирования (включает buffer, flush, ожидание poll).
> E2E ≈ max(data_age_spot, data_age_fut) + calc (~3ms).

**Все 60 сигналов:**
| Метрика | Значение |
|---------|----------|
| E2E min | **128ms** |
| E2E avg | **654ms** |
| E2E max | **3 428ms** (stale данные) |
| emit_lat avg | 0.8ms |
| emit_lat max | 16.4ms |

---

**По парам биржей (отсортировано avg):**

| Пара (spot → fut) | n | E2E avg | E2E max | Примечание |
|-------------------|---|---------|---------|------------|
| bybit → binance | 1 | **125ms** | 125ms | Быстрейшая пара |
| binance → bybit | 1 | **131ms** | 131ms | Быстрая пара |
| bitget → bybit | 10 | **352ms** | 430ms | Стабильная |
| bitget → binance | 10 | **382ms** | 481ms | Стабильная |
| bybit → bitget | 2 | 430ms | 537ms | |
| bitget → gate | 11 | 579ms | 1 922ms | Спайки из-за Gate |
| gate → binance | 10 | 699ms | 2 319ms | Gate spot медленный |
| binance → gate | 3 | 891ms | 1 835ms | Binance spot лаг |
| binance → bitget | 2 | 1 034ms | 1 835ms | Startup аномалии |
| gate → bitget | 5 | 1 134ms | 2 319ms | Gate часто stale |
| gate → bybit | 5 | 1 358ms | 3 424ms | Худшая пара |

---

#### С Binance vs Без Binance

| Сценарий | n | E2E min | E2E avg | E2E max |
|----------|---|---------|---------|---------|
| **С Binance** (spot или fut) | 27 | **128ms** | **589ms** | 2 322ms |
| **Без Binance** | 33 | **188ms** | **706ms** | 3 428ms |

> **Важно: Binance быстрее когда он на стороне FUTURES (data_age_fut 33–50ms).**
> Binance медленнее когда он на стороне SPOT (buffer_age 10.5ms + slow flush спайки 632ms).
> Без Binance хуже в среднем из-за Gate (gate_spot пары = avg 1134–1358ms).
> Фактор Binance: его slow flush (81 событий, max 632ms) — риск, но не доминирующий.

---

#### Типичный "хороший" E2E к сигналу

```
Без Gate на spot стороне, без Binance slow flush:

  buffer_age:   ~15ms
  flush_lat:    ~ 2ms
  poll wait:    ~150ms  (avg полцикла из 300ms)
  pipeline:     ~ 49ms
  calc + emit:  ~  4ms
                ────────
  ИТОГО:        ~220ms   (подтверждено bitget→bybit: avg 352ms включая буфер)

Худший норм. случай (без аномалий):
  buffer_age:   ~45ms  (OKX/Gate)
  flush_lat:    ~ 5ms
  poll wait:    ~250ms
  pipeline:     ~ 74ms
  calc + emit:  ~  4ms
                ────────
  ИТОГО:        ~378ms
```

---

### Путь до Snapshot (Snapshot E2E)

```
Exchange broadcast
    │
    ▼  [E2E до сигнала: см. выше]
    │
SIGNAL EMITTED
    │
    ▼  stream_lag  avg 69ms  │  min 58ms  │  max 99ms
    │  (XREAD latency + Redis Stream delivery)
    ▼
snapshot_start
    │
    ▼  load_history()
    │
    │  load_lat (зависит от истории):
    │    Пара новая (0 rows):     1–105ms  avg 21ms
    │    Пара молодая (1–10):     4–192ms  avg 47ms
    │    Пара с историей (11–100): 8–840ms  avg 216ms
    │    Зрелая пара (>100):     14–1467ms  avg 500ms
    ▼
ПЕРВАЯ СТРОКА SNAPSHOT ЗАПИСАНА
```

#### Итоговые E2E до snapshot по сценариям

| Сценарий | E2E до сигнала | stream_lag | load_lat | **Total** |
|----------|---------------|------------|----------|-----------|
| Лучший (bybit↔binance, новая пара) | 128ms | 58ms | 1ms | **~190ms** |
| Типичный fast (bitget→bybit, молодая) | 352ms | 69ms | 47ms | **~470ms** |
| Типичный slow (bitget→gate, с историей) | 579ms | 69ms | 216ms | **~865ms** |
| Gate spot + зрелая пара | 1 358ms | 69ms | 500ms | **~1 930ms** |
| Worst case (stale + большая история) | 3 428ms | 99ms | 1 467ms | **~5 000ms** |

---

### Резюме: где теряется время

```
Компонент             Вклад в E2E     Управляемо?
─────────────────────────────────────────────────
buffer_age            10–45ms         ✓ уменьшить BATCH_FLUSH_INTERVAL_MS
flush_lat (normal)    1.5–5ms         ✓ хорошо
flush_lat (Binance)   до 632ms спайк  ✓ разбить батч, уменьшить порог
poll wait             0–300ms avg150  ✓ уменьшить SPREAD_POLL_INTERVAL_MS
pipeline_lat          49ms avg        ~ (75% цикла, Redis overhead)
Gate spot data_age    300–3424ms      ✗ зависит от Gate ws latency
stream_lag            58–99ms         ✓ хорошо
load_history          1–1467ms        ✓ зависит от объёма истории
```

---

## Redis Health (из redis_monitor)

```
Memory:    used avg 12.5GB  │  peak 14.8GB   ⚠️ порог 3GB превышен ×4
Ops/sec:   avg 93 900       │  max 152 800
Keys:      avg 53 054       │  max 60 432
Hit rate:  91.3%
Ping RTT:  avg 0.99ms       │  max 2ms
HSET p99:  18µs             ← Redis сам не bottleneck
LPUSH p99:  3µs
Eventloop: avg 167µs        │  max 219µs
```

---

## Запуск системы

```
redis_flush (7 440 keys)
    ▼
subscribe files × 10 (249–532 символов на биржу)
    ▼
process_started × 9   stagger 500ms
    ▼
warmup 10s
    ▼
spread_monitor delay 60s   ← ждёт прогрева коллекторов
    ▼
health_check каждые 30s
```

---

## Открытые риски

| # | Проблема | Данные |
|---|----------|--------|
| 1 | Redis OOM | 12.5GB при пороге 3GB (×4) |
| 2 | Binance slow flush | 81 событий, max 632ms, 41K команд — спайки E2E |
| 3 | Gate spot data stale | 31% пар stale/цикл; gate_spot пары avg 1134–1358ms E2E |
| 4 | Stale signals | 60% сигналов имеют E2E >400ms; oldest leg >1s у 12 сигналов |
| 5 | load_history при >100 rows | avg 500ms, max 1467ms — задержка начала snapshot |
| 6 | OKX buffer_age | 30.9ms avg vs 10–14ms у других |
