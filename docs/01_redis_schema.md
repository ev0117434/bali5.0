# BALI 5.0 — Redis Key Schema

## Соглашения

- Все символы в нормализованном формате: `BTCUSDT` (без разделителей)
- exchange: `binance` | `bybit` | `okx` | `gate` | `bitget`
- market: `spot` | `futures`
- ts: Unix timestamp в **миллисекундах** (int)
- Все значения хранятся как **строки** (Redis hash values)

---

## 1. Primary Keys — Market Data (MD)

### Ключ
```
md:{exchange}:{market}:{symbol}
```

### Тип: Hash

| Поле | Тип значения | Описание |
|------|-------------|----------|
| `b` | string float | Best bid price |
| `a` | string float | Best ask price |
| `ts` | string int (ms) | Время записи в Redis |

### Примеры
```
md:binance:spot:BTCUSDT     → {b: "34000.10", a: "34001.50", ts: "1705312345123"}
md:bybit:futures:ETHUSDT    → {b: "1800.00",  a: "1800.50",  ts: "1705312345456"}
md:okx:spot:SOLUSDT         → {b: "95.10",    a: "95.20",    ts: "1705312345789"}
```

### TTL: нет (перезаписывается при каждом обновлении)

### Доступ
- **Write:** collector (HSET, batched pipeline)
- **Read:** spread_monitor (HMGET b,a,ts), stale_monitor (HMGET ts), snapshot_monitor

---

## 2. Primary Keys — Order Book (OB)

### Ключ
```
ob:{exchange}:{market}:{symbol}
```

### Тип: Hash

| Поле | Описание |
|------|----------|
| `b1` .. `b10` | Bid цена уровня 1..10 (лучшая → худшая) |
| `b1q` .. `b10q` | Bid количество на уровне 1..10 |
| `a1` .. `a10` | Ask цена уровня 1..10 (лучшая → худшая) |
| `a1q` .. `a10q` | Ask количество на уровне 1..10 |

### Итого: 40 полей на ключ

### Примеры
```
ob:binance:spot:BTCUSDT → {
  b1: "34000.10",  b1q: "10.50",
  b2: "34000.00",  b2q: "5.20",
  ...
  b10: "33990.00", b10q: "100.00",
  a1: "34001.50",  a1q: "8.30",
  ...
  a10: "34010.00", a10q: "200.00"
}
```

### TTL: нет

### Доступ
- **Write:** collector (HSET 40 fields, batched pipeline)
- **Read:** snapshot_monitor

---

## 3. Primary Keys — Funding Rate (FR)

### Ключ
```
fr:{exchange}:futures:{symbol}
```

### Тип: Hash

| Поле | Тип значения | Описание |
|------|-------------|----------|
| `fr` | string float | Funding rate (например "0.000100") |
| `fr_ts` | string int (ms) | Время следующей выплаты фандинга |

### Примеры
```
fr:binance:futures:BTCUSDT → {fr: "0.000100", fr_ts: "1705320000000"}
fr:bybit:futures:ETHUSDT   → {fr: "-0.000050", fr_ts: "1705320000000"}
```

### Важно: только futures, нет spot

### TTL: нет

### Доступ
- **Write:** collector (HSET, batched pipeline)
- **Read:** snapshot_monitor

---

## 4. History Keys — MD History

### Ключ
```
md:hist:{exchange}:{market}:{symbol}:{chunk_id}
```

### Тип: List (LPUSH — новые записи в head)

### Формат строки (одна запись в list):
```
{bid},{ask},{ts}
```

### Пример
```
md:hist:binance:spot:BTCUSDT:12345 → [
  "34001.50,34002.00,1705312380000",   ← newest (head)
  "34001.00,34001.80,1705312370000",
  "34000.50,34001.20,1705312360000",
  ...                                  ← oldest (tail)
]
```

### TTL: EXPIRE = 6000 сек (при каждой записи обновляется)

### Chunk ID
```python
chunk_id = int(time.time() / 1200)  # меняется каждые 20 минут
```

### Активные чанки: до 4 (= до 80 минут истории)
```
Чанки в Redis: [chunk_now-3, chunk_now-2, chunk_now-1, chunk_now]
```

---

## 5. History Keys — OB History

### Ключ
```
ob:hist:{exchange}:{market}:{symbol}:{chunk_id}
```

### Тип: List

### Формат строки:
```
{b1},{b1q},{b2},{b2q},...,{b10},{b10q},{a1},{a1q},...,{a10},{a10q},{ts}
```
41 значение через запятую (40 полей OB + ts)

### TTL: EXPIRE = 6000 сек

---

## 6. History Keys — FR History

### Ключ
```
fr:hist:{exchange}:futures:{symbol}:{chunk_id}
```

### Тип: List

### Формат строки:
```
{fr},{fr_ts},{ts}
```
ts = время записи, fr_ts = время следующего фандинга

### TTL: EXPIRE = 6000 сек

---

## 7. System Keys

### hist:config

```
hist:config
```

**Тип:** Hash

| Поле | Описание |
|------|----------|
| `current_chunk` | Текущий chunk_id (int) |
| `chunk_start_ts` | Unix timestamp начала текущего чанка (сек) |
| `chunk_duration` | 1200 (секунд, константа) |

**Обновляется:** при смене чанка в любом collector

---

### Cooldown Keys

```
spread:cooldown:{spot_exch}:{fut_exch}:{symbol}
```

**Тип:** String
**Значение:** "1"
**TTL:** 3600 сек (установлен через SETEX)

**Примеры:**
```
spread:cooldown:binance:bybit:BTCUSDT  TTL=3243s
spread:cooldown:okx:binance:ETHUSDT   TTL=1823s
```

---

## 8. Pub/Sub Channels

| Канал | Publisher | Subscriber | Формат сообщения |
|-------|-----------|------------|-----------------|
| `ch:signals` | spread_monitor | snapshot_monitor | `spot_exch,fut_exch,symbol,ask_spot,bid_fut,spread_pct,ts` |

**Пример сообщения:**
```
binance,bybit,BTCUSDT,45000.10,45676.35,1.5023,1741234567890
```

---

## 9. Объём данных (оценка)

### Количество ключей (оценка при 200 парах в combination)

| Тип ключа | Оценка количества |
|-----------|-----------------|
| md:* | 5 бирж × 2 рынка × ~400 символов = ~4000 |
| ob:* | ~4000 |
| fr:* | 5 бирж × ~400 futures символов = ~2000 |
| md:hist:* | ~4000 × 4 чанка = ~16,000 |
| ob:hist:* | ~4000 × 4 чанка = ~16,000 |
| fr:hist:* | ~2000 × 4 чанка = ~8,000 |
| spread:cooldown:* | не более кол-ва пар = ~4000 |

**Итого ключей:** ~50,000

### Объём памяти (оценка при каждом тике, ресурс не ограничен)

| Тип | Avg размер | Кол-во | Итого |
|-----|-----------|--------|-------|
| md hash | ~100 байт | 4,000 | ~400 KB |
| ob hash | ~800 байт | 4,000 | ~3.2 MB |
| fr hash | ~80 байт | 2,000 | ~160 KB |
| md:hist list | 25 байт × 12,000 записей × 4 чанка | 4,000 ключей | ~4.8 GB |
| ob:hist list | 400 байт × 12,000 × 4 | 4,000 ключей | ~77 GB |
| fr:hist list | 40 байт × ~5 записей × 4 | 2,000 ключей | ~1.6 MB |

**Итого история: ~82 GB** — принято, ресурс не ограничен.

Каждый тик для MD и OB пишется в историю без downsampling.
FR — при каждом изменении (раз в ~8 часов по символу).

---

## 10. Redis Configuration Recommendations

```conf
# /root/bali5.0/redis.conf
# Unix socket — TCP disabled (port 0)

unixsocket /root/bali5.0/redis.sock
unixsocketperm 770
port 0

daemonize yes
maxmemory 0                 # unlimited — ~82 GB история, ресурс не ограничен
maxmemory-policy noeviction # отказать в записи если вдруг закончится память
save ""                     # RDB snapshots отключены — ускоряет запись
appendonly no               # AOF отключён — ускоряет запись
tcp-keepalive 60
hz 20
loglevel notice
logfile /root/bali5.0/logs/redis-server.log
```

Запуск: `bash scripts/redis_setup.sh` (идемпотентно — запускает только если не работает)
Диагностика: `redis-cli -s /root/bali5.0/redis.sock ping`
