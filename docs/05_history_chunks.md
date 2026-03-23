# BALI 5.0 — History & Chunk System

---

## 1. Назначение

История нужна для двух целей:
1. **Snapshot monitor** — дополнить снапшот данными за 1 час ДО сигнала
2. **Диагностика** — понять динамику спреда перед сигналом

---

## 2. Chunk система

### Принцип

Время делится на 20-минутные окна (чанки). Каждый чанк имеет уникальный ID:

```python
CHUNK_DURATION = 1200  # секунд = 20 минут

def current_chunk_id() -> int:
    return int(time.time() / CHUNK_DURATION)
```

### Числовой пример

```
Unix time:   1705312000
chunk_id:    1705312000 // 1200 = 1421093

Граница чанка 1421093: с 1705311600 до 1705312800
Граница чанка 1421094: с 1705312800 до 1705314000
```

### Жизненный цикл чанков

```
Время (мин):  0    20    40    60    80    100
Chunk ID:     n    n+1   n+2   n+3   n+4   n+5

Активные чанки в Redis в момент T:
  T = 0-20:   [n]
  T = 20-40:  [n, n+1]
  T = 40-60:  [n, n+1, n+2]
  T = 60-80:  [n, n+1, n+2, n+3]  ← max 4 чанка = 80 мин
  T = 80-100: [n+1, n+2, n+3, n+4]  ← n удалён (TTL истёк)
  T = 100-120:[n+2, n+3, n+4, n+5]  ← n+1 удалён
```

### TTL стратегия

При каждом LPUSH в hist-ключ устанавливаем EXPIRE = 6000 секунд:

```python
pipe.lpush(hist_key, line)
pipe.expire(hist_key, 6000)  # 100 минут
```

**Почему 6000 сек (100 мин)?**
- Чанк пишется 20 минут
- После завершения чанка он должен жить ещё минимум 80 минут (пока 4 следующих чанка не пройдут)
- 20 + 80 = 100 минут = 6000 секунд
- EXPIRE обновляется при каждой записи → пока чанк активен, TTL не истекает
- После смены чанка — последний EXPIRE был установлен в конце чанка → через 100 мин ключ исчезнет

**Нет необходимости в SCAN + DEL** — Redis сам управляет через TTL.

---

## 3. Структура ключей истории

```
md:hist:{exchange}:{market}:{symbol}:{chunk_id}
ob:hist:{exchange}:{market}:{symbol}:{chunk_id}
fr:hist:{exchange}:futures:{symbol}:{chunk_id}
```

Тип ключа: **List** (Redis LPUSH → новые записи в начале)

```
LRANGE md:hist:binance:spot:BTCUSDT:1421093 0 -1

Результат (новые записи первые):
  "34001.50,34002.00,1705312380000"   ← newest
  "34001.00,34001.80,1705312370000"
  "34000.50,34001.20,1705312360000"
  ...
  "33900.00,33901.00,1705311600000"   ← oldest
```

---

## 4. Форматы строк в истории

### MD History
```
{bid},{ask},{ts_ms}

Пример: "34000.10,34001.50,1705312345123"
```

### OB History (10 уровней)
```
{b1},{b1q},{b2},{b2q},...,{b10},{b10q},{a1},{a1q},...,{a10},{a10q},{ts_ms}

Итого 41 значение.
Пример: "34000.10,10.50,34000.00,5.20,...,34010.00,200.00,1705312345123"
```

### FR History
```
{fr},{fr_ts},{ts_ms}

fr     = значение ставки ("0.000100")
fr_ts  = время следующего фандинга в мс ("1705320000000")
ts_ms  = время записи в мс ("1705312345123")

Пример: "0.000100,1705320000000,1705312345123"
```

---

## 5. Частота истории = частота получения данных

**Правило:** каждое WS-сообщение обновляет primary key И добавляет строку в history.
Никакого downsampling. Никакой дополнительной задержки.

```
WS message received
       │
       ├─► HSET primary key        (батч → pipeline через 250мс или 100 команд)
       └─► LPUSH history key       (в тот же батч, тот же pipeline)
```

| Тип | Когда пишется в историю | Записей/час/ключ | Байт/запись |
|-----|------------------------|----------------|------------|
| MD hist | при каждом bookTicker сообщении | ~12,000 | 25 |
| OB hist | при каждом depth/orderbook сообщении | ~12,000 | 400 |
| FR hist | при каждом funding rate обновлении | ~1-5 | 40 |

**Итого (4000 ключей × 4 чанка): ~82 GB** — ресурс не ограничен.

### chunk_id всегда из ts сообщения (не из time.time())

```python
chunk_id = int(ts_ms / 1000 / 1200)
```

Гарантирует корректное попадание в чанк даже при небольшом расхождении
локального и биржевого времени. Единый принцип для MD, OB, FR.

### EXPIRE — один раз на ключ за чанк (не на каждый LPUSH)

```python
# expire_set — ключи которым уже выставили EXPIRE в текущем чанке
if hist_key not in expire_set:
    pipe.expire(hist_key, 6000)   # TTL 100 минут
    expire_set.add(hist_key)
# При смене чанка: expire_set.clear()
```

**Экономия:** при 4000 пар × 5 потоков данных = 20,000 hist-ключей.
- Без оптимизации: 12,000 EXPIRE/сек × 20,000 ключей = **240 млн команд/сек** ❌
- С expire_set: 20,000 EXPIRE за первые секунды чанка, потом 0 = **~0.27/сек** ✅

---

## 6. Алгоритм чтения истории в Snapshot

### Задача
Для символа `BTCUSDT` (binance spot + bybit futures) в момент сигнала `ts=1705312345123`:
- Нужны данные с `1705308745123` (минус 1 час) до `1705312345123`
- Из MD, OB, FR

### Шаги

```python
async def load_history(spot_exch, fut_exch, symbol, signal_ts_ms):
    """
    Загружает историю за 1 час до сигнала.
    Возвращает список CSV строк, отсортированных по ts.
    """
    history_start_ms = signal_ts_ms - 3_600_000

    # Определяем нужные чанки
    chunk_signal = int(signal_ts_ms / 1000 / 1200)
    chunks = [chunk_signal - 3, chunk_signal - 2, chunk_signal - 1, chunk_signal]
    # Фильтруем отрицательные (на случай если система только запустилась)
    chunks = [c for c in chunks if c >= 0]

    # Один pipeline на все 5 типов данных × 4 чанка = 20 запросов
    pipe = redis.pipeline(transaction=False)
    for chunk in chunks:
        pipe.lrange(f"md:hist:{spot_exch}:spot:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"md:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"ob:hist:{spot_exch}:spot:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"ob:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"fr:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)

    raw_results = await pipe.execute()  # list из 20 элементов

    # Индексирование: chunk[i] → results[i*5 + 0..4]
    # [i*5+0] = md_spot
    # [i*5+1] = md_fut
    # [i*5+2] = ob_spot
    # [i*5+3] = ob_fut
    # [i*5+4] = fr_fut

    # Собираем data по timestamp
    # Ключ: ts_ms → dict с полями
    timeline: dict[int, dict] = {}

    for ci, chunk in enumerate(chunks):
        base = ci * 5

        # MD spot: "bid,ask,ts"
        for raw in raw_results[base + 0]:
            line = raw.decode()
            b, a, ts = line.split(",", 2)
            ts = int(ts)
            if history_start_ms <= ts <= signal_ts_ms:
                d = timeline.setdefault(ts, {})
                d["ask_spot"] = a
                d["bid_spot"] = b

        # MD fut: "bid,ask,ts"
        for raw in raw_results[base + 1]:
            line = raw.decode()
            b, a, ts = line.split(",", 2)
            ts = int(ts)
            if history_start_ms <= ts <= signal_ts_ms:
                d = timeline.setdefault(ts, {})
                d["bid_fut"] = b
                d["ask_fut"] = a

        # OB spot: "b1,b1q,...,b10,b10q,a1,a1q,...,a10,a10q,ts"
        for raw in raw_results[base + 2]:
            line = raw.decode()
            parts = line.split(",")
            ts = int(parts[-1])
            if history_start_ms <= ts <= signal_ts_ms:
                d = timeline.setdefault(ts, {})
                d["ob_spot"] = parts[:-1]  # 40 значений

        # OB fut:
        for raw in raw_results[base + 3]:
            line = raw.decode()
            parts = line.split(",")
            ts = int(parts[-1])
            if history_start_ms <= ts <= signal_ts_ms:
                d = timeline.setdefault(ts, {})
                d["ob_fut"] = parts[:-1]

        # FR fut: "fr,fr_ts,ts"
        for raw in raw_results[base + 4]:
            line = raw.decode()
            fr, fr_ts, ts = line.split(",", 2)
            ts = int(ts)
            if history_start_ms <= ts <= signal_ts_ms:
                d = timeline.setdefault(ts, {})
                d["fr"] = fr
                d["fr_ts"] = fr_ts

    # Строим CSV строки (отсортировано по ts)
    rows = []
    last_ob_spot = [""] * 40  # заполняем последними известными OB
    last_ob_fut  = [""] * 40
    last_fr      = ""
    last_fr_ts   = ""

    for ts in sorted(timeline.keys()):
        d = timeline[ts]

        # Forward-fill OB и FR (они пишутся реже)
        if "ob_spot" in d: last_ob_spot = d["ob_spot"]
        if "ob_fut"  in d: last_ob_fut  = d["ob_fut"]
        if "fr"      in d: last_fr      = d["fr"]
        if "fr_ts"   in d: last_fr_ts   = d["fr_ts"]

        row = build_csv_row(
            spot_exch=spot_exch,
            fut_exch=fut_exch,
            symbol=symbol,
            ask_spot=d.get("ask_spot", ""),
            bid_fut=d.get("bid_fut", ""),
            spread_pct="",  # для исторических строк рассчитать или оставить пустым
            ts=ts,
            fr=last_fr,
            fr_ts=last_fr_ts,
            ob_spot=last_ob_spot,
            ob_fut=last_ob_fut,
        )
        rows.append(row)

    return rows
```

### Forward-fill объяснение

Так как OB пишется в историю каждые 30 секунд, а MD — каждый тик (~300мс),
для временных меток где OB отсутствует — используем последнее известное OB.
Это называется **forward-fill** (ffill) — стандартная практика в time-series.

---

## 7. Структура hist:config ключа

```
hist:config (Hash)

Поля:
  current_chunk   : int    Текущий chunk_id
  chunk_start_ts  : int    Unix timestamp (sec) начала текущего чанка
  chunk_duration  : int    1200 (константа)

Обновляется: в коллекторах при смене чанка
```

```python
async def update_hist_config(pipe, new_chunk_id):
    pipe.hset("hist:config", mapping={
        "current_chunk":  str(new_chunk_id),
        "chunk_start_ts": str(new_chunk_id * 1200),
        "chunk_duration": "1200",
    })
```

---

## 8. Итоговая оценка памяти (без downsampling)

| Тип | Записей/час/ключ | Байт/запись | Ключей | Итого (4 чанка) |
|-----|----------------|------------|--------|----------------|
| MD hist | ~12,000 | 25 | 8,000 | **~4.8 GB** |
| OB hist | ~12,000 | 400 | 8,000 | **~77 GB** |
| FR hist | ~2 | 40 | 4,000 | **~1.6 MB** |

**Итого: ~82 GB** — ресурс не ограничен, downsampling не применяется.

### Redis конфигурация под 82 GB:
```conf
maxmemory 90gb
maxmemory-policy noeviction
save ""
appendonly no
```
