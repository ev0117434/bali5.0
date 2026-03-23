# BALI 5.0 — Exchange Adapters

Каждый коллектор содержит inline-адаптер своей биржи.
Все нормализованные символы в Redis — формат `BTCUSDT`.

---

## 1. Binance

### Идентификаторы
```
name:     "binance"
redis_id: "binance"
```

### WS Endpoints

| Тип | Рынок | URL |
|-----|-------|-----|
| MD (bookTicker) | spot | `wss://stream.binance.com:9443/stream?streams=` |
| MD (bookTicker) | futures | `wss://fstream.binance.com/stream?streams=` |
| OB (depth10) | spot | `wss://stream.binance.com:9443/stream?streams=` |
| OB (depth10) | futures | `wss://fstream.binance.com/stream?streams=` |
| FR (markPrice) | futures | `wss://fstream.binance.com/stream?streams=` |

### Подписка

```python
# Один WS = до 1024 streams (документировано)
# Рекомендуем chunk_size = 300

# MD spot:
streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols_chunk)
url = f"wss://stream.binance.com:9443/stream?streams={streams}"

# OB spot:
streams = "/".join(f"{s.lower()}@depth10@100ms" for s in symbols_chunk)

# FR futures:
streams = "/".join(f"{s.lower()}@markPrice" for s in symbols_chunk)
# Альтернатива для всех символов сразу: "!markPrice@arr@1s"
```

### Формат сообщений

**MD (bookTicker):**
```json
{
  "stream": "btcusdt@bookTicker",
  "data": {
    "u": 400900217,
    "s": "BTCUSDT",
    "b": "34000.10",
    "B": "10.50000",
    "a": "34001.50",
    "A": "5.20000"
  }
}
```
```python
def parse_md(msg: dict) -> tuple:
    data = msg.get("data", msg)
    symbol = data["s"]          # уже нормализован: BTCUSDT
    bid = data["b"]
    ask = data["a"]
    ts = int(time.time() * 1000)  # Binance bookTicker не содержит ts
    return symbol, bid, ask, ts
```

**OB spot (depth10@100ms):**
```json
{
  "stream": "btcusdt@depth10@100ms",
  "data": {
    "lastUpdateId": 1027024,
    "bids": [["34000.10", "10.50"], ["34000.00", "5.20"], ...],
    "asks": [["34001.50", "8.30"], ["34002.00", "12.10"], ...]
  }
}
```
```python
def parse_ob(raw: str) -> tuple | None:
    msg  = json.loads(raw)
    data = msg.get("data", msg)
    if "bids" not in data: return None
    stream = msg.get("stream", "")
    symbol = stream.split("@")[0].upper()
    bids = data["bids"][:10]   # уже отсортированы лучшая цена первой
    asks = data["asks"][:10]
    ts = int(time.time() * 1000)
    return symbol, bids, asks, ts
```

**OB futures (depth10@100ms) — формат отличается от spot!**
```json
{
  "stream": "btcusdt@depth10@100ms",
  "data": {
    "e": "depthUpdate",
    "E": 1700000000000,
    "T": 1700000000000,
    "s": "BTCUSDT",
    "b": [["34000.10", "10.50"], ...],
    "a": [["34001.50", "8.30"], ...]
  }
}
```
```python
def parse_ob_fut(raw: str) -> tuple | None:
    """Futures depth — использует b/a (не bids/asks) + e="depthUpdate"."""
    msg  = json.loads(raw)
    data = msg.get("data", msg)
    if data.get("e") != "depthUpdate": return None
    stream = msg.get("stream", "")
    symbol = stream.split("@")[0].upper() if stream else data.get("s", "")
    bids = data.get("b", [])[:10]
    asks = data.get("a", [])[:10]
    ts   = int(time.time() * 1000)
    return symbol, bids, asks, ts
```

> ⚠️ **ВАЖНО**: Binance futures depth WS использует `"b"`/`"a"` (не `"bids"`/`"asks"`) и имеет поле `"e": "depthUpdate"`.
> Spot и futures — разные парсеры: `parse_ob` для spot, `parse_ob_fut` для futures.

**FR (markPrice futures):**
```json
{
  "stream": "btcusdt@markPrice",
  "data": {
    "e": "markPriceUpdate",
    "E": 1705312345000,
    "s": "BTCUSDT",
    "p": "34005.00",
    "r": "0.00010000",
    "T": 1705320000000
  }
}
```
```python
def parse_fr(msg: dict) -> tuple:
    data = msg.get("data", msg)
    symbol = data["s"]
    rate = data["r"]        # строка "0.00010000"
    fr_ts = data["T"]       # ms, время следующего фандинга
    return symbol, rate, str(fr_ts)
```

### Нормализация символов
```python
# Binance нативный формат = нормализованный
def normalize(native: str) -> str:
    return native.upper()  # BTCUSDT → BTCUSDT

def native(symbol: str) -> str:
    return symbol.lower()  # BTCUSDT → btcusdt (для stream names)
```

### Heartbeat
Binance WS использует автоматический ping/pong в протоколе WebSocket.
Дополнительный application-level ping не требуется.

### Chunk size: 300 символов на соединение

---

## 2. Bybit

### Идентификаторы
```
name:     "bybit"
redis_id: "bybit"
```

### WS Endpoints

| Тип | Рынок | URL |
|-----|-------|-----|
| MD, OB | spot | `wss://stream.bybit.com/v5/public/spot` |
| MD, OB, FR | futures (linear USDT) | `wss://stream.bybit.com/v5/public/linear` |

### Подписка

```python
# Bybit: до 10 topics на одно subscribe сообщение
# НО одно WS соединение может иметь много подписок через несколько subscribe-сообщений
# Chunk для subscribe messages: 10 символов

# MD spot:
subscribe_msg = {
    "op": "subscribe",
    "args": [f"tickers.{sym}" for sym in symbols_chunk]  # до 10
}

# OB:
subscribe_msg = {
    "op": "subscribe",
    "args": [f"orderbook.10.{sym}" for sym in symbols_chunk]
}
```

### Формат сообщений

**MD (tickers):**
```json
{
  "topic": "tickers.BTCUSDT",
  "type": "snapshot",
  "data": {
    "symbol": "BTCUSDT",
    "bid1Price": "34000.10",
    "bid1Size": "10.50",
    "ask1Price": "34001.50",
    "ask1Size": "5.20"
  },
  "ts": 1705312345123
}
```
```python
def parse_md(msg: dict) -> tuple | None:
    if msg.get("op") == "pong": return None
    if "topic" not in msg: return None
    data = msg["data"]
    bid = data.get("bid1Price", "")
    ask = data.get("ask1Price", "")
    if not bid or not ask: return None
    symbol = data["symbol"]
    ts = msg.get("ts", int(time.time() * 1000))
    return symbol, bid, ask, ts
```

**FR в Bybit — через linear tickers:**
```json
{
  "topic": "tickers.BTCUSDT",
  "data": {
    "symbol": "BTCUSDT",
    "fundingRate": "0.00010000",
    "nextFundingTime": "1705320000000",
    "bid1Price": "34000.10",
    "ask1Price": "34001.50"
  }
}
```
```python
# FR приходит вместе с MD в тех же tickers! Одна подписка, два парсера.
def parse_fr_from_ticker(msg: dict) -> tuple | None:
    data = msg.get("data", {})
    fr = data.get("fundingRate")
    fr_ts = data.get("nextFundingTime")
    if not fr: return None
    symbol = data["symbol"]
    return symbol, fr, str(fr_ts) if fr_ts else ""
```

> ⚠️ **ВАЖНО**: Bybit tickers stream — snapshot+delta паттерн.
> `nextFundingTime` присутствует только в `type: "snapshot"` (первое сообщение).
> В `type: "delta"` поле **отсутствует**, если время следующего фандинга не изменилось.
> Парсер возвращает `fr_ts = ""` для delta-сообщений без `nextFundingTime`.
> `write_fr_to_buffer` НЕ включает `fr_ts` в mapping если он пустой — `hset` с частичным
> mapping не удаляет существующие поля в Redis, что позволяет сохранить последнее
> валидное значение из snapshot.

**OB (orderbook.10):**
```json
{
  "topic": "orderbook.10.BTCUSDT",
  "type": "snapshot",
  "data": {
    "s": "BTCUSDT",
    "b": [["34000.10", "10.50"], ...],
    "a": [["34001.50", "8.30"], ...]
  },
  "ts": 1705312345123
}
```
```python
def parse_ob(msg: dict) -> tuple | None:
    if "topic" not in msg: return None
    data = msg["data"]
    symbol = data["s"]
    bids = data["b"][:10]
    asks = data["a"][:10]
    ts = msg.get("ts", int(time.time() * 1000))
    return symbol, bids, asks, ts
```

### Heartbeat (обязательно!)
```python
# Bybit требует ping каждые 20 секунд
async def heartbeat(ws):
    while True:
        await asyncio.sleep(20)
        await ws.send(json.dumps({"op": "ping"}))
        # ожидаем {"op": "pong", "ret_msg": "...", "conn_id": "..."}
```

### Нормализация символов
```python
# Bybit нативный = нормализованный (spot: BTCUSDT, linear: BTCUSDT)
def normalize(native: str) -> str: return native.upper()
def native(symbol: str) -> str: return symbol  # BTCUSDT
```

### Chunk size: 10 символов на subscribe сообщение (одно WS соединение)

### ⚠️ Важно: Bybit linear futures использует USDT-маргинированные контракты

---

## 3. OKX

### Идентификаторы
```
name:     "okx"
redis_id: "okx"
```

### WS Endpoints

| Тип | Рынок | URL |
|-----|-------|-----|
| Все | spot + futures | `wss://ws.okx.com:8443/ws/v5/public` |

### Подписка

```python
# OKX: до 240 args на одно subscribe сообщение

# MD spot (best bid/ask tick-by-tick):
subscribe_msg = {
    "op": "subscribe",
    "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT"} for ...]
}

# MD futures:
{"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}

# OB spot (10 levels):
{"channel": "books", "instId": "BTC-USDT"}   # полный стакан, берём первые 10
# ВАЖНО: OKX не имеет чистого 10-level channel. "books5" = 5 уровней.
# Используем "books" и берём [:10] из bids/asks

# OB futures:
{"channel": "books", "instId": "BTC-USDT-SWAP"}

# FR futures:
{"channel": "funding-rate", "instId": "BTC-USDT-SWAP"}
```

### Формат сообщений

**MD (bbo-tbt):**
```json
{
  "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"},
  "data": [{
    "bids": [["34000.10", "10.50", "0", "1"]],
    "asks": [["34001.50", "8.30", "0", "1"]],
    "ts": "1705312345123"
  }]
}
```
```python
def parse_md(msg: dict) -> tuple | None:
    if "data" not in msg: return None
    inst_id = msg["arg"]["instId"]          # "BTC-USDT" или "BTC-USDT-SWAP"
    symbol = normalize(inst_id)             # → "BTCUSDT"
    d = msg["data"][0]
    bid = d["bids"][0][0] if d["bids"] else ""
    ask = d["asks"][0][0] if d["asks"] else ""
    ts = int(d["ts"])
    return symbol, bid, ask, ts
```

**OB (books):**
```json
{
  "arg": {"channel": "books", "instId": "BTC-USDT"},
  "action": "snapshot",
  "data": [{
    "bids": [["34000.10", "10.50", "0", "1"], ...],
    "asks": [["34001.50", "8.30", "0", "1"], ...],
    "ts": "1705312345123"
  }]
}
```
```python
def parse_ob(msg: dict) -> tuple | None:
    if "data" not in msg: return None
    if msg.get("action") not in ("snapshot", "update"): return None
    inst_id = msg["arg"]["instId"]
    symbol = normalize(inst_id)
    d = msg["data"][0]
    bids = [[b[0], b[1]] for b in d["bids"][:10]]
    asks = [[a[0], a[1]] for a in d["asks"][:10]]
    ts = int(d["ts"])
    return symbol, bids, asks, ts
```

**FR (funding-rate):**
```json
{
  "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
  "data": [{
    "instId": "BTC-USDT-SWAP",
    "fundingRate": "0.00010000",
    "nextFundingTime": "1705320000000"
  }]
}
```
```python
def parse_fr(msg: dict) -> tuple | None:
    if "data" not in msg: return None
    d = msg["data"][0]
    symbol = normalize(d["instId"])       # BTC-USDT-SWAP → BTCUSDT
    rate = d.get("fundingRate", "")
    fr_ts = d.get("nextFundingTime", "")  # уже в мс
    if not rate: return None
    return symbol, rate, str(fr_ts)
```

### Heartbeat
```python
# OKX: отправлять строку "ping" каждые 30 секунд
# В ответ придёт строка "pong"
async def heartbeat(ws):
    while True:
        await asyncio.sleep(25)
        await ws.send("ping")
```

### Нормализация символов
```python
def normalize(inst_id: str) -> str:
    # "BTC-USDT" → "BTCUSDT"
    # "BTC-USDT-SWAP" → "BTCUSDT"
    return inst_id.replace("-SWAP", "").replace("-", "")

def native_spot(symbol: str) -> str:
    # "BTCUSDT" → "BTC-USDT" (нужен файл маппинга из dictionaries)
    return _spot_native_map[symbol]

def native_futures(symbol: str) -> str:
    # "BTCUSDT" → "BTC-USDT-SWAP"
    return _futures_native_map[symbol]
```

### Маппинг native ↔ normalized
OKX native символы хранятся в:
- `dictionaries/okx/data/okx_spot_native.txt` — нативные spot
- `dictionaries/okx/data/okx_futures_native.txt` — нативные futures
- `dictionaries/okx/data/okx_spot.txt` — нормализованные spot
- `dictionaries/okx/data/okx_futures.txt` — нормализованные futures

Загрузка при старте коллектора:
```python
native_to_norm = dict(zip(native_list, norm_list))
norm_to_native = {v: k for k, v in native_to_norm.items()}
```

### Chunk size: 240 args на subscribe сообщение

---

## 4. Gate.io

### Идентификаторы
```
name:     "gate"
redis_id: "gate"
```

### WS Endpoints

| Тип | Рынок | URL |
|-----|-------|-----|
| MD, OB | spot | `wss://api.gateio.ws/ws/v4/` |
| MD, OB, FR | futures | `wss://fx-ws.gateio.ws/v4/ws/usdt` |

### Подписка

```python
import time

# Gate.io: отдельная подписка на каждый символ или пакетная
# spot book_ticker:
subscribe_msg = {
    "time": int(time.time()),
    "channel": "spot.book_ticker",
    "event": "subscribe",
    "payload": ["BTC_USDT", "ETH_USDT", ...]  # несколько символов
}

# futures book_ticker:
{
    "time": int(time.time()),
    "channel": "futures.book_ticker",
    "event": "subscribe",
    "payload": ["BTC_USDT"]
}

# OB spot:
{
    "time": int(time.time()),
    "channel": "spot.order_book",
    "event": "subscribe",
    "payload": ["BTC_USDT", "5", "100ms"]
    # depth: "5" или "10" или "20"
    # interval: "100ms"
}
# ВАЖНО: spot.order_book принимает ОДИН символ за раз!

# OB futures (full snapshot, НЕ инкрементальные дельты):
{
    "channel": "futures.order_book",
    "event": "subscribe",
    "payload": ["BTC_USDT", "100ms", "10"]
    # последний параметр: количество уровней
}
# ⚠️ futures.order_book_update (дельты) НЕ используем:
#    batch_buffer сохраняет только последнее обновление → при пустых дельтах стакан теряется

# FR futures:
{
    "channel": "futures.tickers",
    "event": "subscribe",
    "payload": ["BTC_USDT"]
}
```

### Формат сообщений

**MD spot (book_ticker):**
```json
{
  "time": 1705312345,
  "channel": "spot.book_ticker",
  "event": "update",
  "result": {
    "t": 1705312345123,
    "u": 12345,
    "s": "BTC_USDT",
    "b": "34000.10",
    "B": "10.50",
    "a": "34001.50",
    "A": "5.20"
  }
}
```
```python
def parse_md_spot(msg: dict) -> tuple | None:
    if msg.get("event") == "subscribe": return None
    result = msg.get("result")
    if not result: return None
    symbol = normalize(result["s"])   # BTC_USDT → BTCUSDT
    bid = result["b"]
    ask = result["a"]
    ts = result.get("t", int(time.time() * 1000))
    return symbol, bid, ask, ts
```

**MD futures (book_ticker):**
```json
{
  "channel": "futures.book_ticker",
  "event": "update",
  "result": {
    "t": 1705312345123,
    "s": "BTC_USDT",
    "b": "34000.10",
    "B": "10.50",
    "a": "34001.50",
    "A": "5.20"
  }
}
```

**OB spot (order_book):**
```json
{
  "channel": "spot.order_book",
  "event": "update",
  "result": {
    "t": 1705312345123,
    "s": "BTC_USDT",
    "bids": [["34000.10", "10.50"], ...],
    "asks": [["34001.50", "8.30"], ...]
  }
}
```

**OB futures (futures.order_book snapshot):**
```json
{
  "channel": "futures.order_book",
  "event": "all",
  "result": {
    "t": 1705312345123,
    "contract": "BTC_USDT",
    "bids": [{"p": "34000.10", "s": "10.50"}, ...],
    "asks": [{"p": "34001.50", "s": "8.30"}, ...]
  }
}
```
```python
def parse_ob_fut(raw: str) -> tuple | None:
    msg    = json.loads(raw)
    if msg.get("event") in ("subscribe", "unsubscribe", "update"): return None
    result = msg.get("result")
    if not result: return None
    if msg.get("channel") != "futures.order_book": return None
    symbol = normalize(result.get("contract", ""))
    bids   = [[b["p"], b["s"]] for b in result.get("bids", [])[:10]]
    asks   = [[a["p"], a["s"]] for a in result.get("asks", [])[:10]]
    ts_ms  = int(result.get("t", int(time.time() * 1000)))
    return symbol, bids, asks, ts_ms
```

**FR futures (futures.tickers):**
```json
{
  "channel": "futures.tickers",
  "event": "update",
  "result": [{
    "contract": "BTC_USDT",
    "funding_rate": "0.000100",
    "funding_next_apply": 1705320000
  }]
}
```
```python
def parse_fr(msg: dict) -> tuple | None:
    results = msg.get("result", [])
    if not results: return None
    r = results[0] if isinstance(results, list) else results
    symbol = normalize(r["contract"])
    rate = r.get("funding_rate", "")
    # Gate USDT-perp funds every 8h at 00:00/08:00/16:00 UTC.
    # Always compute next boundary — funding_next_apply is unreliable (often 0).
    now = int(time.time())
    fr_ts_ms = str(((now // 28800) + 1) * 28800 * 1000)
    return symbol, rate, fr_ts_ms
```

> Gate USDT-perp фундит строго каждые 8 часов: 00:00, 08:00, 16:00 UTC.
> `funding_next_apply` ненадёжен (часто `0`) → всегда вычисляем следующую границу:
> `((now // 28800) + 1) * 28800 * 1000`

### Heartbeat
```python
# Gate.io: WS-level ping (Protocol ping frame, не application-level)
# websockets библиотека отправляет автоматически если ping_interval задан
# Дополнительно можно отправить application ping:
# {"time": int(time.time()), "channel": "spot.ping", "event": ""}
# При использовании websockets library достаточно ping_interval=30
```

### Нормализация символов
```python
def normalize(native: str) -> str:
    return native.replace("_", "")  # BTC_USDT → BTCUSDT

def native(symbol: str) -> str:
    return _native_map[symbol]  # BTCUSDT → BTC_USDT (из файла маппинга)
```

### Маппинг: `dictionaries/gate/data/gate_spot_native.txt` и `gate_spot.txt`

### ✅ Подтверждённый формат Gate.io
Из файлов dictionaries: Gate.io использует `BTC_USDT` (underscore) для ОБОИХ рынков:
- spot native:    `BTC_USDT` → нормализуется в `BTCUSDT`
- futures native: `BTC_USDT` → нормализуется в `BTCUSDT`

```python
def normalize(native: str) -> str:
    return native.replace("_", "")  # BTC_USDT → BTCUSDT

def native(symbol: str) -> str:
    return _native_map[symbol]  # BTCUSDT → BTC_USDT
```

### ⚠️ ВАЖНО: Gate.io spot.order_book принимает ОДИН символ за раз
Нужно создавать отдельное subscribe-сообщение для каждого символа.
При большом количестве пар — отправлять subscribe пакетами.

### Chunk size: нет явного лимита, но рекомендуется ≤ 100 символов в одном subscribe

---

## 5. Bitget

### Идентификаторы
```
name:     "bitget"
redis_id: "bitget"
```

### WS Endpoints

| Тип | Рынок | URL |
|-----|-------|-----|
| Все | spot + futures | `wss://ws.bitget.com/v2/ws/public` |

### Подписка

```python
# MD spot:
{
    "op": "subscribe",
    "args": [{"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"}]
}

# MD futures (USDT-margined):
{"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"}

# OB spot (5 levels):
{"instType": "SPOT", "channel": "books5", "instId": "BTCUSDT"}

# OB futures (5 levels):
{"instType": "USDT-FUTURES", "channel": "books5", "instId": "BTCUSDT"}

# ПРОБЛЕМА: books5 даёт только 5 уровней!
# Решение: использовать "books" channel для полного стакана, брать [:10]
{"instType": "SPOT", "channel": "books", "instId": "BTCUSDT"}
```

### Формат сообщений

**MD spot/futures (ticker):**
```json
{
  "action": "snapshot",
  "arg": {"instType": "SPOT", "channel": "ticker", "instId": "BTCUSDT"},
  "data": [{
    "instId": "BTCUSDT",
    "bidPr": "34000.10",
    "bidSz": "10.50",
    "askPr": "34001.50",
    "askSz": "5.20",
    "ts": "1705312345123"
  }]
}
```
```python
def parse_md(msg: dict) -> tuple | None:
    if msg.get("event") == "subscribe": return None
    data_list = msg.get("data", [])
    if not data_list: return None
    d = data_list[0]
    symbol = normalize(d["instId"])   # BTCUSDT (уже нормализован для spot)
    bid = d.get("bidPr", "")
    ask = d.get("askPr", "")
    ts = int(d.get("ts", int(time.time() * 1000)))
    return symbol, bid, ask, ts
```

**FR через ticker futures:**
```json
{
  "data": [{
    "instId": "BTCUSDT",
    "fundingRate": "0.000100",
    "nextSettleTime": "1705320000000"
  }]
}
```
```python
def parse_fr_from_ticker(msg: dict) -> tuple | None:
    d = msg.get("data", [{}])[0]
    fr = d.get("fundingRate")
    if not fr: return None
    symbol = d["instId"].upper()
    fr_ts = d.get("nextSettleTime", "")
    if fr_ts:
        _fr_ts_cache[symbol] = str(fr_ts)
        fr_ts_ms = str(fr_ts)
    elif symbol in _fr_ts_cache:
        fr_ts_ms = _fr_ts_cache[symbol]
    else:
        # Compute next 8h boundary (Bitget USDT-perp: 00:00/08:00/16:00 UTC)
        now = int(time.time())
        fr_ts_ms = str(((now // 28800) + 1) * 28800 * 1000)
    return symbol, str(fr), fr_ts_ms
```

> `nextSettleTime` присутствует только в snapshot-сообщениях, дельты его не содержат.
> Приоритет: значение из API → кэш предыдущего snapshot → вычисленная 8ч граница (fallback).

**OB (books) — snapshot + delta протокол:**
```json
{
  "action": "snapshot",
  "data": [{
    "asks": [["34001.50", "5.20", "0", "0"], ...],
    "bids": [["34000.10", "10.50", "0", "0"], ...],
    "ts": "1705312345123"
  }]
}
```
```json
{
  "action": "update",
  "data": [{
    "bids": [["34000.15", "8.20", "0", "0"]],
    "asks": [],
    "ts": "1705312345234"
  }]
}
```

> ⚠️ **ВАЖНО**: Bitget `books` использует snapshot+delta паттерн.
> Первое сообщение (`action: "snapshot"`) содержит полный стакан (до 200 уровней).
> Последующие (`action: "update"`) содержат **только изменившиеся уровни**, часто
> с одной стороны (например 1 бид и 0 асков). qty=0 означает удаление уровня.

```python
def parse_ob(raw: str) -> tuple | None:
    msg      = json.loads(raw)
    data_list = msg.get("data", [])
    if not data_list: return None
    d      = data_list[0]
    bids   = [[b[0], b[1]] for b in d.get("bids", [])[:10]]
    asks   = [[a[0], a[1]] for a in d.get("asks", [])[:10]]
    symbol = msg.get("arg", {}).get("instId", "").upper()
    if not symbol or (not bids and not asks): return None
    ts_ms  = int(d.get("ts", int(time.time() * 1000)))
    return symbol, bids, asks, ts_ms
```

**OB hist — in-memory full state (важно!):**

Из-за delta-формата `write_ob_to_buffer` поддерживает in-memory `ob_full_state`:
каждый delta-апдейт применяется к нему (`state.update(fields)`), и hist-запись строится
из полного состояния, а не из delta. Это гарантирует, что hist всегда содержит оба
side (≥1 бид и ≥1 аск) и проходит валидацию.

Запись в hist пропускается (`ob_hist_skipped`), если после применения delta
`ob_full_state` всё ещё не содержит ни одного уровня на одной из сторон
(возможно только до получения первого snapshot).

### Heartbeat
```python
# Bitget: отправлять строку "ping" каждые 30 секунд
# В ответ придёт строка "pong"
async def heartbeat(ws):
    while True:
        await asyncio.sleep(25)
        await ws.send("ping")
```

### Нормализация символов
```python
# Bitget spot нативный = нормализованный (BTCUSDT)
# Bitget futures нативный = BTCUSDT (USDT-FUTURES)
def normalize(native: str) -> str: return native.upper()
def native(symbol: str) -> str: return symbol  # уже нормализован
```

### ⚠️ Важно: Bitget books/books5 проблема
- `books5` = 5 уровней
- `books` = до 200 уровней (берём [:10])
- Используем `books` и ограничиваем 10 уровнями в парсере

### Chunk size: нет документированного лимита, рекомендуется ≤ 100 args на subscribe

---

## 6. Сводная таблица

| Exchange | MD channel | OB channel | FR channel | Native format | Heartbeat |
|----------|-----------|-----------|-----------|---------------|-----------|
| Binance | bookTicker | spot: depth10 (bids/asks) / fut: depth10 (depthUpdate b/a) | markPrice | BTCUSDT | auto WS |
| Bybit | tickers.* | orderbook.10.* | tickers.* (linear) | BTCUSDT | ping json 20s |
| OKX | bbo-tbt | books | funding-rate | BTC-USDT(-SWAP) | "ping" str 25s |
| Gate.io | book_ticker | spot: order_book / fut: order_book (snapshot) | futures.tickers | BTC_USDT | auto WS |
| Bitget | ticker | books | ticker (futures) | BTCUSDT | "ping" str 25s |

---

## 7. Особенности по биржам

### Binance
- bookTicker НЕ содержит timestamp → используем `time.time() * 1000`
- depth10@100ms даёт снепшот стакана, не diff
- **Spot OB формат**: `{"bids": [...], "asks": [...]}` → парсер `parse_ob`
- **Futures OB формат**: `{"e": "depthUpdate", "b": [...], "a": [...]}` → парсер `parse_ob_fut`
  (разные ключи — разные парсеры!)

### Bybit
- FR приходит ВМЕСТЕ с MD в tickers → одна подписка на linear ticker даёт и MD и FR
- `type: "snapshot"` — первое сообщение со всеми полями; `type: "delta"` — только изменившиеся поля
- `nextFundingTime` есть только в snapshot-сообщениях → delta-обновления возвращают `fr_ts = ""`
- `write_fr_to_buffer` не включает `fr_ts` в mapping при пустом значении — Redis `hset` с
  частичным mapping сохраняет предыдущее значение поля
- Для OB нужно обрабатывать оба типа (snapshot + delta)

### OKX
- books channel даёт `action: "snapshot"` и `action: "update"` (дельты)
- Для OB snapshot хранить локально и применять дельты? Нет — нам нужна текущая картина
- Решение: кешировать последний snapshot и применять update — сложно
- Альтернатива: использовать `books5` (только 5 уровней) без дельт — проще
- Финальное решение: отдельный WS на `books-l2-tbt` который даёт полный snapshot каждый раз — проверить документацию
- ⚠️ **ТРЕБУЕТ УТОЧНЕНИЯ** — см. docs/07_open_questions.md

### Gate.io
- `fr_ts` всегда вычисляется как следующая 8ч граница (00:00/08:00/16:00 UTC); `funding_next_apply` игнорируется (ненадёжен)
- spot.order_book требует отдельный subscribe на каждый символ
- **Futures OB**: используем `futures.order_book` (полные снепшоты), НЕ `futures.order_book_update`
  (инкрементальные дельты теряются из-за last-write-wins в batch_buffer)

### Bitget
- Использовать `books` channel (не `books5`) и обрезать до 10 уровней
- `books` channel работает по схеме **snapshot + delta**: первое сообщение — полный
  стакан, последующие — только изменившиеся уровни (1-2 уровня, часто только одна сторона)
- **OB hist использует `ob_full_state`**: per-key in-memory словарь, куда применяются
  все дельты (`state.update(fields)` — зеркало Redis `hset`). Hist-запись строится из
  полного состояния → всегда содержит оба side, проходит валидатор
- Без этого 68% hist-записей имели бы формат `price,qty,ts` (3 значения) вместо
  минимально необходимых 5 (1 бид + 1 аск + ts)
