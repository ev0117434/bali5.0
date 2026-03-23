# BALI 5.0 — Детальные алгоритмы

---

## 1. Collector (collector_{exchange}.py)

### Структура
```
collector_binance.py
├── INIT: загрузка символов, создание Redis pool, буферов
├── task_md_spot()      WS → parse → BatchBuffer
├── task_md_fut()       WS → parse → BatchBuffer
├── task_ob_spot()      WS → parse → BatchBuffer
├── task_ob_fut()       WS → parse → BatchBuffer
├── task_fr()           WS → parse → BatchBuffer
├── task_flusher()      каждые 250мс или 100 cmd → Redis pipeline
├── task_metrics()      каждые 5 сек → лог
└── main()              asyncio.gather всех задач
```

### Полный алгоритм

```
INIT:
─────
1. setup_logger("collector_binance") → logs/collector_binance.log
2. Загрузить subscribe файлы:
   spot_symbols  = load_file("dictionaries/subscribe/binance/binance_spot.txt")
   fut_symbols   = load_file("dictionaries/subscribe/binance/binance_futures.txt")
3. Загрузить native-маппинги (только для OKX и Gate.io):
   native_map, norm_map = load_native_maps()
4. Создать redis_pool = aioredis.ConnectionPool.from_url(config.REDIS_URL)  # unix:///root/bali5.0/redis.sock
5. batch_buffer  = {}   # key → {field: value, ...}   (primary HSET)
   hist_buffer   = []   # [(hist_key, line_str), ...]  (history LPUSH)
   cmd_counter   = 0    # счётчик команд в текущем батче
   flush_event   = asyncio.Event()
   # expire_set: ключи которым уже выставили EXPIRE в текущем чанке.
   # EXPIRE ставится ОДИН РАЗ на ключ за время жизни чанка, не на каждый LPUSH.
   expire_set: set[str] = set()
   last_chunk_id = int(time.time() / 1200)

   # Метрики:
   stats = {
     "md_msgs": 0, "ob_msgs": 0, "fr_msgs": 0,
     "flushes": 0, "flush_lat_sum": 0.0, "flush_lat_max": 0.0,
     "batch_sum": 0, "hist_cmds": 0, "reconnects": 0
   }


WS TASK (общий шаблон для MD/OB/FR):
─────────────────────────────────────
async def ws_task(url_builder, symbols, parser, buffer_writer, label):
    backoff = 1
    while True:
        try:
            url = url_builder(symbols)
            log.info(f"[{label}] Connecting to {url[:60]}...")
            async with websockets.connect(
                url,
                ping_interval=20, ping_timeout=30,
                close_timeout=5, max_size=10_000_000
            ) as ws:
                log.info(f"[{label}] Connected. symbols={len(symbols)}")
                backoff = 1  # сброс backoff после успешного подключения

                # Для Bybit/OKX/Bitget: отправить subscribe сообщение
                await send_subscribe(ws, symbols)

                # Запустить heartbeat как отдельную подзадачу
                hb_task = asyncio.create_task(heartbeat(ws, label))

                try:
                    while True:
                        raw = await ws.recv()
                        result = parser(raw)
                        if result is None: continue
                        buffer_writer(result)  # добавить в BatchBuffer
                        stats[f"{label}_msgs"] += 1
                        if cmd_counter >= 100:
                            flush_event.set()  # принудительный flush
                finally:
                    hb_task.cancel()

        except Exception as e:
            stats["reconnects"] += 1
            log.warning(f"[{label}] WS error: {type(e).__name__}: {e}. "
                       f"Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # exponential backoff cap 60s


ПРИНЦИП ЗАПИСИ ИСТОРИИ:
────────────────────────
История пишется с той же частотой, что и получение новых данных.
Каждое WS-сообщение → одна запись в primary (HSET) + одна запись в history (LPUSH).
Никакого downsampling, никакой дополнительной задержки.

EXPIRE оптимизация:
  EXPIRE ставится ОДИН РАЗ на hist-ключ за время жизни чанка.
  expire_set хранит ключи, которым уже выставлен EXPIRE в текущем чанке.
  При смене чанка: expire_set.clear() → следующий LPUSH снова выставит EXPIRE.
  Это сокращает EXPIRE-команды с 12,000/сек до ~4,000 раз за 20 минут.

CHUNK ID из ts сообщения (не из time.time()):
  chunk_id = int(ts_ms / 1000 / 1200)
  Гарантирует что исторические данные попадают в правильный чанк
  даже при небольшом расхождении между биржевым и локальным временем.


BUFFER WRITER для MD:
──────────────────────
def write_md_to_buffer(symbol, bid, ask, ts_ms, market):
    nonlocal cmd_counter
    # Primary
    key = f"md:binance:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}

    # History — каждый тик, chunk_id из ts сообщения
    chunk_id = int(ts_ms / 1000 / 1200)
    hist_key = f"md:hist:binance:{market}:{symbol}:{chunk_id}"
    hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))

    cmd_counter += 2  # HSET primary + LPUSH hist


BUFFER WRITER для OB:
──────────────────────
def write_ob_to_buffer(symbol, bids, asks, ts_ms, market):
    nonlocal cmd_counter
    # Primary
    key = f"ob:binance:{market}:{symbol}"
    fields = {}
    for i, (price, qty) in enumerate(bids[:10], 1):
        fields[f"b{i}"] = price
        fields[f"b{i}q"] = qty
    for i, (price, qty) in enumerate(asks[:10], 1):
        fields[f"a{i}"] = price
        fields[f"a{i}q"] = qty
    batch_buffer[key] = fields

    # History — каждый тик, chunk_id из ts сообщения
    chunk_id = int(ts_ms / 1000 / 1200)
    hist_key = f"ob:hist:binance:{market}:{symbol}:{chunk_id}"
    line = ",".join(
        [f"{p},{q}" for p, q in bids[:10]] +
        [f"{p},{q}" for p, q in asks[:10]] +
        [str(ts_ms)]
    )
    hist_buffer.append((hist_key, line))
    cmd_counter += 2  # HSET primary + LPUSH hist


BUFFER WRITER для FR:
──────────────────────
def write_fr_to_buffer(symbol, rate, fr_ts_ms):
    nonlocal cmd_counter
    # Primary
    key = f"fr:binance:futures:{symbol}"
    ts_ms = int(time.time() * 1000)  # время записи
    batch_buffer[key] = {"fr": rate, "fr_ts": str(fr_ts_ms)}

    # History — каждое полученное обновление (обычно раз в ~8 часов)
    chunk_id = int(ts_ms / 1000 / 1200)
    hist_key = f"fr:hist:binance:futures:{symbol}:{chunk_id}"
    hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))

    cmd_counter += 2  # HSET primary + LPUSH hist


FLUSHER TASK:
─────────────
async def task_flusher():
    redis = aioredis.Redis(connection_pool=redis_pool)

    while True:
        # Ждём trigger: 250ms timeout ИЛИ flush_event (при накоплении 100 команд)
        try:
            await asyncio.wait_for(flush_event.wait(), timeout=0.250)
        except asyncio.TimeoutError:
            pass
        flush_event.clear()

        if not batch_buffer and not hist_buffer:
            continue

        # Swap буферов — атомарно в asyncio (однопоточный event loop, нет гонок)
        # await не происходит во время swap → lock не нужен
        current_batch = batch_buffer.copy()
        current_hist  = hist_buffer.copy()
        batch_buffer.clear()
        hist_buffer.clear()
        cmd_counter = 0

        if not current_batch and not current_hist:
            continue

        t_start = time.monotonic()

        # Проверить смену чанка → сбросить expire_set
        new_chunk_id = int(time.time() / 1200)
        if new_chunk_id != last_chunk_id:
            expire_set.clear()
            log.info(f"Chunk changed: {last_chunk_id} → {new_chunk_id}")
            last_chunk_id = new_chunk_id

        # Построить pipeline
        pipe = redis.pipeline(transaction=False)
        expire_cmds = 0

        # Primary keys (HSET)
        for key, fields in current_batch.items():
            pipe.hset(key, mapping=fields)

        # History keys (LPUSH + EXPIRE только при первом появлении в чанке)
        for hist_key, line in current_hist:
            pipe.lpush(hist_key, line)
            if hist_key not in expire_set:
                pipe.expire(hist_key, 6000)   # TTL 100 минут — один раз на ключ
                expire_set.add(hist_key)
                expire_cmds += 1

        # Выполнить
        try:
            await pipe.execute()
        except Exception as e:
            log.error(f"Redis pipeline error: {e}")

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["flushes"]       += 1
        stats["flush_lat_sum"] += flush_lat_ms
        stats["flush_lat_max"]  = max(stats["flush_lat_max"], flush_lat_ms)
        stats["batch_sum"]     += len(current_batch)
        stats["hist_cmds"]     += len(current_hist)

        if flush_lat_ms > 100:
            log.warning(
                f"Slow flush: {flush_lat_ms:.1f}ms "
                f"primary={len(current_batch)} hist={len(current_hist)} "
                f"expire_new={expire_cmds}"
            )


METRICS TASK:
─────────────
async def task_metrics():
    interval = 5.0
    while True:
        await asyncio.sleep(interval)
        n = stats["flushes"] or 1
        log.info(
            f"METRICS | "
            f"md={stats['md_msgs']/interval:.0f}msg/s "
            f"ob={stats['ob_msgs']/interval:.0f}msg/s "
            f"fr={stats['fr_msgs']/interval:.0f}msg/s "
            f"hist_writes={stats['hist_cmds']/interval:.0f}/s "
            f"flush_lat avg={stats['flush_lat_sum']/n:.1f}ms "
            f"max={stats['flush_lat_max']:.1f}ms "
            f"batch_avg={stats['batch_sum']/n:.0f} "
            f"expire_set_size={len(expire_set)} "
            f"reconnects={stats['reconnects']}"
        )
        # Сброс скользящих счётчиков
        stats["md_msgs"] = stats["ob_msgs"] = stats["fr_msgs"] = 0
        stats["hist_cmds"] = 0
        stats["flushes"] = stats["flush_lat_sum"] = stats["flush_lat_max"] = 0
        stats["batch_sum"] = 0


MAIN:
─────
async def main():
    log.info(f"Starting collector_binance | "
             f"spot={len(spot_symbols)} fut={len(fut_symbols)} symbols")

    tasks = [
        asyncio.create_task(task_md_spot()),
        asyncio.create_task(task_md_fut()),
        asyncio.create_task(task_ob_spot()),
        asyncio.create_task(task_ob_fut()),
        asyncio.create_task(task_fr()),
        asyncio.create_task(task_flusher()),
        asyncio.create_task(task_metrics()),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info("Shutting down...")
        for t in tasks: t.cancel()
```

---

## 2. Spread Monitor (spread_monitor.py)

```
INIT:
─────
1. setup_logger("spread_monitor") → logs/spread_monitor.log
2. Загрузить combination файлы:
   pairs = []
   for fname in glob("dictionaries/combination/*.txt"):
       # Имя файла: binance_spot_bybit_futures.txt
       spot_exch = parse_spot_exch(fname)   # "binance"
       fut_exch  = parse_fut_exch(fname)    # "bybit"
       for symbol in read_lines(fname):
           pairs.append((spot_exch, fut_exch, symbol))
   log.info(f"Loaded {len(pairs)} pairs from {n_files} combination files")

3. Проверить/создать signal/signal.csv (написать header если файл пустой)
4. redis = aioredis.Redis(...)
5. signal_write_lock = asyncio.Lock()  # защита от concurrent write в CSV

LOOP (каждые 300мс):
─────────────────────
async def monitor_loop():
    while True:
        t_cycle_start = time.monotonic()

        # --- Шаг 1: ОДИН pipeline на все чтения ---
        pipe = redis.pipeline(transaction=False)
        for spot_exch, fut_exch, symbol in pairs:
            pipe.hmget(f"md:{spot_exch}:spot:{symbol}", "a", "ts")
            pipe.hmget(f"md:{fut_exch}:futures:{symbol}", "b", "ts")

        try:
            results = await pipe.execute()
        except Exception as e:
            log.error(f"Redis pipeline error: {e}")
            await asyncio.sleep(0.300)
            continue

        # --- Шаг 2: Обработка результатов ---
        signals_count = 0
        stale_count   = 0
        no_data_count = 0
        now_ms = int(time.time() * 1000)

        for i, (spot_exch, fut_exch, symbol) in enumerate(pairs):
            ask_data = results[i * 2]      # [ask, ask_ts]
            bid_data = results[i * 2 + 1]  # [bid, bid_ts]

            # Нет данных
            if not ask_data[0] or not bid_data[0]:
                no_data_count += 1
                continue

            # Проверка staleness данных (не старше 5 сек)
            ask_ts = int(ask_data[1]) if ask_data[1] else 0
            bid_ts = int(bid_data[1]) if bid_data[1] else 0
            if now_ms - ask_ts > 5000 or now_ms - bid_ts > 5000:
                stale_count += 1
                continue

            # Расчёт спреда
            ask_spot = float(ask_data[0])
            bid_fut  = float(bid_data[0])
            if ask_spot <= 0: continue

            spread_pct = (ask_spot - bid_fut) / ask_spot * 100

            if spread_pct >= SPREAD_THRESHOLD:
                await handle_signal(
                    spot_exch, fut_exch, symbol,
                    ask_spot, bid_fut, spread_pct, now_ms
                )
                signals_count += 1

        # --- Шаг 3: Лог цикла ---
        elapsed_ms = (time.monotonic() - t_cycle_start) * 1000
        log.debug(
            f"Cycle: {elapsed_ms:.1f}ms | "
            f"pairs={len(pairs)} no_data={no_data_count} "
            f"stale={stale_count} signals={signals_count}"
        )

        if elapsed_ms > 250:
            log.warning(f"Cycle slow: {elapsed_ms:.1f}ms > 250ms threshold")

        # --- Шаг 4: Sleep оставшееся время ---
        sleep_ms = max(0, 300 - elapsed_ms)
        await asyncio.sleep(sleep_ms / 1000)


HANDLE SIGNAL:
──────────────
async def handle_signal(spot_exch, fut_exch, symbol, ask_spot, bid_fut, spread_pct, ts):
    cooldown_key = f"spread:cooldown:{spot_exch}:{fut_exch}:{symbol}"

    # Атомарная проверка + установка (SET NX с TTL)
    # SET key 1 NX EX 3600 — атомарно: установить если не существует
    was_set = await redis.set(cooldown_key, 1, nx=True, ex=COOLDOWN_SECONDS)
    if not was_set:
        return  # уже в cooldown

    signal_line = (
        f"{spot_exch},{fut_exch},{symbol},"
        f"{ask_spot:.8f},{bid_fut:.8f},{spread_pct:.4f},{ts}"
    )

    # Запись в CSV
    async with signal_write_lock:
        async with aiofiles.open("signal/signal.csv", "a") as f:
            await f.write(signal_line + "\n")

    # Публикация в Redis Pub/Sub
    await redis.publish("ch:signals", signal_line)

    log.info(
        f"SIGNAL | {spot_exch}→{fut_exch} {symbol} "
        f"ask={ask_spot:.2f} bid={bid_fut:.2f} "
        f"spread={spread_pct:.4f}%"
    )
```

---

## 3. Snapshot Monitor (snapshot_monitor.py)

```
INIT:
─────
1. setup_logger("snapshot_monitor") → logs/snapshot_monitor.log
2. redis_sub = aioredis.Redis(...)  # для pub/sub
3. redis_data = aioredis.Redis(...) # для чтения данных
4. Создать директорию signal/snapshot/
5. active_snapshots = {}  # signal_id → asyncio.Task

SUBSCRIBE LOOP:
───────────────
async def subscribe_loop():
    pubsub = redis_sub.pubsub()
    await pubsub.subscribe("ch:signals")
    log.info("Subscribed to ch:signals")

    async for message in pubsub.listen():
        if message["type"] != "message": continue
        signal_str = message["data"].decode()
        log.info(f"Signal received: {signal_str}")

        # Запустить snapshot как отдельную задачу (не блокируем subscribe loop)
        task = asyncio.create_task(run_snapshot(signal_str))
        signal_id = signal_str.split(",")[6]  # ts как id
        active_snapshots[signal_id] = task


RUN SNAPSHOT:
─────────────
async def run_snapshot(signal_str):
    parts = signal_str.split(",")
    spot_exch  = parts[0]
    fut_exch   = parts[1]
    symbol     = parts[2]
    ask_spot   = parts[3]
    bid_fut    = parts[4]
    spread_pct = parts[5]
    signal_ts  = int(parts[6])  # мс

    fname = f"signal/snapshot/{spot_exch}_{fut_exch}_{symbol}_{signal_ts}.csv"
    log.info(f"Snapshot started: {fname}")

    # --- Шаг 1: Загрузить исторические данные ---
    log.info(f"Loading 1h history for {symbol}...")
    history_rows = await load_history(spot_exch, fut_exch, symbol, signal_ts)
    log.info(f"History loaded: {len(history_rows)} rows")

    # --- Шаг 2: Открыть файл и написать header + историю ---
    async with aiofiles.open(fname, "w", newline="") as f:
        await f.write(CSV_HEADER + "\n")
        for row in history_rows:
            await f.write(row + "\n")

    # --- Шаг 3: Реалтайм запись 3500 секунд ---
    t_start = time.monotonic()
    row_count = len(history_rows)
    last_log_time = t_start

    while True:
        elapsed = time.monotonic() - t_start
        if elapsed >= SNAPSHOT_DURATION:
            break

        t_row_start = time.monotonic()

        # Читать текущие данные из Redis (один pipeline)
        row = await read_current_snapshot_row(
            spot_exch, fut_exch, symbol, signal_str
        )

        async with aiofiles.open(fname, "a", newline="") as f:
            await f.write(row + "\n")

        row_count += 1
        row_lat = (time.monotonic() - t_row_start) * 1000

        # Лог каждые 60 секунд
        if time.monotonic() - last_log_time >= 60:
            log.info(
                f"Snapshot {symbol}: elapsed={elapsed:.0f}s "
                f"rows={row_count} row_lat={row_lat:.1f}ms"
            )
            last_log_time = time.monotonic()

        # Спать 300мс минус время записи
        sleep_s = max(0, 0.300 - (time.monotonic() - t_row_start))
        await asyncio.sleep(sleep_s)

    log.info(f"Snapshot done: {fname} | rows={row_count} elapsed={elapsed:.0f}s")


LOAD HISTORY:
─────────────
async def load_history(spot_exch, fut_exch, symbol, signal_ts_ms):
    history_start_ms = signal_ts_ms - 3_600_000  # минус 1 час
    chunk_now = int(signal_ts_ms / 1000 / 1200)
    chunks = [chunk_now - 3, chunk_now - 2, chunk_now - 1, chunk_now]

    log.debug(f"Reading chunks: {chunks}")

    pipe = redis_data.pipeline(transaction=False)
    for chunk in chunks:
        pipe.lrange(f"md:hist:{spot_exch}:spot:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"md:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"ob:hist:{spot_exch}:spot:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"ob:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"fr:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)

    results = await pipe.execute()  # 4 chunks × 5 keys = 20 results

    # Собрать данные по timestamp
    # Структура: ts → {ask_spot, bid_fut, fr, fr_ts, ob_spot:{...}, ob_fut:{...}}
    rows_by_ts = {}

    for chunk_idx, chunk in enumerate(chunks):
        base = chunk_idx * 5
        md_spot_lines  = [l.decode() for l in results[base + 0]]  # newest first
        md_fut_lines   = [l.decode() for l in results[base + 1]]
        ob_spot_lines  = [l.decode() for l in results[base + 2]]
        ob_fut_lines   = [l.decode() for l in results[base + 3]]
        fr_fut_lines   = [l.decode() for l in results[base + 4]]

        for line in md_spot_lines:
            b, a, ts = line.split(",", 2)
            ts = int(ts)
            if history_start_ms <= ts <= signal_ts_ms:
                rows_by_ts.setdefault(ts, {})["ask_spot"] = a
                rows_by_ts[ts]["bid_spot_md"] = b  # bid spot для полноты

        for line in md_fut_lines:
            b, a, ts = line.split(",", 2)
            ts = int(ts)
            if history_start_ms <= ts <= signal_ts_ms:
                rows_by_ts.setdefault(ts, {})["bid_fut"] = b

        for line in ob_spot_lines:
            parts = line.split(",")
            ts = int(parts[-1])
            if history_start_ms <= ts <= signal_ts_ms:
                rows_by_ts.setdefault(ts, {})["ob_spot"] = parts[:-1]

        for line in ob_fut_lines:
            parts = line.split(",")
            ts = int(parts[-1])
            if history_start_ms <= ts <= signal_ts_ms:
                rows_by_ts.setdefault(ts, {})["ob_fut"] = parts[:-1]

        for line in fr_fut_lines:
            fr, fr_ts, ts = line.split(",", 2)
            ts = int(ts)
            if history_start_ms <= ts <= signal_ts_ms:
                rows_by_ts.setdefault(ts, {})["fr"] = fr
                rows_by_ts[ts]["fr_ts"] = fr_ts

    # Строить CSV строки, сортировать по ts
    sorted_ts = sorted(rows_by_ts.keys())
    rows = []
    for ts in sorted_ts:
        row = build_csv_row(rows_by_ts[ts], ts, spot_exch, fut_exch, symbol)
        rows.append(row)

    return rows


READ CURRENT SNAPSHOT ROW:
───────────────────────────
async def read_current_snapshot_row(spot_exch, fut_exch, symbol, signal_str):
    pipe = redis_data.pipeline(transaction=False)
    pipe.hmget(f"md:{spot_exch}:spot:{symbol}", "a", "b")
    pipe.hmget(f"md:{fut_exch}:futures:{symbol}", "b", "a")
    pipe.hgetall(f"ob:{spot_exch}:spot:{symbol}")
    pipe.hgetall(f"ob:{fut_exch}:futures:{symbol}")
    pipe.hmget(f"fr:{fut_exch}:futures:{symbol}", "fr", "fr_ts")
    results = await pipe.execute()

    ts = int(time.time() * 1000)
    md_spot = results[0]   # [ask_spot, bid_spot]
    md_fut  = results[1]   # [bid_fut, ask_fut]
    ob_spot = results[2]   # dict with b1,b1q...
    ob_fut  = results[3]
    fr_data = results[4]   # [fr, fr_ts]

    parts = signal_str.split(",")
    ask_spot   = md_spot[0] or parts[3]
    bid_fut    = md_fut[0]  or parts[4]
    spread_pct = parts[5]
    fr         = fr_data[0] or ""
    fr_ts      = fr_data[1] or ""

    return build_csv_row_full(
        spot_exch, fut_exch, symbol,
        ask_spot, bid_fut, spread_pct, ts,
        fr, fr_ts, ob_spot, ob_fut
    )
```

---

## 4. Stale Monitor (stale_monitor.py)

```
INIT:
─────
1. setup_logger("stale_monitor") → logs/stale_monitor.log
2. redis = aioredis.Redis(...)
3. STALE_THRESHOLD_MS = 360_000  # 360 секунд

LOOP (каждые 30 секунд):
──────────────────────────
async def monitor_loop():
    while True:
        t_start = time.monotonic()
        now_ms = int(time.time() * 1000)

        # Сканируем ключи md:* через SCAN (не KEYS — не блокирует Redis)
        stale_keys = []
        total_keys = 0

        async for key in redis.scan_iter("md:*", count=100):
            total_keys += 1
            ts_raw = await redis.hget(key, "ts")
            if ts_raw is None:
                log.debug(f"Key {key.decode()} has no ts field")
                continue

            ts = int(ts_raw)
            age_ms = now_ms - ts

            if age_ms > STALE_THRESHOLD_MS:
                age_s = age_ms / 1000
                stale_keys.append((key.decode(), age_s))
                log.warning(
                    f"STALE | key={key.decode()} "
                    f"age={age_s:.0f}s last_ts={ts}"
                )

        elapsed = (time.monotonic() - t_start) * 1000
        log.info(
            f"Stale check done: "
            f"total_keys={total_keys} stale={len(stale_keys)} "
            f"elapsed={elapsed:.0f}ms"
        )

        await asyncio.sleep(30)
```

---

## 5. Redis Monitor (redis_monitor.py)

```
LOOP (каждые 30 секунд):
──────────────────────────
async def monitor_loop():
    while True:
        t = time.monotonic()

        try:
            # PING
            await redis.ping()

            # INFO сбор
            info = await redis.info()

            mem_mb      = info["used_memory"] / 1024 / 1024
            mem_peak_mb = info["used_memory_peak"] / 1024 / 1024
            ops_sec     = info["instantaneous_ops_per_sec"]
            clients     = info["connected_clients"]
            hit_rate    = info.get("keyspace_hits", 0) / max(
                info.get("keyspace_hits", 0) + info.get("keyspace_misses", 1), 1
            ) * 100

            # Подсчёт ключей
            keyspace = info.get("db0", {})
            total_keys = keyspace.get("keys", 0)

            elapsed_ms = (time.monotonic() - t) * 1000

            log.info(
                f"REDIS OK | "
                f"mem={mem_mb:.1f}MB peak={mem_peak_mb:.1f}MB "
                f"ops/s={ops_sec} clients={clients} "
                f"keys={total_keys} hit_rate={hit_rate:.1f}% "
                f"ping={elapsed_ms:.1f}ms"
            )

            # Предупреждения
            if mem_mb > 3000:
                log.warning(f"Redis memory high: {mem_mb:.0f}MB")
            if ops_sec > 50000:
                log.warning(f"Redis ops/sec high: {ops_sec}")

        except Exception as e:
            log.error(f"Redis UNREACHABLE: {e}")

        await asyncio.sleep(30)
```

---

## 6. Launcher (launcher.py)

```
ПРОЦЕССЫ:
─────────
PROCESSES = {
    "collector_binance": "collectors/collector_binance.py",
    "collector_bybit":   "collectors/collector_bybit.py",
    "collector_okx":     "collectors/collector_okx.py",
    "collector_gate":    "collectors/collector_gate.py",
    "collector_bitget":  "collectors/collector_bitget.py",
    "redis_monitor":     "monitors/redis_monitor.py",
    "stale_monitor":     "monitors/stale_monitor.py",
    "spread_monitor":    "monitors/spread_monitor.py",
    # "snapshot_monitor" добавляется только с флагом --data
}

ЗАПУСК:
────────
def main():
    setup_logger("launcher")

    # Проверка Redis
    check_redis_available()

    # Создание директорий
    os.makedirs("logs",             exist_ok=True)
    os.makedirs("signal",           exist_ok=True)
    os.makedirs("signal/snapshot",  exist_ok=True)

    # Проверка subscribe файлов
    check_subscribe_files_exist()

    # Запуск процессов
    procs = {}
    for name, script in PROCESSES.items():
        log.info(f"Starting {name}...")
        p = subprocess.Popen(
            [sys.executable, script],
            stdout=subprocess.DEVNULL,  # логи в файл
            stderr=subprocess.DEVNULL,
        )
        procs[name] = p
        log.info(f"Started {name} PID={p.pid}")
        time.sleep(0.5)  # небольшая задержка между стартами

    # Пауза перед мониторами (коллекторам нужно время на заполнение Redis)
    log.info("Waiting 10s for collectors to populate Redis...")
    time.sleep(10)

    # Health check loop
    log.info("All processes started. Entering health check loop...")
    while True:
        time.sleep(30)
        for name, p in list(procs.items()):
            if p.poll() is not None:  # процесс умер
                log.error(f"Process {name} (PID={p.pid}) died! Restarting...")
                script = PROCESSES[name]
                new_p = subprocess.Popen([sys.executable, script],
                                         stdout=subprocess.DEVNULL,
                                         stderr=subprocess.DEVNULL)
                procs[name] = new_p
                log.info(f"Restarted {name} PID={new_p.pid}")


SHUTDOWN (SIGTERM handler):
─────────────────────────────
def handle_sigterm(signum, frame):
    log.info("SIGTERM received. Shutting down all processes...")
    for name, p in procs.items():
        p.terminate()
        log.info(f"Terminated {name} PID={p.pid}")
    sys.exit(0)
```

---

## 7. Chunk ID и TTL

```python
# Текущий chunk ID
def current_chunk_id() -> int:
    return int(time.time() / 1200)

# Пример:
# time.time() = 1705312345
# chunk_id    = 1705312345 // 1200 = 1421093

# Ключ истории:
# md:hist:binance:spot:BTCUSDT:1421093

# TTL при создании/обновлении:
# EXPIRE md:hist:binance:spot:BTCUSDT:1421093 6000
# = 100 минут от последнего обновления
# При следующем chunk (через 20 мин) старый ключ всё ещё жив ещё 80 мин
# Новые ключи создаются с EXPIRE 6000
# Ключи старше 100 мин автоматически удаляются Redis

# Активные чанки в любой момент:
# chunk_now-3 (80-60 мин назад, TTL скоро истечёт)
# chunk_now-2 (60-40 мин назад)
# chunk_now-1 (40-20 мин назад)
# chunk_now   (0-20 мин назад, текущий)
```
