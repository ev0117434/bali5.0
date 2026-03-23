# BALI 5.0 — Review Iterations (Pre-Code)

Итерации поиска ошибок, неточностей и пробелов до написания кода.

---

## Итерация 1: Протокольные и API проблемы

### ✅ НАЙДЕНО: OKX books5 только 5 уровней
**Описание:** Спецификация требует 10 уровней OB. OKX `books5` даёт только 5.
Full `books` channel требует delta management.
**Решение:** Принять 5 уровней для OKX + Bitget, поля 6-10 оставлять пустыми.
**Статус:** Задокументировано в Q1, Q2.

### ✅ НАЙДЕНО: Bybit OB delta updates
**Описание:** Bybit `orderbook.10` присылает delta (`type: "delta"`) с qty=0 для удаления уровня.
Без delta management данные в Redis будут некорректными.
**Решение:** LocalOrderBook cache на каждый символ в коллекторе.
**Статус:** Задокументировано в Q3.

### ✅ НАЙДЕНО: Gate.io FR в секундах, не мс
**Описание:** `funding_next_apply` в Gate.io futures.tickers — Unix timestamp в **секундах**.
Все остальные биржи дают мс. При записи в Redis конвертировать в мс.
**Решение:** В parse_fr Gate.io: `fr_ts_ms = str(fr_ts_sec * 1000)`
**Статус:** Задокументировано в exchange_adapters.

### ✅ НАЙДЕНО: Bybit FR в тех же tickers что MD
**Описание:** Bybit linear tickers содержат и best bid/ask И funding rate.
Нет отдельного FR channel.
**Решение:** В task_md_fut Bybit — парсить оба: MD и FR из одного сообщения.
Отдельного task_fr для Bybit не нужно.
**Статус:** Задокументировано в exchange_adapters.

### ✅ НАЙДЕНО: Binance bookTicker без timestamp
**Описание:** `{symbol}@bookTicker` не содержит `ts`.
**Решение:** time.time() * 1000 при получении сообщения.
**Статус:** Задокументировано в Q5.

---

## Итерация 2: Архитектурные проблемы

### ✅ РЕШЕНО: История MD + OB + FR — каждый тик, без downsampling
**Описание:** Ресурс не ограничен. OB история (~77 GB), MD (~4.8 GB), FR (~1.6 MB).
**Решение:** Писать каждый тик без ограничений. Redis конфиг: `maxmemory 90gb`.
**Статус:** Downsampling убран из алгоритмов и конфига.

### ✅ НАЙДЕНО: LPUSH хранит данные в обратном порядке
**Описание:** `LPUSH` добавляет в начало списка. `LRANGE 0 -1` возвращает newest→oldest.
При чтении истории нужно либо реверсировать, либо учитывать порядок.
**Решение:** В load_history — `sorted(timeline.keys())` сортирует по ts ascending.
Порядок LRANGE не критичен так как мы group by ts.
**Статус:** Учтено в алгоритме load_history.

### ✅ НАЙДЕНО: Нет защиты от конкурентной записи в signal.csv
**Описание:** Если два сигнала приходят в одном цикле spread_monitor (разные пары),
оба вызовут handle_signal. Конкурентная запись в CSV без lock → потенциальная порча файла.
**Решение:** `signal_write_lock = asyncio.Lock()` — уже добавлен в алгоритм.
**Статус:** Учтено.

### ✅ НАЙДЕНО: Одновременный старт нескольких Snapshot для одного сигнала
**Описание:** Если spread_monitor публикует сигнал, snapshot_monitor запускает
asyncio.create_task(). Если одновременно 100 сигналов — 100 параллельных задач
по чтению Redis (каждая делает pipeline с 20 запросами).
**Оценка:** 100 × 20 = 2000 Redis запросов одновременно. Критично?
**Решение:** Ограничить параллельные snapshots семафором.
```python
snapshot_semaphore = asyncio.Semaphore(10)  # max 10 одновременных snapshot
```
**Статус:** Добавить в snapshot_monitor.

### ✅ НАЙДЕНО: flush_lock deadlock риск
**Описание:** В алгоритме task_flusher — `async with flush_lock` используется для замены буфера.
WS tasks тоже пишут в batch_buffer. Если flush занимает > 250ms — следующий flush
будет заблокирован.
**Решение:** Использовать swap-паттерн без lock:
```python
# Атомарная замена буферов Python-атомарностью (GIL)
current_batch, batch_buffer = batch_buffer, {}
current_hist, hist_buffer = hist_buffer, []
cmd_counter = 0
# asyncio однопоточный — await не происходит во время swap
```
В asyncio нет параллельности внутри event loop — lock не нужен для swap.
**Статус:** Упростить код, убрать flush_lock.

### ✅ НАЙДЕНО: Нет обработки KeyboardInterrupt в коллекторах
**Описание:** При Ctrl+C коллектор должен дождаться flush последнего батча.
**Решение:** В main():
```python
try:
    await asyncio.gather(*tasks)
except asyncio.CancelledError:
    # Финальный flush
    await do_final_flush()
    log.info("Shutdown complete")
```
**Статус:** Добавить в алгоритм.

---

## Итерация 3: Производительность

### ✅ НАЙДЕНО: SCAN в stale_monitor блокирует Redis
**Описание:** `SCAN "md:*" count=100` — итерационный SCAN безопасен (не блокирует).
Но потом для каждого ключа делается отдельный `HGET ts` — N отдельных вызовов.
При 4000 ключах = 4000 HGET каждые 30 сек.
**Решение:** Пакетная проверка через pipeline:
```python
keys = []
async for key in redis.scan_iter("md:*", count=500):
    keys.append(key)

# Один pipeline для всех HGET
pipe = redis.pipeline(transaction=False)
for key in keys:
    pipe.hget(key, "ts")
ts_values = await pipe.execute()
```
**Статус:** Добавить в алгоритм stale_monitor.

### ✅ НАЙДЕНО: spread_monitor pipeline размер
**Описание:** При 3842 парах → 7684 HMGET в одном pipeline.
Redis pipeline лимит: нет встроенного лимита, но очень большие пайплайны
могут занять >50ms на разбор ответа Python-стороне.
**Рекомендация:** Разбить на чанки по 1000 пар если цикл > 250ms.
Добавить fallback: если elapsed > 200ms — логировать предупреждение.
**Статус:** Добавить chunked pipeline fallback в spread_monitor.

### ✅ НАЙДЕНО: aioredis connection pool size
**Описание:** Все asyncio tasks в коллекторе используют один pool.
По умолчанию redis-py pool = 10 соединений. При 7 async tasks (md_spot, md_fut,
ob_spot, ob_fut, fr, flusher, metrics) — достаточно.
**Но:** flusher и metrics создают Redis соединение при каждом вызове?
**Решение:** Передавать Redis клиент как параметр или создать один раз в INIT.
```python
redis = aioredis.Redis.from_url(REDIS_URL, max_connections=10, decode_responses=False)
```
**Статус:** Учесть в реализации — один redis клиент на коллектор.

---

## Итерация 4: Логические ошибки

### ✅ НАЙДЕНО: Spread formula — неправильное направление
**Описание:** Формула из спецификации: `(ask_spot - bid_futures) / ask_spot * 100`
Это работает когда `bid_futures > ask_spot` (фьючерс ДОРОЖЕ спота).
В этом случае: `(45000 - 45676) / 45000 * 100 = -1.50%` — отрицательный.

**Пример из спецификации:**
```
ask_spot=45000.10, bid_futures=45676.35, spread=1.5023%
```
Проверим: `(45000.10 - 45676.35) / 45000.10 * 100 = -1.5023%` — отрицательный!

**Но в примере из спецификации написано 1.5023% (позитивный).**

Значит формула в спецификации подразумевает `abs()` или порядок другой:
`(bid_futures - ask_spot) / ask_spot * 100`

**Проверка:** `(45676.35 - 45000.10) / 45000.10 * 100 = 1.5023%` ✓

**Правильная формула:** `spread = (bid_fut - ask_spot) / ask_spot * 100`
Это cash-and-carry арбитраж: покупаем spot по ask, продаём futures по bid.

**Статус:** КРИТИЧЕСКАЯ ОШИБКА В ИСХОДНОЙ СПЕЦИФИКАЦИИ. Исправить везде.

### ✅ НАЙДЕНО: Cooldown ключ — direction specific
**Описание:** Спецификация: "не повторяет его в течение 3600 секунд, по конкретному символу конкретного направления".
Ключ cooldown: `spread:cooldown:{spot_exch}:{fut_exch}:{symbol}` — это уже учитывает direction
(spot_exch и fut_exch определяют direction).
**Статус:** Правильно, дополнительных изменений не нужно.

### ✅ НАЙДЕНО: hist:config обновляется из нескольких коллекторов
**Описание:** Пять коллекторов все обновляют `hist:config`. Race condition?
**Оценка:** HSET atomic. При смене чанка все 5 коллекторов запишут одинаковое значение
(chunk_id вычисляется из time.time() — синхронизировано).
**Статус:** Нет проблемы, но можно опустить hist:config — он нужен только snapshot_monitor.
Snapshot_monitor может сам вычислить chunk_id из signal_ts.

### ✅ НАЙДЕНО: Snapshot load_history — неэффективный timeline merge
**Описание:** При ~10,000 MD строк (1 час по тику) + ~120 OB строк (1 час по 30 сек)
строим timeline[ts] = dict. MD и OB почти никогда не совпадают по ts.
Результат: большинство строк будут либо только MD (без OB) либо только OB (без MD).
**Решение (forward-fill):** Уже описан в алгоритме — правильно.
Но нужно также проверить: зачем OB в timeline если MD уже имеет свой ts?
Snapshot CSV содержит ОБА: md данные и ob данные в одной строке.
Для строк с MD: ob берём из forward-fill последнего OB.
**Статус:** Алгоритм правильный, но требует тщательного тестирования.

### ✅ НАЙДЕНО: Snapshot CSV — спред для исторических строк
**Описание:** CSV header содержит `spread_pct` колонку.
Для реалтайм строк — вычисляем спред на лету.
Для исторических строк — нужно вычислить ретроспективно.
**Решение:**
```python
if ask_spot and bid_fut:
    spread = (float(bid_fut) - float(ask_spot)) / float(ask_spot) * 100
    spread_str = f"{spread:.4f}"
else:
    spread_str = ""  # нет данных
```
**Статус:** Добавить в build_csv_row.

---

## Итерация 5: Граничные случаи

### ✅ НАЙДЕНО: Пустые combination файлы
**Описание:** Если combination файл пустой (нет пересечений) — spread_monitor должен его пропустить.
**Решение:** При загрузке комбинаций: `if not lines: continue`
**Статус:** Добавить проверку.

### ✅ НАЙДЕНО: Subscribe файлы не существуют при старте
**Описание:** Если коллектор запускается до dictionaries/main.py — subscribe файлов нет.
**Решение:** В launcher.py проверять существование subscribe файлов.
В коллекторе: если файл пустой — логировать WARNING и завершить с кодом 1.
**Статус:** Добавить в launcher.

### ✅ НАЙДЕНО: OKX funding-rate channel — нет snapshot при подписке
**Описание:** OKX funding-rate channel не присылает snapshot при подписке.
Первые данные придут только при изменении ставки (каждые 8 часов).
**Решение:** REST запрос при старте для инициализации FR значений. Описано в Q6.
**Реализация:**
```python
async def init_fr_from_rest(redis, fut_symbols_native, norm_map):
    """Загружает текущие FR для всех symbols через REST API OKX."""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        for native in fut_symbols_native:
            url = f"https://www.okx.com/api/v5/public/funding-rate?instId={native}"
            async with session.get(url) as resp:
                data = await resp.json()
                if data["code"] == "0":
                    d = data["data"][0]
                    symbol = norm_map[native]
                    pipe = redis.pipeline()
                    pipe.hset(f"fr:okx:futures:{symbol}", mapping={
                        "fr": d["fundingRate"],
                        "fr_ts": d["nextFundingTime"]
                    })
                    await pipe.execute()
                await asyncio.sleep(0.1)  # rate limit
```
**Статус:** Добавить в collector_okx init.

### ✅ НАЙДЕНО: Binance FR — первое значение
**Аналогично OKX:** Binance `!markPrice@arr@1s` — приходит каждую секунду.
**Статус:** Нет проблемы — markPrice приходит часто.

### ✅ НАЙДЕНО: Очень большой snapshot файл
**Описание:** Исторические строки (~10,000) + реалтайм (~11,667) = ~21,667 строк × 90+ полей.
Каждая строка примерно 500 байт → 10+ MB на файл. Нормально.
**Статус:** OK.

### ✅ НАЙДЕНО: Закрытие aiofiles в snapshot при ошибке
**Описание:** Если при записи snapshot возникнет исключение —
`async with aiofiles.open(...)` закроет файл автоматически.
Но следующие итерации (append) — файл открывается заново при каждой строке.
**Рекомендация:** Открыть файл один раз на всё время snapshot:
```python
async with aiofiles.open(fname, "w", newline="") as f:
    await f.write(header + "\n")
    for hist_row in history_rows:
        await f.write(hist_row + "\n")

    t_start = time.monotonic()
    while (time.monotonic() - t_start) < SNAPSHOT_DURATION:
        row = await read_current_snapshot_row(...)
        await f.write(row + "\n")
        await f.flush()  # убедиться что данные на диске
        await asyncio.sleep(SNAPSHOT_INTERVAL_MS / 1000)
```
**Статус:** Исправить алгоритм snapshot.

---

## Итерация 6: Документация и структура

### ✅ НАЙДЕНО: Нет requirements.txt
**Зависимости системы:**
```
websockets>=12.0
redis[hiredis]>=5.0    # redis-py с hiredis ускорителем
aiofiles>=23.0
aiohttp>=3.9           # для REST запросов OKX FR init
```
**Статус:** Создать requirements.txt.

### ✅ НАЙДЕНО: Нет обработки сигнала SIGTERM в launcher на Linux
**Описание:** Launcher должен корректно завершать дочерние процессы при остановке.
**Решение:**
```python
import signal
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)
```
**Статус:** Учтено в алгоритме launcher.

### ✅ НАЙДЕНО: Нет проверки версии Python
**Решение:** В launcher.py:
```python
import sys
if sys.version_info < (3, 10):
    sys.exit("Python 3.10+ required")
```
**Статус:** Добавить.

### ✅ НАЙДЕНО: Нет .gitignore для logs/ и signal/
**Текущий .gitignore:** не содержит logs/ и signal/
**Решение:** Добавить в .gitignore:
```
logs/
signal/signal.csv
signal/snapshot/
```
**Статус:** Обновить .gitignore.

---

## Итерация 7: Безопасность данных

### ✅ НАЙДЕНО: Redis без аутентификации
**Описание:** Конфиг предполагает Redis без пароля на localhost.
**Рекомендация:** Добавить `REDIS_PASSWORD = ""` в config, поддержать `redis://:password@host:port/0`
**Статус:** Добавить параметр в config (опционально).

### ✅ НАЙДЕНО: Нет валидации числовых данных из WS
**Описание:** Если биржа прислала "NaN", "inf" или пустую строку в bid/ask —
float("NaN") в Python валиден, но `(NaN - NaN) / NaN` = NaN, что не > threshold.
Нужна валидация:
```python
def safe_float(s: str) -> float | None:
    try:
        v = float(s)
        if v <= 0 or v != v:  # <= 0 или NaN
            return None
        return v
    except (ValueError, TypeError):
        return None
```
**Статус:** Добавить в spread_monitor и snapshot_monitor.

---

## Итоговый чеклист изменений к плану

| # | Изменение | Файл |
|---|-----------|------|
| 1 | Spread formula: `(bid_fut - ask_spot) / ask_spot * 100` | algorithms, config |
| 2 | OKX + Bitget OB: 5 levels (books5) | exchange_adapters |
| 3 | Bybit OB: delta management (LocalOrderBook) | algorithms |
| 4 | Gate.io FR: умножать fr_ts на 1000 (сек→мс) | exchange_adapters |
| 5 | Binance FR: один поток `!markPrice@arr@1s` | exchange_adapters |
| 6 | OKX FR: REST init при старте + зависимость aiohttp | algorithms, config |
| 7 | Убрать flush_lock, использовать asyncio swap | algorithms |
| 8 | Snapshot semaphore (max 10 параллельных) | algorithms |
| 9 | Stale monitor: batch pipeline HGET | algorithms |
| 10 | Snapshot: держать файл открытым всё время | algorithms |
| 11 | Snapshot: вычислять spread для исторических строк | algorithms |
| 12 | hist:config — необязателен (snapshot вычисляет chunk сам) | redis_schema |
| 13 | safe_float() валидация в spread и snapshot | algorithms |
| 14 | .gitignore: добавить logs/, signal/ | .gitignore |
| 15 | requirements.txt с зависимостями | requirements.txt |
| 16 | Python version check в launcher | launcher |
| 17 | История: частота = частота WS данных, без downsampling | config, algorithms |
| 18 | chunk_id из ts сообщения (не time.time()) — единый принцип | algorithms |
| 19 | EXPIRE один раз на ключ/чанк через expire_set (убрал 240М EXPIRE/сек) | algorithms |
| 20 | Добавлен FR buffer writer (отсутствовал в алгоритмах) | algorithms |
| 21 | Убран flush_lock (asyncio однопоточный, swap атомарен) | algorithms |
| 22 | METRICS логирует hist_writes/s и expire_set_size | algorithms |
| 18 | subscribe_loop в snapshot: graceful handling | algorithms |
