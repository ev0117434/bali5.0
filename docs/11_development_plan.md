# BALI 5.0 — Development Plan

Подход: **Вертикальный срез** — Binance эталон с полными тестами → остальные 4 биржи по образцу → мониторы → snapshot → ветки.

Весь код пишется в ветке `data`. Ветка `trade` создаётся в конце из `data` двумя изменениями.

---

## Обзор фаз

| Фаза | Что делаем | Git push | Результат |
|------|-----------|---------|---------|
| 0 | Инфраструктура | ✅ в data | config, logger, requirements |
| 1 | Binance эталон | ✅ в data | MD+OB+FR работают, Redis заполнен |
| 2 | 4 биржи по образцу | ✅ в data | Все коллекторы работают |
| 3 | Мониторы (redis, stale, spread) | ✅ в data | Сигналы в signal.csv |
| 4 | Snapshot monitor | ✅ в data | Снапшоты после сигналов |
| 5 | Launcher + интеграция | ✅ в data | Вся система одной командой |
| 6 | Ветка trade | ✅ в trade | Обе ветки в GitHub |

---

## ФАЗА 0 — Инфраструктура

### Цель
Создать фундамент который используют все остальные модули.
Никакой бизнес-логики — только shared utilities.

### Файлы для написания

```
requirements.txt
config.py
logger_setup.py
```

### Детали

**requirements.txt**
```
websockets>=12.0
redis[hiredis]>=5.0
aiofiles>=23.0
aiohttp>=3.9
```

**config.py** — все константы из `docs/06_config.md`
Ключевые: REDIS_URL, BATCH_FLUSH_INTERVAL_MS=250, BATCH_MAX_COMMANDS=100,
CHUNK_DURATION=1200, CHUNK_TTL=6000, HISTORY_ENABLED=True,
SPREAD_THRESHOLD=1.00, COOLDOWN_SECONDS=3600, SNAPSHOT_DURATION=3500

**logger_setup.py** — функция `setup_logger(name)` из `docs/04_logging.md`
RotatingFileHandler: 10MB, 5 backups, формат `[ts.ms] [LEVEL] [name] msg`

### Тесты

**Smoke (test_infra.py):**
```python
def test_config_imports():
    from config import REDIS_URL, HISTORY_ENABLED, SPREAD_THRESHOLD
    assert SPREAD_THRESHOLD == 1.00
    assert HISTORY_ENABLED is True

def test_logger_creates_file():
    from logger_setup import setup_logger
    log = setup_logger("test_infra")
    log.info("test message")
    assert Path("logs/test_infra.log").exists()

def test_redis_connection():
    import redis
    r = redis.Redis.from_url("redis://localhost:6379")
    assert r.ping()
```

**Запуск:**
```bash
pip install -r requirements.txt
pytest tests/test_infra.py -v
```

### Критерий готовности
- [ ] `pytest tests/test_infra.py` — всё зелёное
- [ ] `logs/` директория создаётся
- [ ] Redis пингуется

### Git
```bash
git checkout main
git checkout -b data
git add config.py logger_setup.py requirements.txt
git commit -m "phase-0: infrastructure — config, logger, requirements"
git push -u origin data
```

### Ссылки
- `docs/06_config.md` — все константы
- `docs/04_logging.md` — logger setup

---

## ФАЗА 1 — Binance Эталон (collector_binance.py)

### Цель
Доказать что вся архитектура работает на одной бирже.
Binance → 5 WS потоков → Redis primary keys → Redis history keys.
Здесь пишутся самые полные тесты — они станут образцом для других бирж.

### Файлы для написания

```
collectors/
└── collector_binance.py

tests/
├── unit/
│   ├── test_binance_parsers.py      ← unit тесты парсеров
│   └── test_chunk_logic.py          ← unit тесты chunk ID и форматов строк
└── smoke/
    └── test_binance_smoke.py        ← smoke: Redis заполнен после 15 сек
```

### Структура collector_binance.py
По алгоритму из `docs/03_algorithms.md`:
- `INIT`: загрузка символов, redis pool, batch_buffer, hist_buffer, expire_set
- `task_md_spot()` / `task_md_fut()` — WS bookTicker
- `task_ob_spot()` / `task_ob_fut()` — WS depth10@100ms
- `task_fr()` — WS !markPrice@arr@1s (один поток на все символы)
- `task_flusher()` — pipeline каждые 250мс или 100 команд
- `task_metrics()` — лог каждые 5 сек

### Unit тесты (test_binance_parsers.py)

```python
# Тест парсера MD
def test_parse_md_bookTicker():
    raw = json.dumps({
        "stream": "btcusdt@bookTicker",
        "data": {"s": "BTCUSDT", "b": "34000.10", "a": "34001.50"}
    })
    symbol, bid, ask, ts = parse_md(raw)
    assert symbol == "BTCUSDT"
    assert bid == "34000.10"
    assert ask == "34001.50"
    assert isinstance(ts, int) and ts > 0

# Тест парсера OB
def test_parse_ob_depth10():
    raw = json.dumps({
        "stream": "btcusdt@depth10@100ms",
        "data": {
            "bids": [["34000.10", "10.50"]] * 10,
            "asks": [["34001.50", "8.30"]] * 10
        }
    })
    symbol, bids, asks, ts = parse_ob(raw)
    assert symbol == "BTCUSDT"
    assert len(bids) == 10
    assert bids[0] == ["34000.10", "10.50"]

# Тест парсера FR (!markPrice@arr@1s)
def test_parse_fr_markprice_arr():
    raw = json.dumps({
        "stream": "!markPrice@arr@1s",
        "data": [{"s": "BTCUSDT", "r": "0.00010000", "T": 1705320000000}]
    })
    results = parse_fr(raw)
    assert ("BTCUSDT", "0.00010000", "1705320000000") in results

# Тест что парсер возвращает None на мусор
def test_parse_md_invalid():
    assert parse_md("{}") is None
    assert parse_md('{"stream": "ping"}') is None

# Тест отрицательного bid/ask
def test_parse_md_zero_price():
    raw = json.dumps({"data": {"s": "BTCUSDT", "b": "0", "a": "34001.50"}})
    result = parse_md(raw)
    assert result is None  # нулевой bid = невалидный
```

**test_chunk_logic.py:**
```python
def test_chunk_id_from_ts():
    ts_ms = 1705312345000
    chunk = int(ts_ms / 1000 / 1200)
    assert chunk == 1421093

def test_chunk_id_consistent_within_window():
    # Два ts в одном 20-минутном окне → один chunk_id
    ts1 = 1705311600000   # начало окна
    ts2 = 1705312799000   # конец окна (ts1 + 1199 сек)
    assert int(ts1/1000/1200) == int(ts2/1000/1200)

def test_chunk_id_changes_at_boundary():
    ts_before = 1705311600000 - 1000  # 1 сек до границы
    ts_after  = 1705311600000         # граница
    assert int(ts_before/1000/1200) != int(ts_after/1000/1200)

def test_ob_hist_line_format():
    bids = [["34000.10", "10.50"]] * 10
    asks = [["34001.50", "8.30"]] * 10
    ts = 1705312345000
    line = build_ob_hist_line(bids, asks, ts)
    parts = line.split(",")
    assert len(parts) == 41   # 20 bid fields + 20 ask fields + ts
    assert parts[-1] == str(ts)

def test_md_hist_line_format():
    line = build_md_hist_line("34000.10", "34001.50", 1705312345000)
    assert line == "34000.10,34001.50,1705312345000"
```

### Smoke тест (test_binance_smoke.py)

```python
"""
Запускает collector_binance на 15 секунд.
Проверяет что Redis заполнен нужными ключами.
Требует: Redis запущен, подписки в dictionaries/subscribe/binance/
"""
import subprocess, time, redis, pytest

WAIT_SEC = 15

@pytest.fixture(scope="module")
def redis_client():
    r = redis.Redis(decode_responses=True)
    # Очистить тестовые ключи
    for key in r.scan_iter("md:binance:*"):
        r.delete(key)
    yield r

@pytest.fixture(scope="module", autouse=True)
def run_collector(redis_client):
    proc = subprocess.Popen(["python", "collectors/collector_binance.py"])
    time.sleep(WAIT_SEC)
    yield
    proc.terminate()
    proc.wait()

def test_md_spot_keys_populated(redis_client):
    keys = list(redis_client.scan_iter("md:binance:spot:*"))
    assert len(keys) > 100, f"Expected >100 md:spot keys, got {len(keys)}"

def test_md_futures_keys_populated(redis_client):
    keys = list(redis_client.scan_iter("md:binance:futures:*"))
    assert len(keys) > 50

def test_md_key_has_all_fields(redis_client):
    keys = list(redis_client.scan_iter("md:binance:spot:BTC*"))
    assert keys, "No BTCUSDT-like md keys found"
    data = redis_client.hgetall(keys[0])
    assert "b" in data and "a" in data and "ts" in data
    assert float(data["b"]) > 0
    assert float(data["a"]) > float(data["b"])   # ask > bid

def test_ob_spot_keys_populated(redis_client):
    keys = list(redis_client.scan_iter("ob:binance:spot:*"))
    assert len(keys) > 50

def test_ob_key_has_10_levels(redis_client):
    keys = list(redis_client.scan_iter("ob:binance:spot:BTC*"))
    data = redis_client.hgetall(keys[0])
    for i in range(1, 11):
        assert f"b{i}" in data, f"Missing b{i}"
        assert f"a{i}" in data, f"Missing a{i}"

def test_fr_futures_keys_populated(redis_client):
    keys = list(redis_client.scan_iter("fr:binance:futures:*"))
    assert len(keys) > 20

def test_fr_key_has_rate(redis_client):
    keys = list(redis_client.scan_iter("fr:binance:futures:BTC*"))
    data = redis_client.hgetall(keys[0])
    assert "fr" in data and "fr_ts" in data

def test_hist_md_keys_exist_when_enabled(redis_client):
    from config import HISTORY_ENABLED
    if not HISTORY_ENABLED:
        pytest.skip("HISTORY_ENABLED=False")
    keys = list(redis_client.scan_iter("md:hist:binance:spot:*"))
    assert len(keys) > 50, "No history keys found"

def test_hist_md_key_has_entries(redis_client):
    from config import HISTORY_ENABLED
    if not HISTORY_ENABLED:
        pytest.skip("HISTORY_ENABLED=False")
    keys = list(redis_client.scan_iter("md:hist:binance:spot:BTC*"))
    length = redis_client.llen(keys[0])
    assert length > 10, f"Expected >10 hist entries, got {length}"

def test_md_ts_is_fresh(redis_client):
    """Данные не старше 5 секунд"""
    import time
    keys = list(redis_client.scan_iter("md:binance:spot:BTC*"))
    ts = int(redis_client.hget(keys[0], "ts"))
    age_ms = int(time.time() * 1000) - ts
    assert age_ms < 5000, f"Data stale: age={age_ms}ms"
```

### Ручная проверка (дополнительно)
```bash
# Запустить коллектор
python collectors/collector_binance.py

# В другом терминале:
redis-cli hgetall "md:binance:spot:BTCUSDT"
redis-cli hgetall "ob:binance:spot:BTCUSDT"
redis-cli hgetall "fr:binance:futures:BTCUSDT"
redis-cli llen "md:hist:binance:spot:BTCUSDT:$(python3 -c 'import time; print(int(time.time()/1200))')"
tail -f logs/collector_binance.log | grep METRICS
```

### Критерий готовности
- [ ] `pytest tests/unit/test_binance_parsers.py -v` — всё зелёное
- [ ] `pytest tests/unit/test_chunk_logic.py -v` — всё зелёное
- [ ] `pytest tests/smoke/test_binance_smoke.py -v` — всё зелёное
- [ ] METRICS лог показывает `md>500msg/s`, `flush_lat avg<10ms`
- [ ] `ob:binance:spot:BTCUSDT` содержит все 10 уровней
- [ ] `md:hist:binance:spot:BTCUSDT:{chunk}` растёт (llen увеличивается)

### Git
```bash
git add collectors/collector_binance.py tests/
git commit -m "phase-1: Binance collector (MD+OB+FR) with full test suite"
git push origin data
```

### Ссылки
- `docs/02_exchange_adapters.md` → секция Binance (endpoints, форматы, парсеры)
- `docs/03_algorithms.md` → BUFFER WRITER для MD/OB/FR, FLUSHER TASK
- `docs/01_redis_schema.md` → форматы ключей и полей

---

## ФАЗА 2 — Остальные 4 биржи

### Цель
По образцу Binance написать коллекторы для Bybit, OKX, Gate.io, Bitget.
Каждая биржа — отдельный коммит. Unit тесты только на специфику биржи (парсеры).
Smoke тест — общий шаблон из Фазы 1, меняется только префикс ключей.

### Файлы для написания

```
collectors/
├── collector_bybit.py
├── collector_okx.py
├── collector_gate.py
└── collector_bitget.py

tests/unit/
├── test_bybit_parsers.py
├── test_okx_parsers.py
├── test_gate_parsers.py
└── test_bitget_parsers.py

tests/smoke/
├── test_bybit_smoke.py
├── test_okx_smoke.py
├── test_gate_smoke.py
└── test_bitget_smoke.py
```

### Специфика каждой биржи

**Bybit** (`docs/02_exchange_adapters.md` → секция Bybit):
- MD и FR в одном `tickers.*` потоке → парсить оба из одного сообщения
- OB: `type: "delta"` требует LocalOrderBook (кеш + apply delta)
- Heartbeat: json `{"op":"ping"}` каждые 20 сек
- Chunk по 10 символов на subscribe

**OKX** (`docs/02_exchange_adapters.md` → секция OKX):
- Символы нативные: `BTC-USDT`, `BTC-USDT-SWAP` → normalize → BTCUSDT
- MD: канал `bbo-tbt`
- OB: канал `books5` → 5 уровней, поля b6-b10/a6-a10 = пустые строки ""
- FR: REST init при старте + WS `funding-rate` channel
- Heartbeat: строка `"ping"` каждые 25 сек

**Gate.io** (`docs/02_exchange_adapters.md` → секция Gate.io):
- Символы: `BTC_USDT` → normalize → BTCUSDT
- Spot и Futures — разные WS URL
- FR: `funding_next_apply` в секундах → умножить на 1000

**Bitget** (`docs/02_exchange_adapters.md` → секция Bitget):
- OB: канал `books` (не `books5`) → [:10] в парсере
- FR через `ticker` futures
- Heartbeat: строка `"ping"` каждые 25 сек

### Unit тесты (только специфика биржи)

```python
# test_bybit_parsers.py
def test_bybit_parse_md_and_fr_from_same_ticker():
    """Bybit: один ticker содержит и MD и FR"""
    raw = {"topic": "tickers.BTCUSDT", "data": {
        "bid1Price": "34000", "ask1Price": "34001",
        "fundingRate": "0.0001", "nextFundingTime": "1705320000000"
    }, "ts": 1705312345000}
    md = parse_md(raw)
    fr = parse_fr_from_ticker(raw)
    assert md == ("BTCUSDT", "34000", "34001", 1705312345000)
    assert fr == ("BTCUSDT", "0.0001", "1705320000000")

def test_bybit_ob_delta_apply():
    """Bybit OB delta: qty=0 означает удаление уровня"""
    ob = LocalOrderBook()
    ob.apply_snapshot("BTCUSDT", [["34000","10"],["33999","5"]], [["34001","8"]])
    ob.apply_delta("BTCUSDT", [["34000","0"]], [])  # удалить bid 34000
    bids = ob.get_bids("BTCUSDT")
    assert all(p != "34000" for p, _ in bids)

# test_okx_parsers.py
def test_okx_normalize_spot():
    assert normalize("BTC-USDT") == "BTCUSDT"

def test_okx_normalize_futures():
    assert normalize("BTC-USDT-SWAP") == "BTCUSDT"

def test_okx_ob_books5_fills_5_levels():
    """OKX books5 → только 5 уровней, остальные пустые"""
    raw = {"arg": {"channel": "books5", "instId": "BTC-USDT"},
           "data": [{"bids": [["34000","10","0","1"]]*5,
                     "asks": [["34001","8","0","1"]]*5, "ts": "123"}]}
    symbol, bids, asks, ts = parse_ob(raw)
    assert len(bids) == 5
    assert len(asks) == 5

# test_gate_parsers.py
def test_gate_normalize():
    assert normalize("BTC_USDT") == "BTCUSDT"

def test_gate_fr_ts_converted_to_ms():
    """Gate.io funding_next_apply в секундах → нужно *1000"""
    raw = {"channel": "futures.tickers", "event": "update",
           "result": [{"contract": "BTC_USDT",
                        "funding_rate": "0.0001",
                        "funding_next_apply": 1705320000}]}  # секунды!
    symbol, rate, fr_ts = parse_fr(raw)
    assert fr_ts == "1705320000000"  # миллисекунды
```

### Smoke тест (шаблон — меняется только exchange name)
```python
# test_bybit_smoke.py  (аналогично binance, меняем префикс "bybit")
def test_md_spot_keys_populated(redis_client):
    keys = list(redis_client.scan_iter("md:bybit:spot:*"))
    assert len(keys) > 50
# ... и т.д.
```

### Порядок написания бирж
1. **Bybit** — близко к Binance, главная сложность: delta OB
2. **Gate.io** — две WS URL (spot/futures), FR в секундах
3. **Bitget** — самая простая (нативный формат = нормализованный)
4. **OKX** — самая сложная (native map, REST FR init, books5)

### Критерий готовности (для каждой биржи)
- [ ] `pytest tests/unit/test_{exchange}_parsers.py -v` — зелёный
- [ ] `pytest tests/smoke/test_{exchange}_smoke.py -v` — зелёный
- [ ] METRICS лог: нет reconnects в первые 5 минут
- [ ] `ob:{exchange}:spot:BTCUSDT` присутствует в Redis

### Git (коммит на каждую биржу)
```bash
# Bybit
git add collectors/collector_bybit.py tests/unit/test_bybit_parsers.py tests/smoke/test_bybit_smoke.py
git commit -m "phase-2a: Bybit collector (MD+OB delta+FR)"
git push origin data

# OKX
git commit -m "phase-2b: OKX collector (books5, native map, REST FR init)"
# Gate.io
git commit -m "phase-2c: Gate.io collector (spot+futures WS, FR seconds→ms)"
# Bitget
git commit -m "phase-2d: Bitget collector (books→10levels, FR via ticker)"
```

### Ссылки
- `docs/02_exchange_adapters.md` → секция каждой биржи
- `docs/07_open_questions.md` → Q1(OKX OB), Q3(Bybit delta), Q4(Gate format), Q13(Binance FR)
- `docs/03_algorithms.md` → BUFFER WRITER шаблоны

---

## ФАЗА 3 — Мониторы (redis, stale, spread)

### Цель
Первые сигналы в `signal/signal.csv`.
Подтвердить что спред-логика правильная и cooldown работает.

### Файлы для написания

```
monitors/
├── redis_monitor.py
├── stale_monitor.py
└── spread_monitor.py

tests/unit/
├── test_spread_logic.py
└── test_stale_logic.py

tests/smoke/
└── test_spread_smoke.py
```

### Unit тесты (test_spread_logic.py)

```python
def test_spread_formula_correct():
    """Правильная формула: (bid_fut - ask_spot) / ask_spot * 100"""
    ask_spot = 45000.10
    bid_fut  = 45676.35
    spread = (bid_fut - ask_spot) / ask_spot * 100
    assert abs(spread - 1.5023) < 0.001

def test_spread_below_threshold_no_signal():
    ask_spot = 45000.0
    bid_fut  = 45400.0   # spread = 0.888%
    spread = (bid_fut - ask_spot) / ask_spot * 100
    assert spread < 1.00   # ниже порога

def test_spread_above_threshold_is_signal():
    ask_spot = 45000.0
    bid_fut  = 45600.0   # spread = 1.333%
    spread = (bid_fut - ask_spot) / ask_spot * 100
    assert spread >= 1.00

def test_spread_negative_ignored():
    """bid_fut < ask_spot → отрицательный спред → игнорируем"""
    ask_spot = 45000.0
    bid_fut  = 44900.0
    spread = (bid_fut - ask_spot) / ask_spot * 100
    assert spread < 0  # отрицательный, не пройдёт порог

def test_signal_csv_format():
    """Строка сигнала соответствует формату"""
    line = build_signal_line("binance","bybit","BTCUSDT",45000.10,45676.35,1.5023,1741234567890)
    parts = line.split(",")
    assert len(parts) == 7
    assert parts[0] == "binance"
    assert parts[2] == "BTCUSDT"
    assert float(parts[5]) == 1.5023

def test_cooldown_key_format():
    key = build_cooldown_key("binance","bybit","BTCUSDT")
    assert key == "spread:cooldown:binance:bybit:BTCUSDT"

def test_stale_data_skipped():
    """Данные старше SPREAD_DATA_STALE_MS пропускаются"""
    import time
    old_ts = int((time.time() - 10) * 1000)  # 10 секунд назад
    assert is_stale(old_ts, stale_ms=5000) is True

def test_fresh_data_not_stale():
    import time
    fresh_ts = int(time.time() * 1000)
    assert is_stale(fresh_ts, stale_ms=5000) is False
```

### Smoke тест (test_spread_smoke.py)

```python
"""
Требует: все 5 коллекторов запущены и Redis заполнен.
Проверяет что spread_monitor делает циклы без ошибок.
"""
def test_spread_monitor_runs_cycles():
    proc = subprocess.Popen(["python", "monitors/spread_monitor.py"])
    time.sleep(10)
    proc.terminate()
    # Проверить лог
    log = Path("logs/spread_monitor.log").read_text()
    assert "Cycle:" in log
    assert "ERROR" not in log

def test_spread_monitor_loads_all_combinations():
    log = Path("logs/spread_monitor.log").read_text()
    # При старте логируется количество пар
    import re
    match = re.search(r"pairs=(\d+)", log)
    assert match and int(match.group(1)) > 100

def test_signal_csv_header_exists():
    time.sleep(2)  # дать время на создание файла
    csv = Path("signal/signal.csv")
    assert csv.exists()
    header = csv.read_text().split("\n")[0]
    assert "spot_exch" in header and "spread_pct" in header

def test_cooldown_in_redis_after_signal():
    """Если сигнал был — cooldown ключ должен существовать"""
    import redis
    r = redis.Redis(decode_responses=True)
    cooldown_keys = list(r.scan_iter("spread:cooldown:*"))
    # Может быть 0 если спреда нет — это нормально, проверяем механизм отдельно
    # Проверяем что TTL ключей если они есть — правильный
    for key in cooldown_keys[:5]:
        ttl = r.ttl(key)
        assert 0 < ttl <= 3600
```

### Ручная проверка spread_monitor
```bash
python monitors/spread_monitor.py &
tail -f logs/spread_monitor.log
# Ищем строки: Cycle: Xms | pairs=YYYY
# Ищем строки: SIGNAL | ...
cat signal/signal.csv
```

### Критерий готовности
- [ ] `pytest tests/unit/test_spread_logic.py -v` — зелёный
- [ ] Цикл spread_monitor < 250ms при 3000+ парах
- [ ] При ручной проверке: если спред есть → строка в signal.csv
- [ ] Cooldown ключ в Redis с TTL=3600 после сигнала
- [ ] `redis_monitor.log` показывает здоровье Redis каждые 30 сек
- [ ] `stale_monitor.log` показывает scan каждые 30 сек

### Git
```bash
git add monitors/redis_monitor.py monitors/stale_monitor.py monitors/spread_monitor.py tests/
git commit -m "phase-3: monitors — redis health, stale detection, spread signals"
git push origin data
```

### Ссылки
- `docs/03_algorithms.md` → spread_monitor LOOP, HANDLE SIGNAL
- `docs/08_review_iterations.md` → итерация 4 (spread formula fix)
- `docs/01_redis_schema.md` → cooldown keys, ch:signals

---

## ФАЗА 4 — Snapshot Monitor

### Цель
После сигнала создавать CSV с OB данными и историей за 1 час.
Только ветка `data`. Требует HISTORY_ENABLED=True и заполненной истории.

### Файлы для написания

```
monitors/
└── snapshot_monitor.py

tests/unit/
├── test_snapshot_csv.py
└── test_history_reader.py

tests/smoke/
└── test_snapshot_smoke.py
```

### Unit тесты (test_snapshot_csv.py)

```python
def test_snapshot_header_column_count():
    """CSV header должен иметь ровно 90 колонок"""
    from config import SNAPSHOT_CSV_HEADER
    cols = SNAPSHOT_CSV_HEADER.split(",")
    assert len(cols) == 90, f"Expected 90, got {len(cols)}"

def test_build_csv_row_correct_columns():
    """build_csv_row возвращает строку с 90 полями"""
    row = build_csv_row(
        spot_exch="binance", fut_exch="bybit", symbol="BTCUSDT",
        ask_spot="45000.10", bid_fut="45676.35", spread_pct="1.5023",
        ts=1705312345000, fr="0.0001", fr_ts="1705320000000",
        ob_spot=["34000","10"]*20,   # 40 значений
        ob_fut=["45000","10"]*20,
    )
    assert len(row.split(",")) == 90

def test_spread_calculated_for_hist_row():
    """Для исторических строк spread вычисляется ретроспективно"""
    ask = "45000.10"
    bid = "45676.35"
    spread = calc_spread(ask, bid)
    assert abs(spread - 1.5023) < 0.001

def test_forward_fill_ob():
    """OB forward-fill: если OB нет в строке — берём последний известный"""
    last_ob = ["34000","10"] * 20
    row = build_csv_row(..., ob_spot=None)
    # ob берётся из last_ob через forward-fill
    assert "34000" in row
```

**test_history_reader.py:**
```python
@pytest.mark.asyncio
async def test_load_history_returns_sorted_rows(redis_with_hist_data):
    """Исторические строки отсортированы по ts ascending"""
    rows = await load_history("binance","bybit","BTCUSDT", signal_ts_ms=NOW_MS)
    timestamps = [int(r.split(",")[6]) for r in rows]
    assert timestamps == sorted(timestamps)

@pytest.mark.asyncio
async def test_load_history_filters_by_window(redis_with_hist_data):
    """Только данные за последний час до сигнала"""
    signal_ts = NOW_MS
    rows = await load_history("binance","bybit","BTCUSDT", signal_ts)
    for row in rows:
        ts = int(row.split(",")[6])
        assert signal_ts - 3_600_000 <= ts <= signal_ts

@pytest.mark.asyncio
async def test_load_history_single_pipeline(redis_with_hist_data, mocker):
    """Все данные читаются одним Redis pipeline"""
    mock_pipe = mocker.patch("redis.asyncio.Redis.pipeline")
    await load_history("binance","bybit","BTCUSDT", NOW_MS)
    # pipeline.execute() вызван ровно один раз
    mock_pipe.return_value.__aenter__.return_value.execute.assert_called_once()
```

### Smoke тест (test_snapshot_smoke.py)

```python
def test_snapshot_created_after_signal(redis_client, tmp_signal):
    """
    Публикуем тестовый сигнал в ch:signals,
    ждём 30 сек, проверяем что файл создан и растёт.
    """
    proc = subprocess.Popen(["python", "monitors/snapshot_monitor.py"])
    time.sleep(3)   # дать время на подписку

    r = redis.Redis()
    signal = "binance,bybit,BTCUSDT,45000.10,45676.35,1.5023,1705312345890"
    r.publish("ch:signals", signal)

    time.sleep(10)  # дать время на создание файла и несколько строк

    snapshots = list(Path("signal/snapshot").glob("*.csv"))
    assert snapshots, "No snapshot files created"

    csv_file = snapshots[0]
    lines = csv_file.read_text().splitlines()
    assert lines[0].startswith("spot_exch")   # header
    assert len(lines) > 5                      # несколько строк данных

    time.sleep(5)
    lines_after = csv_file.read_text().splitlines()
    assert len(lines_after) > len(lines)       # файл продолжает расти

    proc.terminate()
```

### Критерий готовности
- [ ] `pytest tests/unit/test_snapshot_csv.py -v` — зелёный
- [ ] `pytest tests/unit/test_history_reader.py -v` — зелёный
- [ ] `pytest tests/smoke/test_snapshot_smoke.py -v` — зелёный
- [ ] После ручного `redis-cli publish ch:signals "..."` — файл создаётся
- [ ] Файл содержит исторические строки + реалтайм строки
- [ ] Через 3500 сек файл закрывается (лог: "Snapshot done")

### Git
```bash
git add monitors/snapshot_monitor.py tests/
git commit -m "phase-4: snapshot_monitor — 1h history + realtime OB recording"
git push origin data
```

### Ссылки
- `docs/03_algorithms.md` → RUN SNAPSHOT, LOAD HISTORY, READ CURRENT SNAPSHOT ROW
- `docs/05_history_chunks.md` → алгоритм чтения истории, forward-fill
- `docs/01_redis_schema.md` → форматы hist строк (MD=3 поля, OB=41, FR=3)

---

## ФАЗА 5 — Launcher + Интеграционный тест

### Цель
Вся система запускается одной командой.
Интеграционный тест: от старта до сигнала.

### Файлы для написания

```
launcher.py

tests/integration/
└── test_full_system.py
```

### Интеграционный тест (test_full_system.py)

```python
"""
Полный интеграционный тест: запускает всю систему через launcher,
ждёт 60 сек, проверяет состояние Redis и логов.
Медленный тест — запускать отдельно: pytest tests/integration/ -v -s
"""
@pytest.fixture(scope="module")
def system(tmp_path):
    proc = subprocess.Popen(["python", "launcher.py"])
    time.sleep(60)   # дать системе прогреться
    yield proc
    proc.terminate()

def test_all_collectors_running(system):
    """Все 5 коллекторов живы"""
    import psutil
    scripts = ["collector_binance","collector_bybit","collector_okx",
               "collector_gate","collector_bitget"]
    running = [p.cmdline() for p in psutil.process_iter() if p.status() == "running"]
    for script in scripts:
        assert any(script in str(cmd) for cmd in running)

def test_all_exchanges_in_redis():
    r = redis.Redis(decode_responses=True)
    for exch in ["binance","bybit","okx","gate","bitget"]:
        keys = list(r.scan_iter(f"md:{exch}:spot:*"))
        assert len(keys) > 10, f"No md keys for {exch}"

def test_logs_no_critical_errors():
    for log_file in Path("logs").glob("collector_*.log"):
        content = log_file.read_text()
        assert "CRITICAL" not in content
        # Reconnects в норме (< 3 за 60 сек)
        reconnects = content.count("Reconnecting")
        assert reconnects < 3, f"{log_file.name}: too many reconnects"

def test_spread_monitor_active():
    log = Path("logs/spread_monitor.log").read_text()
    cycles = log.count("Cycle:")
    assert cycles > 50, f"Expected >50 cycles in 60s, got {cycles}"
```

### Критерий готовности
- [ ] `python launcher.py` запускает все процессы без ошибок
- [ ] Через 60 сек все 5 бирж в Redis
- [ ] `pytest tests/integration/test_full_system.py -v -s` — зелёный
- [ ] `logs/launcher.log` показывает все PID

### Git
```bash
git add launcher.py tests/integration/
git commit -m "phase-5: launcher with health-check + integration test"
git push origin data
```

### Ссылки
- `docs/09_launch.md` — порядок запуска, health check, порядок процессов

---

## ФАЗА 6 — Создание веток trade и финальный пуш

### Цель
Создать ветку `trade` из `data`. Убедиться что обе ветки работают.

### Действия

```bash
# Убедиться что data актуальна и тесты зелёные
git checkout data
pytest tests/ -v --ignore=tests/integration
git status   # должно быть чисто

# Создать trade из data
git checkout -b trade data

# Изменение 1: выключить историю
# В config.py: HISTORY_ENABLED = True → False
sed -i 's/HISTORY_ENABLED = True/HISTORY_ENABLED = False/' config.py

# Изменение 2: удалить snapshot_monitor
rm monitors/snapshot_monitor.py

# Коммит
git add config.py
git rm monitors/snapshot_monitor.py
git commit -m "trade: HISTORY_ENABLED=False, no snapshot_monitor"
git push -u origin trade

# Вернуться на data
git checkout data
```

### Тест ветки trade

```bash
# Проверить что история не пишется
python collectors/collector_binance.py &
sleep 15
redis-cli keys "md:hist:*"  # должно быть ПУСТО
redis-cli keys "md:binance:*" | wc -l  # primary ключи должны быть

# Убедиться что spread_monitor работает
python monitors/spread_monitor.py &
sleep 10
cat signal/signal.csv
```

### Финальная проверка обеих веток

```bash
# trade: smoke тест без истории
git checkout trade
pytest tests/smoke/ -v   # history тесты будут SKIPPED (HISTORY_ENABLED=False)

# data: полный тест
git checkout data
pytest tests/ -v --ignore=tests/integration
```

### Критерий готовности
- [ ] `git branch -a` показывает `trade` и `data` в origin
- [ ] В `trade`: `redis-cli keys "md:hist:*"` — пусто
- [ ] В `data`: история заполняется
- [ ] Оба `python launcher.py` (на каждой ветке) запускаются без ошибок

### Git (финальный)
```bash
# На data — финальный тег
git checkout data
git tag v1.0.0-data
git push origin v1.0.0-data

git checkout trade
git tag v1.0.0-trade
git push origin v1.0.0-trade
```

---

## Структура tests/

```
tests/
├── conftest.py               ← общие fixtures (redis_client, tmp cleanup)
├── unit/
│   ├── test_binance_parsers.py
│   ├── test_bybit_parsers.py
│   ├── test_okx_parsers.py
│   ├── test_gate_parsers.py
│   ├── test_bitget_parsers.py
│   ├── test_chunk_logic.py
│   ├── test_spread_logic.py
│   ├── test_stale_logic.py
│   ├── test_snapshot_csv.py
│   └── test_history_reader.py
├── smoke/
│   ├── test_binance_smoke.py
│   ├── test_bybit_smoke.py
│   ├── test_okx_smoke.py
│   ├── test_gate_smoke.py
│   ├── test_bitget_smoke.py
│   └── test_spread_smoke.py
│   └── test_snapshot_smoke.py
└── integration/
    └── test_full_system.py
```

---

## Сводная таблица фаз

| Фаза | Файлы кода | Файлы тестов | Unit | Smoke | Git push | Ссылки |
|------|-----------|-------------|------|-------|---------|--------|
| 0 | config.py, logger_setup.py, requirements.txt | test_infra.py | 3 | 1 | `data` создаётся | doc/06 |
| 1 | collector_binance.py | test_binance_parsers.py, test_chunk_logic.py, test_binance_smoke.py | 8 | 9 | `data` | doc/02,03,01 |
| 2a | collector_bybit.py | test_bybit_parsers.py, test_bybit_smoke.py | 3 | 5 | `data` | doc/02,07 |
| 2b | collector_okx.py | test_okx_parsers.py, test_okx_smoke.py | 3 | 5 | `data` | doc/02,07 |
| 2c | collector_gate.py | test_gate_parsers.py, test_gate_smoke.py | 2 | 5 | `data` | doc/02,07 |
| 2d | collector_bitget.py | test_bitget_parsers.py, test_bitget_smoke.py | 2 | 5 | `data` | doc/02,07 |
| 3 | redis_monitor.py, stale_monitor.py, spread_monitor.py | test_spread_logic.py, test_spread_smoke.py | 8 | 4 | `data` | doc/03,08,01 |
| 4 | snapshot_monitor.py | test_snapshot_csv.py, test_history_reader.py, test_snapshot_smoke.py | 6 | 2 | `data` | doc/03,05,01 |
| 5 | launcher.py | test_full_system.py | — | — | `data` | doc/09 |
| 6 | config.py (HISTORY_ENABLED=False) | — | — | re-run smoke | `trade` создаётся | doc/00,10 |
