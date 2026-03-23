# BALI 5.0 — Карта документации

Индекс всех документов проекта. Для каждого файла: тема, ключевые разделы, кому полезен, статус актуальности.

---

## Навигация

| # | Файл | Тема | Статус |
|---|------|------|--------|
| — | **[00_docs_map.md](./00_docs_map.md)** | Этот файл — карта документации | ✅ |
| 00 | [00_architecture.md](./00_architecture.md) | Архитектура системы (C4 + ADR) | ✅ |
| 01 | [01_redis_schema.md](./01_redis_schema.md) | Схема ключей Redis | ✅ |
| 02 | [02_exchange_adapters.md](./02_exchange_adapters.md) | WebSocket адаптеры бирж | ⚠️ |
| 03 | [03_algorithms.md](./03_algorithms.md) | Алгоритмы всех компонентов | ✅ |
| 04 | [04_logging.md](./04_logging.md) | NDJSON логирование — события и jq | ✅ |
| 05 | [05_history_chunks.md](./05_history_chunks.md) | Система исторических чанков | ✅ |
| 06 | [06_config.md](./06_config.md) | Справочник config.py | ✅ |
| 07 | [07_open_questions.md](./07_open_questions.md) | Принятые решения (Q&A) | ✅ |
| 08 | [08_review_iterations.md](./08_review_iterations.md) | Итерации code review | ✅ |
| 09 | [09_launch.md](./09_launch.md) | Запуск и операционная диагностика | ✅ |
| 10 | [10_project_structure.md](./10_project_structure.md) | Структура файлов и веток | ✅ |
| 11 | [11_development_plan.md](./11_development_plan.md) | Фазовый план разработки | ⚠️ |
| 12 | [12_data_flows.md](./12_data_flows.md) | Потоки данных + реальные метрики из логов | ✅ |

**Статусы:** ✅ актуален · ⚠️ частично устарел

---

## Детальное описание каждого документа

---

### 00_architecture.md — Архитектура системы

**Кому:** всем, кто хочет понять систему в целом. Отправная точка.

**Что внутри:**
- C4-диаграммы трёх уровней: System Context (биржи → BALI → файлы), Container (коллекторы / мониторы / Redis), Component (внутренности коллектора Binance — 7 asyncio задач)
- Стек технологий: Python 3.10+, asyncio, websockets, redis-py, aiofiles
- Поток данных: WS → parse → BatchBuffer → Redis pipeline → primary keys + history keys
- 7 Architectural Decision Records (ADR-001 … ADR-007):
  - ADR-001: один процесс на биржу (не 15 процессов)
  - ADR-002: история через LPUSH (не отдельную БД)
  - ADR-003: TTL вместо явного удаления
  - ADR-004: cooldown в Redis (SET NX EX), не в памяти
  - ADR-005: Redis Stream для сигналов spread→snapshot
  - ADR-006: pipeline batching (250ms / 100 команд)
  - ADR-007: паттерн exchange adapter
- Стратегия веток: `main` (словари), `data` (история + snapshot), `trade` (без истории)

---

### 01_redis_schema.md — Схема ключей Redis

**Кому:** разработчикам, работающим с Redis напрямую; при отладке данных.

**Что внутри:**
- Конвенции: нормализованные символы (`BTCUSDT`), имена бирж, тип рынка (`spot`/`futures`), все значения как строки, ts в миллисекундах
- Первичные ключи (overwrite on update):
  - `md:{exchange}:{market}:{symbol}` → hash `{b, a, ts}`
  - `ob:{exchange}:{market}:{symbol}` → hash `{b1..b10, b1q..b10q, a1..a10, a1q..a10q}`
  - `fr:{exchange}:futures:{symbol}` → hash `{fr, fr_ts}`
- History ключи (LPUSH, newest first):
  - `md:hist:{exchange}:{market}:{symbol}:{chunk_id}` → list `"bid,ask,ts_ms"`
  - `ob:hist:{exchange}:{market}:{symbol}:{chunk_id}` → list `"b1,b1q,...,a10,a10q,ts_ms"`
  - `fr:hist:{exchange}:futures:{symbol}:{chunk_id}` → list `"fr,fr_ts,ts_ms"`
- Системные ключи: `spread:cooldown:*` (SET NX EX 3600), `stream:signals` (Redis Stream), `snapshot:stream_id` (cursor)
- Оценка объёма: ~50K ключей, ~82GB памяти при полных 4 чанках
- Рекомендации Redis: unix socket, без RDB/AOF, TTL=6000s

---

### 02_exchange_adapters.md — WebSocket адаптеры ⚠️

**Кому:** разработчикам, работающим с конкретной биржей.

**Что внутри:**
- WS endpoints и форматы сообщений для всех 5 бирж: Binance, Bybit, OKX, Gate.io, Bitget
- Parsing functions: `parse_md()`, `parse_ob()`, `parse_fr()`, нормализация символов
- Особенности каждой биржи:
  - Binance: нет timestamp в bookTicker → используем `time.time()`
  - Bybit: delta OB → LocalOrderBook cache
  - OKX: 5 уровней OB (books5), FR через REST API при старте
  - Gate.io: FR в секундах → конвертировать в ms; вычислять следующий 8h boundary
  - Bitget: delta OB → накапливать полное состояние

**Статус ⚠️:** большой файл (~10K строк), может содержать устаревшие детали API. Сверять с актуальным кодом коллекторов.

---

### 03_algorithms.md — Алгоритмы компонентов

**Кому:** при реализации новых компонентов или понимании логики существующих.

**Что внутри:**
- Полный псевдокод каждого компонента:
  - **Коллектор:** INIT → 5 WS задач (MD spot/fut, OB spot/fut, FR) → flusher → metrics
  - **Flusher:** 250ms таймер OR 100 команд → pipeline execute → сброс буфера
  - **History writer:** per-tick без downsampling, expire_set оптимизация (EXPIRE один раз на ключ на чанк)
  - **OB history:** rate limit 10 Hz (100ms min interval) per symbol
  - **Spread Monitor:** 300ms loop → один pipeline для всех пар → handle_signal (SET NX cooldown → CSV → XADD)
  - **Snapshot Monitor:** XREAD Block → load 1h history (4 чанка) → forward-fill → realtime 3500s
  - **Stale Monitor:** SCAN `md:*` каждые 30s → batch HGET ts → warn if age > 360s
  - **Redis Monitor:** INFO каждые 30s → проверка памяти / ops / фрагментации / клиентов
  - **Launcher:** supervise 8 процессов → health check 30s → restart on failure

---

### 04_logging.md — NDJSON логирование

**Кому:** всем при диагностике и мониторинге в production.

**Что внутри:**
- Принципы: один файл на процесс, file handler = NDJSON, console = текст WARNING+
- Формат файла: каждая строка — один JSON объект `{"ts":..., "component":..., "event":..., ...}`
- Универсальный envelope: `ts` (Unix ms), `component` (имя процесса), `event` (тип)
- **Полный каталог событий** по компонентам:

  | Компонент | Ключевые события |
  |-----------|-----------------|
  | Коллекторы | `collector_start`, `ws_connect/connected/disconnect`, `slow_flush`, `chunk_rotate`, `redis_error`, `metrics_interval` |
  | spread_monitor | `signal`, `cycle`, `cycle_slow`, `spread_summary` (30s агрегат) |
  | redis_monitor | `redis_health` (nested memory/ops/latency), `redis_warn` (per threshold) |
  | stale_monitor | `stale_scan`, `stale_key` |
  | snapshot_monitor | `signal_received` (с `stream_lag_ms`), `snapshot_start/complete`, `snapshot_row_slow` |
  | launcher | `process_started/died/restarted`, `shutdown` |

- Структура `metrics_interval` (вложенные объекты: `ingestion`, `primary_flush`, `history_flush`, `latency`, `state`)
- **15+ jq-команд** для диагностики: сигналы, latency, реконнекты, parse errors, ошибки

---

### 05_history_chunks.md — Система исторических чанков

**Кому:** при работе с историческими данными; разработке snapshot_monitor.

**Что внутри:**
- Принцип чанков: `chunk_id = int(ts_ms / 1_200_000)` → 20-минутные окна
- Жизненный цикл: чанк живёт 100 минут (5× duration) через TTL=6000s, авто-удаляется Redis
- 4 активных чанка = ~80 минут скользящего окна истории
- `expire_set` оптимизация: EXPIRE выполняется только один раз на ключ на чанк → экономия ~240M команд/сек
- Rate limiting OB history: максимум 10 Hz (100ms) на символ через `ob_hist_last_ts` gate
- Алгоритм загрузки истории для snapshot:
  1. Загрузить 4 чанка для каждого типа (MD/OB/FR) — 20+ Redis запросов
  2. Объединить по timestamp
  3. Forward-fill OB и FR для пропущенных тиков
- Форматы данных в history lists: MD (`bid,ask,ts`), OB (40 полей + ts), FR (`fr,fr_ts,ts`)
- Оценка памяти: OB 10Hz → ~230GB за 4 чанка (доминирующий тип)

---

### 06_config.md — Справочник конфигурации

**Кому:** при изменении параметров; понимании порогов и интервалов.

**Что внутри:**
- Все константы `config.py` с текущими значениями и обоснованием:

  | Группа | Параметры |
  |--------|-----------|
  | Branch | `HISTORY_ENABLED` (True=data, False=trade) |
  | Redis | unix socket path, DB=0, URL |
  | Batching | `BATCH_FLUSH_INTERVAL_MS=250`, `BATCH_MAX_COMMANDS=100`, `HIST_FLUSH_INTERVAL_MS=300`, `OB_HIST_MIN_INTERVAL_MS=100` |
  | WebSocket | ping_interval=20s, chunk sizes (Binance=300, Bybit=10, OKX=100, Gate=50, Bitget=50) |
  | History | `CHUNK_DURATION=1200s`, `CHUNK_TTL=6000s`, `MAX_HISTORY_CHUNKS=4` |
  | Мониторы | `STALE_THRESHOLD=360s`, `SPREAD_THRESHOLD=1.00%`, `COOLDOWN=3600s`, `SNAPSHOT_DURATION=3500s` |
  | Пути | `dictionaries/`, `signal/`, `signal/snapshot/`, `logs/` |
  | CSV заголовки | signal (7 полей), snapshot (90+ полей) |
  | Метрики | `METRICS_LOG_INTERVAL=5s` |

---

### 07_open_questions.md — Принятые решения

**Кому:** при возникновении вопросов «почему именно так сделано».

**Что внутри:** 13 вопросов, возникших до реализации, с принятыми решениями:

| # | Вопрос | Решение |
|---|--------|---------|
| Q1-2 | OKX/Bitget глубина OB | books5 (5 уровней), поля 6-10 пустые |
| Q3 | Bybit delta OB | LocalOrderBook cache, накапливать дельты |
| Q4 | Gate.io futures символы | native_futures.txt с нормализацией |
| Q5 | Binance нет timestamp | `time.time() * 1000` |
| Q6 | OKX FR инициализация | REST API при старте |
| Q7 | Пустой OB в истории | Forward-fill последнего известного OB |
| Q8 | Отрицательный спред | Игнорировать (< threshold) |
| Q9 | no_data шум при старте | 10s задержка + DEBUG уровень |
| Q10 | Ротация signal.csv | Не требуется по спеку |
| Q11 | Неполная история | Graceful handling + warning |
| Q12 | Параллельные snapshots | OK (разные файлы) |
| Q13 | Binance FR stream | `!markPrice@arr@1s` |

---

### 08_review_iterations.md — Итерации ревью

**Кому:** при отладке нетривиальных багов; понимании «почему вот это именно так».

**Что внутри:** 7 итераций code review с найденными и исправленными проблемами:

| Итерация | Тема | Ключевые находки |
|----------|------|-----------------|
| 1 | Protocol/API | OKX 5 уровней, Bybit delta, Gate FR в секундах |
| 2 | Архитектура | CSV write lock, flush_lock убран, SCAN batching |
| 3 | Производительность | pipeline chunking в spread_monitor |
| 4 | **Логика** | 🔴 **КРИТИЧНО: формула спреда** `(bid_fut - ask_spot) / ask_spot`, cooldown по направлению |
| 5 | Edge cases | Пустые combination files, OKX FR без snapshot |
| 6 | Документация | requirements.txt, SIGTERM, .gitignore |
| 7 | Безопасность | Redis auth, NaN/inf валидация |

> **Важно:** итерация 4 содержит исправление знака формулы спреда — критический баг.

---

### 09_launch.md — Запуск и диагностика

**Кому:** DevOps; при первом запуске; при диагностике production-проблем.

**Что внутри:**
- Порядок запуска: Redis → `dictionaries/main.py` → `launcher.py [--data]`
- Задержки при старте: 0.5s между процессами, 10s warmup коллекторов до запуска мониторов
- Варианты запуска:
  ```bash
  python launcher.py          # trade branch (без snapshot_monitor)
  python launcher.py --data   # data branch (с snapshot_monitor)
  ```
- Список 8/9 управляемых процессов
- Health check логика: poll каждые 30s, restart при выходе
- Диагностические команды: `ps`, `redis-cli info`, подсчёт ключей, проверка cooldown-ов, просмотр signal.csv

---

### 10_project_structure.md — Структура проекта

**Кому:** при онбординге; при создании новых файлов.

**Что внутри:**
- Полная карта файлов:
  ```
  config.py, logger_setup.py, launcher.py, requirements.txt
  collectors/  (5 файлов)
  monitors/    (4 файла)
  dictionaries/
  signal/
  logs/
  tests/
  docs/
  ```
- Стратегия веток и их различия:

  | Параметр | main | data | trade |
  |----------|------|------|-------|
  | HISTORY_ENABLED | False | True | False |
  | История в Redis | — | ✅ | — |
  | snapshot_monitor | — | ✅ | — |

- Граф зависимостей: `config` и `logger_setup` импортируются всеми; коллекторы изолированы друг от друга
- 7 фаз реализации: Phase 0 (инфра) → Phase 1 (Binance) → Phase 2 (4 биржи) → Phase 3 (мониторы) → Phase 4 (launcher) → Phase 5 (ветки) → Phase 6 (snapshot на data)

---

### 11_development_plan.md — Фазовый план разработки ⚠️

**Кому:** при возобновлении разработки с нуля; понимании тест-сьюта.

**Что внутри:**
- Таблица фаз с deliverables, тестами и git workflow
- Phase 0 (инфра): `requirements.txt`, `config.py`, `logger_setup.py`, smoke tests
- Phase 1 (Binance): unit тесты парсеров (MD/OB/FR, невалидные данные, нулевые цены), тесты chunk логики (границы, форматы), smoke test (заполнение Redis, freshness timestamp)
- Phases 2-6: аналогично для остальных компонентов

**Статус ⚠️:** большой файл (~10K+ строк), структура тестов может расходиться с текущим `tests/` директорией. Phase 0-1 актуальны.

---

## Специализированные документы (superpowers/)

Расположены в `docs/superpowers/`:

| Файл | Тема |
|------|------|
| `specs/2026-03-23-json-metrics-schema-design.md` | Полная JSON-схема всех метрик — 30+ событий с примерами, карта задержек L1-L11, пробелы в инструментировании |
| `plans/2026-03-23-json-metrics-implementation.md` | Plan реализации NDJSON метрик — 10 задач с TDD шагами, примерами кода, коммит-командами |
| `specs/2026-03-23-dashboard-design.md` | Дизайн TUI дашборда (Textual) |
| `plans/2026-03-23-dashboard.md` | Plan реализации дашборда |
| `specs/2026-03-23-redis-unix-socket-design.md` | Дизайн перехода на Redis unix socket |
| `plans/2026-03-23-redis-unix-socket.md` | Plan перехода на Redis unix socket |

---

## С чего начать

| Цель | Документ |
|------|----------|
| Понять систему в целом | `00_architecture.md` |
| Запустить систему | `09_launch.md` |
| Разобраться с данными в Redis | `01_redis_schema.md` |
| Диагностировать проблему по логам | `04_logging.md` |
| Понять параметры и пороги | `06_config.md` |
| Добавить новую биржу | `02_exchange_adapters.md` + `03_algorithms.md` |
| Разобраться с историей/snapshot | `05_history_chunks.md` |
| Понять «почему именно так» | `07_open_questions.md` + `08_review_iterations.md` |
| Добавить новые метрики | `specs/2026-03-23-json-metrics-schema-design.md` |
