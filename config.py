# config.py
"""
BALI 5.0 — Центральный конфиг.
Все константы системы. Не импортировать тяжёлые зависимости здесь.
"""

# ── Branch flag ───────────────────────────────────────────────────────────
# True  → ветка data: пишем history (md:hist:*, ob:hist:*, fr:hist:*) + snapshot
# False → ветка trade: только primary ключи (md:*, ob:*, fr:*), история не пишется
HISTORY_ENABLED = True

# ── Биржи ──────────────────────────────────────────────────────────────────
EXCHANGES = ["binance", "bybit", "okx", "gate", "bitget"]

# ── Redis ──────────────────────────────────────────────────────────────────
REDIS_SOCKET_PATH = "/root/bali5.0/redis.sock"
REDIS_DB          = 0
REDIS_URL         = f"unix://{REDIS_SOCKET_PATH}?db={REDIS_DB}"

# ── Батчинг коллекторов ────────────────────────────────────────────────────
BATCH_FLUSH_INTERVAL_MS         = 50   # мс: макс время до следующего primary flush
BINANCE_BATCH_FLUSH_INTERVAL_MS = 100  # мс: Binance flush — 50ms даёт слишком много slow flushes
HIST_FLUSH_INTERVAL_MS  = 300   # мс: интервал отдельного hist-flusher (lpush)
HIST_FLUSH_CHUNK        = 5000  # макс команд в одном pipeline hist-flush (предотвращает ~37k-cmd спайки)
OB_HIST_MIN_INTERVAL_MS = 100   # мс: мин интервал между hist-записями OB (10 Hz)

# ── Параметры WebSocket ────────────────────────────────────────────────────
WS_PING_INTERVAL   = 20    # сек: ping_interval для websockets
WS_PING_TIMEOUT    = 30    # сек: ping_timeout
WS_CLOSE_TIMEOUT   = 5     # сек: close_timeout
WS_MAX_SIZE        = 10_000_000  # байт: max message size (10 MB)

# WS chunk sizes (символов на одно соединение / subscribe message)
WS_CHUNK_BINANCE   = 300   # Binance: до 1024 streams, но 300 достаточно
WS_CHUNK_BYBIT     = 10    # Bybit: 10 topics на subscribe message
WS_CHUNK_OKX       = 100   # OKX: до 240 args, берём 100 для надёжности
WS_CHUNK_GATE      = 50    # Gate.io: нет явного лимита
WS_CHUNK_BITGET    = 50    # Bitget: нет явного лимита

# Heartbeat интервалы (сек)
HEARTBEAT_BYBIT    = 20    # Bybit: json {"op": "ping"} каждые 20 сек
HEARTBEAT_OKX      = 25    # OKX: строка "ping" каждые 25 сек
HEARTBEAT_BITGET   = 25    # Bitget: строка "ping" каждые 25 сек
# Binance и Gate.io: автоматический WS-level ping (задаётся ping_interval)

# Reconnect backoff
WS_RECONNECT_INIT  = 1     # сек: начальная задержка
WS_RECONNECT_MAX   = 60    # сек: максимальная задержка

# ── История (Chunks) ───────────────────────────────────────────────────────
CHUNK_DURATION       = 1200    # сек: 20 минут на чанк
CHUNK_TTL            = 6000    # сек: TTL каждого hist-ключа (100 мин)
MAX_HISTORY_CHUNKS   = 4       # чанков в активном окне (= 80 мин)

# История: без downsampling — пишем каждый тик
# Ресурс не ограничен, история >= 1 час для всех типов данных (MD, OB, FR)

# ── Stale Monitor ─────────────────────────────────────────────────────────
STALE_THRESHOLD_SECONDS = 360  # сек: ключ считается stale если не обновлялся
STALE_CHECK_INTERVAL    = 30   # сек: как часто проверять

# ── Redis Monitor ─────────────────────────────────────────────────────────
REDIS_CHECK_INTERVAL       = 30   # сек
REDIS_MEMORY_WARN_MB       = 3000 # MB: предупреждение если выше
REDIS_OPS_WARN_PER_SEC     = 200000  # ops/sec: предупреждение если выше
REDIS_BLOCKED_WARN_THRESHOLD = 1  # snapshot_monitor всегда держит 1 blocked (XREAD block=2000)
                                  # предупреждаем только если blocked > этого порога

# ── Spread Monitor ────────────────────────────────────────────────────────
SPREAD_POLL_INTERVAL_MS = 100  # мс: интервал сканирования
SPREAD_THRESHOLD        = 1.00 # %: минимальный спред для сигнала
COOLDOWN_SECONDS        = 3600 # сек: кулдаун после сигнала

# Проверка staleness данных в spread_monitor
SPREAD_DATA_STALE_MS    = 5000 # мс: данные старше 5 сек пропускаем

# ── Snapshot Monitor ──────────────────────────────────────────────────────
SNAPSHOT_DURATION       = 3500 # сек: длительность записи снапшота
SNAPSHOT_INTERVAL_MS    = 100  # мс: интервал строки в снапшоте
HISTORY_LOOKBACK_MS     = 3_600_000  # мс = 1 час истории до сигнала

# ── Пути к файлам ─────────────────────────────────────────────────────────
DICTIONARIES_DIR   = "dictionaries"
SUBSCRIBE_DIR      = f"{DICTIONARIES_DIR}/subscribe"
COMBINATION_DIR    = f"{DICTIONARIES_DIR}/combination"
SIGNAL_DIR         = "signal"
SNAPSHOT_DIR       = f"{SIGNAL_DIR}/snapshot"
LOGS_DIR           = "logs"

SIGNAL_CSV         = f"{SIGNAL_DIR}/signal.csv"
SIGNAL_CSV_HEADER  = "spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts"

# ── Snapshot CSV ───────────────────────────────────────────────────────────
SNAPSHOT_CSV_HEADER = (
    "spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts,"
    "fr,fr_time,"
    "s_b1,s_b2,s_b3,s_b4,s_b5,s_b6,s_b7,s_b8,s_b9,s_b10,"
    "s_bq1,s_bq2,s_bq3,s_bq4,s_bq5,s_bq6,s_bq7,s_bq8,s_bq9,s_bq10,"
    "s_a1,s_a2,s_a3,s_a4,s_a5,s_a6,s_a7,s_a8,s_a9,s_a10,"
    "s_aq1,s_aq2,s_aq3,s_aq4,s_aq5,s_aq6,s_aq7,s_aq8,s_aq9,s_aq10,"
    "f_b1,f_b2,f_b3,f_b4,f_b5,f_b6,f_b7,f_b8,f_b9,f_b10,"
    "f_bq1,f_bq2,f_bq3,f_bq4,f_bq5,f_bq6,f_bq7,f_bq8,f_bq9,f_bq10,"
    "f_a1,f_a2,f_a3,f_a4,f_a5,f_a6,f_a7,f_a8,f_a9,f_a10,"
    "f_aq1,f_aq2,f_aq3,f_aq4,f_aq5,f_aq6,f_aq7,f_aq8,f_aq9,f_aq10"
)

# ── Redis Stream (сигналы spread → snapshot) ──────────────────────────────
STREAM_SIGNALS          = "stream:signals"
STREAM_SIGNALS_MAXLEN   = 1000   # хранить не более N последних сигналов
STREAM_LAST_ID_KEY      = "snapshot:stream_id"  # куда snapshot_monitor пишет свой курсор

# ── Metrics ───────────────────────────────────────────────────────────────
METRICS_LOG_INTERVAL = 5   # сек: как часто логировать метрики коллекторов
