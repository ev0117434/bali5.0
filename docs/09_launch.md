# BALI 5.0 — Launch & Process Management

---

## 1. Порядок запуска

```
Шаг 0: Запустить Redis (однократно при старте; launcher делает это автоматически)
    bash scripts/redis_setup.sh

Шаг 1: Запустить dictionaries/main.py (однократно, ~70 сек)
Шаг 2: python launcher.py [--data]
        └── вызывает ensure_redis() → scripts/redis_setup.sh автоматически
```

### Детальный порядок в launcher.py

```
t=0s    Проверка Redis (PING)
t=0s    Создание директорий: logs/, signal/, signal/snapshot/
t=0s    Проверка subscribe файлов (должны существовать после step 1)
t=0s    Старт collector_binance
t=0.5s  Старт collector_bybit
t=1.0s  Старт collector_okx
t=1.5s  Старт collector_gate
t=2.0s  Старт collector_bitget
t=2.5s  Старт redis_monitor
t=3.0s  Старт stale_monitor

[ожидание 10 сек — коллекторы заполняют Redis]

t=13s   Старт spread_monitor
t=13.5s Старт snapshot_monitor (только --data)

t=43s+  Health check loop: каждые 30 сек проверять все процессы
```

---

## 2. Использование

```bash
# Запустить Redis (если не запущен)
bash scripts/redis_setup.sh

# Запустить сначала dictionaries (однократно или при обновлении пар)
python -m dictionaries.main

# Ветка trade (только сигналы)
python launcher.py

# Ветка data (сигналы + снапшоты)
python launcher.py --data

# Остановить всё
Ctrl+C  или  kill -TERM <launcher_pid>
```

---

## 3. Структура проекта при запуске

```
bali5.0/
├── launcher.py
├── config.py
├── logger_setup.py
│
├── collectors/
│   ├── collector_binance.py   # запускается: python collectors/collector_binance.py
│   ├── collector_bybit.py
│   ├── collector_okx.py
│   ├── collector_gate.py
│   └── collector_bitget.py
│
├── monitors/
│   ├── redis_monitor.py
│   ├── stale_monitor.py
│   ├── spread_monitor.py
│   └── snapshot_monitor.py
│
├── dictionaries/              # уже существует ✓
│   ├── subscribe/             # создаётся main.py
│   └── combination/           # создаётся main.py
│
├── signal/                    # создаётся launcher
│   ├── signal.csv
│   └── snapshot/
│
└── logs/                      # создаётся launcher
    ├── launcher.log
    ├── collector_binance.log
    └── ...
```

---

## 4. Health Check логика

```python
PROCESSES = {
    "collector_binance": ["python", "collectors/collector_binance.py"],
    "collector_bybit":   ["python", "collectors/collector_bybit.py"],
    "collector_okx":     ["python", "collectors/collector_okx.py"],
    "collector_gate":    ["python", "collectors/collector_gate.py"],
    "collector_bitget":  ["python", "collectors/collector_bitget.py"],
    "redis_monitor":     ["python", "monitors/redis_monitor.py"],
    "stale_monitor":     ["python", "monitors/stale_monitor.py"],
    "spread_monitor":    ["python", "monitors/spread_monitor.py"],
}

# Restart tracking (предотвращает restart loop)
restart_counts = {name: 0 for name in PROCESSES}
MAX_RESTARTS = 10  # после 10 restart подряд — не перезапускать, лог ERROR

def health_check(procs):
    for name, p in list(procs.items()):
        if p.poll() is not None:  # процесс завершился
            exit_code = p.poll()
            log.error(f"Process {name} exited (code={exit_code})")

            if restart_counts[name] >= MAX_RESTARTS:
                log.error(f"Process {name} exceeded max restarts ({MAX_RESTARTS}). Not restarting.")
                continue

            restart_counts[name] += 1
            cmd = PROCESSES[name]
            new_p = subprocess.Popen(cmd, ...)
            procs[name] = new_p
            log.info(f"Restarted {name} (attempt {restart_counts[name]}) PID={new_p.pid}")
```

---

## 5. Каждый модуль — самодостаточный

Каждый коллектор и монитор можно запустить отдельно для тестирования:

```bash
# Тест одного коллектора
python collectors/collector_binance.py

# Тест spread monitor (нужен Redis с данными)
python monitors/spread_monitor.py

# Тест stale monitor
python monitors/stale_monitor.py
```

---

## 6. Переменные окружения (override config)

```bash
# Переопределить Redis host
REDIS_HOST=192.168.1.100 python launcher.py

# Включить DEBUG на консоли (для разработки)
LOG_CONSOLE_LEVEL=DEBUG python collectors/collector_binance.py
```

В config.py:
```python
import os
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
```

---

## 7. Диагностика при проблемах

```bash
# Показать все запущенные процессы
ps aux | grep "python collectors"
ps aux | grep "python monitors"

# Хвост основных логов
tail -f logs/spread_monitor.log
tail -f logs/collector_binance.log

# Проверить Redis
redis-cli -s /root/bali5.0/redis.sock ping
redis-cli -s /root/bali5.0/redis.sock info memory
redis-cli -s /root/bali5.0/redis.sock info stats

# Количество заполненных MD ключей
redis-cli -s /root/bali5.0/redis.sock keys "md:*" | wc -l

# Проверить конкретный символ
redis-cli -s /root/bali5.0/redis.sock hgetall "md:binance:spot:BTCUSDT"

# Все активные cooldown
redis-cli -s /root/bali5.0/redis.sock keys "spread:cooldown:*" | wc -l

# Остановить Redis
redis-cli -s /root/bali5.0/redis.sock shutdown nosave

# Посмотреть tail signal.csv
tail -20 signal/signal.csv

# Стале-ключи прямо сейчас
python monitors/stale_monitor.py
```
