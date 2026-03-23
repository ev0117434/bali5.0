#!/usr/bin/env python3
# collectors/collector_binance.py
"""
BALI 5.0 — Binance collector.

Собирает MD (bookTicker) + OB (depth10@100ms) + FR (markPrice) для spot и futures.
7 asyncio tasks: md_spot, md_fut, ob_spot, ob_fut, fr, flusher, metrics.

Redis ключи:
  md:binance:spot:{symbol}     → {b, a, ts}
  md:binance:futures:{symbol}  → {b, a, ts}
  ob:binance:spot:{symbol}     → {b1..b10, b1q..b10q, a1..a10, a1q..a10q}
  ob:binance:futures:{symbol}  → {b1..b10, b1q..b10q, a1..a10, a1q..a10q}
  fr:binance:futures:{symbol}  → {fr, fr_ts}

History (только при HISTORY_ENABLED=True):
  md:hist:binance:{market}:{symbol}:{chunk_id}  → list "bid,ask,ts_ms"
  ob:hist:binance:{market}:{symbol}:{chunk_id}  → list "b1p,b1q,...,a10p,a10q,ts_ms"
  fr:hist:binance:futures:{symbol}:{chunk_id}   → list "rate,fr_ts,write_ts"
"""

import asyncio
import json
import sys
import time
from pathlib import Path

import websockets
import redis.asyncio as aioredis

# Ensure project root on path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from logger_setup import setup_logger

log = setup_logger("collector_binance")

# ── WS endpoints ───────────────────────────────────────────────────────────
_WS_SPOT    = "wss://stream.binance.com:9443/stream?streams="
_WS_FUTURES = "wss://fstream.binance.com/stream?streams="


# ── Symbol loading ─────────────────────────────────────────────────────────

def _load_symbols(filepath: str) -> list[str]:
    p = Path(filepath)
    if not p.exists():
        log.error(_evt("symbol_file_missing", exchange="binance", path=str(p)))
        return []
    lines = [l.strip().upper() for l in p.read_text().splitlines() if l.strip()]
    log.info(_evt("symbols_loaded", exchange="binance", count=len(lines), path=str(p)))
    return lines


# ── Parsers ────────────────────────────────────────────────────────────────

def parse_md(raw: str):
    """Parse bookTicker message → (symbol, bid, ask, ts_ms) or None."""
    _t = time.monotonic()
    try:
        msg = json.loads(raw)
        data = msg.get("data", msg)
        # Skip non-bookTicker messages (e.g. combined stream control messages)
        if "s" not in data or "b" not in data:
            stats["parse_errors"] += 1
            return None
        symbol = data["s"]
        bid    = data["b"]
        ask    = data["a"]
        ts_ms  = int(time.time() * 1000)  # Binance bookTicker has no ts
        return symbol, bid, ask, ts_ms
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


def parse_ob(raw: str):
    """Parse spot depth10@100ms → (symbol, bids, asks, ts_ms) or None.
    Spot format: data = {"lastUpdateId": ..., "bids": [...], "asks": [...]}
    """
    _t = time.monotonic()
    try:
        msg  = json.loads(raw)
        data = msg.get("data", msg)
        if "bids" not in data:
            stats["parse_errors"] += 1
            return None
        stream = msg.get("stream", "")
        symbol = stream.split("@")[0].upper() if stream else data.get("s", "")
        bids   = data["bids"][:10]
        asks   = data["asks"][:10]
        ts_ms  = int(time.time() * 1000)
        return symbol, bids, asks, ts_ms
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


def parse_ob_fut(raw: str):
    """Parse futures depth10@100ms → (symbol, bids, asks, ts_ms) or None.
    Futures format: data = {"e": "depthUpdate", "s": ..., "b": [...], "a": [...]}
    Binance futures depth uses b/a keys, NOT bids/asks like spot.
    """
    _t = time.monotonic()
    try:
        msg  = json.loads(raw)
        data = msg.get("data", msg)
        if data.get("e") != "depthUpdate":
            stats["parse_errors"] += 1
            return None
        stream = msg.get("stream", "")
        symbol = stream.split("@")[0].upper() if stream else data.get("s", "")
        bids   = data.get("b", [])[:10]
        asks   = data.get("a", [])[:10]
        ts_ms  = int(time.time() * 1000)
        return symbol, bids, asks, ts_ms
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


def parse_fr(raw: str):
    """
    Parse markPrice message (single OR arr).
    Returns list of (symbol, rate, fr_ts_str) tuples.
    """
    _t = time.monotonic()
    try:
        msg  = json.loads(raw)
        data = msg.get("data", msg)

        # !markPrice@arr@1s → data is a list
        if isinstance(data, list):
            results = []
            for item in data:
                if item.get("e") != "markPriceUpdate":
                    continue
                symbol = item["s"]
                rate   = item.get("r", "")
                fr_ts  = item.get("T", 0)
                if rate and fr_ts:
                    results.append((symbol, rate, str(fr_ts)))
            return results

        # Single symbol markPrice
        if data.get("e") == "markPriceUpdate":
            symbol = data["s"]
            rate   = data.get("r", "")
            fr_ts  = data.get("T", 0)
            if rate and fr_ts:
                return [(symbol, rate, str(fr_ts))]
        return []
    except Exception:
        stats["parse_errors"] += 1
        return []
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


# ── Shared mutable state ───────────────────────────────────────────────────
# (module-level, safe in single asyncio event loop)

batch_buffer: dict  = {}    # key → {field: value}
hist_buffer:  list  = []    # [(hist_key, line_str), ...]
flush_event         = None  # set in main()
expire_set:   set   = set()
last_chunk_id: int  = 0
ob_hist_last_ts: dict = {}  # hist_key → last write ts_ms (OB 10 Hz gate)
buffer_write_ts: dict[str, float] = {}  # key → time.monotonic() when first written

stats: dict = {
    "md_msgs": 0, "ob_msgs": 0, "fr_msgs": 0,
    "flushes": 0, "flush_lat_sum": 0.0, "flush_lat_max": 0.0,
    "batch_sum": 0,
    "hist_flushes": 0, "hist_flush_lat_sum": 0.0, "hist_flush_lat_max": 0.0,
    "hist_cmds": 0, "ob_hist_skipped": 0,
    "reconnects": 0,
    # ── new fields ────────────────────────────────────────────────────────
    "parse_errors":          0,
    "parse_lat_sum":         0.0,   # microseconds
    "parse_lat_max":         0.0,
    "flush_slow_count":      0,
    "hist_flush_slow_count": 0,
    "buffer_age_sum":        0.0,   # ms; accumulated at each flush
    "buffer_age_max":        0.0,
    "e2e_lat_sum":           0.0,   # ms; NOT used for Binance (no exchange ts)
    "e2e_lat_max":           0.0,
    "e2e_lat_count":         0,
}

_COMPONENT = "collector_binance"


def _evt(event: str, **kwargs) -> dict:
    """Build a metrics event envelope with ts/component/event."""
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}


# ── Buffer writers ─────────────────────────────────────────────────────────

def write_md_to_buffer(symbol: str, bid: str, ask: str, ts_ms: int, market: str):
    key = f"md:binance:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:binance:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))


def write_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    key    = f"ob:binance:{market}:{symbol}"
    fields = {}
    for i, (price, qty) in enumerate(bids[:10], 1):
        fields[f"b{i}"]  = price
        fields[f"b{i}q"] = qty
    for i, (price, qty) in enumerate(asks[:10], 1):
        fields[f"a{i}"]  = price
        fields[f"a{i}q"] = qty
    batch_buffer[key] = fields
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"ob:hist:binance:{market}:{symbol}:{chunk_id}"
        last_ts  = ob_hist_last_ts.get(hist_key, 0)
        if ts_ms - last_ts >= config.OB_HIST_MIN_INTERVAL_MS:
            ob_hist_last_ts[hist_key] = ts_ms
            bid_parts = [f"{p},{q}" for p, q in bids[:10]]
            ask_parts = [f"{p},{q}" for p, q in asks[:10]]
            hist_buffer.append((hist_key, ",".join(bid_parts + ask_parts + [str(ts_ms)])))
        else:
            stats["ob_hist_skipped"] += 1


def write_fr_to_buffer(symbol: str, rate: str, fr_ts_ms: str):
    key    = f"fr:binance:futures:{symbol}"
    ts_ms  = int(time.time() * 1000)
    batch_buffer[key] = {"fr": rate, "fr_ts": fr_ts_ms}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:binance:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))


# ── WS tasks ───────────────────────────────────────────────────────────────

async def _ws_recv_loop(url: str, parser, label: str, symbols: list = None):
    """
    Connect to url, receive messages forever, call parser.
    parser returns None / single result / list of results.
    Each result is passed to the appropriate buffer writer via label.
    """
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(_evt("ws_connect", exchange="binance", stream=label,
                          url_prefix=url[:80], symbols=len(symbols) if symbols else 0))
            async with websockets.connect(
                url,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="binance", stream=label))
                backoff = config.WS_RECONNECT_INIT  # reset on success

                async for raw in ws:
                    result = parser(raw)
                    if result is None:
                        continue

                    if label == "md_spot":
                        write_md_to_buffer(*result, market="spot")
                        stats["md_msgs"] += 1
                    elif label == "md_fut":
                        write_md_to_buffer(*result, market="futures")
                        stats["md_msgs"] += 1
                    elif label == "ob_spot":
                        write_ob_to_buffer(*result, market="spot")
                        stats["ob_msgs"] += 1
                    elif label == "ob_fut":
                        write_ob_to_buffer(*result, market="futures")
                        stats["ob_msgs"] += 1
                    elif label == "fr":
                        # parse_fr returns a list
                        for item in result:
                            write_fr_to_buffer(*item)
                        stats["fr_msgs"] += len(result)


        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="binance", stream=label,
                             error_type=type(exc).__name__, error_msg=str(exc),
                             reconnect_backoff_s=backoff,
                             reconnects_total=stats["reconnects"]))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


async def task_md_spot(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@bookTicker" for s in chunk)
        url = _WS_SPOT + streams
        coros.append(_ws_recv_loop(url, parse_md, "md_spot", symbols=chunk))
    await asyncio.gather(*coros)


async def task_md_fut(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@bookTicker" for s in chunk)
        url = _WS_FUTURES + streams
        coros.append(_ws_recv_loop(url, parse_md, "md_fut", symbols=chunk))
    await asyncio.gather(*coros)


async def task_ob_spot(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@depth10@100ms" for s in chunk)
        url = _WS_SPOT + streams
        coros.append(_ws_recv_loop(url, parse_ob, "ob_spot", symbols=chunk))
    await asyncio.gather(*coros)


async def task_ob_fut(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@depth10@100ms" for s in chunk)
        url = _WS_FUTURES + streams
        coros.append(_ws_recv_loop(url, parse_ob_fut, "ob_fut", symbols=chunk))
    await asyncio.gather(*coros)


async def task_fr():
    """FR via !markPrice@arr@1s — single stream for all futures symbols."""
    url = _WS_FUTURES + "!markPrice@arr@1s"
    await _ws_recv_loop(url, parse_fr, "fr")


# ── Flusher (primary: hset only) ───────────────────────────────────────────

async def task_flusher(redis: aioredis.Redis):

    while True:
        try:
            await asyncio.wait_for(
                flush_event.wait(),
                timeout=config.BINANCE_BATCH_FLUSH_INTERVAL_MS / 1000,
            )
        except asyncio.TimeoutError:
            pass
        flush_event.clear()

        if not batch_buffer:
            continue

        current_batch = batch_buffer.copy()
        batch_buffer.clear()

        # measure age of oldest pending entry before flushing
        if buffer_write_ts:
            oldest_write = min(buffer_write_ts.values())
            age_ms = (time.monotonic() - oldest_write) * 1000
            stats["buffer_age_sum"] += age_ms
            if age_ms > stats["buffer_age_max"]:
                stats["buffer_age_max"] = age_ms
        buffer_write_ts.clear()

        t_start = time.monotonic()
        pipe = redis.pipeline(transaction=False)
        for key, fields in current_batch.items():
            pipe.hset(key, mapping=fields)

        try:
            await pipe.execute()
        except Exception as exc:
            log.error(_evt("redis_error", exchange="binance", pipeline_type="primary",
                           error_type=type(exc).__name__, error_msg=str(exc)))

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["flushes"]       += 1
        stats["flush_lat_sum"] += flush_lat_ms
        stats["flush_lat_max"]  = max(stats["flush_lat_max"], flush_lat_ms)
        stats["batch_sum"]     += len(current_batch)

        if flush_lat_ms > 50:
            stats["flush_slow_count"] += 1
            log.warning(_evt("slow_flush", exchange="binance", flush_type="primary",
                             lat_ms=round(flush_lat_ms, 1), threshold_ms=50.0,
                             cmds=len(current_batch)))


# ── Hist flusher (lpush, independent 300ms timer) ──────────────────────────

async def task_hist_flusher(redis: aioredis.Redis):
    global last_chunk_id

    while True:
        await asyncio.sleep(config.HIST_FLUSH_INTERVAL_MS / 1000)

        if not hist_buffer:
            continue

        current_hist = hist_buffer.copy()
        hist_buffer.clear()

        new_chunk_id = int(time.time() / config.CHUNK_DURATION)
        if new_chunk_id != last_chunk_id:
            expire_set.clear()
            ob_hist_last_ts.clear()
            log.info(_evt("chunk_rotate", exchange="binance",
                          chunk_id_prev=last_chunk_id, chunk_id_new=new_chunk_id,
                          expire_keys_reset=len(expire_set)))
            last_chunk_id = new_chunk_id

        t_start = time.monotonic()
        expire_cmds = 0
        total_sent  = 0

        # Отправляем чанками чтобы не блокировать event loop (при 8k msg/s
        # за 300ms может накопиться до ~37k команд в одном pipeline).
        for i in range(0, len(current_hist), config.HIST_FLUSH_CHUNK):
            chunk = current_hist[i:i + config.HIST_FLUSH_CHUNK]
            pipe = redis.pipeline(transaction=False)
            for hist_key, line in chunk:
                pipe.lpush(hist_key, line)
                if hist_key not in expire_set:
                    pipe.expire(hist_key, config.CHUNK_TTL)
                    expire_set.add(hist_key)
                    expire_cmds += 1
            try:
                await pipe.execute()
            except Exception as exc:
                log.error(_evt("redis_error", exchange="binance", pipeline_type="history",
                               error_type=type(exc).__name__, error_msg=str(exc)))
            total_sent += len(chunk)
            # Уступаем event loop между чанками
            await asyncio.sleep(0)

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["hist_flushes"]       += 1
        stats["hist_flush_lat_sum"] += flush_lat_ms
        stats["hist_flush_lat_max"]  = max(stats["hist_flush_lat_max"], flush_lat_ms)
        stats["hist_cmds"]          += len(current_hist)

        if flush_lat_ms > 100:
            stats["hist_flush_slow_count"] += 1
            log.warning(_evt("slow_flush", exchange="binance", flush_type="history",
                             lat_ms=round(flush_lat_ms, 1), threshold_ms=100.0,
                             cmds=len(current_hist), expire_cmds=expire_cmds))


# ── Metrics ────────────────────────────────────────────────────────────────

async def task_metrics():
    interval = config.METRICS_LOG_INTERVAL
    while True:
        await asyncio.sleep(interval)
        n  = max(stats["flushes"], 1)
        nh = max(stats["hist_flushes"], 1)
        nm = max(stats["md_msgs"] + stats["ob_msgs"] + stats["fr_msgs"], 1)

        log.info(_evt("metrics_interval", exchange="binance",
            interval_s=round(interval, 1),
            ingestion={
                "md_msgs":            stats["md_msgs"],
                "md_msgs_per_s":      round(stats["md_msgs"] / interval, 1),
                "ob_msgs":            stats["ob_msgs"],
                "ob_msgs_per_s":      round(stats["ob_msgs"] / interval, 1),
                "fr_msgs":            stats["fr_msgs"],
                "fr_msgs_per_s":      round(stats["fr_msgs"] / interval, 1),
                "parse_errors":       stats["parse_errors"],
                "parse_errors_per_s": round(stats["parse_errors"] / interval, 1),
            },
            primary_flush={
                "count":       stats["flushes"],
                "count_per_s": round(stats["flushes"] / interval, 1),
                "batch_avg":   round(stats["batch_sum"] / n, 1),
                "lat_avg_ms":  round(stats["flush_lat_sum"] / n, 1),
                "lat_max_ms":  round(stats["flush_lat_max"], 1),
                "slow_count":  stats["flush_slow_count"],
            },
            history_flush={
                "count":        stats["hist_flushes"],
                "count_per_s":  round(stats["hist_flushes"] / interval, 1),
                "cmds_total":   stats["hist_cmds"],
                "cmds_per_s":   round(stats["hist_cmds"] / interval, 1),
                "lat_avg_ms":   round(stats["hist_flush_lat_sum"] / nh, 1),
                "lat_max_ms":   round(stats["hist_flush_lat_max"], 1),
                "slow_count":   stats["hist_flush_slow_count"],
                "ob_skipped":   stats["ob_hist_skipped"],
                "ob_skipped_per_s": round(stats["ob_hist_skipped"] / interval, 1),
            },
            latency={
                "parse_avg_us":      round(stats["parse_lat_sum"] / nm, 1),
                "parse_max_us":      round(stats["parse_lat_max"], 1),
                "buffer_age_avg_ms": round(stats["buffer_age_sum"] / n, 1),
                "buffer_age_max_ms": round(stats["buffer_age_max"], 1),
                # NOTE: Binance bookTicker has NO exchange timestamp.
                # e2e_lat fields are intentionally omitted for Binance.
            },
            state={
                "expire_keys_tracked": len(expire_set),
                "reconnects":          stats["reconnects"],
                "active_streams":      sum(1 for t in asyncio.all_tasks()
                                           if t.get_name().startswith("ws_")),
            },
        ))

        # Reset all stats including new fields
        for k in list(stats.keys()):
            stats[k] = 0
        # Reset float max fields to 0.0 (explicit for clarity)
        for k in ("flush_lat_max", "hist_flush_lat_max", "parse_lat_max",
                  "buffer_age_max", "e2e_lat_max"):
            stats[k] = 0.0


# ── Entry point ────────────────────────────────────────────────────────────

async def main():
    global flush_event, last_chunk_id

    flush_event   = asyncio.Event()
    last_chunk_id = int(time.time() / config.CHUNK_DURATION)

    spot_symbols = _load_symbols(
        f"{config.SUBSCRIBE_DIR}/binance/binance_spot.txt"
    )
    fut_symbols = _load_symbols(
        f"{config.SUBSCRIBE_DIR}/binance/binance_futures.txt"
    )

    if not spot_symbols and not fut_symbols:
        log.error(_evt("no_symbols", exchange="binance"))
        return

    redis_pool = aioredis.ConnectionPool.from_url(
        config.REDIS_URL, max_connections=10
    )
    redis = aioredis.Redis(connection_pool=redis_pool)

    log.info(_evt("collector_start", exchange="binance",
                  spot_symbols=len(spot_symbols), fut_symbols=len(fut_symbols),
                  history_enabled=config.HISTORY_ENABLED))

    tasks = [
        asyncio.create_task(task_md_spot(spot_symbols), name="ws_md_spot"),
        asyncio.create_task(task_md_fut(fut_symbols),   name="ws_md_fut"),
        asyncio.create_task(task_ob_spot(spot_symbols), name="ws_ob_spot"),
        asyncio.create_task(task_ob_fut(fut_symbols),   name="ws_ob_fut"),
        asyncio.create_task(task_fr(),                  name="ws_fr"),
        asyncio.create_task(task_flusher(redis),        name="flusher"),
        asyncio.create_task(task_hist_flusher(redis),   name="hist_flusher"),
        asyncio.create_task(task_metrics(),             name="metrics"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info(_evt("collector_stop", exchange="binance", reason="KeyboardInterrupt"))
    finally:
        for t in tasks:
            t.cancel()
        await redis.aclose()
        await redis_pool.aclose()
        log.info(_evt("collector_stop", exchange="binance", reason="stopped"))


if __name__ == "__main__":
    asyncio.run(main())
