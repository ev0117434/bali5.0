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
        log.error(f"Symbol file not found: {filepath}")
        return []
    lines = [l.strip().upper() for l in p.read_text().splitlines() if l.strip()]
    log.info(f"Loaded {len(lines)} symbols from {filepath}")
    return lines


# ── Parsers ────────────────────────────────────────────────────────────────

def parse_md(raw: str):
    """Parse bookTicker message → (symbol, bid, ask, ts_ms) or None."""
    try:
        msg = json.loads(raw)
        data = msg.get("data", msg)
        # Skip non-bookTicker messages (e.g. combined stream control messages)
        if "s" not in data or "b" not in data:
            return None
        symbol = data["s"]
        bid    = data["b"]
        ask    = data["a"]
        ts_ms  = int(time.time() * 1000)  # Binance bookTicker has no ts
        return symbol, bid, ask, ts_ms
    except Exception:
        return None


def parse_ob(raw: str):
    """Parse depth10@100ms message → (symbol, bids, asks, ts_ms) or None."""
    try:
        msg  = json.loads(raw)
        data = msg.get("data", msg)
        if "bids" not in data:
            return None
        stream = msg.get("stream", "")
        symbol = stream.split("@")[0].upper() if stream else data.get("s", "")
        bids   = data["bids"][:10]
        asks   = data["asks"][:10]
        ts_ms  = int(time.time() * 1000)
        return symbol, bids, asks, ts_ms
    except Exception:
        return None


def parse_fr(raw: str):
    """
    Parse markPrice message (single OR arr).
    Returns list of (symbol, rate, fr_ts_str) tuples.
    """
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
        return []


# ── Shared mutable state ───────────────────────────────────────────────────
# (module-level, safe in single asyncio event loop)

batch_buffer: dict  = {}    # key → {field: value}
hist_buffer:  list  = []    # [(hist_key, line_str), ...]
cmd_counter:  int   = 0
flush_event         = None  # set in main()
expire_set:   set   = set()
last_chunk_id: int  = 0
ob_hist_last_ts: dict = {}  # hist_key → last write ts_ms (OB 10 Hz gate)

stats: dict = {
    "md_msgs": 0, "ob_msgs": 0, "fr_msgs": 0,
    "flushes": 0, "flush_lat_sum": 0.0, "flush_lat_max": 0.0,
    "batch_sum": 0,
    "hist_flushes": 0, "hist_flush_lat_sum": 0.0, "hist_flush_lat_max": 0.0,
    "hist_cmds": 0, "ob_hist_skipped": 0,
    "reconnects": 0,
}


# ── Buffer writers ─────────────────────────────────────────────────────────

def write_md_to_buffer(symbol: str, bid: str, ask: str, ts_ms: int, market: str):
    global cmd_counter
    key = f"md:binance:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    cmd_counter += 1

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:binance:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))


def write_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    global cmd_counter
    key    = f"ob:binance:{market}:{symbol}"
    fields = {}
    for i, (price, qty) in enumerate(bids[:10], 1):
        fields[f"b{i}"]  = price
        fields[f"b{i}q"] = qty
    for i, (price, qty) in enumerate(asks[:10], 1):
        fields[f"a{i}"]  = price
        fields[f"a{i}q"] = qty
    batch_buffer[key] = fields
    cmd_counter += 1

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
    global cmd_counter
    key    = f"fr:binance:futures:{symbol}"
    ts_ms  = int(time.time() * 1000)
    batch_buffer[key] = {"fr": rate, "fr_ts": fr_ts_ms}
    cmd_counter += 1

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:binance:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))


# ── WS tasks ───────────────────────────────────────────────────────────────

async def _ws_recv_loop(url: str, parser, label: str):
    """
    Connect to url, receive messages forever, call parser.
    parser returns None / single result / list of results.
    Each result is passed to the appropriate buffer writer via label.
    """
    global cmd_counter
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(f"[{label}] Connecting: {url[:80]}...")
            async with websockets.connect(
                url,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(f"[{label}] Connected")
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

                    if cmd_counter >= config.BATCH_MAX_COMMANDS:
                        flush_event.set()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(
                f"[{label}] WS error: {type(exc).__name__}: {exc}. "
                f"Reconnecting in {backoff}s..."
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


async def task_md_spot(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@bookTicker" for s in chunk)
        url = _WS_SPOT + streams
        coros.append(_ws_recv_loop(url, parse_md, "md_spot"))
    await asyncio.gather(*coros)


async def task_md_fut(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@bookTicker" for s in chunk)
        url = _WS_FUTURES + streams
        coros.append(_ws_recv_loop(url, parse_md, "md_fut"))
    await asyncio.gather(*coros)


async def task_ob_spot(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@depth10@100ms" for s in chunk)
        url = _WS_SPOT + streams
        coros.append(_ws_recv_loop(url, parse_ob, "ob_spot"))
    await asyncio.gather(*coros)


async def task_ob_fut(symbols: list[str]):
    chunks = [symbols[i:i + config.WS_CHUNK_BINANCE]
              for i in range(0, len(symbols), config.WS_CHUNK_BINANCE)]
    coros = []
    for chunk in chunks:
        streams = "/".join(f"{s.lower()}@depth10@100ms" for s in chunk)
        url = _WS_FUTURES + streams
        coros.append(_ws_recv_loop(url, parse_ob, "ob_fut"))
    await asyncio.gather(*coros)


async def task_fr():
    """FR via !markPrice@arr@1s — single stream for all futures symbols."""
    url = _WS_FUTURES + "!markPrice@arr@1s"
    await _ws_recv_loop(url, parse_fr, "fr")


# ── Flusher (primary: hset only) ───────────────────────────────────────────

async def task_flusher(redis: aioredis.Redis):
    global cmd_counter

    while True:
        try:
            await asyncio.wait_for(
                flush_event.wait(),
                timeout=config.BATCH_FLUSH_INTERVAL_MS / 1000,
            )
        except asyncio.TimeoutError:
            pass
        flush_event.clear()

        if not batch_buffer:
            continue

        current_batch = batch_buffer.copy()
        batch_buffer.clear()
        cmd_counter = 0

        t_start = time.monotonic()
        pipe = redis.pipeline(transaction=False)
        for key, fields in current_batch.items():
            pipe.hset(key, mapping=fields)

        try:
            await pipe.execute()
        except Exception as exc:
            log.error(f"Redis pipeline error: {exc}")

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["flushes"]       += 1
        stats["flush_lat_sum"] += flush_lat_ms
        stats["flush_lat_max"]  = max(stats["flush_lat_max"], flush_lat_ms)
        stats["batch_sum"]     += len(current_batch)

        if flush_lat_ms > 50:
            log.warning(f"Slow primary flush: {flush_lat_ms:.1f}ms keys={len(current_batch)}")


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
            log.info(f"Chunk changed: {last_chunk_id} → {new_chunk_id}")
            last_chunk_id = new_chunk_id

        t_start = time.monotonic()
        pipe = redis.pipeline(transaction=False)
        expire_cmds = 0
        for hist_key, line in current_hist:
            pipe.lpush(hist_key, line)
            if hist_key not in expire_set:
                pipe.expire(hist_key, config.CHUNK_TTL)
                expire_set.add(hist_key)
                expire_cmds += 1

        try:
            await pipe.execute()
        except Exception as exc:
            log.error(f"Redis hist pipeline error: {exc}")

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["hist_flushes"]       += 1
        stats["hist_flush_lat_sum"] += flush_lat_ms
        stats["hist_flush_lat_max"]  = max(stats["hist_flush_lat_max"], flush_lat_ms)
        stats["hist_cmds"]          += len(current_hist)

        if flush_lat_ms > 100:
            log.warning(
                f"Slow hist flush: {flush_lat_ms:.1f}ms "
                f"cmds={len(current_hist)} expire_new={expire_cmds}"
            )


# ── Metrics ────────────────────────────────────────────────────────────────

async def task_metrics():
    interval = config.METRICS_LOG_INTERVAL
    while True:
        await asyncio.sleep(interval)
        n  = stats["flushes"] or 1
        nh = stats["hist_flushes"] or 1
        log.info(
            f"METRICS | "
            f"md={stats['md_msgs'] / interval:.0f}msg/s "
            f"ob={stats['ob_msgs'] / interval:.0f}msg/s "
            f"fr={stats['fr_msgs'] / interval:.0f}msg/s | "
            f"flush_lat avg={stats['flush_lat_sum'] / n:.1f}ms "
            f"max={stats['flush_lat_max']:.1f}ms "
            f"batch_avg={stats['batch_sum'] / n:.0f} | "
            f"hist_writes={stats['hist_cmds'] / interval:.0f}/s "
            f"hist_flush_lat avg={stats['hist_flush_lat_sum'] / nh:.1f}ms "
            f"max={stats['hist_flush_lat_max']:.1f}ms "
            f"ob_skip={stats['ob_hist_skipped'] / interval:.0f}/s | "
            f"expire_set={len(expire_set)} "
            f"reconnects={stats['reconnects']}"
        )
        stats["md_msgs"] = stats["ob_msgs"] = stats["fr_msgs"] = 0
        stats["hist_cmds"] = stats["ob_hist_skipped"] = 0
        stats["flushes"] = stats["flush_lat_sum"] = stats["flush_lat_max"] = 0
        stats["batch_sum"] = 0
        stats["hist_flushes"] = stats["hist_flush_lat_sum"] = stats["hist_flush_lat_max"] = 0


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
        log.error("No symbols loaded — exiting")
        return

    redis_pool = aioredis.ConnectionPool.from_url(
        config.REDIS_URL, max_connections=10
    )
    redis = aioredis.Redis(connection_pool=redis_pool)

    log.info(
        f"Starting collector_binance | "
        f"spot={len(spot_symbols)} fut={len(fut_symbols)} | "
        f"HISTORY_ENABLED={config.HISTORY_ENABLED}"
    )

    tasks = [
        asyncio.create_task(task_md_spot(spot_symbols)),
        asyncio.create_task(task_md_fut(fut_symbols)),
        asyncio.create_task(task_ob_spot(spot_symbols)),
        asyncio.create_task(task_ob_fut(fut_symbols)),
        asyncio.create_task(task_fr()),
        asyncio.create_task(task_flusher(redis)),
        asyncio.create_task(task_hist_flusher(redis)),
        asyncio.create_task(task_metrics()),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — shutting down")
    finally:
        for t in tasks:
            t.cancel()
        await redis.aclose()
        await redis_pool.aclose()
        log.info("collector_binance stopped")


if __name__ == "__main__":
    asyncio.run(main())
