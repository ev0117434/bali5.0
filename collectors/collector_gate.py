#!/usr/bin/env python3
# collectors/collector_gate.py
"""
BALI 5.0 — Gate.io collector.

WS endpoints:
  spot:    wss://api.gateio.ws/ws/v4/
  futures: wss://fx-ws.gateio.ws/v4/ws/usdt

Native format: BTC_USDT (underscore for both spot and futures)
Normalized:    BTCUSDT

NOTE: spot.order_book accepts one symbol per subscribe payload item.

Redis ключи:
  md:gate:spot:{symbol}     → {b, a, ts}
  md:gate:futures:{symbol}  → {b, a, ts}
  ob:gate:spot:{symbol}     → {b1..b10, b1q..b10q, a1..a10, a1q..a10q}
  ob:gate:futures:{symbol}  → same
  fr:gate:futures:{symbol}  → {fr, fr_ts}
"""

import asyncio
import json
import sys
import time
from pathlib import Path

import websockets
import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from logger_setup import setup_logger

log = setup_logger("collector_gate")

_WS_SPOT    = "wss://api.gateio.ws/ws/v4/"
_WS_FUTURES = "wss://fx-ws.gateio.ws/v4/ws/usdt"


# ── Symbol maps ─────────────────────────────────────────────────────────────

def _load_symbols(filepath: str) -> list[str]:
    p = Path(filepath)
    if not p.exists():
        log.error(f"Symbol file not found: {filepath}")
        return []
    return [l.strip() for l in p.read_text().splitlines() if l.strip()]


def _load_native_map(norm_file: str, native_file: str) -> dict[str, str]:
    """Load normalized→native mapping."""
    norms   = _load_symbols(norm_file)
    natives = _load_symbols(native_file)
    return dict(zip(norms, natives))


def _normalize(native: str) -> str:
    """BTC_USDT → BTCUSDT"""
    return native.replace("_", "")


# ── Parsers ────────────────────────────────────────────────────────────────

def parse_md_spot(raw: str):
    """Parse Gate.io spot.book_ticker → (symbol, bid, ask, ts_ms) or None."""
    try:
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe"):
            return None
        result = msg.get("result")
        if not result:
            return None
        channel = msg.get("channel", "")
        if channel != "spot.book_ticker":
            return None
        symbol = _normalize(result["s"])
        bid    = result.get("b", "")
        ask    = result.get("a", "")
        if not bid or not ask:
            return None
        ts_ms = int(result.get("t", int(time.time() * 1000)))
        return symbol, bid, ask, ts_ms
    except Exception:
        return None


def parse_md_fut(raw: str):
    """Parse Gate.io futures.book_ticker → (symbol, bid, ask, ts_ms) or None."""
    try:
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe"):
            return None
        result = msg.get("result")
        if not result:
            return None
        channel = msg.get("channel", "")
        if channel != "futures.book_ticker":
            return None
        symbol = _normalize(result.get("s", ""))
        bid    = result.get("b", "")
        ask    = result.get("a", "")
        if not symbol or not bid or not ask:
            return None
        ts_ms = int(result.get("t", int(time.time() * 1000)))
        return symbol, bid, ask, ts_ms
    except Exception:
        return None


def parse_ob_spot(raw: str):
    """Parse Gate.io spot.order_book → (symbol, bids, asks, ts_ms) or None."""
    try:
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe"):
            return None
        result = msg.get("result")
        if not result:
            return None
        channel = msg.get("channel", "")
        if channel != "spot.order_book":
            return None
        symbol = _normalize(result.get("s", ""))
        bids   = result.get("bids", [])[:10]
        asks   = result.get("asks", [])[:10]
        if not symbol:
            return None
        ts_ms = int(result.get("t", int(time.time() * 1000)))
        return symbol, bids, asks, ts_ms
    except Exception:
        return None


def parse_ob_fut(raw: str):
    """Parse Gate.io futures.order_book_update → (symbol, bids, asks, ts_ms) or None."""
    try:
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe"):
            return None
        result = msg.get("result")
        if not result:
            return None
        channel = msg.get("channel", "")
        if channel != "futures.order_book_update":
            return None
        symbol = _normalize(result.get("s", ""))
        bids   = [[b["p"], b["s"]] for b in result.get("b", [])[:10]]
        asks   = [[a["p"], a["s"]] for a in result.get("a", [])[:10]]
        if not symbol:
            return None
        ts_ms = int(result.get("t", int(time.time() * 1000)))
        return symbol, bids, asks, ts_ms
    except Exception:
        return None


def parse_fr(raw: str):
    """Parse Gate.io futures.tickers → (symbol, rate, fr_ts_str) or None."""
    try:
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe"):
            return None
        result = msg.get("result", [])
        if not result:
            return None
        r = result[0] if isinstance(result, list) else result
        symbol = _normalize(r.get("contract", ""))
        rate   = r.get("funding_rate", "")
        # funding_next_apply is in SECONDS — convert to ms
        fr_ts_sec = r.get("funding_next_apply", 0)
        if not rate or not symbol:
            return None
        fr_ts_ms = str(int(fr_ts_sec) * 1000)
        return symbol, str(rate), fr_ts_ms
    except Exception:
        return None


# ── Shared state ───────────────────────────────────────────────────────────

batch_buffer: dict = {}
hist_buffer:  list = []
cmd_counter:  int  = 0
flush_event         = None
expire_set:   set  = set()
last_chunk_id: int = 0

stats: dict = {
    "md_msgs": 0, "ob_msgs": 0, "fr_msgs": 0,
    "flushes": 0, "flush_lat_sum": 0.0, "flush_lat_max": 0.0,
    "batch_sum": 0, "hist_cmds": 0, "reconnects": 0,
}


def write_md_to_buffer(symbol: str, bid: str, ask: str, ts_ms: int, market: str):
    global cmd_counter
    key = f"md:gate:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:gate:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))
        cmd_counter += 2
    else:
        cmd_counter += 1


def write_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    global cmd_counter
    key    = f"ob:gate:{market}:{symbol}"
    fields = {}
    for i, (price, qty) in enumerate(bids[:10], 1):
        fields[f"b{i}"]  = str(price)
        fields[f"b{i}q"] = str(qty)
    for i, (price, qty) in enumerate(asks[:10], 1):
        fields[f"a{i}"]  = str(price)
        fields[f"a{i}q"] = str(qty)
    batch_buffer[key] = fields
    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"ob:hist:gate:{market}:{symbol}:{chunk_id}"
        bid_parts = [f"{p},{q}" for p, q in bids[:10]]
        ask_parts = [f"{p},{q}" for p, q in asks[:10]]
        hist_buffer.append((hist_key, ",".join(bid_parts + ask_parts + [str(ts_ms)])))
        cmd_counter += 2
    else:
        cmd_counter += 1


def write_fr_to_buffer(symbol: str, rate: str, fr_ts_ms: str):
    global cmd_counter
    key   = f"fr:gate:futures:{symbol}"
    ts_ms = int(time.time() * 1000)
    batch_buffer[key] = {"fr": rate, "fr_ts": fr_ts_ms}
    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:gate:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))
        cmd_counter += 2
    else:
        cmd_counter += 1


# ── WS task ────────────────────────────────────────────────────────────────

async def _ws_task(url: str, label: str, subscribe_fn, parse_fn, write_fn,
                   market: str, stat_key: str):
    """
    Connect to url, call subscribe_fn(ws) to subscribe,
    then parse/write in a loop.
    """
    global cmd_counter
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(f"[{label}] Connecting: {url}")
            async with websockets.connect(
                url,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(f"[{label}] Connected")
                backoff = config.WS_RECONNECT_INIT

                await subscribe_fn(ws)

                async for raw in ws:
                    result = parse_fn(raw)
                    if result is not None:
                        write_fn(*result, market=market)
                        stats[stat_key] += 1
                    if cmd_counter >= config.BATCH_MAX_COMMANDS:
                        flush_event.set()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(f"[{label}] WS error: {type(exc).__name__}: {exc}. "
                        f"Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


async def task_md_spot(native_spot: list[str]):
    chunk_size = config.WS_CHUNK_GATE

    async def subscribe(ws):
        for i in range(0, len(native_spot), chunk_size):
            chunk = native_spot[i:i + chunk_size]
            msg = {
                "time": int(time.time()),
                "channel": "spot.book_ticker",
                "event": "subscribe",
                "payload": chunk,
            }
            await ws.send(json.dumps(msg))

    await _ws_task(_WS_SPOT, "md_spot", subscribe, parse_md_spot,
                   write_md_to_buffer, market="spot", stat_key="md_msgs")


async def task_md_fut(native_fut: list[str]):
    chunk_size = config.WS_CHUNK_GATE

    async def subscribe(ws):
        for i in range(0, len(native_fut), chunk_size):
            chunk = native_fut[i:i + chunk_size]
            msg = {
                "time": int(time.time()),
                "channel": "futures.book_ticker",
                "event": "subscribe",
                "payload": chunk,
            }
            await ws.send(json.dumps(msg))

    await _ws_task(_WS_FUTURES, "md_fut", subscribe, parse_md_fut,
                   write_md_to_buffer, market="futures", stat_key="md_msgs")


async def task_ob_spot(native_spot: list[str]):
    """
    spot.order_book: subscribe one symbol at a time.
    Use chunked sends to avoid flooding.
    """
    global cmd_counter
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            async with websockets.connect(
                _WS_SPOT,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(f"[ob_spot] Connected. symbols={len(native_spot)}")
                backoff = config.WS_RECONNECT_INIT

                # Subscribe each symbol individually
                for sym in native_spot:
                    msg = {
                        "time": int(time.time()),
                        "channel": "spot.order_book",
                        "event": "subscribe",
                        "payload": [sym, "10", "100ms"],
                    }
                    await ws.send(json.dumps(msg))
                    await asyncio.sleep(0.01)  # slight pacing

                async for raw in ws:
                    result = parse_ob_spot(raw)
                    if result is not None:
                        write_ob_to_buffer(*result, market="spot")
                        stats["ob_msgs"] += 1
                    if cmd_counter >= config.BATCH_MAX_COMMANDS:
                        flush_event.set()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(f"[ob_spot] WS error: {type(exc).__name__}: {exc}. "
                        f"Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


async def task_ob_fut(native_fut: list[str]):
    chunk_size = config.WS_CHUNK_GATE

    async def subscribe(ws):
        for i in range(0, len(native_fut), chunk_size):
            chunk = native_fut[i:i + chunk_size]
            for sym in chunk:
                msg = {
                    "time": int(time.time()),
                    "channel": "futures.order_book_update",
                    "event": "subscribe",
                    "payload": [sym, "100ms", "10"],
                }
                await ws.send(json.dumps(msg))

    await _ws_task(_WS_FUTURES, "ob_fut", subscribe, parse_ob_fut,
                   write_ob_to_buffer, market="futures", stat_key="ob_msgs")


async def task_fr(native_fut: list[str]):
    chunk_size = config.WS_CHUNK_GATE

    async def subscribe(ws):
        for i in range(0, len(native_fut), chunk_size):
            chunk = native_fut[i:i + chunk_size]
            msg = {
                "time": int(time.time()),
                "channel": "futures.tickers",
                "event": "subscribe",
                "payload": chunk,
            }
            await ws.send(json.dumps(msg))

    global cmd_counter
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            async with websockets.connect(
                _WS_FUTURES,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(f"[fr] Connected. symbols={len(native_fut)}")
                backoff = config.WS_RECONNECT_INIT
                await subscribe(ws)

                async for raw in ws:
                    result = parse_fr(raw)
                    if result is not None:
                        write_fr_to_buffer(*result)
                        stats["fr_msgs"] += 1
                    if cmd_counter >= config.BATCH_MAX_COMMANDS:
                        flush_event.set()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(f"[fr] WS error: {type(exc).__name__}: {exc}. "
                        f"Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


# ── Flusher / Metrics ──────────────────────────────────────────────────────

async def task_flusher(redis: aioredis.Redis):
    global cmd_counter, last_chunk_id

    while True:
        try:
            await asyncio.wait_for(
                flush_event.wait(),
                timeout=config.BATCH_FLUSH_INTERVAL_MS / 1000,
            )
        except asyncio.TimeoutError:
            pass
        flush_event.clear()

        if not batch_buffer and not hist_buffer:
            continue

        current_batch = batch_buffer.copy()
        current_hist  = hist_buffer.copy()
        batch_buffer.clear()
        hist_buffer.clear()
        cmd_counter = 0

        if not current_batch and not current_hist:
            continue

        t_start = time.monotonic()

        new_chunk_id = int(time.time() / config.CHUNK_DURATION)
        if new_chunk_id != last_chunk_id:
            expire_set.clear()
            log.info(f"Chunk changed: {last_chunk_id} → {new_chunk_id}")
            last_chunk_id = new_chunk_id

        pipe = redis.pipeline(transaction=False)
        expire_cmds = 0

        for key, fields in current_batch.items():
            pipe.hset(key, mapping=fields)

        for hist_key, line in current_hist:
            pipe.lpush(hist_key, line)
            if hist_key not in expire_set:
                pipe.expire(hist_key, config.CHUNK_TTL)
                expire_set.add(hist_key)
                expire_cmds += 1

        try:
            await pipe.execute()
        except Exception as exc:
            log.error(f"Redis pipeline error: {exc}")

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


async def task_metrics():
    interval = config.METRICS_LOG_INTERVAL
    while True:
        await asyncio.sleep(interval)
        n = stats["flushes"] or 1
        log.info(
            f"METRICS | "
            f"md={stats['md_msgs'] / interval:.0f}msg/s "
            f"ob={stats['ob_msgs'] / interval:.0f}msg/s "
            f"fr={stats['fr_msgs'] / interval:.0f}msg/s "
            f"hist_writes={stats['hist_cmds'] / interval:.0f}/s "
            f"flush_lat avg={stats['flush_lat_sum'] / n:.1f}ms "
            f"max={stats['flush_lat_max']:.1f}ms "
            f"batch_avg={stats['batch_sum'] / n:.0f} "
            f"expire_set_size={len(expire_set)} "
            f"reconnects={stats['reconnects']}"
        )
        stats["md_msgs"] = stats["ob_msgs"] = stats["fr_msgs"] = 0
        stats["hist_cmds"] = 0
        stats["flushes"] = stats["flush_lat_sum"] = stats["flush_lat_max"] = 0
        stats["batch_sum"] = 0


# ── Entry point ────────────────────────────────────────────────────────────

async def main():
    global flush_event, last_chunk_id

    flush_event   = asyncio.Event()
    last_chunk_id = int(time.time() / config.CHUNK_DURATION)

    norm_spot = _load_symbols(f"{config.SUBSCRIBE_DIR}/gate/gate_spot.txt")
    norm_fut  = _load_symbols(f"{config.SUBSCRIBE_DIR}/gate/gate_futures.txt")

    spot_map = _load_native_map(
        f"{config.DICTIONARIES_DIR}/gate/data/gate_spot.txt",
        f"{config.DICTIONARIES_DIR}/gate/data/gate_spot_native.txt",
    )
    fut_map = _load_native_map(
        f"{config.DICTIONARIES_DIR}/gate/data/gate_futures.txt",
        f"{config.DICTIONARIES_DIR}/gate/data/gate_futures_native.txt",
    )

    native_spot = [spot_map[s] for s in norm_spot if s in spot_map]
    native_fut  = [fut_map[s]  for s in norm_fut  if s in fut_map]

    log.info(
        f"Starting collector_gate | "
        f"spot={len(native_spot)} fut={len(native_fut)} | "
        f"HISTORY_ENABLED={config.HISTORY_ENABLED}"
    )

    redis_pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, max_connections=10)
    redis      = aioredis.Redis(connection_pool=redis_pool)

    tasks = [
        asyncio.create_task(task_md_spot(native_spot)),
        asyncio.create_task(task_md_fut(native_fut)),
        asyncio.create_task(task_ob_spot(native_spot)),
        asyncio.create_task(task_ob_fut(native_fut)),
        asyncio.create_task(task_fr(native_fut)),
        asyncio.create_task(task_flusher(redis)),
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
        log.info("collector_gate stopped")


if __name__ == "__main__":
    asyncio.run(main())
