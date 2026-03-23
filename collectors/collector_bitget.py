#!/usr/bin/env python3
# collectors/collector_bitget.py
"""
BALI 5.0 — Bitget collector.

Single WS endpoint: wss://ws.bitget.com/v2/ws/public
MD: ticker channel (SPOT + USDT-FUTURES)
OB: books channel (take [:10]) for SPOT + USDT-FUTURES
FR: extracted from futures ticker alongside MD

Native format = normalized (BTCUSDT for both spot and futures)

Redis ключи:
  md:bitget:spot:{symbol}     → {b, a, ts}
  md:bitget:futures:{symbol}  → {b, a, ts}
  ob:bitget:spot:{symbol}     → {b1..b10, b1q..b10q, a1..a10, a1q..a10q}
  ob:bitget:futures:{symbol}  → same
  fr:bitget:futures:{symbol}  → {fr, fr_ts}
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

log = setup_logger("collector_bitget")

_WS_URL = "wss://ws.bitget.com/v2/ws/public"


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
    """Parse Bitget ticker message → (symbol, bid, ask, ts_ms) or None."""
    try:
        if raw == "pong":
            return None
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe", "error"):
            return None
        data_list = msg.get("data", [])
        if not data_list:
            return None
        d      = data_list[0]
        symbol = d.get("instId", "").upper()
        bid    = d.get("bidPr", "")
        ask    = d.get("askPr", "")
        ts_str = d.get("ts", "")
        if not symbol or not bid or not ask:
            return None
        ts_ms = int(ts_str) if ts_str else int(time.time() * 1000)
        return symbol, bid, ask, ts_ms
    except Exception:
        return None


def parse_fr_from_ticker(raw: str):
    """Extract FR from Bitget futures ticker → (symbol, rate, fr_ts_str) or None."""
    try:
        if raw == "pong":
            return None
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe", "error"):
            return None
        data_list = msg.get("data", [])
        if not data_list:
            return None
        d     = data_list[0]
        fr    = d.get("fundingRate", "")
        fr_ts = d.get("nextSettleTime", "")
        if not fr:
            return None
        symbol = d.get("instId", "").upper()
        return symbol, str(fr), str(fr_ts)
    except Exception:
        return None


def parse_ob(raw: str):
    """Parse Bitget books message → (symbol, bids, asks, ts_ms) or None."""
    try:
        if raw == "pong":
            return None
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe", "error"):
            return None
        data_list = msg.get("data", [])
        if not data_list:
            return None
        d      = data_list[0]
        bids   = [[b[0], b[1]] for b in d.get("bids", [])[:10]]
        asks   = [[a[0], a[1]] for a in d.get("asks", [])[:10]]
        arg    = msg.get("arg", {})
        symbol = arg.get("instId", "").upper()
        if not symbol or (not bids and not asks):
            return None
        ts_str = d.get("ts", "")
        ts_ms  = int(ts_str) if ts_str else int(time.time() * 1000)
        return symbol, bids, asks, ts_ms
    except Exception:
        return None


# ── Shared state ───────────────────────────────────────────────────────────

batch_buffer: dict = {}
hist_buffer:  list = []
cmd_counter:  int  = 0
flush_event         = None
expire_set:   set  = set()
last_chunk_id: int = 0
ob_hist_last_ts: dict = {}  # hist_key → last write ts_ms (OB 10 Hz gate)

stats: dict = {
    "md_msgs": 0, "ob_msgs": 0, "fr_msgs": 0,
    "flushes": 0, "flush_lat_sum": 0.0, "flush_lat_max": 0.0,
    "batch_sum": 0,
    "hist_flushes": 0, "hist_flush_lat_sum": 0.0, "hist_flush_lat_max": 0.0,
    "hist_cmds": 0, "ob_hist_skipped": 0,
    "reconnects": 0,
}


def write_md_to_buffer(symbol: str, bid: str, ask: str, ts_ms: int, market: str):
    global cmd_counter
    key = f"md:bitget:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    cmd_counter += 1

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:bitget:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))


def write_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    global cmd_counter
    key    = f"ob:bitget:{market}:{symbol}"
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
        hist_key = f"ob:hist:bitget:{market}:{symbol}:{chunk_id}"
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
    key   = f"fr:bitget:futures:{symbol}"
    ts_ms = int(time.time() * 1000)
    batch_buffer[key] = {"fr": rate, "fr_ts": fr_ts_ms}
    cmd_counter += 1

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:bitget:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))


# ── Heartbeat ──────────────────────────────────────────────────────────────

async def _heartbeat(ws, label: str):
    while True:
        await asyncio.sleep(config.HEARTBEAT_BITGET)
        try:
            await ws.send("ping")
        except Exception as exc:
            log.debug(f"[{label}] heartbeat error: {exc}")
            return


# ── WS task ────────────────────────────────────────────────────────────────

async def _ws_task(label: str, args_list: list[dict], parse_fn, write_fn,
                   market: str, stat_key: str, also_parse_fr: bool = False):
    global cmd_counter
    backoff = config.WS_RECONNECT_INIT
    chunk_size = config.WS_CHUNK_BITGET

    while True:
        try:
            log.info(f"[{label}] Connecting: {_WS_URL} args={len(args_list)}")
            async with websockets.connect(
                _WS_URL,
                ping_interval=None,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(f"[{label}] Connected")
                backoff = config.WS_RECONNECT_INIT

                for i in range(0, len(args_list), chunk_size):
                    chunk = args_list[i:i + chunk_size]
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))

                hb_task = asyncio.create_task(_heartbeat(ws, label))
                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        result = parse_fn(raw)
                        if result is not None:
                            write_fn(*result, market=market)
                            stats[stat_key] += 1

                        if also_parse_fr:
                            fr_result = parse_fr_from_ticker(raw)
                            if fr_result is not None:
                                write_fr_to_buffer(*fr_result)
                                stats["fr_msgs"] += 1

                        if cmd_counter >= config.BATCH_MAX_COMMANDS:
                            flush_event.set()
                finally:
                    hb_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(f"[{label}] WS error: {type(exc).__name__}: {exc}. "
                        f"Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


async def task_md_spot(symbols: list[str]):
    args = [{"instType": "SPOT", "channel": "ticker", "instId": s}
            for s in symbols]
    await _ws_task("md_spot", args, parse_md, write_md_to_buffer,
                   market="spot", stat_key="md_msgs")


async def task_md_fut(symbols: list[str]):
    # also_parse_fr=True — futures ticker contains fundingRate
    args = [{"instType": "USDT-FUTURES", "channel": "ticker", "instId": s}
            for s in symbols]
    await _ws_task("md_fut", args, parse_md, write_md_to_buffer,
                   market="futures", stat_key="md_msgs", also_parse_fr=True)


async def task_ob_spot(symbols: list[str]):
    args = [{"instType": "SPOT", "channel": "books", "instId": s}
            for s in symbols]
    await _ws_task("ob_spot", args, parse_ob, write_ob_to_buffer,
                   market="spot", stat_key="ob_msgs")


async def task_ob_fut(symbols: list[str]):
    args = [{"instType": "USDT-FUTURES", "channel": "books", "instId": s}
            for s in symbols]
    await _ws_task("ob_fut", args, parse_ob, write_ob_to_buffer,
                   market="futures", stat_key="ob_msgs")


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

    spot_symbols = _load_symbols(f"{config.SUBSCRIBE_DIR}/bitget/bitget_spot.txt")
    fut_symbols  = _load_symbols(f"{config.SUBSCRIBE_DIR}/bitget/bitget_futures.txt")

    if not spot_symbols and not fut_symbols:
        log.error("No symbols loaded — exiting")
        return

    log.info(
        f"Starting collector_bitget | "
        f"spot={len(spot_symbols)} fut={len(fut_symbols)} | "
        f"HISTORY_ENABLED={config.HISTORY_ENABLED}"
    )

    redis_pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, max_connections=10)
    redis      = aioredis.Redis(connection_pool=redis_pool)

    tasks = [
        asyncio.create_task(task_md_spot(spot_symbols)),
        asyncio.create_task(task_md_fut(fut_symbols)),
        asyncio.create_task(task_ob_spot(spot_symbols)),
        asyncio.create_task(task_ob_fut(fut_symbols)),
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
        log.info("collector_bitget stopped")


if __name__ == "__main__":
    asyncio.run(main())
