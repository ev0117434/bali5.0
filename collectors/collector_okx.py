#!/usr/bin/env python3
# collectors/collector_okx.py
"""
BALI 5.0 — OKX collector.

Single WS endpoint for all channels: wss://ws.okx.com:8443/ws/v5/public
MD: bbo-tbt channel (best bid/offer tick-by-tick)
OB: books channel (snapshot + updates, take [:10])
FR: funding-rate channel

Native symbols: BTC-USDT (spot), BTC-USDT-SWAP (futures)
Normalized:     BTCUSDT

Redis ключи:
  md:okx:spot:{symbol}     → {b, a, ts}
  md:okx:futures:{symbol}  → {b, a, ts}
  ob:okx:spot:{symbol}     → {b1..b10, b1q..b10q, a1..a10, a1q..a10q}
  ob:okx:futures:{symbol}  → same
  fr:okx:futures:{symbol}  → {fr, fr_ts}
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

log = setup_logger("collector_okx")

_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


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
    if len(norms) != len(natives):
        log.warning(
            f"Symbol count mismatch: norm={len(norms)} native={len(natives)} "
            f"({norm_file} vs {native_file})"
        )
    return dict(zip(norms, natives))


def _normalize(inst_id: str) -> str:
    """BTC-USDT → BTCUSDT, BTC-USDT-SWAP → BTCUSDT"""
    return inst_id.replace("-SWAP", "").replace("-", "")


# ── Parsers ────────────────────────────────────────────────────────────────

def parse_md(raw: str):
    """Parse OKX bbo-tbt message → (symbol, bid, ask, ts_ms) or None."""
    try:
        msg = json.loads(raw)
        if raw.strip() == "pong":
            return None
        if "data" not in msg:
            return None
        arg = msg.get("arg", {})
        if arg.get("channel") != "bbo-tbt":
            return None
        inst_id = arg.get("instId", "")
        symbol  = _normalize(inst_id)
        d       = msg["data"][0]
        bid = d["bids"][0][0] if d.get("bids") else ""
        ask = d["asks"][0][0] if d.get("asks") else ""
        if not bid or not ask:
            return None
        ts_ms = int(d["ts"])
        return symbol, bid, ask, ts_ms
    except Exception:
        return None


def parse_ob(raw: str):
    """Parse OKX books message (snapshot or update) → (symbol, bids, asks, ts_ms) or None."""
    try:
        msg = json.loads(raw)
        if "data" not in msg:
            return None
        action = msg.get("action", "")
        if action not in ("snapshot", "update"):
            return None
        arg     = msg.get("arg", {})
        inst_id = arg.get("instId", "")
        symbol  = _normalize(inst_id)
        d       = msg["data"][0]
        bids = [[b[0], b[1]] for b in d.get("bids", [])[:10]]
        asks = [[a[0], a[1]] for a in d.get("asks", [])[:10]]
        if not bids and not asks:
            return None
        ts_ms = int(d["ts"])
        return symbol, bids, asks, ts_ms
    except Exception:
        return None


def parse_fr(raw: str):
    """Parse OKX funding-rate message → (symbol, rate, fr_ts_str) or None."""
    try:
        msg = json.loads(raw)
        if "data" not in msg:
            return None
        arg = msg.get("arg", {})
        if arg.get("channel") != "funding-rate":
            return None
        d      = msg["data"][0]
        symbol = _normalize(d["instId"])
        rate   = d.get("fundingRate", "")
        fr_ts  = d.get("nextFundingTime", "")
        if not rate:
            return None
        return symbol, str(rate), str(fr_ts)
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
    key = f"md:okx:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:okx:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))
        cmd_counter += 2
    else:
        cmd_counter += 1


def write_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    global cmd_counter
    key    = f"ob:okx:{market}:{symbol}"
    fields = {}
    for i, (price, qty) in enumerate(bids[:10], 1):
        fields[f"b{i}"]  = price
        fields[f"b{i}q"] = qty
    for i, (price, qty) in enumerate(asks[:10], 1):
        fields[f"a{i}"]  = price
        fields[f"a{i}q"] = qty
    batch_buffer[key] = fields
    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"ob:hist:okx:{market}:{symbol}:{chunk_id}"
        bid_parts = [f"{p},{q}" for p, q in bids[:10]]
        ask_parts = [f"{p},{q}" for p, q in asks[:10]]
        hist_buffer.append((hist_key, ",".join(bid_parts + ask_parts + [str(ts_ms)])))
        cmd_counter += 2
    else:
        cmd_counter += 1


def write_fr_to_buffer(symbol: str, rate: str, fr_ts_ms: str):
    global cmd_counter
    key   = f"fr:okx:futures:{symbol}"
    ts_ms = int(time.time() * 1000)
    batch_buffer[key] = {"fr": rate, "fr_ts": fr_ts_ms}
    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:okx:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))
        cmd_counter += 2
    else:
        cmd_counter += 1


# ── Heartbeat ──────────────────────────────────────────────────────────────

async def _heartbeat(ws, label: str):
    while True:
        await asyncio.sleep(config.HEARTBEAT_OKX)
        try:
            await ws.send("ping")
        except Exception as exc:
            log.debug(f"[{label}] heartbeat error: {exc}")
            return


# ── WS task (all channels on one connection) ───────────────────────────────

async def _ws_channel_task(label: str, args_list: list[dict], parse_fn, write_fn,
                           market: str, stat_key: str):
    """
    Subscribe to args_list on OKX WS. Re-subscribe on reconnect.
    parse_fn returns (symbol, ...) or None.
    write_fn(*result, market=market).
    """
    global cmd_counter
    backoff = config.WS_RECONNECT_INIT

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

                # Send subscribe in chunks of WS_CHUNK_OKX
                chunk_size = config.WS_CHUNK_OKX
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


async def task_md_spot(native_spot_list: list[str]):
    args = [{"channel": "bbo-tbt", "instId": n} for n in native_spot_list]
    await _ws_channel_task("md_spot", args, parse_md, write_md_to_buffer,
                            market="spot", stat_key="md_msgs")


async def task_md_fut(native_fut_list: list[str]):
    args = [{"channel": "bbo-tbt", "instId": n} for n in native_fut_list]
    await _ws_channel_task("md_fut", args, parse_md, write_md_to_buffer,
                            market="futures", stat_key="md_msgs")


async def task_ob_spot(native_spot_list: list[str]):
    args = [{"channel": "books", "instId": n} for n in native_spot_list]
    await _ws_channel_task("ob_spot", args, parse_ob, write_ob_to_buffer,
                            market="spot", stat_key="ob_msgs")


async def task_ob_fut(native_fut_list: list[str]):
    args = [{"channel": "books", "instId": n} for n in native_fut_list]
    await _ws_channel_task("ob_fut", args, parse_ob, write_ob_to_buffer,
                            market="futures", stat_key="ob_msgs")


async def task_fr(native_fut_list: list[str]):
    """FR via funding-rate channel — parse_fn handles the write directly."""
    global cmd_counter
    backoff = config.WS_RECONNECT_INIT

    args = [{"channel": "funding-rate", "instId": n} for n in native_fut_list]

    while True:
        try:
            async with websockets.connect(
                _WS_URL,
                ping_interval=None,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(f"[fr] Connected. symbols={len(native_fut_list)}")
                backoff = config.WS_RECONNECT_INIT

                chunk_size = config.WS_CHUNK_OKX
                for i in range(0, len(args), chunk_size):
                    chunk = args[i:i + chunk_size]
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))

                hb_task = asyncio.create_task(_heartbeat(ws, "fr"))
                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        result = parse_fr(raw)
                        if result is not None:
                            write_fr_to_buffer(*result)
                            stats["fr_msgs"] += 1
                        if cmd_counter >= config.BATCH_MAX_COMMANDS:
                            flush_event.set()
                finally:
                    hb_task.cancel()

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

    # Load normalized subscribe lists
    norm_spot = _load_symbols(f"{config.SUBSCRIBE_DIR}/okx/okx_spot.txt")
    norm_fut  = _load_symbols(f"{config.SUBSCRIBE_DIR}/okx/okx_futures.txt")

    # Build norm→native maps
    spot_map = _load_native_map(
        f"{config.DICTIONARIES_DIR}/okx/data/okx_spot.txt",
        f"{config.DICTIONARIES_DIR}/okx/data/okx_spot_native.txt",
    )
    fut_map = _load_native_map(
        f"{config.DICTIONARIES_DIR}/okx/data/okx_futures.txt",
        f"{config.DICTIONARIES_DIR}/okx/data/okx_futures_native.txt",
    )

    native_spot = [spot_map[s] for s in norm_spot if s in spot_map]
    native_fut  = [fut_map[s]  for s in norm_fut  if s in fut_map]

    log.info(
        f"Starting collector_okx | "
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
        log.info("collector_okx stopped")


if __name__ == "__main__":
    asyncio.run(main())
