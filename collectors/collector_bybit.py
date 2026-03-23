#!/usr/bin/env python3
# collectors/collector_bybit.py
"""
BALI 5.0 — Bybit collector.

MD+FR via tickers (spot + linear futures).
OB via orderbook.10.* (spot + linear futures).
FR arrives together with MD in linear tickers stream.

Redis ключи:
  md:bybit:spot:{symbol}     → {b, a, ts}
  md:bybit:futures:{symbol}  → {b, a, ts}
  ob:bybit:spot:{symbol}     → {b1..b10, b1q..b10q, a1..a10, a1q..a10q}
  ob:bybit:futures:{symbol}  → same
  fr:bybit:futures:{symbol}  → {fr, fr_ts}
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

log = setup_logger("collector_bybit")

_WS_SPOT    = "wss://stream.bybit.com/v5/public/spot"
_WS_FUTURES = "wss://stream.bybit.com/v5/public/linear"

_COMPONENT = "collector_bybit"


def _evt(event: str, **kwargs) -> dict:
    """Build a metrics event envelope with ts/component/event."""
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}


def _load_symbols(filepath: str) -> list[str]:
    p = Path(filepath)
    if not p.exists():
        log.error(_evt("symbol_file_missing", exchange="bybit", path=str(p)))
        return []
    lines = [l.strip().upper() for l in p.read_text().splitlines() if l.strip()]
    log.info(_evt("symbols_loaded", exchange="bybit", count=len(lines), path=str(p)))
    return lines


# ── Parsers ────────────────────────────────────────────────────────────────

def parse_md(raw: str):
    """Parse Bybit tickers message → (symbol, bid, ask, ts_ms) or None."""
    _t = time.monotonic()
    try:
        msg = json.loads(raw)
        if msg.get("op") == "pong":
            return None
        if "topic" not in msg:
            return None
        data = msg.get("data", {})
        bid = data.get("bid1Price", "")
        ask = data.get("ask1Price", "")
        if not bid or not ask:
            return None
        symbol = data["symbol"]
        ts_ms  = int(msg.get("ts", int(time.time() * 1000)))
        # e2e latency
        e2e_ms = time.time() * 1000 - ts_ms
        if e2e_ms >= 0:
            stats["e2e_lat_sum"] += e2e_ms
            stats["e2e_lat_count"] += 1
            if e2e_ms > stats["e2e_lat_max"]:
                stats["e2e_lat_max"] = e2e_ms
        return symbol, bid, ask, ts_ms
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


def parse_fr_from_ticker(raw: str):
    """Extract FR from Bybit linear tickers → (symbol, rate, fr_ts_str) or None."""
    _t = time.monotonic()
    try:
        msg = json.loads(raw)
        if "topic" not in msg:
            return None
        data = msg.get("data", {})
        fr    = data.get("fundingRate")
        fr_ts = data.get("nextFundingTime")
        if not fr:
            return None
        symbol = data["symbol"]
        return symbol, str(fr), str(fr_ts) if fr_ts else ""
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


def parse_ob(raw: str):
    """Parse Bybit orderbook.10 message → (symbol, bids, asks, ts_ms) or None."""
    _t = time.monotonic()
    try:
        msg = json.loads(raw)
        if msg.get("op") == "pong":
            return None
        if "topic" not in msg:
            return None
        data   = msg.get("data", {})
        symbol = data.get("s", "")
        if not symbol:
            return None
        bids = [[b[0], b[1]] for b in data.get("b", [])[:10]]
        asks = [[a[0], a[1]] for a in data.get("a", [])[:10]]
        if not bids and not asks:
            return None
        ts_ms = int(msg.get("ts", int(time.time() * 1000)))
        # e2e latency
        e2e_ms = time.time() * 1000 - ts_ms
        if e2e_ms >= 0:
            stats["e2e_lat_sum"] += e2e_ms
            stats["e2e_lat_count"] += 1
            if e2e_ms > stats["e2e_lat_max"]:
                stats["e2e_lat_max"] = e2e_ms
        return symbol, bids, asks, ts_ms
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


# ── Shared state ───────────────────────────────────────────────────────────

batch_buffer: dict = {}
hist_buffer:  list = []
flush_event         = None
expire_set:   set  = set()
last_chunk_id: int = 0
ob_hist_last_ts: dict = {}  # hist_key → last write ts_ms (OB 10 Hz gate)
_fr_ts_cache:   dict = {}   # symbol → last known fr_ts_ms (absent in delta tickers)
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
    "e2e_lat_sum":           0.0,   # ms
    "e2e_lat_max":           0.0,
    "e2e_lat_count":         0,
}


def write_md_to_buffer(symbol: str, bid: str, ask: str, ts_ms: int, market: str):
    key = f"md:bybit:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:bybit:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))


def write_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    key    = f"ob:bybit:{market}:{symbol}"
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

    if config.HISTORY_ENABLED and bids and asks:
        # Пропускаем дельты с пустой одной стороной (snapshot+delta паттерн Bybit):
        # неполная hist-строка приводит к пустым ask/bid колонкам в снапшоте.
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"ob:hist:bybit:{market}:{symbol}:{chunk_id}"
        last_ts  = ob_hist_last_ts.get(hist_key, 0)
        if ts_ms - last_ts >= config.OB_HIST_MIN_INTERVAL_MS:
            ob_hist_last_ts[hist_key] = ts_ms
            bid_parts = [f"{p},{q}" for p, q in bids[:10]]
            ask_parts = [f"{p},{q}" for p, q in asks[:10]]
            hist_buffer.append((hist_key, ",".join(bid_parts + ask_parts + [str(ts_ms)])))
        else:
            stats["ob_hist_skipped"] += 1


def write_md_from_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    """Write MD key from orderbook.1 data (best bid/ask only)."""
    if not bids or not asks:
        return
    bid = bids[0][0]
    ask = asks[0][0]
    key = f"md:bybit:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:bybit:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))


def write_fr_to_buffer(symbol: str, rate: str, fr_ts_ms: str):
    # Cache fr_ts_ms when present (delta updates omit nextFundingTime)
    if fr_ts_ms:
        _fr_ts_cache[symbol] = fr_ts_ms
    effective_fr_ts = _fr_ts_cache.get(symbol, "")

    key   = f"fr:bybit:futures:{symbol}"
    ts_ms = int(time.time() * 1000)
    fields: dict = {"fr": rate}
    if effective_fr_ts:
        fields["fr_ts"] = effective_fr_ts
    batch_buffer[key] = fields
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED and effective_fr_ts:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:bybit:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{effective_fr_ts},{ts_ms}"))


# ── Heartbeat ──────────────────────────────────────────────────────────────

async def _heartbeat(ws, label: str):
    while True:
        await asyncio.sleep(config.HEARTBEAT_BYBIT)
        try:
            await ws.send(json.dumps({"op": "ping"}))
        except Exception as exc:
            log.debug(_evt("heartbeat_error", exchange="bybit", error_msg=str(exc)))
            return


# ── WS task ────────────────────────────────────────────────────────────────

async def _ws_task(url: str, symbols: list[str], label: str, market: str,
                   chunk_size: int, topic_builder, parse_fn, write_fn,
                   also_parse_fr: bool = False):
    """
    Single WS connection: subscribe in chunks of chunk_size per message.
    """
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(_evt("ws_connect", exchange="bybit", stream=label,
                          url=url, symbols=len(symbols)))
            async with websockets.connect(
                url,
                ping_interval=None,      # manual heartbeat
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="bybit", stream=label))
                backoff = config.WS_RECONNECT_INIT

                # Subscribe in chunks
                for i in range(0, len(symbols), chunk_size):
                    chunk = symbols[i:i + chunk_size]
                    args  = [topic_builder(s) for s in chunk]
                    msg   = json.dumps({"op": "subscribe", "args": args})
                    await ws.send(msg)

                hb_task = asyncio.create_task(_heartbeat(ws, label), name=f"hb_{label}")
                try:
                    async for raw in ws:
                        result = parse_fn(raw)
                        if result is not None:
                            write_fn(*result, market=market)
                            if label.startswith("md"):
                                stats["md_msgs"] += 1
                            elif label.startswith("ob"):
                                stats["ob_msgs"] += 1

                        if also_parse_fr:
                            fr_result = parse_fr_from_ticker(raw)
                            if fr_result is not None:
                                write_fr_to_buffer(*fr_result)
                                stats["fr_msgs"] += 1

                finally:
                    hb_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="bybit", stream=label,
                             error_type=type(exc).__name__, error_msg=str(exc),
                             reconnect_backoff_s=backoff,
                             reconnects_total=stats["reconnects"]))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


async def task_md_spot(symbols: list[str]):
    # Bybit V5 spot tickers do not include bid1Price/ask1Price — use orderbook.1 instead.
    # Supported spot depths: 1, 50 (not 10, not 25).
    await _ws_task(
        _WS_SPOT, symbols, "md_spot", "spot",
        chunk_size=config.WS_CHUNK_BYBIT,
        topic_builder=lambda s: f"orderbook.1.{s}",
        parse_fn=parse_ob,
        write_fn=write_md_from_ob_to_buffer,
    )


async def task_md_fut(symbols: list[str]):
    # also_parse_fr=True because FR comes in linear tickers
    await _ws_task(
        _WS_FUTURES, symbols, "md_fut", "futures",
        chunk_size=config.WS_CHUNK_BYBIT,
        topic_builder=lambda s: f"tickers.{s}",
        parse_fn=parse_md,
        write_fn=write_md_to_buffer,
        also_parse_fr=True,
    )


async def task_ob_spot(symbols: list[str]):
    # Bybit V5 spot supports depths: 1, 50 (not 10)
    await _ws_task(
        _WS_SPOT, symbols, "ob_spot", "spot",
        chunk_size=config.WS_CHUNK_BYBIT,
        topic_builder=lambda s: f"orderbook.50.{s}",
        parse_fn=parse_ob,
        write_fn=write_ob_to_buffer,
    )


async def task_ob_fut(symbols: list[str]):
    # Bybit V5 linear supports depths: 1, 25, 50, 200 (not 10)
    await _ws_task(
        _WS_FUTURES, symbols, "ob_fut", "futures",
        chunk_size=config.WS_CHUNK_BYBIT,
        topic_builder=lambda s: f"orderbook.50.{s}",
        parse_fn=parse_ob,
        write_fn=write_ob_to_buffer,
    )


# ── Flusher (primary: hset only) ───────────────────────────────────────────

async def task_flusher(redis: aioredis.Redis):

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
            log.error(_evt("redis_error", exchange="bybit", pipeline_type="primary",
                           error_type=type(exc).__name__, error_msg=str(exc)))

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["flushes"]       += 1
        stats["flush_lat_sum"] += flush_lat_ms
        stats["flush_lat_max"]  = max(stats["flush_lat_max"], flush_lat_ms)
        stats["batch_sum"]     += len(current_batch)

        if flush_lat_ms > 50:
            stats["flush_slow_count"] += 1
            log.warning(_evt("slow_flush", exchange="bybit", flush_type="primary",
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
            log.info(_evt("chunk_rotate", exchange="bybit",
                          chunk_id_prev=last_chunk_id, chunk_id_new=new_chunk_id,
                          expire_keys_reset=len(expire_set)))
            last_chunk_id = new_chunk_id

        t_start = time.monotonic()
        expire_cmds = 0
        total_sent  = 0

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
                log.error(_evt("redis_error", exchange="bybit", pipeline_type="history",
                               error_type=type(exc).__name__, error_msg=str(exc)))
            total_sent += len(chunk)
            await asyncio.sleep(0)

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["hist_flushes"]       += 1
        stats["hist_flush_lat_sum"] += flush_lat_ms
        stats["hist_flush_lat_max"]  = max(stats["hist_flush_lat_max"], flush_lat_ms)
        stats["hist_cmds"]          += len(current_hist)

        if flush_lat_ms > 100:
            stats["hist_flush_slow_count"] += 1
            log.warning(_evt("slow_flush", exchange="bybit", flush_type="history",
                             lat_ms=round(flush_lat_ms, 1), threshold_ms=100.0,
                             cmds=len(current_hist), expire_cmds=expire_cmds))


async def task_metrics():
    interval = config.METRICS_LOG_INTERVAL
    while True:
        await asyncio.sleep(interval)
        n  = max(stats["flushes"], 1)
        nh = max(stats["hist_flushes"], 1)
        nm = max(stats["md_msgs"] + stats["ob_msgs"] + stats["fr_msgs"], 1)

        log.info(_evt("metrics_interval", exchange="bybit",
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
                "e2e_avg_ms":        round(stats["e2e_lat_sum"] / max(stats["e2e_lat_count"], 1), 1),
                "e2e_max_ms":        round(stats["e2e_lat_max"], 1),
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

    spot_symbols = _load_symbols(f"{config.SUBSCRIBE_DIR}/bybit/bybit_spot.txt")
    fut_symbols  = _load_symbols(f"{config.SUBSCRIBE_DIR}/bybit/bybit_futures.txt")

    if not spot_symbols and not fut_symbols:
        log.error(_evt("no_symbols", exchange="bybit"))
        return

    redis_pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, max_connections=10)
    redis      = aioredis.Redis(connection_pool=redis_pool)

    log.info(_evt("collector_start", exchange="bybit",
                  spot_symbols=len(spot_symbols), fut_symbols=len(fut_symbols),
                  history_enabled=config.HISTORY_ENABLED))

    tasks = [
        asyncio.create_task(task_md_spot(spot_symbols), name="ws_md_spot"),
        asyncio.create_task(task_md_fut(fut_symbols),   name="ws_md_fut"),
        asyncio.create_task(task_ob_spot(spot_symbols), name="ws_ob_spot"),
        asyncio.create_task(task_ob_fut(fut_symbols),   name="ws_ob_fut"),
        asyncio.create_task(task_flusher(redis),        name="flusher"),
        asyncio.create_task(task_hist_flusher(redis),   name="hist_flusher"),
        asyncio.create_task(task_metrics(),             name="metrics"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info(_evt("collector_stop", exchange="bybit", reason="KeyboardInterrupt"))
    finally:
        for t in tasks:
            t.cancel()
        await redis.aclose()
        await redis_pool.aclose()
        log.info(_evt("collector_stop", exchange="bybit", reason="stopped"))


if __name__ == "__main__":
    asyncio.run(main())
