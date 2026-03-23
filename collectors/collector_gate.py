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

_COMPONENT = "collector_gate"


def _evt(event: str, **kwargs) -> dict:
    """Build a metrics event envelope with ts/component/event."""
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}


# ── Symbol maps ─────────────────────────────────────────────────────────────

def _load_symbols(filepath: str) -> list[str]:
    p = Path(filepath)
    if not p.exists():
        log.error(_evt("symbol_file_missing", exchange="gate", path=str(p)))
        return []
    lines = [l.strip() for l in p.read_text().splitlines() if l.strip()]
    log.info(_evt("symbols_loaded", exchange="gate", count=len(lines), path=str(p)))
    return lines


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
    _t = time.monotonic()
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


def parse_md_fut(raw: str):
    """Parse Gate.io futures.book_ticker → (symbol, bid, ask, ts_ms) or None."""
    _t = time.monotonic()
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


def parse_ob_spot(raw: str):
    """Parse Gate.io spot.order_book → (symbol, bids, asks, ts_ms) or None."""
    _t = time.monotonic()
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


def parse_ob_fut(raw: str):
    """Parse Gate.io futures.order_book snapshot → (symbol, bids, asks, ts_ms) or None.
    Uses futures.order_book (full snapshots, event="all") instead of
    futures.order_book_update (incremental diffs) to always have a complete book.
    """
    _t = time.monotonic()
    try:
        msg = json.loads(raw)
        if msg.get("event") in ("subscribe", "unsubscribe", "update"):
            return None
        result = msg.get("result")
        if not result:
            return None
        channel = msg.get("channel", "")
        if channel != "futures.order_book":
            return None
        symbol = _normalize(result.get("contract", "") or result.get("s", ""))
        bids   = [[b["p"], b["s"]] for b in result.get("bids", [])[:10]]
        asks   = [[a["p"], a["s"]] for a in result.get("asks", [])[:10]]
        if not symbol:
            return None
        ts_ms = int(result.get("t", int(time.time() * 1000)))
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


def parse_fr(raw: str):
    """Parse Gate.io futures.tickers → (symbol, rate, fr_ts_str) or None."""
    _t = time.monotonic()
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
        if not rate or not symbol:
            return None
        # Gate USDT-perp funds every 8h at 00:00/08:00/16:00 UTC.
        # Always compute next boundary — funding_next_apply is unreliable (often 0).
        now = int(time.time())
        fr_ts_ms = str(((now // 28800) + 1) * 28800 * 1000)
        return symbol, str(rate), fr_ts_ms
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
    key = f"md:gate:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:gate:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))


def write_ob_to_buffer(symbol: str, bids: list, asks: list, ts_ms: int, market: str):
    key    = f"ob:gate:{market}:{symbol}"
    fields = {}
    for i, (price, qty) in enumerate(bids[:10], 1):
        fields[f"b{i}"]  = str(price)
        fields[f"b{i}q"] = str(qty)
    for i, (price, qty) in enumerate(asks[:10], 1):
        fields[f"a{i}"]  = str(price)
        fields[f"a{i}q"] = str(qty)
    batch_buffer[key] = fields
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"ob:hist:gate:{market}:{symbol}:{chunk_id}"
        last_ts  = ob_hist_last_ts.get(hist_key, 0)
        if ts_ms - last_ts >= config.OB_HIST_MIN_INTERVAL_MS:
            ob_hist_last_ts[hist_key] = ts_ms
            bid_parts = [f"{p},{q}" for p, q in bids[:10]]
            ask_parts = [f"{p},{q}" for p, q in asks[:10]]
            hist_buffer.append((hist_key, ",".join(bid_parts + ask_parts + [str(ts_ms)])))
        else:
            stats["ob_hist_skipped"] += 1


def write_fr_to_buffer(symbol: str, rate: str, fr_ts_ms: str):
    key   = f"fr:gate:futures:{symbol}"
    ts_ms = int(time.time() * 1000)
    # Always write fr_ts (even ""); Gate messages are full snapshots, no delta pattern.
    batch_buffer[key] = {"fr": rate, "fr_ts": fr_ts_ms}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:gate:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))


# ── WS task ────────────────────────────────────────────────────────────────

async def _ws_task(url: str, label: str, subscribe_fn, parse_fn, write_fn,
                   market: str, stat_key: str):
    """
    Connect to url, call subscribe_fn(ws) to subscribe,
    then parse/write in a loop.
    """
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(_evt("ws_connect", exchange="gate", stream=label, url=url))
            async with websockets.connect(
                url,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="gate", stream=label))
                backoff = config.WS_RECONNECT_INIT

                await subscribe_fn(ws)

                async for raw in ws:
                    result = parse_fn(raw)
                    if result is not None:
                        write_fn(*result, market=market)
                        stats[stat_key] += 1

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="gate", stream=label,
                             error_type=type(exc).__name__, error_msg=str(exc),
                             reconnect_backoff_s=backoff,
                             reconnects_total=stats["reconnects"]))
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
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(_evt("ws_connect", exchange="gate", stream="ob_spot", url=_WS_SPOT))
            async with websockets.connect(
                _WS_SPOT,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="gate", stream="ob_spot",
                              symbols=len(native_spot)))
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

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="gate", stream="ob_spot",
                             error_type=type(exc).__name__, error_msg=str(exc),
                             reconnect_backoff_s=backoff,
                             reconnects_total=stats["reconnects"]))
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
                    "channel": "futures.order_book",
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

    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(_evt("ws_connect", exchange="gate", stream="fr", url=_WS_FUTURES))
            async with websockets.connect(
                _WS_FUTURES,
                ping_interval=config.WS_PING_INTERVAL,
                ping_timeout=config.WS_PING_TIMEOUT,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="gate", stream="fr",
                              symbols=len(native_fut)))
                backoff = config.WS_RECONNECT_INIT
                await subscribe(ws)

                async for raw in ws:
                    result = parse_fr(raw)
                    if result is not None:
                        write_fr_to_buffer(*result)
                        stats["fr_msgs"] += 1

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="gate", stream="fr",
                             error_type=type(exc).__name__, error_msg=str(exc),
                             reconnect_backoff_s=backoff,
                             reconnects_total=stats["reconnects"]))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


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
            log.error(_evt("redis_error", exchange="gate", pipeline_type="primary",
                           error_type=type(exc).__name__, error_msg=str(exc)))

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["flushes"]       += 1
        stats["flush_lat_sum"] += flush_lat_ms
        stats["flush_lat_max"]  = max(stats["flush_lat_max"], flush_lat_ms)
        stats["batch_sum"]     += len(current_batch)

        if flush_lat_ms > 50:
            stats["flush_slow_count"] += 1
            log.warning(_evt("slow_flush", exchange="gate", flush_type="primary",
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
            log.info(_evt("chunk_rotate", exchange="gate",
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
                log.error(_evt("redis_error", exchange="gate", pipeline_type="history",
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
            log.warning(_evt("slow_flush", exchange="gate", flush_type="history",
                             lat_ms=round(flush_lat_ms, 1), threshold_ms=100.0,
                             cmds=len(current_hist), expire_cmds=expire_cmds))


async def task_metrics():
    interval = config.METRICS_LOG_INTERVAL
    while True:
        await asyncio.sleep(interval)
        n  = max(stats["flushes"], 1)
        nh = max(stats["hist_flushes"], 1)
        nm = max(stats["md_msgs"] + stats["ob_msgs"] + stats["fr_msgs"], 1)

        log.info(_evt("metrics_interval", exchange="gate",
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

    if not native_spot and not native_fut:
        log.error(_evt("no_symbols", exchange="gate"))
        return

    log.info(_evt("collector_start", exchange="gate",
                  spot_symbols=len(native_spot), fut_symbols=len(native_fut),
                  history_enabled=config.HISTORY_ENABLED))

    redis_pool = aioredis.ConnectionPool.from_url(config.REDIS_URL, max_connections=10)
    redis      = aioredis.Redis(connection_pool=redis_pool)

    tasks = [
        asyncio.create_task(task_md_spot(native_spot), name="ws_md_spot"),
        asyncio.create_task(task_md_fut(native_fut),   name="ws_md_fut"),
        asyncio.create_task(task_ob_spot(native_spot), name="ws_ob_spot"),
        asyncio.create_task(task_ob_fut(native_fut),   name="ws_ob_fut"),
        asyncio.create_task(task_fr(native_fut),       name="ws_fr"),
        asyncio.create_task(task_flusher(redis),       name="flusher"),
        asyncio.create_task(task_hist_flusher(redis),  name="hist_flusher"),
        asyncio.create_task(task_metrics(),            name="metrics"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info(_evt("collector_stop", exchange="gate", reason="KeyboardInterrupt"))
    finally:
        for t in tasks:
            t.cancel()
        await redis.aclose()
        await redis_pool.aclose()
        log.info(_evt("collector_stop", exchange="gate", reason="stopped"))


if __name__ == "__main__":
    asyncio.run(main())
