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

_COMPONENT = "collector_okx"


def _evt(event: str, **kwargs) -> dict:
    """Build a metrics event envelope with ts/component/event."""
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}


# ── Symbol maps ─────────────────────────────────────────────────────────────

def _load_symbols(filepath: str) -> list[str]:
    p = Path(filepath)
    if not p.exists():
        log.error(_evt("symbol_file_missing", exchange="okx", path=str(p)))
        return []
    lines = [l.strip() for l in p.read_text().splitlines() if l.strip()]
    log.info(_evt("symbols_loaded", exchange="okx", count=len(lines), path=str(p)))
    return lines


def _load_native_map(norm_file: str, native_file: str) -> dict[str, str]:
    """Load normalized→native mapping."""
    norms   = _load_symbols(norm_file)
    natives = _load_symbols(native_file)
    if len(norms) != len(natives):
        log.warning(_evt("symbol_count_mismatch", exchange="okx",
                         norm=len(norms), native=len(natives)))
    return dict(zip(norms, natives))


def _normalize(inst_id: str) -> str:
    """BTC-USDT → BTCUSDT, BTC-USDT-SWAP → BTCUSDT"""
    return inst_id.replace("-SWAP", "").replace("-", "")


# ── Parsers ────────────────────────────────────────────────────────────────

def parse_md(raw: str):
    """Parse OKX bbo-tbt message → (symbol, bid, ask, ts_ms) or None."""
    _t = time.monotonic()
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


def parse_ob(raw: str):
    """Parse OKX books message → (action, symbol, bids, asks, ts_ms) or None.

    action is "snapshot" or "update".
    bids/asks contain ALL levels from the message (not truncated),
    so the caller can merge them into a local book state.
    """
    _t = time.monotonic()
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
        bids = [[b[0], b[1]] for b in d.get("bids", [])]
        asks = [[a[0], a[1]] for a in d.get("asks", [])]
        if not bids and not asks:
            return None
        ts_ms = int(d["ts"])
        # e2e latency
        e2e_ms = time.time() * 1000 - ts_ms
        if e2e_ms >= 0:
            stats["e2e_lat_sum"] += e2e_ms
            stats["e2e_lat_count"] += 1
            if e2e_ms > stats["e2e_lat_max"]:
                stats["e2e_lat_max"] = e2e_ms
        return action, symbol, bids, asks, ts_ms
    except Exception:
        stats["parse_errors"] += 1
        return None
    finally:
        elapsed_us = (time.monotonic() - _t) * 1_000_000
        stats["parse_lat_sum"] += elapsed_us
        if elapsed_us > stats["parse_lat_max"]:
            stats["parse_lat_max"] = elapsed_us


def parse_fr(raw: str):
    """Parse OKX funding-rate message → (symbol, rate, fr_ts_str) or None."""
    _t = time.monotonic()
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
        # e2e latency: use nextFundingTime is not a message timestamp, skip e2e here
        return symbol, str(rate), str(fr_ts)
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

# Merged local order book state: f"{market}:{symbol}" → {"bids": {price: qty}, "asks": {price: qty}}
# Used to accumulate snapshot+deltas so hist always reflects the full book state.
ob_book: dict[str, dict] = {}

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
    key = f"md:okx:{market}:{symbol}"
    batch_buffer[key] = {"b": bid, "a": ask, "ts": str(ts_ms)}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"md:hist:okx:{market}:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{bid},{ask},{ts_ms}"))


def _merge_ob_book(book_side: dict, levels: list, descending: bool) -> dict:
    """Merge delta levels into a book side dict {price_str: qty_str}.

    Levels where qty == "0" are removed (OKX delete signal).
    Returns a new sorted dict of top-10 levels.
    """
    for price, qty in levels:
        if qty == "0":
            book_side.pop(price, None)
        else:
            book_side[price] = qty
    sorted_prices = sorted(book_side, key=lambda p: float(p), reverse=descending)
    return {p: book_side[p] for p in sorted_prices[:10]}


def write_ob_to_buffer(action: str, symbol: str, bids: list, asks: list,
                       ts_ms: int, market: str):
    book_key = f"{market}:{symbol}"

    if action == "snapshot":
        # Replace book wholesale
        ob_book[book_key] = {
            "bids": {p: q for p, q in bids[:10]},
            "asks": {p: q for p, q in asks[:10]},
        }
    elif book_key in ob_book:
        # Merge delta into existing book
        ob_book[book_key]["bids"] = _merge_ob_book(ob_book[book_key]["bids"], bids, descending=True)
        ob_book[book_key]["asks"] = _merge_ob_book(ob_book[book_key]["asks"], asks, descending=False)
    else:
        # Delta arrived before snapshot — skip until snapshot initialises the book
        return

    merged_bids = list(ob_book[book_key]["bids"].items())  # [(price, qty), ...]
    merged_asks = list(ob_book[book_key]["asks"].items())
    if not merged_bids or not merged_asks:
        return

    key    = f"ob:okx:{market}:{symbol}"
    fields = {}
    for i, (price, qty) in enumerate(merged_bids, 1):
        fields[f"b{i}"]  = price
        fields[f"b{i}q"] = qty
    for i, (price, qty) in enumerate(merged_asks, 1):
        fields[f"a{i}"]  = price
        fields[f"a{i}q"] = qty
    batch_buffer[key] = fields
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"ob:hist:okx:{market}:{symbol}:{chunk_id}"
        last_ts  = ob_hist_last_ts.get(hist_key, 0)
        if ts_ms - last_ts >= config.OB_HIST_MIN_INTERVAL_MS:
            ob_hist_last_ts[hist_key] = ts_ms
            bid_parts = [f"{p},{q}" for p, q in merged_bids]
            ask_parts = [f"{p},{q}" for p, q in merged_asks]
            hist_buffer.append((hist_key, ",".join(bid_parts + ask_parts + [str(ts_ms)])))
        else:
            stats["ob_hist_skipped"] += 1


def write_fr_to_buffer(symbol: str, rate: str, fr_ts_ms: str):
    key   = f"fr:okx:futures:{symbol}"
    ts_ms = int(time.time() * 1000)
    batch_buffer[key] = {"fr": rate, "fr_ts": fr_ts_ms}
    if key not in buffer_write_ts:
        buffer_write_ts[key] = time.monotonic()

    if config.HISTORY_ENABLED:
        chunk_id = int(ts_ms / 1000 / config.CHUNK_DURATION)
        hist_key = f"fr:hist:okx:futures:{symbol}:{chunk_id}"
        hist_buffer.append((hist_key, f"{rate},{fr_ts_ms},{ts_ms}"))


# ── Heartbeat ──────────────────────────────────────────────────────────────

async def _heartbeat(ws, label: str):
    while True:
        await asyncio.sleep(config.HEARTBEAT_OKX)
        try:
            await ws.send("ping")
        except Exception as exc:
            log.debug(_evt("heartbeat_error", exchange="okx", error_msg=str(exc)))
            return


# ── WS task (all channels on one connection) ───────────────────────────────

async def _ws_channel_task(label: str, args_list: list[dict], parse_fn, write_fn,
                           market: str, stat_key: str):
    """
    Subscribe to args_list on OKX WS. Re-subscribe on reconnect.
    parse_fn returns (symbol, ...) or None.
    write_fn(*result, market=market).
    """
    backoff = config.WS_RECONNECT_INIT

    while True:
        try:
            log.info(_evt("ws_connect", exchange="okx", stream=label,
                          url=_WS_URL, args=len(args_list)))
            async with websockets.connect(
                _WS_URL,
                ping_interval=None,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="okx", stream=label))
                backoff = config.WS_RECONNECT_INIT

                # Send subscribe in chunks of WS_CHUNK_OKX
                chunk_size = config.WS_CHUNK_OKX
                for i in range(0, len(args_list), chunk_size):
                    chunk = args_list[i:i + chunk_size]
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))

                hb_task = asyncio.create_task(_heartbeat(ws, label), name=f"hb_{label}")
                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        result = parse_fn(raw)
                        if result is not None:
                            write_fn(*result, market=market)
                            stats[stat_key] += 1

                finally:
                    hb_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="okx", stream=label,
                             error_type=type(exc).__name__, error_msg=str(exc),
                             reconnect_backoff_s=backoff,
                             reconnects_total=stats["reconnects"]))
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


async def _task_ob(label: str, native_list: list[str], market: str):
    """Dedicated OB task: handles snapshot+delta merge via write_ob_to_buffer.

    Cannot use _ws_channel_task because parse_ob now returns 5 elements
    (action, symbol, bids, asks, ts_ms) and write_ob_to_buffer needs action.
    """
    backoff = config.WS_RECONNECT_INIT
    args = [{"channel": "books", "instId": n} for n in native_list]

    while True:
        try:
            log.info(_evt("ws_connect", exchange="okx", stream=label,
                          url=_WS_URL, symbols=len(native_list)))
            async with websockets.connect(
                _WS_URL,
                ping_interval=None,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="okx", stream=label))
                backoff = config.WS_RECONNECT_INIT

                # Clear stale book state on reconnect (new snapshot incoming)
                keys_to_clear = [k for k in ob_book if k.startswith(f"{market}:")]
                for k in keys_to_clear:
                    del ob_book[k]

                chunk_size = config.WS_CHUNK_OKX
                for i in range(0, len(args), chunk_size):
                    await ws.send(json.dumps({"op": "subscribe", "args": args[i:i + chunk_size]}))

                hb_task = asyncio.create_task(_heartbeat(ws, label), name=f"hb_{label}")
                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        result = parse_ob(raw)
                        if result is not None:
                            action, symbol, bids, asks, ts_ms = result
                            write_ob_to_buffer(action, symbol, bids, asks, ts_ms, market=market)
                            stats["ob_msgs"] += 1
                finally:
                    hb_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="okx", stream=label,
                             error_type=type(exc).__name__, error_msg=str(exc),
                             reconnect_backoff_s=backoff,
                             reconnects_total=stats["reconnects"]))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, config.WS_RECONNECT_MAX)


async def task_ob_spot(native_spot_list: list[str]):
    await _task_ob("ob_spot", native_spot_list, market="spot")


async def task_ob_fut(native_fut_list: list[str]):
    await _task_ob("ob_fut", native_fut_list, market="futures")


async def task_fr(native_fut_list: list[str]):
    """FR via funding-rate channel — parse_fn handles the write directly."""
    backoff = config.WS_RECONNECT_INIT

    args = [{"channel": "funding-rate", "instId": n} for n in native_fut_list]

    while True:
        try:
            log.info(_evt("ws_connect", exchange="okx", stream="fr",
                          url=_WS_URL, symbols=len(native_fut_list)))
            async with websockets.connect(
                _WS_URL,
                ping_interval=None,
                close_timeout=config.WS_CLOSE_TIMEOUT,
                max_size=config.WS_MAX_SIZE,
            ) as ws:
                log.info(_evt("ws_connected", exchange="okx", stream="fr"))
                backoff = config.WS_RECONNECT_INIT

                chunk_size = config.WS_CHUNK_OKX
                for i in range(0, len(args), chunk_size):
                    chunk = args[i:i + chunk_size]
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))

                hb_task = asyncio.create_task(_heartbeat(ws, "fr"), name="hb_fr")
                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        result = parse_fr(raw)
                        if result is not None:
                            write_fr_to_buffer(*result)
                            stats["fr_msgs"] += 1
                finally:
                    hb_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stats["reconnects"] += 1
            log.warning(_evt("ws_disconnect", exchange="okx", stream="fr",
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
            log.error(_evt("redis_error", exchange="okx", pipeline_type="primary",
                           error_type=type(exc).__name__, error_msg=str(exc)))

        flush_lat_ms = (time.monotonic() - t_start) * 1000
        stats["flushes"]       += 1
        stats["flush_lat_sum"] += flush_lat_ms
        stats["flush_lat_max"]  = max(stats["flush_lat_max"], flush_lat_ms)
        stats["batch_sum"]     += len(current_batch)

        if flush_lat_ms > 50:
            stats["flush_slow_count"] += 1
            log.warning(_evt("slow_flush", exchange="okx", flush_type="primary",
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
            log.info(_evt("chunk_rotate", exchange="okx",
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
                log.error(_evt("redis_error", exchange="okx", pipeline_type="history",
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
            log.warning(_evt("slow_flush", exchange="okx", flush_type="history",
                             lat_ms=round(flush_lat_ms, 1), threshold_ms=100.0,
                             cmds=len(current_hist), expire_cmds=expire_cmds))


async def task_metrics():
    interval = config.METRICS_LOG_INTERVAL
    while True:
        await asyncio.sleep(interval)
        n  = max(stats["flushes"], 1)
        nh = max(stats["hist_flushes"], 1)
        nm = max(stats["md_msgs"] + stats["ob_msgs"] + stats["fr_msgs"], 1)

        log.info(_evt("metrics_interval", exchange="okx",
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

    if not native_spot and not native_fut:
        log.error(_evt("no_symbols", exchange="okx"))
        return

    log.info(_evt("collector_start", exchange="okx",
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
        log.info(_evt("collector_stop", exchange="okx", reason="KeyboardInterrupt"))
    finally:
        for t in tasks:
            t.cancel()
        await redis.aclose()
        await redis_pool.aclose()
        log.info(_evt("collector_stop", exchange="okx", reason="stopped"))


if __name__ == "__main__":
    asyncio.run(main())
