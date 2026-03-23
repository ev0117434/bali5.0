#!/usr/bin/env python3
# monitors/snapshot_monitor.py
"""
BALI 5.0 — Snapshot monitor (data branch only).

Reads signals from Redis Stream stream:signals (XREAD with blocking).
Persists last-read cursor in snapshot:stream_id — survives crashes without losing signals.
On signal: loads 1h of historical data, then records real-time snapshot for 3500s.

Each snapshot file: signal/snapshot/{spot}_{fut}_{symbol}_{ts}.csv
Contains: header + historical rows (past 1h) + real-time rows (next ~58 min)

Only runs in data branch (HISTORY_ENABLED=True).
Output: logs/snapshot_monitor.log, signal/snapshot/*.csv
"""

import asyncio
import os
import sys
import time
from pathlib import Path

import aiofiles
import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from logger_setup import setup_logger

log = setup_logger("snapshot_monitor")

# Only runs in data branch
if not config.HISTORY_ENABLED:
    log.error("snapshot_monitor requires HISTORY_ENABLED=True (data branch only)")
    sys.exit(1)

_CSV_HEADER = config.SNAPSHOT_CSV_HEADER


# ── CSV row building ───────────────────────────────────────────────────────

def _ob_fields_to_list(ob_data: dict, prefix: str) -> list[str]:
    """
    Extract 10 bid or ask levels from OB hash.
    prefix: "b" for bids, "a" for asks
    Returns: [price1, qty1, price2, qty2, ..., price10, qty10] flat list for CSV.
    """
    prices = []
    qtys   = []
    for i in range(1, 11):
        p = ob_data.get(f"{prefix}{i}", "") if ob_data else ""
        q = ob_data.get(f"{prefix}{i}q", "") if ob_data else ""
        prices.append(str(p) if p else "")
        qtys.append(str(q) if q else "")
    return prices + qtys


def build_snapshot_row(
    spot_exch: str, fut_exch: str, symbol: str,
    ask_spot: str, bid_fut: str, spread_pct: str, ts_ms: int,
    fr: str, fr_ts: str,
    ob_spot: dict, ob_fut: dict,
) -> str:
    """Build one CSV row (no trailing newline)."""
    s_bids  = _ob_fields_to_list(ob_spot, "b")
    s_asks  = _ob_fields_to_list(ob_spot, "a")
    f_bids  = _ob_fields_to_list(ob_fut,  "b")
    f_asks  = _ob_fields_to_list(ob_fut,  "a")

    parts = [
        spot_exch, fut_exch, symbol,
        str(ask_spot), str(bid_fut), str(spread_pct), str(ts_ms),
        str(fr or ""), str(fr_ts or ""),
    ] + s_bids + s_asks + f_bids + f_asks

    return ",".join(parts)


def ob_list_to_dict(line_parts: list[str]) -> dict:
    """
    Convert OB history line parts back to {b1, b1q, ..., a10, a10q} dict.
    OB line format: b1p,b1q,b2p,b2q,...,b10p,b10q,a1p,a1q,...,a10p,a10q
    (20 pairs = 40 values, no ts here — ts is stripped before calling)
    """
    d = {}
    idx = 0
    for i in range(1, 11):
        if idx + 1 < len(line_parts):
            d[f"b{i}"]  = line_parts[idx]
            d[f"b{i}q"] = line_parts[idx + 1]
        idx += 2
    for i in range(1, 11):
        if idx + 1 < len(line_parts):
            d[f"a{i}"]  = line_parts[idx]
            d[f"a{i}q"] = line_parts[idx + 1]
        idx += 2
    return d


# ── History loading ────────────────────────────────────────────────────────

async def load_history(
    redis_data: aioredis.Redis,
    spot_exch: str, fut_exch: str, symbol: str,
    signal_ts_ms: int,
) -> list[str]:
    """
    Load 1 hour of history before signal_ts_ms.
    Returns sorted list of CSV row strings.
    """
    history_start_ms = signal_ts_ms - config.HISTORY_LOOKBACK_MS
    chunk_now = int(signal_ts_ms / 1000 / config.CHUNK_DURATION)
    chunks    = [chunk_now - 3, chunk_now - 2, chunk_now - 1, chunk_now]

    log.debug(f"Reading history chunks: {chunks}")

    pipe = redis_data.pipeline(transaction=False)
    for chunk in chunks:
        pipe.lrange(f"md:hist:{spot_exch}:spot:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"md:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"ob:hist:{spot_exch}:spot:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"ob:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)
        pipe.lrange(f"fr:hist:{fut_exch}:futures:{symbol}:{chunk}", 0, -1)

    results = await pipe.execute()  # 20 results

    # Collect data by timestamp
    rows_by_ts: dict = {}

    for chunk_idx in range(len(chunks)):
        base = chunk_idx * 5
        md_spot_lines = [l.decode() for l in results[base]]
        md_fut_lines  = [l.decode() for l in results[base + 1]]
        ob_spot_lines = [l.decode() for l in results[base + 2]]
        ob_fut_lines  = [l.decode() for l in results[base + 3]]
        fr_fut_lines  = [l.decode() for l in results[base + 4]]

        for line in md_spot_lines:
            try:
                b, a, ts_str = line.split(",", 2)
                ts = int(ts_str)
                if history_start_ms <= ts <= signal_ts_ms:
                    rows_by_ts.setdefault(ts, {})
                    rows_by_ts[ts]["ask_spot"] = a
                    rows_by_ts[ts]["bid_spot"] = b
            except (ValueError, KeyError):
                pass

        for line in md_fut_lines:
            try:
                b, a, ts_str = line.split(",", 2)
                ts = int(ts_str)
                if history_start_ms <= ts <= signal_ts_ms:
                    rows_by_ts.setdefault(ts, {})
                    rows_by_ts[ts]["bid_fut"] = b
            except (ValueError, KeyError):
                pass

        for line in ob_spot_lines:
            try:
                parts = line.split(",")
                ts = int(parts[-1])
                if history_start_ms <= ts <= signal_ts_ms:
                    rows_by_ts.setdefault(ts, {})
                    rows_by_ts[ts]["ob_spot"] = ob_list_to_dict(parts[:-1])
            except (ValueError, IndexError):
                pass

        for line in ob_fut_lines:
            try:
                parts = line.split(",")
                ts = int(parts[-1])
                if history_start_ms <= ts <= signal_ts_ms:
                    rows_by_ts.setdefault(ts, {})
                    rows_by_ts[ts]["ob_fut"] = ob_list_to_dict(parts[:-1])
            except (ValueError, IndexError):
                pass

        for line in fr_fut_lines:
            try:
                fr, fr_ts, ts_str = line.split(",", 2)
                ts = int(ts_str)
                if history_start_ms <= ts <= signal_ts_ms:
                    rows_by_ts.setdefault(ts, {})
                    rows_by_ts[ts]["fr"]    = fr
                    rows_by_ts[ts]["fr_ts"] = fr_ts
            except (ValueError, KeyError):
                pass

    # Forward-fill OB (OB arrives less frequently than MD)
    sorted_ts = sorted(rows_by_ts.keys())
    last_ob_spot: dict = {}
    last_ob_fut:  dict = {}
    last_fr:  str = ""
    last_fr_ts: str = ""

    rows = []
    for ts in sorted_ts:
        row_data = rows_by_ts[ts]

        # Forward fill OB
        if "ob_spot" in row_data:
            last_ob_spot = row_data["ob_spot"]
        if "ob_fut" in row_data:
            last_ob_fut = row_data["ob_fut"]
        if "fr" in row_data:
            last_fr    = row_data["fr"]
            last_fr_ts = row_data.get("fr_ts", "")

        ask_spot = row_data.get("ask_spot", "")
        bid_fut  = row_data.get("bid_fut", "")
        if not ask_spot or not bid_fut:
            continue  # skip rows with incomplete MD data

        ask_f = float(ask_spot)
        bid_f = float(bid_fut)
        spread_pct = f"{(bid_f - ask_f) / ask_f * 100:.4f}" if ask_f > 0 else ""

        csv_row = build_snapshot_row(
            spot_exch, fut_exch, symbol,
            ask_spot, bid_fut, spread_pct, ts,
            last_fr, last_fr_ts,
            last_ob_spot, last_ob_fut,
        )
        rows.append(csv_row)

    return rows


# ── Real-time snapshot row ─────────────────────────────────────────────────

async def read_current_row(
    redis_data: aioredis.Redis,
    spot_exch: str, fut_exch: str, symbol: str,
    signal_ask_spot: str, signal_bid_fut: str, signal_spread_pct: str,
) -> str:
    pipe = redis_data.pipeline(transaction=False)
    pipe.hmget(f"md:{spot_exch}:spot:{symbol}", "a", "b")
    pipe.hmget(f"md:{fut_exch}:futures:{symbol}", "b", "a")
    pipe.hgetall(f"ob:{spot_exch}:spot:{symbol}")
    pipe.hgetall(f"ob:{fut_exch}:futures:{symbol}")
    pipe.hmget(f"fr:{fut_exch}:futures:{symbol}", "fr", "fr_ts")
    results = await pipe.execute()

    ts_ms   = int(time.time() * 1000)
    md_spot = results[0]   # [ask, bid]
    md_fut  = results[1]   # [bid, ask]
    ob_spot = results[2]   # hash dict
    ob_fut  = results[3]
    fr_data = results[4]   # [fr, fr_ts]

    ask_spot = (md_spot[0] or b"").decode() if isinstance(md_spot[0], bytes) \
               else (md_spot[0] or signal_ask_spot)
    bid_fut  = (md_fut[0] or b"").decode() if isinstance(md_fut[0], bytes) \
               else (md_fut[0] or signal_bid_fut)

    # Decode bytes keys in OB dicts
    def _decode_dict(d):
        if not d:
            return {}
        return {
            (k.decode() if isinstance(k, bytes) else k):
            (v.decode() if isinstance(v, bytes) else v)
            for k, v in d.items()
        }

    ob_spot_d = _decode_dict(ob_spot)
    ob_fut_d  = _decode_dict(ob_fut)

    fr    = ""
    fr_ts = ""
    if fr_data and fr_data[0]:
        fr    = fr_data[0].decode() if isinstance(fr_data[0], bytes) else fr_data[0]
    if fr_data and fr_data[1]:
        fr_ts = fr_data[1].decode() if isinstance(fr_data[1], bytes) else fr_data[1]

    ask_f = float(ask_spot) if ask_spot else 0
    bid_f = float(bid_fut)  if bid_fut  else 0
    spread_pct = f"{(bid_f - ask_f) / ask_f * 100:.4f}" if ask_f > 0 else signal_spread_pct

    return build_snapshot_row(
        spot_exch, fut_exch, symbol,
        ask_spot, bid_fut, spread_pct, ts_ms,
        fr, fr_ts, ob_spot_d, ob_fut_d,
    )


# ── Snapshot task ──────────────────────────────────────────────────────────

async def run_snapshot(redis_data: aioredis.Redis, signal_str: str):
    parts = signal_str.split(",")
    if len(parts) < 7:
        log.error(f"Invalid signal string: {signal_str}")
        return

    spot_exch  = parts[0]
    fut_exch   = parts[1]
    symbol     = parts[2]
    ask_spot   = parts[3]
    bid_fut    = parts[4]
    spread_pct = parts[5]
    signal_ts  = int(parts[6])

    Path(config.SNAPSHOT_DIR).mkdir(parents=True, exist_ok=True)
    fname = (
        f"{config.SNAPSHOT_DIR}/"
        f"{spot_exch}_{fut_exch}_{symbol}_{signal_ts}.csv"
    )
    log.info(f"Snapshot started: {fname}")

    # Step 1: Load history
    log.info(f"Loading 1h history for {symbol}...")
    history_rows = await load_history(redis_data, spot_exch, fut_exch, symbol, signal_ts)
    log.info(f"History loaded: {len(history_rows)} rows")

    # Step 2: Write header + history
    async with aiofiles.open(fname, "w") as f:
        await f.write(_CSV_HEADER + "\n")
        for row in history_rows:
            await f.write(row + "\n")

    # Step 3: Real-time recording
    t_start    = time.monotonic()
    row_count  = len(history_rows)
    last_log   = t_start

    while True:
        elapsed = time.monotonic() - t_start
        if elapsed >= config.SNAPSHOT_DURATION:
            break

        t_row_start = time.monotonic()

        try:
            row = await read_current_row(
                redis_data, spot_exch, fut_exch, symbol,
                ask_spot, bid_fut, spread_pct,
            )
            async with aiofiles.open(fname, "a") as f:
                await f.write(row + "\n")
            row_count += 1
        except Exception as exc:
            log.warning(f"Error writing snapshot row: {exc}")

        row_lat = (time.monotonic() - t_row_start) * 1000

        if time.monotonic() - last_log >= 60:
            log.info(
                f"Snapshot {symbol}: elapsed={elapsed:.0f}s "
                f"rows={row_count} row_lat={row_lat:.1f}ms"
            )
            last_log = time.monotonic()

        sleep_s = max(0, config.SNAPSHOT_INTERVAL_MS / 1000 - (time.monotonic() - t_row_start))
        await asyncio.sleep(sleep_s)

    log.info(f"Snapshot done: {fname} | rows={row_count} elapsed={elapsed:.0f}s")


# ── Stream read loop ───────────────────────────────────────────────────────

async def subscribe_loop(redis_sub: aioredis.Redis, redis_data: aioredis.Redis):
    """
    Reads signals from Redis Stream instead of pub/sub.
    Persists last-read ID so missed signals are replayed after a crash.
    """
    active_snapshots: dict = {}

    # Resume from last processed ID, or start from current tip on first run
    last_id_raw = await redis_sub.get(config.STREAM_LAST_ID_KEY)
    if last_id_raw:
        last_id = last_id_raw.decode()
        log.info(f"Resuming stream from id={last_id}")
    else:
        last_id = "$"
        log.info("First run: starting from current stream position")

    log.info(f"Reading stream {config.STREAM_SIGNALS} (block=2s)")

    while True:
        try:
            results = await redis_sub.xread(
                {config.STREAM_SIGNALS: last_id},
                count=100,
                block=2000,   # ждём до 2 сек новых сообщений
            )
        except Exception as exc:
            log.error(f"XREAD error: {exc}")
            await asyncio.sleep(1)
            continue

        if not results:
            continue

        for _stream_name, messages in results:
            for msg_id, fields in messages:
                msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id

                signal_str = fields.get(b"data") or fields.get("data", b"")
                if isinstance(signal_str, bytes):
                    signal_str = signal_str.decode()

                log.info(f"Signal received (id={msg_id_str}): {signal_str}")

                parts = signal_str.split(",")
                if len(parts) >= 7:
                    snap_key = f"{parts[0]}_{parts[1]}_{parts[2]}_{parts[6]}"
                else:
                    snap_key = msg_id_str

                if snap_key in active_snapshots and not active_snapshots[snap_key].done():
                    log.debug(f"Snapshot {snap_key} already running")
                else:
                    task = asyncio.create_task(run_snapshot(redis_data, signal_str))
                    active_snapshots[snap_key] = task

                # Persist cursor so we can resume after a crash
                last_id = msg_id_str
                await redis_sub.set(config.STREAM_LAST_ID_KEY, last_id)

                # Clean up completed tasks
                for k in list(active_snapshots):
                    if active_snapshots[k].done():
                        del active_snapshots[k]


# ── Entry point ────────────────────────────────────────────────────────────

async def main():
    Path(config.SNAPSHOT_DIR).mkdir(parents=True, exist_ok=True)

    redis_sub  = aioredis.Redis.from_url(config.REDIS_URL)
    redis_data = aioredis.Redis.from_url(config.REDIS_URL)

    log.info(
        f"snapshot_monitor started | "
        f"duration={config.SNAPSHOT_DURATION}s "
        f"interval={config.SNAPSHOT_INTERVAL_MS}ms "
        f"lookback={config.HISTORY_LOOKBACK_MS // 1000 // 60}min"
    )

    try:
        await subscribe_loop(redis_sub, redis_data)
    except KeyboardInterrupt:
        log.info("snapshot_monitor stopped")
    finally:
        await redis_sub.aclose()
        await redis_data.aclose()


if __name__ == "__main__":
    asyncio.run(main())
