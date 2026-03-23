#!/usr/bin/env python3
# monitors/spread_monitor.py
"""
BALI 5.0 — Spread monitor.

Scans all spot×futures pairs every 300ms via a single Redis pipeline.
If spread_pct >= SPREAD_THRESHOLD and cooldown not active:
  - appends line to signal/signal.csv
  - writes to Redis Stream stream:signals (for snapshot_monitor)

Spread formula: (bid_futures - ask_spot) / ask_spot * 100
Positive spread means futures trade ABOVE spot → cash-and-carry opportunity.

Output: logs/spread_monitor.log, signal/signal.csv
"""

import asyncio
import glob
import os
import sys
import time
from pathlib import Path

import aiofiles
import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from logger_setup import setup_logger

log = setup_logger("spread_monitor")

_COMPONENT = "spread_monitor"


def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}


def _p99(values: list) -> float:
    """Compute 99th percentile using statistics.quantiles (stdlib, Python 3.8+)."""
    from statistics import quantiles
    if len(values) < 2:
        return round(values[0], 1) if values else 0.0
    return round(quantiles(values, n=100)[98], 1)  # index 98 = 99th percentile


# ── Summary state ──────────────────────────────────────────────────────────

_summary: dict = {
    "cycles": 0,
    "cycle_lat_sum": 0.0,
    "cycle_lat_max": 0.0,
    "cycle_lats": [],       # list of floats for p99 calculation
    "slow_cycles": 0,
    "signals": 0,
    "no_data_sum": 0,
    "stale_sum": 0,
    "_last_summary_ts": 0.0,  # will be set on first use
}
_SUMMARY_INTERVAL = 30.0


# ── Load combination pairs ─────────────────────────────────────────────────

def _parse_combo_filename(fname: str):
    """
    Parse 'dictionaries/combination/binance_spot_bybit_futures.txt'
    → ('binance', 'bybit')
    """
    base = Path(fname).stem   # binance_spot_bybit_futures
    parts = base.split("_")
    # Format: {spot_exch}_spot_{fut_exch}_futures
    spot_exch = parts[0]
    fut_exch  = parts[2]
    return spot_exch, fut_exch


def load_pairs() -> list[tuple[str, str, str]]:
    """
    Load all (spot_exch, fut_exch, symbol) triples from combination/*.txt files.
    """
    pairs = []
    pattern = os.path.join(config.COMBINATION_DIR, "*.txt")
    files   = glob.glob(pattern)

    if not files:
        log.warning(_evt("no_pairs",
                         combination_dir=str(config.COMBINATION_DIR),
                         retry_in_s=60))
        return []

    for fname in files:
        try:
            spot_exch, fut_exch = _parse_combo_filename(fname)
        except (IndexError, ValueError) as exc:
            log.warning(_evt("no_pairs",
                             combination_dir=str(config.COMBINATION_DIR),
                             retry_in_s=60))
            continue

        with open(fname) as f:
            for line in f:
                symbol = line.strip().upper()
                if symbol:
                    pairs.append((spot_exch, fut_exch, symbol))

    log.info(_evt("pairs_loaded",
                  pairs=len(pairs),
                  files=len(files),
                  combination_dir=str(config.COMBINATION_DIR)))
    return pairs


# ── Signal CSV setup ───────────────────────────────────────────────────────

async def ensure_signal_csv():
    Path(config.SIGNAL_DIR).mkdir(parents=True, exist_ok=True)
    csv_path = config.SIGNAL_CSV
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        async with aiofiles.open(csv_path, "w") as f:
            await f.write(config.SIGNAL_CSV_HEADER + "\n")
        log.info(_evt("csv_created", path=str(csv_path)))


# ── Handle signal ──────────────────────────────────────────────────────────

async def handle_signal(
    redis: aioredis.Redis,
    write_lock: asyncio.Lock,
    spot_exch: str,
    fut_exch: str,
    symbol: str,
    ask_spot: float,
    bid_fut: float,
    spread_pct: float,
    ts_ms: int,
    ts_spot: int,
    ts_fut: int,
):
    cooldown_key = f"spread:cooldown:{spot_exch}:{fut_exch}:{symbol}"

    # Atomic check-and-set: returns True if key was set (cooldown was NOT active)
    was_set = await redis.set(cooldown_key, 1, nx=True, ex=config.COOLDOWN_SECONDS)
    if not was_set:
        return  # already in cooldown

    signal_line = (
        f"{spot_exch},{fut_exch},{symbol},"
        f"{ask_spot:.8f},{bid_fut:.8f},{spread_pct:.4f},{ts_ms}"
    )

    now_ms = int(time.time() * 1000)
    t_emit_start = time.monotonic()

    # Write to CSV
    async with write_lock:
        async with aiofiles.open(config.SIGNAL_CSV, "a") as f:
            await f.write(signal_line + "\n")

    # Write to Redis Stream for snapshot_monitor (buffered, survives restarts)
    await redis.xadd(
        config.STREAM_SIGNALS,
        {"data": signal_line},
        maxlen=config.STREAM_SIGNALS_MAXLEN,
        approximate=True,
    )

    emit_lat_ms = (time.monotonic() - t_emit_start) * 1000

    log.info(_evt("signal",
                  spot_exchange=spot_exch, fut_exchange=fut_exch, symbol=symbol,
                  ask_spot=float(ask_spot), bid_fut=float(bid_fut),
                  spread_pct=round(spread_pct, 4),
                  data_age_spot_ms=int(now_ms - ts_spot),
                  data_age_fut_ms=int(now_ms - ts_fut),
                  cooldown_applied=False,
                  emit_lat_ms=round(emit_lat_ms, 1)))
    _summary["signals"] += 1


# ── Main monitor loop ──────────────────────────────────────────────────────

async def monitor_loop(redis: aioredis.Redis, pairs: list):
    write_lock = asyncio.Lock()

    log.info(_evt("spread_monitor_start",
                  pairs=len(pairs),
                  threshold_pct=config.SPREAD_THRESHOLD,
                  poll_interval_ms=config.SPREAD_POLL_INTERVAL_MS,
                  stale_threshold_ms=config.SPREAD_DATA_STALE_MS))

    while True:
        t_cycle = time.monotonic()
        now_ms  = int(time.time() * 1000)

        # Single pipeline: 2 HMGET per pair
        pipe = redis.pipeline(transaction=False)
        for spot_exch, fut_exch, symbol in pairs:
            pipe.hmget(f"md:{spot_exch}:spot:{symbol}", "a", "ts")
            pipe.hmget(f"md:{fut_exch}:futures:{symbol}", "b", "ts")

        try:
            t_pipe_start = time.monotonic()
            results = await pipe.execute()
            pipeline_lat_ms = (time.monotonic() - t_pipe_start) * 1000
        except Exception as exc:
            log.error(_evt("redis_error",
                           error_type=type(exc).__name__, error_msg=str(exc)))
            await asyncio.sleep(config.SPREAD_POLL_INTERVAL_MS / 1000)
            continue

        # Process results
        t_calc_start   = time.monotonic()
        signals_count  = 0
        stale_count    = 0
        no_data_count  = 0
        pairs_ok       = 0

        tasks = []

        for i, (spot_exch, fut_exch, symbol) in enumerate(pairs):
            ask_data = results[i * 2]       # [ask_spot, ts_spot]
            bid_data = results[i * 2 + 1]   # [bid_fut, ts_fut]

            if not ask_data[0] or not bid_data[0]:
                no_data_count += 1
                continue

            ask_ts = int(ask_data[1]) if ask_data[1] else 0
            bid_ts = int(bid_data[1]) if bid_data[1] else 0
            if now_ms - ask_ts > config.SPREAD_DATA_STALE_MS or \
               now_ms - bid_ts > config.SPREAD_DATA_STALE_MS:
                stale_count += 1
                continue

            pairs_ok += 1
            ask_spot = float(ask_data[0])
            bid_fut  = float(bid_data[0])
            if ask_spot <= 0:
                continue

            # Corrected formula: positive when futures > spot (arbitrage opportunity)
            spread_pct = (bid_fut - ask_spot) / ask_spot * 100

            if spread_pct >= config.SPREAD_THRESHOLD:
                signals_count += 1
                task = asyncio.create_task(
                    handle_signal(
                        redis, write_lock,
                        spot_exch, fut_exch, symbol,
                        ask_spot, bid_fut, spread_pct, now_ms,
                        ask_ts, bid_ts,
                    )
                )
                tasks.append(task)

        # Wait for all signal tasks (usually 0 or very few)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        calc_lat_ms = (time.monotonic() - t_calc_start) * 1000
        elapsed_ms  = (time.monotonic() - t_cycle) * 1000

        # Accumulate summary
        _summary["cycles"] += 1
        _summary["cycle_lat_sum"] += elapsed_ms
        if elapsed_ms > _summary["cycle_lat_max"]:
            _summary["cycle_lat_max"] = elapsed_ms
        _summary["cycle_lats"].append(elapsed_ms)
        _summary["no_data_sum"] += no_data_count
        _summary["stale_sum"] += stale_count

        log.debug(_evt("cycle",
                       pairs_total=len(pairs), pairs_ok=pairs_ok,
                       pairs_no_data=no_data_count, pairs_stale=stale_count,
                       signals=signals_count,
                       cycle_lat_ms=round(elapsed_ms, 1),
                       pipeline_lat_ms=round(pipeline_lat_ms, 1),
                       calc_lat_ms=round(calc_lat_ms, 1)))

        if elapsed_ms > 250:
            _summary["slow_cycles"] += 1
            log.warning(_evt("cycle_slow",
                             cycle_lat_ms=round(elapsed_ms, 1), threshold_ms=250.0,
                             pipeline_lat_ms=round(pipeline_lat_ms, 1),
                             calc_lat_ms=round(calc_lat_ms, 1)))

        # Periodic summary every 30s
        if _summary["_last_summary_ts"] == 0.0:
            _summary["_last_summary_ts"] = time.monotonic()
        elif time.monotonic() - _summary["_last_summary_ts"] >= _SUMMARY_INTERVAL:
            c = max(_summary["cycles"], 1)
            log.info(_evt("spread_summary",
                          interval_s=round(time.monotonic() - _summary["_last_summary_ts"], 1),
                          cycles=_summary["cycles"],
                          cycle_lat_avg_ms=round(_summary["cycle_lat_sum"] / c, 1),
                          cycle_lat_max_ms=round(_summary["cycle_lat_max"], 1),
                          cycle_lat_p99_ms=_p99(_summary["cycle_lats"]),
                          slow_cycles=_summary["slow_cycles"],
                          signals_total=_summary["signals"],
                          pairs_no_data_avg=round(_summary["no_data_sum"] / c, 1),
                          pairs_stale_avg=round(_summary["stale_sum"] / c, 1)))
            # reset
            _summary.update({"cycles": 0, "cycle_lat_sum": 0.0, "cycle_lat_max": 0.0,
                              "cycle_lats": [], "slow_cycles": 0, "signals": 0,
                              "no_data_sum": 0, "stale_sum": 0,
                              "_last_summary_ts": time.monotonic()})

        sleep_ms = max(0, config.SPREAD_POLL_INTERVAL_MS - elapsed_ms)
        await asyncio.sleep(sleep_ms / 1000)


async def main():
    pairs = load_pairs()
    if not pairs:
        log.warning(_evt("no_pairs",
                         combination_dir=str(config.COMBINATION_DIR),
                         retry_in_s=60))
        # Don't exit — wait in case files appear later
        await asyncio.sleep(60)
        pairs = load_pairs()
        if not pairs:
            log.error(_evt("no_pairs",
                           combination_dir=str(config.COMBINATION_DIR),
                           retry_in_s=0))
            return

    await ensure_signal_csv()

    redis = aioredis.Redis.from_url(config.REDIS_URL)
    try:
        await monitor_loop(redis, pairs)
    except KeyboardInterrupt:
        log.info(_evt("spread_monitor_stop", reason="KeyboardInterrupt"))
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
