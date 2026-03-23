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
        log.warning(f"No combination files found in {config.COMBINATION_DIR}")
        return []

    for fname in files:
        try:
            spot_exch, fut_exch = _parse_combo_filename(fname)
        except (IndexError, ValueError) as exc:
            log.warning(f"Could not parse combo filename {fname}: {exc}")
            continue

        with open(fname) as f:
            for line in f:
                symbol = line.strip().upper()
                if symbol:
                    pairs.append((spot_exch, fut_exch, symbol))

    log.info(
        f"Loaded {len(pairs)} pairs from {len(files)} combination files "
        f"in {config.COMBINATION_DIR}"
    )
    return pairs


# ── Signal CSV setup ───────────────────────────────────────────────────────

async def ensure_signal_csv():
    Path(config.SIGNAL_DIR).mkdir(parents=True, exist_ok=True)
    csv_path = config.SIGNAL_CSV
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        async with aiofiles.open(csv_path, "w") as f:
            await f.write(config.SIGNAL_CSV_HEADER + "\n")
        log.info(f"Created {csv_path} with header")


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

    log.info(
        f"SIGNAL | {spot_exch}→{fut_exch} {symbol} "
        f"ask_spot={ask_spot:.4f} bid_fut={bid_fut:.4f} "
        f"spread={spread_pct:.4f}%"
    )


# ── Main monitor loop ──────────────────────────────────────────────────────

async def monitor_loop(redis: aioredis.Redis, pairs: list):
    write_lock = asyncio.Lock()

    log.info(
        f"Spread monitor loop started | "
        f"pairs={len(pairs)} "
        f"threshold={config.SPREAD_THRESHOLD}% "
        f"interval={config.SPREAD_POLL_INTERVAL_MS}ms "
        f"stale_ms={config.SPREAD_DATA_STALE_MS}ms"
    )

    while True:
        t_cycle_start = time.monotonic()
        now_ms        = int(time.time() * 1000)

        # Single pipeline: 2 HMGET per pair
        pipe = redis.pipeline(transaction=False)
        for spot_exch, fut_exch, symbol in pairs:
            pipe.hmget(f"md:{spot_exch}:spot:{symbol}", "a", "ts")
            pipe.hmget(f"md:{fut_exch}:futures:{symbol}", "b", "ts")

        try:
            results = await pipe.execute()
        except Exception as exc:
            log.error(f"Redis pipeline error: {exc}")
            await asyncio.sleep(config.SPREAD_POLL_INTERVAL_MS / 1000)
            continue

        # Process results
        signals_count  = 0
        stale_count    = 0
        no_data_count  = 0

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
                    )
                )
                tasks.append(task)

        # Wait for all signal tasks (usually 0 or very few)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        elapsed_ms = (time.monotonic() - t_cycle_start) * 1000
        log.debug(
            f"Cycle: {elapsed_ms:.1f}ms | "
            f"pairs={len(pairs)} no_data={no_data_count} "
            f"stale={stale_count} signals={signals_count}"
        )

        if elapsed_ms > 250:
            log.warning(f"Cycle slow: {elapsed_ms:.1f}ms > 250ms threshold")

        sleep_ms = max(0, config.SPREAD_POLL_INTERVAL_MS - elapsed_ms)
        await asyncio.sleep(sleep_ms / 1000)


async def main():
    pairs = load_pairs()
    if not pairs:
        log.warning(
            "No pairs loaded. "
            "Run dictionaries/main.py first to generate combination files."
        )
        # Don't exit — wait in case files appear later
        await asyncio.sleep(60)
        pairs = load_pairs()
        if not pairs:
            log.error("Still no pairs after retry. Exiting.")
            return

    await ensure_signal_csv()

    redis = aioredis.Redis.from_url(config.REDIS_URL)
    try:
        await monitor_loop(redis, pairs)
    except KeyboardInterrupt:
        log.info("spread_monitor stopped")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
