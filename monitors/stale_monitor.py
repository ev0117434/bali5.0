#!/usr/bin/env python3
# monitors/stale_monitor.py
"""
BALI 5.0 — Stale data monitor.

Scans md:* keys every 30 seconds.
If a key's 'ts' field is older than STALE_THRESHOLD_SECONDS — logs a WARNING.
Uses SCAN (not KEYS) to avoid blocking Redis.
Output: logs/stale_monitor.log
"""

import asyncio
import sys
import time
from pathlib import Path

import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from logger_setup import setup_logger

log = setup_logger("stale_monitor")

STALE_THRESHOLD_MS = config.STALE_THRESHOLD_SECONDS * 1000


async def monitor_loop(redis: aioredis.Redis):
    log.info(
        f"Stale monitor started | "
        f"threshold={config.STALE_THRESHOLD_SECONDS}s "
        f"interval={config.STALE_CHECK_INTERVAL}s"
    )

    while True:
        t_start   = time.monotonic()
        now_ms    = int(time.time() * 1000)
        stale     = []
        total     = 0

        async for key in redis.scan_iter("md:*", count=100):
            total += 1
            ts_raw = await redis.hget(key, "ts")
            if ts_raw is None:
                log.debug(f"Key {key.decode()} has no ts field")
                continue
            try:
                ts     = int(ts_raw)
                age_ms = now_ms - ts
                if age_ms > STALE_THRESHOLD_MS:
                    age_s = age_ms / 1000
                    stale.append((key.decode(), age_s))
                    log.warning(
                        f"STALE | key={key.decode()} "
                        f"age={age_s:.0f}s last_ts={ts}"
                    )
            except (ValueError, TypeError) as exc:
                log.debug(f"Key {key.decode()} ts parse error: {exc}")

        elapsed_ms = (time.monotonic() - t_start) * 1000
        log.info(
            f"Stale check done: "
            f"total_keys={total} stale={len(stale)} "
            f"elapsed={elapsed_ms:.0f}ms"
        )

        await asyncio.sleep(config.STALE_CHECK_INTERVAL)


async def main():
    redis = aioredis.Redis.from_url(config.REDIS_URL)
    try:
        await monitor_loop(redis)
    except KeyboardInterrupt:
        log.info("stale_monitor stopped")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
