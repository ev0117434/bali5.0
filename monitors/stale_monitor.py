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

_COMPONENT = "stale_monitor"


def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}


async def monitor_loop(redis: aioredis.Redis):
    log.info(_evt("stale_monitor_start",
                  threshold_s=config.STALE_THRESHOLD_SECONDS,
                  interval_s=config.STALE_CHECK_INTERVAL))

    while True:
        t_start   = time.monotonic()
        now_ms    = int(time.time() * 1000)
        stale     = []
        total     = 0

        async for key in redis.scan_iter("md:*", count=100):
            if key.startswith(b"md:hist:"):
                continue
            total += 1
            try:
                ts_raw = await redis.hget(key, "ts")
            except aioredis.ResponseError:
                log.debug(_evt("key_skip", key=key.decode(), reason="not_a_hash"))
                continue
            if ts_raw is None:
                log.debug(_evt("key_skip", key=key.decode(), reason="no_ts_field"))
                continue
            try:
                ts     = int(ts_raw)
                age_ms = now_ms - ts
                if age_ms > STALE_THRESHOLD_MS:
                    age_s = age_ms / 1000
                    stale.append((key.decode(), age_s))
                    log.warning(_evt("stale_key",
                                     key=key.decode(),
                                     age_s=round(age_s, 0),
                                     last_ts=ts,
                                     threshold_s=config.STALE_THRESHOLD_SECONDS))
            except (ValueError, TypeError) as exc:
                log.debug(_evt("key_skip", key=key.decode(), reason="ts_parse_error", error_msg=str(exc)))

        elapsed_ms = (time.monotonic() - t_start) * 1000
        log.info(_evt("stale_scan",
                      total_keys=total,
                      stale_keys=len(stale),
                      scan_lat_ms=round(elapsed_ms, 0),
                      threshold_s=config.STALE_THRESHOLD_SECONDS))

        await asyncio.sleep(config.STALE_CHECK_INTERVAL)


async def main():
    redis = aioredis.Redis.from_url(config.REDIS_URL)
    try:
        await monitor_loop(redis)
    except KeyboardInterrupt:
        log.info(_evt("stale_monitor_stop", reason="KeyboardInterrupt"))
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
