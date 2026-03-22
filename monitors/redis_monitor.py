#!/usr/bin/env python3
# monitors/redis_monitor.py
"""
BALI 5.0 — Redis health monitor.

Runs every 30 seconds. Logs memory, ops/sec, clients, key count.
Warns if memory > 3000 MB or ops/sec > 50000.
Output: logs/redis_monitor.log
"""

import asyncio
import sys
import time
from pathlib import Path

import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from logger_setup import setup_logger

log = setup_logger("redis_monitor")


async def monitor_loop(redis: aioredis.Redis):
    log.info(
        f"Redis monitor started | "
        f"interval={config.REDIS_CHECK_INTERVAL}s "
        f"mem_warn={config.REDIS_MEMORY_WARN_MB}MB "
        f"ops_warn={config.REDIS_OPS_WARN_PER_SEC}/s"
    )

    while True:
        t_start = time.monotonic()

        try:
            await redis.ping()

            info = await redis.info()

            mem_mb      = info["used_memory"] / 1024 / 1024
            mem_peak_mb = info["used_memory_peak"] / 1024 / 1024
            ops_sec     = info.get("instantaneous_ops_per_sec", 0)
            clients     = info.get("connected_clients", 0)
            keyspace    = info.get("db0", {})
            total_keys  = keyspace.get("keys", 0) if isinstance(keyspace, dict) else 0
            hits        = info.get("keyspace_hits", 0)
            misses      = info.get("keyspace_misses", 0)
            hit_rate    = hits / max(hits + misses, 1) * 100

            elapsed_ms = (time.monotonic() - t_start) * 1000

            log.info(
                f"REDIS OK | "
                f"mem={mem_mb:.1f}MB peak={mem_peak_mb:.1f}MB "
                f"ops/s={ops_sec} clients={clients} "
                f"keys={total_keys} hit_rate={hit_rate:.1f}% "
                f"ping={elapsed_ms:.1f}ms"
            )

            if mem_mb > config.REDIS_MEMORY_WARN_MB:
                log.warning(f"Redis memory high: {mem_mb:.0f}MB > {config.REDIS_MEMORY_WARN_MB}MB")

            if ops_sec > config.REDIS_OPS_WARN_PER_SEC:
                log.warning(f"Redis ops/sec high: {ops_sec} > {config.REDIS_OPS_WARN_PER_SEC}")

        except Exception as exc:
            log.error(f"Redis UNREACHABLE: {exc}")

        await asyncio.sleep(config.REDIS_CHECK_INTERVAL)


async def main():
    redis = aioredis.Redis.from_url(config.REDIS_URL)
    try:
        await monitor_loop(redis)
    except KeyboardInterrupt:
        log.info("redis_monitor stopped")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
