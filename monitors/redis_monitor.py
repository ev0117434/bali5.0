#!/usr/bin/env python3
# monitors/redis_monitor.py
"""
BALI 5.0 — Redis health monitor.

Runs every 30 seconds. Logs memory, ops/sec, clients, key count,
RSS, fragmentation ratio, blocked clients, eventloop duration,
network kbps, and lpush/hset p99 latency.
Warns if memory > 3000 MB or ops/sec > 200000.
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

_COMPONENT = "redis_monitor"


def _evt(event: str, **kwargs) -> dict:
    return {"ts": int(time.time() * 1000), "component": _COMPONENT, "event": event, **kwargs}


async def monitor_loop(redis: aioredis.Redis):
    log.info(_evt("redis_monitor_start",
                  interval_s=config.REDIS_CHECK_INTERVAL,
                  memory_warn_mb=config.REDIS_MEMORY_WARN_MB,
                  ops_warn_per_sec=config.REDIS_OPS_WARN_PER_SEC,
                  frag_warn=1.5))

    while True:
        t_start = time.monotonic()

        try:
            await redis.ping()

            info = await redis.info("all")

            mem_mb      = info["used_memory"] / 1024 / 1024
            mem_peak_mb = info["used_memory_peak"] / 1024 / 1024
            mem_rss_mb  = info.get("used_memory_rss", 0) / 1024 / 1024
            frag_ratio  = info.get("mem_fragmentation_ratio", 0.0)
            ops_sec     = info.get("instantaneous_ops_per_sec", 0)
            clients     = info.get("connected_clients", 0)
            blocked     = info.get("blocked_clients", 0)
            keyspace    = info.get("db0", {})
            total_keys  = keyspace.get("keys", 0) if isinstance(keyspace, dict) else 0
            hits        = info.get("keyspace_hits", 0)
            misses      = info.get("keyspace_misses", 0)
            hit_rate    = hits / max(hits + misses, 1) * 100
            net_in_kbps = info.get("instantaneous_input_kbps", 0.0)
            net_out_kbps= info.get("instantaneous_output_kbps", 0.0)
            el_us       = info.get("instantaneous_eventloop_duration_usec", 0)

            # p99 latency from latency_percentiles (usec)
            # redis-py parses "latency_percentiles_usec_lpush:p50=1,p99=3" → dict value
            lpush_info = info.get("latency_percentiles_usec_lpush", {})
            hset_info  = info.get("latency_percentiles_usec_hset", {})
            lpush_p99  = lpush_info.get("p99", 0) if isinstance(lpush_info, dict) else 0
            hset_p99   = hset_info.get("p99", 0)  if isinstance(hset_info, dict)  else 0

            elapsed_ms = (time.monotonic() - t_start) * 1000

            warnings = []
            if mem_mb > config.REDIS_MEMORY_WARN_MB:
                warnings.append("memory_high")
            if ops_sec > config.REDIS_OPS_WARN_PER_SEC:
                warnings.append("ops_high")
            if frag_ratio > 1.5:
                warnings.append("fragmentation_high")
            if blocked > config.REDIS_BLOCKED_WARN_THRESHOLD:
                warnings.append("blocked_clients")

            log.info(_evt("redis_health",
                status="warn" if warnings else "ok",
                memory={"used_mb": round(mem_mb, 1), "rss_mb": round(mem_rss_mb, 1),
                        "peak_mb": round(mem_peak_mb, 1), "frag_ratio": round(frag_ratio, 2)},
                ops={"per_sec": ops_sec, "clients": clients, "blocked": blocked,
                     "keys": total_keys, "hit_rate_pct": round(hit_rate, 1)},
                network={"in_kbps": int(net_in_kbps), "out_kbps": int(net_out_kbps)},
                latency={"ping_ms": round(elapsed_ms, 1), "eventloop_us": el_us,
                         "lpush_p99_us": lpush_p99, "hset_p99_us": hset_p99},
                warnings=warnings))

            for w in warnings:
                value = {"memory_high": mem_mb, "ops_high": float(ops_sec),
                         "fragmentation_high": frag_ratio, "blocked_clients": float(blocked)}[w]
                threshold = {"memory_high": float(config.REDIS_MEMORY_WARN_MB),
                             "ops_high": float(config.REDIS_OPS_WARN_PER_SEC),
                             "fragmentation_high": 1.5,
                             "blocked_clients": float(config.REDIS_BLOCKED_WARN_THRESHOLD)}[w]
                log.warning(_evt("redis_warn", warning=w,
                                 value=round(value, 2), threshold=threshold))

        except Exception as exc:
            log.error(_evt("redis_unreachable",
                           error_type=type(exc).__name__, error_msg=str(exc)))

        await asyncio.sleep(config.REDIS_CHECK_INTERVAL)


async def main():
    redis = aioredis.Redis.from_url(config.REDIS_URL)
    try:
        await monitor_loop(redis)
    except KeyboardInterrupt:
        log.info(_evt("redis_monitor_stop", reason="KeyboardInterrupt"))
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
