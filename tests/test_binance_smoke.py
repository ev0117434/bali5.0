# tests/test_binance_smoke.py
"""
Smoke test for Binance collector — requires Redis and internet access.
Runs collector for 15 seconds, verifies Redis keys are populated.

Usage:
    pytest tests/test_binance_smoke.py -v -s --timeout=60
"""

import asyncio
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="module")
def redis_client():
    import redis
    r = redis.Redis.from_url("redis://localhost:6379")
    try:
        r.ping()
    except Exception:
        pytest.skip("Redis not available")
    yield r
    r.close()


def test_binance_collector_smoke(redis_client):
    """
    Start collector_binance.py as subprocess for 15 seconds.
    Verify that MD, OB, FR keys appear in Redis.
    """
    proc = subprocess.Popen(
        [sys.executable, "collectors/collector_binance.py"],
        cwd=str(Path(__file__).parent.parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        time.sleep(15)  # allow time to connect and receive data
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    # Check MD keys exist
    md_spot_keys = redis_client.keys("md:binance:spot:*")
    assert len(md_spot_keys) > 0, (
        "No md:binance:spot:* keys found in Redis after 15s"
    )

    md_fut_keys = redis_client.keys("md:binance:futures:*")
    assert len(md_fut_keys) > 0, (
        "No md:binance:futures:* keys found in Redis after 15s"
    )

    # Check OB keys exist
    ob_spot_keys = redis_client.keys("ob:binance:spot:*")
    assert len(ob_spot_keys) > 0, (
        "No ob:binance:spot:* keys found in Redis after 15s"
    )

    ob_fut_keys = redis_client.keys("ob:binance:futures:*")
    assert len(ob_fut_keys) > 0, (
        "No ob:binance:futures:* keys found in Redis after 15s"
    )

    # Check FR keys exist
    fr_keys = redis_client.keys("fr:binance:futures:*")
    assert len(fr_keys) > 0, (
        "No fr:binance:futures:* keys found in Redis after 15s"
    )

    # Validate MD key structure for BTCUSDT
    btc_md = redis_client.hgetall("md:binance:spot:BTCUSDT")
    assert b"b" in btc_md, "md:binance:spot:BTCUSDT missing 'b' (bid) field"
    assert b"a" in btc_md, "md:binance:spot:BTCUSDT missing 'a' (ask) field"
    assert b"ts" in btc_md, "md:binance:spot:BTCUSDT missing 'ts' field"

    bid = float(btc_md[b"b"])
    ask = float(btc_md[b"a"])
    ts  = int(btc_md[b"ts"])

    assert bid > 0, f"bid must be positive, got {bid}"
    assert ask > 0, f"ask must be positive, got {ask}"
    assert ask >= bid, f"ask ({ask}) must be >= bid ({bid})"

    # ts should be recent (within last 60 seconds)
    now_ms = int(time.time() * 1000)
    assert now_ms - ts < 60_000, f"MD timestamp too old: {now_ms - ts}ms ago"

    # Validate OB key structure for BTCUSDT
    btc_ob = redis_client.hgetall("ob:binance:spot:BTCUSDT")
    assert b"b1" in btc_ob, "ob:binance:spot:BTCUSDT missing 'b1' field"
    assert b"a1" in btc_ob, "ob:binance:spot:BTCUSDT missing 'a1' field"
    assert b"b1q" in btc_ob, "ob:binance:spot:BTCUSDT missing 'b1q' field"

    # Validate FR key
    btc_fr = redis_client.hgetall("fr:binance:futures:BTCUSDT")
    assert b"fr" in btc_fr, "fr:binance:futures:BTCUSDT missing 'fr' field"
    assert b"fr_ts" in btc_fr, "fr:binance:futures:BTCUSDT missing 'fr_ts' field"

    fr_rate = float(btc_fr[b"fr"])
    assert -0.01 < fr_rate < 0.01, f"FR rate {fr_rate} out of expected range"

    print(f"\n✓ Smoke test passed:")
    print(f"  md_spot={len(md_spot_keys)} keys, md_fut={len(md_fut_keys)} keys")
    print(f"  ob_spot={len(ob_spot_keys)} keys, ob_fut={len(ob_fut_keys)} keys")
    print(f"  fr={len(fr_keys)} keys")
    print(f"  BTCUSDT bid={bid:.2f} ask={ask:.2f} fr={fr_rate:.8f}")
