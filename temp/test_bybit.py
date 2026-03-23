#!/usr/bin/env python3
"""
Диагностика Bybit MD / OB / FR
Подключается к spot и futures, подписывается на 3 символа,
печатает сырые сообщения и результаты парсеров.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

import websockets

sys.path.insert(0, str(Path(__file__).parent.parent))
from collectors.collector_bybit import parse_md, parse_ob, parse_fr_from_ticker

_WS_SPOT    = "wss://stream.bybit.com/v5/public/spot"
_WS_FUTURES = "wss://stream.bybit.com/v5/public/linear"

TEST_SPOT    = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
TEST_FUTURES = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

DURATION     = 30  # секунд

counters = {
    "raw_spot_md": 0, "raw_fut_md": 0,
    "raw_spot_ob": 0, "raw_fut_ob": 0,
    "raw_fut_fr": 0,
    "parsed_spot_md": 0, "parsed_fut_md": 0,
    "parsed_spot_ob": 0, "parsed_fut_ob": 0,
    "parsed_fr": 0,
    "skipped_empty_ob": 0,
    "skipped_no_topic": 0,
}

MAX_PRINTS = 3  # сколько сообщений каждого типа печатать полностью
prints = {k: 0 for k in ["spot_md", "fut_md", "spot_ob", "fut_ob", "fr", "unknown_spot", "unknown_fut"]}


def show(label: str, raw: str, parsed=None):
    key = label.replace(" ", "_")
    if prints.get(key, 0) < MAX_PRINTS:
        prints[key] = prints.get(key, 0) + 1
        try:
            pretty = json.dumps(json.loads(raw), indent=2)
        except Exception:
            pretty = raw[:300]
        print(f"\n{'='*60}")
        print(f"[{label}] RAW:")
        print(pretty[:800])
        if parsed is not None:
            print(f"  → PARSED: {parsed}")
        print(f"{'='*60}")


async def test_spot_md():
    print(f"\n[spot_md] Connecting {_WS_SPOT} ...")
    async with websockets.connect(_WS_SPOT, ping_interval=None, max_size=2**23) as ws:
        sub = {"op": "subscribe", "args": [f"tickers.{s}" for s in TEST_SPOT]}
        await ws.send(json.dumps(sub))
        print(f"[spot_md] Subscribed: {sub['args']}")
        deadline = time.time() + DURATION
        async for raw in ws:
            if time.time() > deadline:
                break
            msg = json.loads(raw)
            counters["raw_spot_md"] += 1
            if "topic" not in msg:
                counters["skipped_no_topic"] += 1
                show("unknown_spot", raw)
                continue
            parsed = parse_md(raw)
            if parsed:
                counters["parsed_spot_md"] += 1
                show("spot_md", raw, parsed)


async def test_fut_md():
    print(f"\n[fut_md] Connecting {_WS_FUTURES} ...")
    async with websockets.connect(_WS_FUTURES, ping_interval=None, max_size=2**23) as ws:
        sub = {"op": "subscribe", "args": [f"tickers.{s}" for s in TEST_FUTURES]}
        await ws.send(json.dumps(sub))
        print(f"[fut_md] Subscribed: {sub['args']}")
        deadline = time.time() + DURATION
        async for raw in ws:
            if time.time() > deadline:
                break
            msg = json.loads(raw)
            counters["raw_fut_md"] += 1
            if "topic" not in msg:
                counters["skipped_no_topic"] += 1
                continue
            parsed = parse_md(raw)
            if parsed:
                counters["parsed_fut_md"] += 1
                show("fut_md", raw, parsed)
            fr_parsed = parse_fr_from_ticker(raw)
            if fr_parsed:
                counters["parsed_fr"] += 1
                show("fr", raw, fr_parsed)


async def test_spot_ob():
    print(f"\n[spot_ob] Connecting {_WS_SPOT} ...")
    async with websockets.connect(_WS_SPOT, ping_interval=None, max_size=2**23) as ws:
        sub = {"op": "subscribe", "args": [f"orderbook.10.{s}" for s in TEST_SPOT]}
        await ws.send(json.dumps(sub))
        print(f"[spot_ob] Subscribed: {sub['args']}")
        deadline = time.time() + DURATION
        async for raw in ws:
            if time.time() > deadline:
                break
            msg = json.loads(raw)
            counters["raw_spot_ob"] += 1
            if "topic" not in msg:
                counters["skipped_no_topic"] += 1
                show("unknown_spot", raw)
                continue
            # Показываем структуру data
            data = msg.get("data", {})
            b = data.get("b", [])
            a = data.get("a", [])
            s_field = data.get("s", "")
            if prints.get("spot_ob_raw", 0) < MAX_PRINTS:
                prints["spot_ob_raw"] = prints.get("spot_ob_raw", 0) + 1
                print(f"\n[spot_ob] topic={msg.get('topic')} type={msg.get('type')} "
                      f"data.s={s_field!r} b_len={len(b)} a_len={len(a)}")
                if b:
                    print(f"  bids[0]: {b[0]}")
                if a:
                    print(f"  asks[0]: {a[0]}")
            parsed = parse_ob(raw)
            if parsed:
                counters["parsed_spot_ob"] += 1
                show("spot_ob", raw, f"symbol={parsed[0]} bids={len(parsed[1])} asks={len(parsed[2])}")
            else:
                counters["skipped_empty_ob"] += 1


async def test_fut_ob():
    print(f"\n[fut_ob] Connecting {_WS_FUTURES} ...")
    async with websockets.connect(_WS_FUTURES, ping_interval=None, max_size=2**23) as ws:
        sub = {"op": "subscribe", "args": [f"orderbook.10.{s}" for s in TEST_FUTURES]}
        await ws.send(json.dumps(sub))
        print(f"[fut_ob] Subscribed: {sub['args']}")
        deadline = time.time() + DURATION
        async for raw in ws:
            if time.time() > deadline:
                break
            msg = json.loads(raw)
            counters["raw_fut_ob"] += 1
            if "topic" not in msg:
                counters["skipped_no_topic"] += 1
                continue
            data = msg.get("data", {})
            b = data.get("b", [])
            a = data.get("a", [])
            s_field = data.get("s", "")
            if prints.get("fut_ob_raw", 0) < MAX_PRINTS:
                prints["fut_ob_raw"] = prints.get("fut_ob_raw", 0) + 1
                print(f"\n[fut_ob] topic={msg.get('topic')} type={msg.get('type')} "
                      f"data.s={s_field!r} b_len={len(b)} a_len={len(a)}")
                if b:
                    print(f"  bids[0]: {b[0]}")
                if a:
                    print(f"  asks[0]: {a[0]}")
            parsed = parse_ob(raw)
            if parsed:
                counters["parsed_fut_ob"] += 1
                show("fut_ob", raw, f"symbol={parsed[0]} bids={len(parsed[1])} asks={len(parsed[2])}")
            else:
                counters["skipped_empty_ob"] += 1


async def main():
    print(f"Тестируем Bybit MD/OB/FR на {TEST_SPOT} | Длительность: {DURATION}s")
    print("Запускаем 4 соединения параллельно...\n")

    await asyncio.gather(
        test_spot_md(),
        test_fut_md(),
        test_spot_ob(),
        test_fut_ob(),
    )

    print("\n" + "="*60)
    print("ИТОГИ ТЕСТА")
    print("="*60)
    print(f"RAW сообщений spot_md:   {counters['raw_spot_md']}")
    print(f"RAW сообщений fut_md:    {counters['raw_fut_md']}")
    print(f"RAW сообщений spot_ob:   {counters['raw_spot_ob']}")
    print(f"RAW сообщений fut_ob:    {counters['raw_fut_ob']}")
    print()
    print(f"Успешно parsed spot_md:  {counters['parsed_spot_md']}")
    print(f"Успешно parsed fut_md:   {counters['parsed_fut_md']}")
    print(f"Успешно parsed spot_ob:  {counters['parsed_spot_ob']}")
    print(f"Успешно parsed fut_ob:   {counters['parsed_fut_ob']}")
    print(f"Успешно parsed FR:       {counters['parsed_fr']}")
    print()
    print(f"Пропущено (нет topic):   {counters['skipped_no_topic']}")
    print(f"Пропущено (пустой OB):   {counters['skipped_empty_ob']}")
    print()
    # Диагностика
    if counters['raw_spot_ob'] > 0 and counters['parsed_spot_ob'] == 0:
        print("⚠️  BUG: spot_ob получает данные, но parse_ob возвращает None ВСЕГДА")
    if counters['raw_fut_ob'] > 0 and counters['parsed_fut_ob'] == 0:
        print("⚠️  BUG: fut_ob получает данные, но parse_ob возвращает None ВСЕГДА")
    if counters['raw_spot_ob'] == 0:
        print("⚠️  BUG: spot_ob не получает НИКАКИХ сообщений от Bybit")
    if counters['raw_fut_ob'] == 0:
        print("⚠️  BUG: fut_ob не получает НИКАКИХ сообщений от Bybit")
    if counters['skipped_empty_ob'] > 0:
        empty_pct = counters['skipped_empty_ob'] / max(counters['raw_spot_ob'] + counters['raw_fut_ob'], 1) * 100
        print(f"ℹ️  {counters['skipped_empty_ob']} OB сообщений с пустыми b/a ({empty_pct:.1f}%) — delta-обновления")


if __name__ == "__main__":
    asyncio.run(main())
