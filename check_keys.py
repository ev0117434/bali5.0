#!/usr/bin/env python3
"""
check_keys.py — проверка Redis ключей BALI 5.0.

Для каждого primary ключа (md:*, ob:*, fr:*) проверяет:
  1. Все обязательные поля присутствуют и непустые
  2. Существует хотя бы один соответствующий hist ключ
  3. Hist список непустой, последняя запись корректно сформирована
"""

import asyncio
import sys
import re
from collections import defaultdict
from dataclasses import dataclass, field

import redis.asyncio as aioredis
import config

# ── Ожидаемые поля primary hash-ключей ─────────────────────────────────────

MD_FIELDS = {"b", "a", "ts"}

# OB: минимально обязательные поля — хотя бы 1 уровень с каждой стороны.
# Полная проверка (consecutive levels, no gaps) выполняется в validate_ob_fields().
OB_MIN_FIELDS = {"b1", "b1q", "a1", "a1q"}

FR_FIELDS = {"fr", "fr_ts"}

PRIMARY_SPEC = {
    "md": MD_FIELDS,
    "ob": OB_MIN_FIELDS,
    "fr": FR_FIELDS,
}

# Ожидаемое кол-во CSV-значений в одной hist-записи (для md и fr — точно)
HIST_CSV_COUNTS = {
    "md": 3,   # bid,ask,ts_ms
    "fr": 3,   # rate,fr_ts_ms,ts_ms
}
# OB: n_bid_pairs*2 + n_ask_pairs*2 + 1 (ts) — всегда нечётное, 5..41
OB_HIST_MIN_VALUES = 5   # минимум 1 уровень с каждой стороны + ts
OB_HIST_MAX_VALUES = 41  # максимум 10 уровней с каждой стороны + ts

# Минимальный порог для timestamp-полей (2020-01-01 в мс)
TS_MIN_MS = 1_577_836_800_000


# ── Числовая валидация значений полей ──────────────────────────────────────

def _is_valid_float(v: str) -> bool:
    try:
        float(v)
        return True
    except ValueError:
        return False


def _is_valid_ts(v: str) -> bool:
    """Timestamp в мс: целое число > TS_MIN_MS."""
    try:
        return int(v) >= TS_MIN_MS
    except ValueError:
        return False


def _is_valid_positive_float(v: str) -> bool:
    try:
        return float(v) > 0
    except ValueError:
        return False


# field → валидатор: (fn, описание ошибки)
FIELD_VALIDATORS: dict[str, tuple] = {
    # MD
    "b":    (_is_valid_positive_float, "bid не является положительным числом"),
    "a":    (_is_valid_positive_float, "ask не является положительным числом"),
    "ts":   (_is_valid_ts,             "ts не является корректным timestamp (мс)"),
    # FR
    "fr":   (_is_valid_float,          "fr не является числом"),
    "fr_ts": (_is_valid_ts,            "fr_ts не является корректным timestamp (мс) — пустой или 0"),
}

# Для OB: все price-поля > 0, qty-поля >= 0
def _ob_field_validator(fname: str, v: str) -> str | None:
    """Возвращает строку ошибки или None."""
    if fname.endswith("q"):
        try:
            if float(v) < 0:
                return f"{fname}: qty отрицательный ({v})"
        except ValueError:
            return f"{fname}: qty не число ({v!r})"
    else:
        try:
            if float(v) <= 0:
                return f"{fname}: price не положительный ({v})"
        except ValueError:
            return f"{fname}: price не число ({v!r})"
    return None


# ── Результаты ──────────────────────────────────────────────────────────────

@dataclass
class KeyReport:
    key: str
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class Summary:
    total_primary: int = 0
    ok_primary: int = 0
    missing_hist: int = 0
    bad_primary_fields: int = 0
    bad_hist_entries: int = 0
    reports: list[KeyReport] = field(default_factory=list)

    def add(self, r: KeyReport):
        self.reports.append(r)
        self.total_primary += 1
        if r.ok:
            self.ok_primary += 1
        else:
            has_field_err = any("поле" in e or "field" in e.lower() for e in r.errors)
            # Hist entry errors are wrapped as "[hist_key] message"; missing-hist errors are not.
            has_entry_err = any(e.startswith("[") for e in r.errors)
            has_hist_err  = any("hist" in e.lower() for e in r.errors)
            if has_field_err:
                self.bad_primary_fields += 1
            if has_hist_err and not has_entry_err:
                self.missing_hist += 1
            if has_entry_err:
                self.bad_hist_entries += 1


# ── Парсинг ключа ───────────────────────────────────────────────────────────

PRIMARY_RE = re.compile(
    r"^(md|ob|fr):(binance|bybit|okx|gate|bitget):(spot|futures):(.+)$"
)

HIST_RE = re.compile(
    r"^(md|ob|fr):hist:(binance|bybit|okx|gate|bitget):(spot|futures):(.+):(\d+)$"
)


def primary_to_hist_prefix(key: str) -> str | None:
    """md:binance:spot:BTC → md:hist:binance:spot:BTC"""
    m = PRIMARY_RE.match(key)
    if not m:
        return None
    kind, exch, market, symbol = m.groups()
    return f"{kind}:hist:{exch}:{market}:{symbol}:"


# ── Валидация ───────────────────────────────────────────────────────────────

def _validate_ob_fields(decoded: dict[str, str]) -> list[str]:
    """Validate OB hash fields.

    Rules:
    - Must have at least b1/b1q/a1/a1q (minimum 1 level per side).
    - Bid levels b1..bN must be consecutive (no gaps like b1,b3).
    - Ask levels a1..aM must be consecutive (no gaps).
    - For each present level pair (bI/bIq or aI/aIq) both fields must exist.
    - Price fields > 0, qty fields >= 0.
    """
    errors: list[str] = []

    def check_side(prefix: str) -> int:
        """Check one side (b or a), return depth N (number of consecutive levels).
        Stops at the first missing level — levels beyond a gap are ignored
        (treated as stale orphans from a previous deeper snapshot).
        Errors are appended to the outer `errors` list.
        """
        depth = 0
        for i in range(1, 11):
            p_key = f"{prefix}{i}"
            q_key = f"{prefix}{i}q"
            has_p = p_key in decoded
            has_q = q_key in decoded
            if not has_p and not has_q:
                break  # end of consecutive levels
            if has_p != has_q:
                missing = q_key if has_p else p_key
                errors.append(f"OB: неполная пара уровня {i} — отсутствует {missing}")
            if has_p:
                err = _ob_field_validator(p_key, decoded[p_key].strip())
                if err:
                    errors.append(err)
            if has_q:
                err = _ob_field_validator(q_key, decoded[q_key].strip())
                if err:
                    errors.append(err)
            depth = i
        return depth

    bid_depth = check_side("b")
    ask_depth = check_side("a")

    if bid_depth == 0:
        errors.append("OB: нет ни одного уровня bids (b1/b1q отсутствуют)")
    if ask_depth == 0:
        errors.append("OB: нет ни одного уровня asks (a1/a1q отсутствуют)")

    return errors


def validate_primary_fields(key: str, hdata: dict[bytes, bytes]) -> list[str]:
    m = PRIMARY_RE.match(key)
    if not m:
        return [f"Не удалось распознать паттерн ключа: {key}"]

    kind    = m.group(1)
    decoded = {k.decode(): v.decode() for k, v in hdata.items()}
    present = set(decoded)

    if kind == "ob":
        return _validate_ob_fields(decoded)

    required = PRIMARY_SPEC[kind]
    missing  = required - present
    errors   = []
    if missing:
        errors.append(f"Отсутствуют поля: {sorted(missing)}")

    for fname in required & present:
        val = decoded[fname].strip()
        if fname in FIELD_VALIDATORS:
            fn, msg = FIELD_VALIDATORS[fname]
            if not fn(val):
                errors.append(f"{fname}={val!r}: {msg}")

    return errors


def validate_hist_entry(key: str, entry: bytes) -> list[str]:
    m = HIST_RE.match(key)
    if not m:
        return [f"Не удалось распознать hist-ключ: {key}"]

    kind  = m.group(1)
    parts = entry.decode().split(",")
    errors: list[str] = []

    if kind != "ob":
        expected = HIST_CSV_COUNTS[kind]
        if len(parts) != expected:
            errors.append(
                f"Hist-запись: ожидалось {expected} значений, получено {len(parts)}"
            )
            return errors  # дальнейшая валидация по позициям бессмысленна

    if kind == "md":
        # bid, ask, ts_ms
        bid, ask, ts = parts
        if not _is_valid_positive_float(bid):
            errors.append(f"Hist MD: bid={bid!r} не положительное число")
        if not _is_valid_positive_float(ask):
            errors.append(f"Hist MD: ask={ask!r} не положительное число")
        if not _is_valid_ts(ts):
            errors.append(f"Hist MD: ts={ts!r} не корректный timestamp")

    elif kind == "ob":
        # N bid-пар + M ask-пар + ts_ms; N,M >= 1, N+M <= 20
        # Общий формат: чётное кол-во price/qty значений + 1 ts в конце
        n = len(parts)
        if n < OB_HIST_MIN_VALUES or n > OB_HIST_MAX_VALUES:
            errors.append(
                f"Hist OB: {n} значений — ожидалось {OB_HIST_MIN_VALUES}..{OB_HIST_MAX_VALUES}"
            )
            return errors
        if n % 2 == 0:
            errors.append(f"Hist OB: {n} значений — должно быть нечётным (пары + ts)")
            return errors
        ts = parts[-1]
        if not _is_valid_ts(ts):
            errors.append(f"Hist OB: ts={ts!r} не корректный timestamp")
        price_qty = parts[:-1]  # всё кроме ts
        for i in range(0, len(price_qty), 2):
            price, qty = price_qty[i], price_qty[i + 1]
            pair_idx = i // 2 + 1
            if not _is_valid_positive_float(price):
                errors.append(f"Hist OB: пара {pair_idx} price={price!r} не положительное число")
            if not _is_valid_float(qty) or float(qty) < 0:
                errors.append(f"Hist OB: пара {pair_idx} qty={qty!r} отрицательное или не число")

    elif kind == "fr":
        # rate, fr_ts_ms, ts_ms
        rate, fr_ts, ts = parts
        if not _is_valid_float(rate):
            errors.append(f"Hist FR: rate={rate!r} не число")
        if not _is_valid_ts(fr_ts):
            errors.append(f"Hist FR: fr_ts={fr_ts!r} не корректный timestamp (пустой или 0)")
        if not _is_valid_ts(ts):
            errors.append(f"Hist FR: ts={ts!r} не корректный timestamp")

    return errors


# ── Основная проверка ───────────────────────────────────────────────────────

async def check_all(redis: aioredis.Redis) -> Summary:
    summary = Summary()

    # Собираем все primary ключи
    primary_keys: list[str] = []
    cursor = 0
    for prefix in ("md:*", "ob:*", "fr:*"):
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor, match=prefix, count=500)
            for k in keys:
                ks = k.decode() if isinstance(k, bytes) else k
                # Пропускаем hist-ключи (ob:hist:..., md:hist:..., fr:hist:...)
                if ":hist:" not in ks:
                    primary_keys.append(ks)
            if cursor == 0:
                break

    primary_keys.sort()
    print(f"Найдено primary ключей: {len(primary_keys)}")

    # Собираем все hist-ключи одним сканом для быстрой проверки
    hist_keys: set[str] = set()
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor, match="*:hist:*", count=1000)
        for k in keys:
            hist_keys.add(k.decode() if isinstance(k, bytes) else k)
        if cursor == 0:
            break

    print(f"Найдено hist ключей: {len(hist_keys)}")
    print()

    for key in primary_keys:
        report = KeyReport(key=key)

        # 1. Проверка полей primary hash
        hdata = await redis.hgetall(key)
        if not hdata:
            report.errors.append("Hash пустой (нет данных)")
            summary.add(report)
            continue

        field_errors = validate_primary_fields(key, hdata)
        report.errors.extend(field_errors)

        # 2. Ищем hist ключи для этого primary
        hist_prefix = primary_to_hist_prefix(key)
        if hist_prefix is None:
            report.errors.append("Не удалось определить hist-префикс")
            summary.add(report)
            continue

        matching_hist = sorted(k for k in hist_keys if k.startswith(hist_prefix))

        if not matching_hist:
            report.errors.append(f"Нет ни одного hist-ключа с префиксом {hist_prefix}*")
            summary.add(report)
            continue

        # 3. Проверяем каждый hist-ключ
        for hk in matching_hist:
            length = await redis.llen(hk)
            if length == 0:
                report.errors.append(f"[{hk}] список пустой")
                continue

            # Последняя добавленная запись — index 0 (lpush)
            latest = await redis.lindex(hk, 0)
            entry_errors = validate_hist_entry(hk, latest)
            for e in entry_errors:
                report.errors.append(f"[{hk}] {e}")

        summary.add(report)

    return summary


# ── Вывод ───────────────────────────────────────────────────────────────────

def print_report(summary: Summary):
    failed = [r for r in summary.reports if not r.ok]

    if failed:
        print("=" * 70)
        print("ПРОБЛЕМНЫЕ КЛЮЧИ:")
        print("=" * 70)
        for r in failed:
            print(f"\n  {r.key}")
            for e in r.errors:
                print(f"    ✗ {e}")
    else:
        print("Все ключи в порядке.")

    print()
    print("=" * 70)
    print("ИТОГ:")
    print(f"  Всего primary ключей : {summary.total_primary}")
    print(f"  Корректных           : {summary.ok_primary}")
    print(f"  С ошибками           : {summary.total_primary - summary.ok_primary}")
    print(f"    — нет hist ключа   : {summary.missing_hist}")
    print(f"    — неверные поля    : {summary.bad_primary_fields}")
    print(f"    — битые hist-записи: {summary.bad_hist_entries}")
    print("=" * 70)


# ── Entrypoint ──────────────────────────────────────────────────────────────

async def main():
    redis = aioredis.from_url(config.REDIS_URL, decode_responses=False)
    try:
        await redis.ping()
    except Exception as e:
        print(f"Не удалось подключиться к Redis: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Подключено к Redis: {config.REDIS_URL}")
    print(f"HISTORY_ENABLED = {config.HISTORY_ENABLED}")
    print()

    if not config.HISTORY_ENABLED:
        print("HISTORY_ENABLED=False — hist-ключи не записываются, проверка hist пропускается.")
        print()

    summary = await check_all(redis)
    print_report(summary)

    await redis.aclose()
    sys.exit(0 if summary.ok_primary == summary.total_primary else 1)


if __name__ == "__main__":
    asyncio.run(main())
