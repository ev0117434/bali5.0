#!/usr/bin/env python3
"""
logs_to_csv.py — конвертер NDJSON-логов BALI 5.0 в CSV.

Читает все *.log из папки logs/, пишет CSV в logs/csv/.
Сигнальные события (signal, signal_received, stream_read) не включаются.

Выходные файлы:
  launcher.csv                — жизненный цикл запуска
  redis_health.csv            — метрики Redis каждые 30 сек
  redis_warnings.csv          — предупреждения Redis (frag_high, mem_high)
  stale_scans.csv             — результаты сканирования stale-ключей
  stale_keys.csv              — список stale-ключей с возрастом
  spread_cycles.csv           — каждый цикл spread_monitor (пары, задержки)
  spread_summaries.csv        — агрегаты spread_monitor каждые 30 сек
  snapshot_events.csv         — старт снапшотов и загрузка истории
  snapshot_progress.csv       — прогресс записи снапшотов
  collectors_metrics.csv      — метрики производительности коллекторов (5 сек)
  collectors_ws_events.csv    — WS-соединения, разрывы, реконнекты
  collectors_flush_alerts.csv — медленные flush (slow_flush)
  collectors_chunk_rotates.csv— ротации history-чанков
"""

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

LOGS_DIR = Path(__file__).parent / "logs"
CSV_DIR  = LOGS_DIR / "csv"


# ── Утилиты ──────────────────────────────────────────────────────────────────

def ts_to_dt(ts_ms: int) -> str:
    """Unix ms → строка 'YYYY-MM-DD HH:MM:SS.mmm' (UTC)."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]


def read_ndjson(path: Path) -> list[dict]:
    records = []
    with open(path, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  [warn] {path.name}:{lineno} — JSON error: {e}", file=sys.stderr)
    return records


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print(f"  [пусто]  {path.name}")
        return
    # Собираем порядок колонок, сохраняя порядок появления
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for k in row:
            if k not in seen:
                fieldnames.append(k)
                seen.add(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  [ok]     {path.name:<40s} {len(rows):>6} строк  {len(fieldnames):>3} колонок")


# ── Обработчики ──────────────────────────────────────────────────────────────

def process_launcher(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        ev = d.get("event", "")
        row = {
            "ts":                    d["ts"],
            "datetime":              ts_to_dt(d["ts"]),
            "event":                 ev,
            "level":                 d.get("level"),
            # launcher_start
            "history_enabled":       d.get("history_enabled"),
            "processes":             "|".join(d.get("processes", [])) if d.get("processes") else None,
            # redis events
            "redis_url":             d.get("url"),
            "redis_attempt":         d.get("attempt"),
            "keys_removed":          d.get("keys_removed"),
            # redis_setup_stdout
            "output":                d.get("output"),
            # subscribe_file_ok
            "subscribe_path":        d.get("path"),
            "subscribe_count":       d.get("count"),
            # process_started
            "process_name":          d.get("name"),
            "pid":                   d.get("pid"),
            "script":                d.get("script"),
            # timing
            "seconds":               d.get("seconds"),
            "health_check_interval": d.get("health_check_interval_s"),
        }
        rows.append(row)
    return rows


def process_redis_health(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "redis_health":
            continue
        m   = d.get("memory",  {})
        op  = d.get("ops",     {})
        net = d.get("network", {})
        lat = d.get("latency", {})
        rows.append({
            "ts":                d["ts"],
            "datetime":          ts_to_dt(d["ts"]),
            "status":            d.get("status"),
            "warnings":          "|".join(d.get("warnings", [])),
            # memory
            "mem_used_mb":       m.get("used_mb"),
            "mem_rss_mb":        m.get("rss_mb"),
            "mem_peak_mb":       m.get("peak_mb"),
            "mem_frag_ratio":    m.get("frag_ratio"),
            # ops
            "ops_per_sec":       op.get("per_sec"),
            "clients":           op.get("clients"),
            "clients_blocked":   op.get("blocked"),
            "keys":              op.get("keys"),
            "hit_rate_pct":      op.get("hit_rate_pct"),
            # network
            "net_in_kbps":       net.get("in_kbps"),
            "net_out_kbps":      net.get("out_kbps"),
            # latency
            "lat_ping_ms":       lat.get("ping_ms"),
            "lat_eventloop_us":  lat.get("eventloop_us"),
            "lat_lpush_p99_us":  lat.get("lpush_p99_us"),
            "lat_hset_p99_us":   lat.get("hset_p99_us"),
        })
    return rows


def process_redis_warnings(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "redis_warn":
            continue
        rows.append({
            "ts":         d["ts"],
            "datetime":   ts_to_dt(d["ts"]),
            "warning":    d.get("warning"),
            "value":      d.get("value"),
            "threshold":  d.get("threshold"),
        })
    return rows


def process_stale_scans(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "stale_scan":
            continue
        rows.append({
            "ts":            d["ts"],
            "datetime":      ts_to_dt(d["ts"]),
            "total_keys":    d.get("total_keys"),
            "stale_keys":    d.get("stale_keys"),
            "scan_lat_ms":   d.get("scan_lat_ms"),
            "threshold_s":   d.get("threshold_s"),
        })
    return rows


def process_stale_keys(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "stale_key":
            continue
        rows.append({
            "ts":           d["ts"],
            "datetime":     ts_to_dt(d["ts"]),
            "key":          d.get("key"),
            "age_s":        d.get("age_s"),
            "last_ts":      d.get("last_ts"),
            "threshold_s":  d.get("threshold_s"),
        })
    return rows


def process_spread_cycles(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "cycle":
            continue
        rows.append({
            "ts":               d["ts"],
            "datetime":         ts_to_dt(d["ts"]),
            "pairs_total":      d.get("pairs_total"),
            "pairs_ok":         d.get("pairs_ok"),
            "pairs_no_data":    d.get("pairs_no_data"),
            "pairs_stale":      d.get("pairs_stale"),
            "signals":          d.get("signals"),
            "cycle_lat_ms":     d.get("cycle_lat_ms"),
            "pipeline_lat_ms":  d.get("pipeline_lat_ms"),
            "calc_lat_ms":      d.get("calc_lat_ms"),
        })
    return rows


def process_spread_summaries(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        ev = d.get("event", "")
        if ev == "spread_monitor_start":
            rows.append({
                "ts":                  d["ts"],
                "datetime":            ts_to_dt(d["ts"]),
                "event":               ev,
                "pairs":               d.get("pairs"),
                "threshold_pct":       d.get("threshold_pct"),
                "poll_interval_ms":    d.get("poll_interval_ms"),
                "stale_threshold_ms":  d.get("stale_threshold_ms"),
            })
        elif ev == "pairs_loaded":
            rows.append({
                "ts":               d["ts"],
                "datetime":         ts_to_dt(d["ts"]),
                "event":            ev,
                "pairs":            d.get("pairs"),
                "files":            d.get("files"),
                "combination_dir":  d.get("combination_dir"),
            })
        elif ev == "spread_summary":
            rows.append({
                "ts":                   d["ts"],
                "datetime":             ts_to_dt(d["ts"]),
                "event":                ev,
                "interval_s":           d.get("interval_s"),
                "cycles":               d.get("cycles"),
                "cycle_lat_avg_ms":     d.get("cycle_lat_avg_ms"),
                "cycle_lat_max_ms":     d.get("cycle_lat_max_ms"),
                "cycle_lat_p99_ms":     d.get("cycle_lat_p99_ms"),
                "slow_cycles":          d.get("slow_cycles"),
                "signals_total":        d.get("signals_total"),
                "pairs_no_data_avg":    d.get("pairs_no_data_avg"),
                "pairs_stale_avg":      d.get("pairs_stale_avg"),
            })
    return rows


def process_snapshot_events(records: list[dict]) -> list[dict]:
    """snapshot_start, history_load_start, history_loaded, snapshot_monitor_start."""
    INCLUDE = {
        "snapshot_monitor_start",
        "snapshot_start",
        "history_load_start",
        "history_loaded",
        "snapshot_complete",
        "snapshot_error",
    }
    rows = []
    for d in records:
        ev = d.get("event", "")
        if ev not in INCLUDE:
            continue
        row = {
            "ts":              d["ts"],
            "datetime":        ts_to_dt(d["ts"]),
            "event":           ev,
            "symbol":          d.get("symbol"),
            "file":            d.get("file"),
            # snapshot_monitor_start
            "duration_s":      d.get("duration_s"),
            "interval_ms":     d.get("interval_ms"),
            "lookback_min":    d.get("lookback_min"),
            # history_load_start
            "lookback_ms":     d.get("lookback_ms"),
            "chunks_to_load":  d.get("chunks_to_load"),
            # history_loaded
            "rows_total":      d.get("rows_total"),
            "load_lat_ms":     d.get("load_lat_ms"),
            # snapshot_complete / error
            "total_rows":      d.get("total_rows"),
            "error":           d.get("error"),
        }
        rows.append(row)
    return rows


def process_snapshot_progress(records: list[dict]) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "snapshot_progress":
            continue
        rows.append({
            "ts":               d["ts"],
            "datetime":         ts_to_dt(d["ts"]),
            "symbol":           d.get("symbol"),
            "file":             d.get("file"),
            "elapsed_s":        d.get("elapsed_s"),
            "remaining_s":      d.get("remaining_s"),
            "rows_written":     d.get("rows_written"),
            "row_lat_avg_ms":   d.get("row_lat_avg_ms"),
            "row_lat_max_ms":   d.get("row_lat_max_ms"),
            "row_lat_last_ms":  d.get("row_lat_last_ms"),
        })
    return rows


def process_collector_metrics(records: list[dict], exchange: str) -> list[dict]:
    """metrics_interval — подробная телеметрия каждые 5 сек."""
    rows = []
    for d in records:
        if d.get("event") != "metrics_interval":
            continue
        ing  = d.get("ingestion",      {})
        pfl  = d.get("primary_flush",  {})
        hfl  = d.get("history_flush",  {})
        lat  = d.get("latency",        {})
        st   = d.get("state",          {})
        rows.append({
            "ts":                          d["ts"],
            "datetime":                    ts_to_dt(d["ts"]),
            "exchange":                    exchange,
            "interval_s":                  d.get("interval_s"),
            # ingestion
            "ing_md_msgs":                 ing.get("md_msgs"),
            "ing_md_msgs_per_s":           ing.get("md_msgs_per_s"),
            "ing_ob_msgs":                 ing.get("ob_msgs"),
            "ing_ob_msgs_per_s":           ing.get("ob_msgs_per_s"),
            "ing_fr_msgs":                 ing.get("fr_msgs"),
            "ing_fr_msgs_per_s":           ing.get("fr_msgs_per_s"),
            "ing_parse_errors":            ing.get("parse_errors"),
            "ing_parse_errors_per_s":      ing.get("parse_errors_per_s"),
            # primary flush
            "pfl_count":                   pfl.get("count"),
            "pfl_count_per_s":             pfl.get("count_per_s"),
            "pfl_batch_avg":               pfl.get("batch_avg"),
            "pfl_lat_avg_ms":              pfl.get("lat_avg_ms"),
            "pfl_lat_max_ms":              pfl.get("lat_max_ms"),
            "pfl_slow_count":              pfl.get("slow_count"),
            # history flush
            "hfl_count":                   hfl.get("count"),
            "hfl_count_per_s":             hfl.get("count_per_s"),
            "hfl_cmds_total":              hfl.get("cmds_total"),
            "hfl_cmds_per_s":              hfl.get("cmds_per_s"),
            "hfl_lat_avg_ms":              hfl.get("lat_avg_ms"),
            "hfl_lat_max_ms":              hfl.get("lat_max_ms"),
            "hfl_slow_count":              hfl.get("slow_count"),
            "hfl_ob_skipped":              hfl.get("ob_skipped"),
            "hfl_ob_skipped_per_s":        hfl.get("ob_skipped_per_s"),
            # latency
            "lat_parse_avg_us":            lat.get("parse_avg_us"),
            "lat_parse_max_us":            lat.get("parse_max_us"),
            "lat_buffer_age_avg_ms":       lat.get("buffer_age_avg_ms"),
            "lat_buffer_age_max_ms":       lat.get("buffer_age_max_ms"),
            # state
            "state_expire_keys_tracked":   st.get("expire_keys_tracked"),
            "state_reconnects":            st.get("reconnects"),
            "state_active_streams":        st.get("active_streams"),
        })
    return rows


def process_collector_ws_events(records: list[dict], exchange: str) -> list[dict]:
    """ws_connect, ws_connected, ws_disconnect + collector_start, symbols_loaded."""
    INCLUDE = {
        "symbols_loaded",
        "collector_start",
        "ws_connect",
        "ws_connected",
        "ws_disconnect",
        "ws_error",
        "ws_reconnect",
    }
    rows = []
    for d in records:
        ev = d.get("event", "")
        if ev not in INCLUDE:
            continue
        rows.append({
            "ts":                  d["ts"],
            "datetime":            ts_to_dt(d["ts"]),
            "exchange":            exchange,
            "event":               ev,
            "level":               d.get("level"),
            "stream":              d.get("stream"),
            # symbols_loaded / collector_start
            "count":               d.get("count"),
            "path":                d.get("path"),
            "spot_symbols":        d.get("spot_symbols"),
            "fut_symbols":         d.get("fut_symbols"),
            "history_enabled":     d.get("history_enabled"),
            # ws_connect
            "url_prefix":          d.get("url_prefix"),
            "symbols":             d.get("symbols"),
            # ws_disconnect / ws_error
            "error_type":          d.get("error_type"),
            "error_msg":           d.get("error_msg"),
            "reconnect_backoff_s": d.get("reconnect_backoff_s"),
            "reconnects_total":    d.get("reconnects_total"),
        })
    return rows


def process_collector_flush_alerts(records: list[dict], exchange: str) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "slow_flush":
            continue
        rows.append({
            "ts":           d["ts"],
            "datetime":     ts_to_dt(d["ts"]),
            "exchange":     exchange,
            "flush_type":   d.get("flush_type"),
            "lat_ms":       d.get("lat_ms"),
            "threshold_ms": d.get("threshold_ms"),
            "cmds":         d.get("cmds"),
            "level":        d.get("level"),
        })
    return rows


def process_collector_chunk_rotates(records: list[dict], exchange: str) -> list[dict]:
    rows = []
    for d in records:
        if d.get("event") != "chunk_rotate":
            continue
        rows.append({
            "ts":                  d["ts"],
            "datetime":            ts_to_dt(d["ts"]),
            "exchange":            exchange,
            "chunk_id_prev":       d.get("chunk_id_prev"),
            "chunk_id_new":        d.get("chunk_id_new"),
            "expire_keys_reset":   d.get("expire_keys_reset"),
        })
    return rows


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Логи:  {LOGS_DIR}")
    print(f"CSV:   {CSV_DIR}\n")

    # ── Launcher ─────────────────────────────────────────────────────────────
    path = LOGS_DIR / "launcher.log"
    if path.exists():
        write_csv(process_launcher(read_ndjson(path)), CSV_DIR / "launcher.csv")

    # ── Redis monitor ────────────────────────────────────────────────────────
    path = LOGS_DIR / "redis_monitor.log"
    if path.exists():
        records = read_ndjson(path)
        write_csv(process_redis_health(records),   CSV_DIR / "redis_health.csv")
        write_csv(process_redis_warnings(records), CSV_DIR / "redis_warnings.csv")

    # ── Stale monitor ────────────────────────────────────────────────────────
    path = LOGS_DIR / "stale_monitor.log"
    if path.exists():
        records = read_ndjson(path)
        write_csv(process_stale_scans(records), CSV_DIR / "stale_scans.csv")
        write_csv(process_stale_keys(records),  CSV_DIR / "stale_keys.csv")

    # ── Spread monitor ───────────────────────────────────────────────────────
    path = LOGS_DIR / "spread_monitor.log"
    if path.exists():
        records = read_ndjson(path)
        write_csv(process_spread_cycles(records),    CSV_DIR / "spread_cycles.csv")
        write_csv(process_spread_summaries(records), CSV_DIR / "spread_summaries.csv")

    # ── Snapshot monitor ─────────────────────────────────────────────────────
    path = LOGS_DIR / "snapshot_monitor.log"
    if path.exists():
        records = read_ndjson(path)
        write_csv(process_snapshot_events(records),   CSV_DIR / "snapshot_events.csv")
        write_csv(process_snapshot_progress(records), CSV_DIR / "snapshot_progress.csv")

    # ── Collectors ───────────────────────────────────────────────────────────
    all_metrics        : list[dict] = []
    all_ws_events      : list[dict] = []
    all_flush_alerts   : list[dict] = []
    all_chunk_rotates  : list[dict] = []

    for log_file in sorted(LOGS_DIR.glob("collector_*.log")):
        exchange = log_file.stem.removeprefix("collector_")
        records  = read_ndjson(log_file)
        all_metrics.extend(       process_collector_metrics(records, exchange)       )
        all_ws_events.extend(     process_collector_ws_events(records, exchange)     )
        all_flush_alerts.extend(  process_collector_flush_alerts(records, exchange)  )
        all_chunk_rotates.extend( process_collector_chunk_rotates(records, exchange) )

    # Сортировка по ts для корректного временного порядка
    for lst in (all_metrics, all_ws_events, all_flush_alerts, all_chunk_rotates):
        lst.sort(key=lambda r: r["ts"])

    write_csv(all_metrics,       CSV_DIR / "collectors_metrics.csv")
    write_csv(all_ws_events,     CSV_DIR / "collectors_ws_events.csv")
    write_csv(all_flush_alerts,  CSV_DIR / "collectors_flush_alerts.csv")
    write_csv(all_chunk_rotates, CSV_DIR / "collectors_chunk_rotates.csv")

    print("\nГотово.")


if __name__ == "__main__":
    main()
