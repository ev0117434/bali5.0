# tests/test_dashboard_watcher.py
"""Unit tests for LogWatcher — uses temp files, no real logs needed."""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from dashboard.state import DashboardState
from dashboard.watcher import LogWatcher


@pytest.mark.asyncio
async def test_reads_existing_tail_on_startup(tmp_path):
    """Watcher reads last N lines from an existing file at startup."""
    log_file = tmp_path / "collector_binance.log"
    metrics = (
        "[2026-03-23 12:44:38.000] [INFO    ] [collector_binance] "
        "METRICS | md=842msg/s ob=420msg/s fr=12msg/s | "
        "flush_lat avg=1.2ms max=4.8ms batch_avg=84 | "
        "hist_writes=145/s hist_flush_lat avg=2.1ms max=8.3ms "
        "ob_skip=5/s | expire_set=12 reconnects=0\n"
    )
    log_file.write_text(metrics * 10)  # 10 lines, all within tail window

    state   = DashboardState()
    watcher = LogWatcher(state, log_dir=tmp_path)
    task    = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert state.collectors["binance"].md_msgs_s == 842


@pytest.mark.asyncio
async def test_picks_up_new_appended_lines(tmp_path):
    """Watcher detects lines appended after startup."""
    log_file = tmp_path / "collector_bybit.log"
    log_file.write_text("")

    state   = DashboardState()
    watcher = LogWatcher(state, log_dir=tmp_path)
    task    = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.05)

    metrics = (
        "[2026-03-23 12:44:38.000] [INFO    ] [collector_bybit] "
        "METRICS | md=310msg/s ob=155msg/s fr=10msg/s | "
        "flush_lat avg=0.9ms max=3.1ms batch_avg=31 | "
        "hist_writes=80/s hist_flush_lat avg=1.8ms max=6.2ms "
        "ob_skip=2/s | expire_set=5 reconnects=0\n"
    )
    with open(log_file, "a") as f:
        f.write(metrics)

    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert state.collectors["bybit"].md_msgs_s == 310


@pytest.mark.asyncio
async def test_missing_log_file_does_not_crash(tmp_path):
    """Watcher must not crash if a log file does not yet exist."""
    state   = DashboardState()
    watcher = LogWatcher(state, log_dir=tmp_path)
    task    = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # reaching here without exception = pass
