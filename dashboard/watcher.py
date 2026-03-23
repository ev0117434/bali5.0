# dashboard/watcher.py
"""
BALI 5.0 Dashboard — async log file watcher.
Tails each collector and monitor log file; feeds new lines into StateParser.
"""
from __future__ import annotations
import asyncio
from pathlib import Path

from dashboard.state import DashboardState, EXCHANGES
from dashboard.parser import StateParser

LOG_SOURCES: dict[str, str] = {
    **{f"collector_{e}": e for e in EXCHANGES},
    "redis_monitor":    "redis_monitor",
    "stale_monitor":    "stale_monitor",
    "spread_monitor":   "spread_monitor",
    "snapshot_monitor": "snapshot_monitor",
}

TAIL_LINES    = 100    # lines to read from end of file on startup
POLL_INTERVAL = 0.1   # seconds between size checks


class LogWatcher:
    """
    Tails all BALI log files asynchronously.
    Cancels cleanly on asyncio.CancelledError.
    Must be run from the project root (log_dir defaults to Path("logs")).
    """

    def __init__(self, state: DashboardState, log_dir: Path | str | None = None) -> None:
        self._state   = state
        self._parser  = StateParser(state)
        self._log_dir = Path(log_dir) if log_dir else Path("logs")

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._tail(stem, source))
            for stem, source in LOG_SOURCES.items()
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _tail(self, stem: str, source: str) -> None:
        log_path = self._log_dir / f"{stem}.log"

        while not log_path.exists():
            await asyncio.sleep(POLL_INTERVAL)

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f.readlines()[-TAIL_LINES:]:
                self._parser.parse_line(source, line.rstrip())
            pos = f.tell()

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            try:
                size = log_path.stat().st_size
            except FileNotFoundError:
                await asyncio.sleep(POLL_INTERVAL)
                continue
            if size < pos:
                pos = 0   # log was rotated
            if size > pos:
                with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(pos)
                    for line in f:
                        self._parser.parse_line(source, line.rstrip())
                    pos = f.tell()
