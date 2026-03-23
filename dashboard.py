#!/usr/bin/env python3
# dashboard.py
"""
BALI 5.0 — Terminal dashboard.

Run from project root:
    python dashboard.py

Reads logs/ in real-time. Press q to quit, r to force refresh.
"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Static
from textual.containers import Horizontal
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.console import Group

import config
from dashboard.state import (
    DashboardState, EXCHANGES, STREAM_LABELS,
    resolve_stream_status, METRICS_STALE_S,
)
from dashboard.watcher import LogWatcher

_STREAM_LABEL = {
    "md_spot": "MD  spot",
    "md_fut":  "MD  fut ",
    "ob_spot": "OB  spot",
    "ob_fut":  "OB  fut ",
    "fr":      "FR  fut ",
}


def _status_sym(stream, metrics_ts: float) -> Text:
    s = resolve_stream_status(stream, metrics_ts)
    return {
        "connected":   Text("●", style="bold green"),
        "reconnecting":Text("↻", style="bold yellow"),
        "dead":        Text("✗", style="bold red"),
        "unknown":     Text("○", style="dim"),
    }[s]


def _rc_text(n: int) -> Text:
    if n == 0:
        return Text("0↻", style="dim")
    return Text(f"{n}↻", style="bold yellow" if n < 5 else "bold red")


class DashboardApp(App):
    BINDINGS = [("q", "quit", "Quit"), ("r", "refresh_now", "Refresh")]

    def __init__(self) -> None:
        super().__init__()
        self._state   = DashboardState()
        self._watcher = LogWatcher(self._state, log_dir=Path(config.LOGS_DIR))

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(id="streams")
        with Horizontal():
            yield Static(id="redis",    classes="half")
            yield Static(id="monitors", classes="half")
        yield Static(id="signals")
        yield Footer()

    DEFAULT_CSS = """
    Screen { background: #0d1117; }
    #streams  { border: solid #21262d; padding: 0 1; height: auto; }
    Horizontal { height: auto; }
    .half { width: 50%; border: solid #21262d; padding: 0 1; height: auto; }
    #signals  { border: solid #f0a500; padding: 0 1; height: auto; }
    """

    def on_mount(self) -> None:
        self._watch_task = asyncio.create_task(self._watcher.run())
        self.set_interval(2.0, self._refresh)
        self._refresh()

    def on_unmount(self) -> None:
        self._watch_task.cancel()

    def action_refresh_now(self) -> None:
        self._refresh()

    def _refresh(self) -> None:
        self.query_one("#streams",  Static).update(self._build_streams())
        self.query_one("#redis",    Static).update(self._build_redis())
        self.query_one("#monitors", Static).update(self._build_monitors())
        self.query_one("#signals",  Static).update(self._build_signals())

    # ── Renderers (return Rich renderables) ──────────────────────────────

    def _build_streams(self) -> Table:
        state = self._state
        now   = time.monotonic()

        t = Table(show_header=True, header_style="bold cyan",
                  box=None, padding=(0, 1), expand=True)
        t.add_column("STREAM", style="dim", width=10)
        for e in EXCHANGES:
            t.add_column(e[:6].upper(), justify="center", min_width=12)

        for label in STREAM_LABELS:
            row = [Text(_STREAM_LABEL[label], style="cyan")]
            for e in EXCHANGES:
                c  = state.collectors[e]
                st = c.streams[label]
                row.append(Text.assemble(_status_sym(st, c.last_metrics_ts), " ", _rc_text(st.reconnects)))
            t.add_row(*row)

        t.add_row("")  # spacer

        def _dead(e):
            c = state.collectors[e]
            return c.last_metrics_ts == 0.0 or (now - c.last_metrics_ts) > METRICS_STALE_S

        # md+ob msg/s
        r1 = [Text("md+ob /s", style="dim")]
        for e in EXCHANGES:
            c = state.collectors[e]
            r1.append(Text("—", style="dim") if _dead(e)
                      else Text(f"{c.md_msgs_s}+{c.ob_msgs_s}", style="green"))
        t.add_row(*r1)

        for lbl, avg_a, max_a in [
            ("flush a/m", "flush_avg_ms", "flush_max_ms"),
            ("hist  a/m", "hist_avg_ms",  "hist_max_ms"),
        ]:
            rx = [Text(lbl, style="dim")]
            for e in EXCHANGES:
                if _dead(e):
                    rx.append(Text("—", style="dim"))
                else:
                    c   = state.collectors[e]
                    avg = getattr(c, avg_a)
                    mx  = getattr(c, max_a)
                    col = "red" if mx > 50 else ("yellow" if mx > 10 else "green")
                    rx.append(Text(f"{avg:.1f}/{mx:.1f}", style=col))
            t.add_row(*rx)

        return t

    def _build_redis(self) -> Panel:
        r   = self._state.redis
        ok  = r.last_update_ts > 0 and (time.monotonic() - r.last_update_ts) < 90

        def v(val, fmt, warn=None) -> Text:
            s = fmt.format(val)
            if not ok:               return Text(s, style="dim")
            if warn and val > warn:  return Text(s, style="bold yellow")
            return Text(s, style="green")

        rows = [
            Text.assemble("mem    ", v(r.mem_mb,       "{:.0f}MB",  config.REDIS_MEMORY_WARN_MB)),
            Text.assemble("ops/s  ", v(r.ops_s,        "{}",        config.REDIS_OPS_WARN_PER_SEC)),
            Text.assemble("ping   ", v(r.ping_ms,       "{:.1f}ms")),
            Text.assemble("lpush  ", v(r.lpush_p99_us,  "{}µs")),
            Text.assemble("hset   ", v(r.hset_p99_us,   "{}µs")),
            Text.assemble("frag   ", v(r.frag,          "{:.2f}",   1.5)),
            Text.assemble("keys   ", v(r.keys,          "{}")),
            Text.assemble("hit    ", v(r.hit_rate,      "{:.1f}%")),
        ]
        return Panel(Group(*rows), title="REDIS", border_style="cyan")

    def _build_monitors(self) -> Panel:
        now   = time.monotonic()
        state = self._state
        rows  = []
        for name, lv in state.monitors.items():
            alive = lv.last_write_ts > 0 and (now - lv.last_write_ts) < 90
            sym   = Text("✓ ", style="bold green") if alive else Text("✗ ", style="bold red")
            rows.append(Text.assemble(sym, name))

        rows.append(Text(""))
        st  = state.stale
        col = "bold red" if st.stale_count > 0 else "green"
        rows.append(Text(f"stale {st.stale_count}/{st.total_keys}  {st.last_scan_time}", style=col))
        return Panel(Group(*rows), title="MONITORS", border_style="cyan")

    def _build_signals(self) -> Panel:
        state = self._state
        rows  = []
        for s in state.signals:
            rows.append(Text.assemble(
                Text("▸ ", style="bold yellow"),
                Text(f"{s.time_str}  ", style="dim"),
                Text(f"{s.symbol:<10}", style="bold white"),
                Text(f"{s.spot_exch}→{s.fut_exch}  ", style="cyan"),
                Text(f"+{s.spread_pct:.2f}%", style="bold green"),
            ))
        if not rows:
            rows.append(Text("no signals yet", style="dim"))
        return Panel(
            Group(*rows),
            title=f"SIGNALS  today: {state.signals_today}",
            border_style="yellow",
        )


if __name__ == "__main__":
    DashboardApp().run()
