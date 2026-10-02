"""
dashboard.py — Live progress on stderr while datadelta works.

WHY STDERR?
  stdout belongs to the report (or the JSON document). Progress is drawn
  on stderr, so `datadelta diff a.csv b.csv --json | jq` stays clean and
  a redirected report never contains a frame of the live display. The
  CLI prints the report only after the progress context has closed.

THREE SINKS, ONE CHOICE (make_progress)
  LiveDashboard  stderr is a terminal that can redraw. A Rich Live view,
                 redrawn 8 times a second: a header (data ▲, the command,
                 a clock), one row per stage (status glyph, label, bar,
                 note, time) and a running severity tally. It is
                 transient: on exit it collapses into one dim line,
                 "✓ 9 stages · 1.4s".
  PlainProgress  stderr is a file, a pipe or a CI log. One plain line per
                 finished stage: "[datadelta] distribution done 12/12 0.4s".
                 Also on TERM=dumb, and on a pipe even when FORCE_COLOR or
                 TTY_COMPATIBLE makes Rich call it a terminal: those force
                 colors, but a redrawing view would fill the log with
                 cursor-control sequences.
  NullProgress   --quiet. Nothing at all.

  Every glyph and color comes from theme.py. Labels, notes and summaries
  can contain column and file names, so they are always rich.text.Text
  (never parsed as markup) with control characters made visible
  (safetext.printable), and table cells are measured in terminal cells,
  so CJK names keep the alignment.
"""

from __future__ import annotations

import contextlib
import os
import threading
import time
from dataclasses import dataclass, replace
from typing      import Any, ContextManager

from rich.console import Console, Group, RenderableType
from rich.live    import Live
from rich.rule    import Rule
from rich.table   import Table
from rich.text    import Text

from .         import theme
from .progress import NullProgress, ProgressSink, StageStatus, error_summary, plural
from .safetext import printable


BAR_WIDTH    = 24           # cells of the progress bar
LABEL_WIDTH  = 14           # "distribution", "clean before" fit
SPINNER_HZ   = 8            # spinner frames per second
PLAIN_PREFIX = "[datadelta]"
SEVERITIES   = ("FAIL", "WARN", "INFO")    # what the tally line shows

_MUTED = theme.rich_style("muted")


def _now() -> float:
    """Monotonic clock for stage timings (tests replace it with a fake)."""
    return time.perf_counter()


# ─────────────────────────────────────────────────────────────────────────────
# Stage bookkeeping shared by LiveDashboard and PlainProgress
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _Stage:
    key:       str
    label:     str
    status:    str          = "pending"     # pending | running | done | failed | skipped
    total:     int | None   = None
    completed: int          = 0
    note:      str          = ""
    summary:   str          = ""
    started:   float | None = None
    ended:     float | None = None

    def seconds(self, now: float) -> float:
        if self.started is None:
            return 0.0
        return (self.ended if self.ended is not None else now) - self.started


class _StageTracker:
    """
    Implements the ProgressSink methods by recording state: ordered stage
    rows and the severity counts. Subclasses decide how to show it. A lock
    guards the state because Rich Live renders from its own thread.
    """

    def __init__(self) -> None:
        self.stages: dict[str, _Stage] = {}
        self.counts: dict[str, int]    = {severity: 0 for severity in SEVERITIES}
        self._lock = threading.RLock()

    def _stage(self, key: str, label: str | None = None) -> _Stage:
        stage = self.stages.get(key)
        if stage is None:
            stage = self.stages[key] = _Stage(key=key, label=label or key)
        return stage

    # ── ProgressSink ──────────────────────────────────────────────────────

    def stage_start(self, key: str, label: str, total: int | None = None, note: str = "") -> None:
        with self._lock:
            stage = self._stage(key, label)
            stage.label, stage.total, stage.note = label, total, note
            stage.status, stage.completed, stage.summary = "running", 0, ""
            stage.started, stage.ended = _now(), None

    def advance(self, key: str, n: int = 1, note: str = "") -> None:
        with self._lock:
            stage = self._stage(key)
            if stage.started is None:
                stage.status, stage.started = "running", _now()
            stage.completed += n
            if note:
                stage.note = note

    def stage_end(self, key: str, status: StageStatus = "done", summary: str = "") -> None:
        with self._lock:
            stage = self._stage(key)
            if stage.started is None:
                stage.started = _now()
            stage.status, stage.summary, stage.ended = status, summary, _now()

    def finding(self, severity: str) -> None:
        with self._lock:
            self.counts[severity] = self.counts.get(severity, 0) + 1

    def tally(self, counts: dict[str, int]) -> None:
        """The authoritative counts (after the scenario lens) replace the running ones."""
        with self._lock:
            self.counts = {severity: int(counts.get(severity, 0)) for severity in SEVERITIES}


# ─────────────────────────────────────────────────────────────────────────────
# LiveDashboard — stderr is a terminal
# ─────────────────────────────────────────────────────────────────────────────

class LiveDashboard(_StageTracker):
    """
    A transient Rich Live view on `console` (stderr by default):

        data ▲   diff · ETL Validation                              00:01.4
        ──────────────────────────────────────────────────────────────────
        ✓ load before    etl_before.csv            1,000 rows · 5 cols  0.1s
        ◐ distribution   ━━━━━━━━──────── 3/5      revenue               0.3s
        ──────────────────────────────────────────────────────────────────
        ▲ 1 fail   ● 1 warn   · 0 info

    Use it as a context manager. Print other stderr lines inside the
    block through the same console, so Live keeps them above the view.
    """

    def __init__(self, title: str, console: Console | None = None, refresh_per_second: int = 8) -> None:
        super().__init__()
        self.title              = title
        self.console            = console if console is not None else Console(stderr=True)
        self.refresh_per_second = refresh_per_second
        self._opened            = _now()
        self._live: Live | None = None
        self._error             = False

    # ── Rendering ─────────────────────────────────────────────────────────

    def render(self) -> RenderableType:
        """The whole view as one renderable (header, rule, stage rows, rule, tally)."""
        with self._lock:
            now    = _now()
            stages = [replace(stage) for stage in self.stages.values()]   # a snapshot
            counts = dict(self.counts)

        header = Table.grid(expand=True)
        header.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
        header.add_column(justify="right", no_wrap=True)
        header.add_row(
            Text.assemble(_wordmark(), "   ", Text(printable(self.title))),
            Text(_clock(now - self._opened), style=_MUTED),
        )

        rows = Table.grid(expand=True, padding=(0, 1))
        rows.add_column(no_wrap=True)                                    # glyph
        rows.add_column(no_wrap=True, min_width=LABEL_WIDTH)             # label
        rows.add_column(no_wrap=True)                                    # bar + count
        rows.add_column(ratio=1, no_wrap=True, overflow="ellipsis")      # note / summary
        rows.add_column(justify="right", no_wrap=True)                   # seconds
        for stage in stages:
            rows.add_row(
                _glyph(stage, now - self._opened),
                Text(printable(stage.label), style=_MUTED if stage.status == "pending" else ""),
                _bar(stage),
                _detail(stage),
                Text("" if stage.status == "pending" else f"{stage.seconds(now):.1f}s", style=_MUTED),
            )

        rule = Rule(style=theme.rich_style("rule"), characters=theme.RULE_CHAR)
        return Group(header, rule, rows, rule, _tally(counts))

    def closing_line(self) -> Text:
        """The one dim line left behind: "✓ 9 stages · 1.4s" or "× failed at load before"."""
        with self._lock:
            failed   = next((s for s in self.stages.values() if s.status == "failed"), None)
            finished = sum(1 for s in self.stages.values() if s.status in ("done", "skipped"))
        if failed is not None:
            return Text(f"{theme.STAGE_GLYPH['failed']} failed at {printable(failed.label)}", style=_MUTED)
        if self._error:
            return Text(f"{theme.STAGE_GLYPH['failed']} stopped", style=_MUTED)
        seconds = _now() - self._opened
        return Text(
            f"{theme.STAGE_GLYPH['done']} {plural(finished, 'stage')} {theme.SEP} {seconds:.1f}s",
            style = _MUTED,
        )

    # ── Context manager ───────────────────────────────────────────────────

    def __enter__(self) -> "LiveDashboard":
        self._opened = _now()
        self._live = Live(
            console            = self.console,
            refresh_per_second = self.refresh_per_second,
            transient          = True,          # the view disappears; closing_line() stays
            redirect_stdout    = False,         # stdout belongs to the report
            redirect_stderr    = True,
            get_renderable     = self.render,
        )
        self._live.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        if exc_type is not None:
            self._error = True
            with self._lock:
                for stage in self.stages.values():
                    if stage.status == "running":           # interrupted mid-stage
                        stage.status, stage.ended = "failed", _now()
                        stage.summary = error_summary(exc) if exc is not None else ""
        if self._live is not None:
            self._live.stop()
            self._live = None
        self.console.print(self.closing_line(), soft_wrap=True)
        return False


def _wordmark() -> Text:
    """The wordmark: "data" in bold ink, the ▲ mark in vermilion."""
    word, mark = theme.BRAND.rsplit(" ", 1)
    return Text.assemble((word, "bold"), " ", (mark, theme.rich_style("vermilion", bold=True)))


def _clock(seconds: float) -> str:
    """Elapsed time as mm:ss.s, e.g. 00:03.2."""
    minutes, rest = divmod(max(seconds, 0.0), 60)
    return f"{int(minutes):02d}:{rest:04.1f}"


def _glyph(stage: _Stage, elapsed: float) -> Text:
    if stage.status == "running":
        frame = theme.SPINNER_FRAMES[int(elapsed * SPINNER_HZ) % len(theme.SPINNER_FRAMES)]
        return Text(frame, style=theme.rich_style("vermilion"))
    styles = {"done": theme.rich_style("sage"), "failed": theme.rich_style("vermilion", bold=True)}
    return Text(theme.STAGE_GLYPH[stage.status], style=styles.get(stage.status, _MUTED))


def _bar(stage: _Stage) -> Text:
    """━━━━──── 3/12 for stages that know their total; nothing otherwise."""
    if stage.total is None:
        return Text("")
    ratio  = 1.0 if stage.total <= 0 else min(stage.completed / stage.total, 1.0)
    filled = round(BAR_WIDTH * ratio)
    return Text.assemble(
        (theme.BAR_FULL * filled, theme.rich_style("vermilion")),
        (theme.BAR_EMPTY * (BAR_WIDTH - filled), _MUTED),
        f" {stage.completed}/{stage.total}",
    )


def _detail(stage: _Stage) -> Text:
    """While running: the note (current item). When finished: the summary."""
    if stage.status == "running":
        return Text(printable(stage.note), style=_MUTED)
    if stage.status == "failed":
        return Text(printable(stage.summary), style=theme.rich_style("vermilion"))
    if stage.status == "skipped":
        return Text(printable(stage.summary), style=_MUTED)
    return Text(printable(stage.summary))


def _tally(counts: dict[str, int]) -> Text:
    """▲ 1 fail   ● 2 warn   · 0 info — zero counts stay muted."""
    line = Text()
    for i, severity in enumerate(SEVERITIES):
        if i:
            line.append("   ")
        n = counts.get(severity, 0)
        line.append(
            f"{theme.SEVERITY_GLYPH[severity]} {n} {theme.SEVERITY_WORD[severity]}",
            style = theme.severity_style(severity) if n else _MUTED,
        )
    return line


# ─────────────────────────────────────────────────────────────────────────────
# PlainProgress — stderr is not a terminal
# ─────────────────────────────────────────────────────────────────────────────

class PlainProgress(_StageTracker):
    """
    One line per finished stage and nothing else:

        [datadelta] load before done 1,000 rows · 5 cols 0.1s
        [datadelta] distribution done 5/5 2 findings 0.4s
    """

    def __init__(self, console: Console | None = None) -> None:
        super().__init__()
        self.console = console if console is not None else Console(stderr=True)

    def stage_end(self, key: str, status: StageStatus = "done", summary: str = "") -> None:
        super().stage_end(key, status, summary)
        with self._lock:
            stage = self.stages[key]
            parts = [PLAIN_PREFIX, stage.label, status]
            if stage.total is not None:
                parts.append(f"{stage.completed}/{stage.total}")
            if summary:
                parts.append(summary)
            parts.append(f"{stage.seconds(_now()):.1f}s")
        # Text, not markup: labels and summaries may contain "[" from data.
        self.console.print(Text(printable(" ".join(parts))), soft_wrap=True)

    def __enter__(self) -> "PlainProgress":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────────────────────

def make_progress(
    title:   str,
    *,
    quiet:   bool           = False,
    console: Console | None = None,
) -> ContextManager[ProgressSink]:
    """
    The progress sink for one command, as a context manager:
      quiet                     → NullProgress (wrapped in contextlib.nullcontext)
      _can_redraw(console)      → LiveDashboard
      otherwise                 → PlainProgress
    The default console is Console(stderr=True). Pass the console the
    command prints its own stderr lines with, so a live view and those
    lines never overwrite each other.
    """
    if quiet:
        return contextlib.nullcontext(NullProgress())
    console = console if console is not None else Console(stderr=True)
    if _can_redraw(console):
        return LiveDashboard(title, console=console)
    return PlainProgress(console=console)


def _can_redraw(console: Console) -> bool:
    """
    A screen Live can redraw: Rich's is_terminal, except a dumb terminal,
    and except that FORCE_COLOR / TTY_COMPATIBLE only force colors on a
    pipe (CI logs often set them; Rich then reports a pipe as a terminal).
    """
    if not console.is_terminal or console.is_dumb_terminal:
        return False
    if os.environ.get("FORCE_COLOR") or os.environ.get("TTY_COMPATIBLE"):
        isatty = getattr(console.file, "isatty", None)
        try:
            return bool(isatty and isatty())
        except ValueError:                  # a closed file; Rich treats it the same way
            return False
    return True
