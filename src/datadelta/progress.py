"""
progress.py — The observer interface for live progress reporting.

WHY AN OBSERVER?
  Loading, cleaning, diffing and metric evaluation should not know how
  progress is displayed. They accept an optional `ProgressSink` and call
  four or five tiny methods on it (stage_start / advance / stage_end /
  finding / tally). The CLI passes a Rich Live dashboard on stderr; tests
  pass a RecordingProgress and assert on the event list; library callers
  pass nothing and get NullProgress, which does nothing at all.

  A stage is identified by a short key such as "load.before" or "clean".
  The dashboard shows one row per key, in the order stages start.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing      import Literal, Protocol, runtime_checkable


StageStatus = Literal["done", "skipped", "failed"]


# ─────────────────────────────────────────────────────────────────────────────
# Protocol
# ─────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class ProgressSink(Protocol):
    """Anything that can receive progress events."""

    def stage_start(self, key: str, label: str, total: int | None = None, note: str = "") -> None: ...
    def advance(self, key: str, n: int = 1, note: str = "") -> None: ...
    def stage_end(self, key: str, status: StageStatus = "done", summary: str = "") -> None: ...
    def finding(self, severity: str) -> None: ...                 # FAIL / WARN / INFO / PASS
    def tally(self, counts: dict[str, int]) -> None: ...          # authoritative counts after the scenario lens


# ─────────────────────────────────────────────────────────────────────────────
# Null implementation — the default everywhere
# ─────────────────────────────────────────────────────────────────────────────

class NullProgress:
    """Ignores every event. Used when the caller passes no sink."""

    def stage_start(self, key: str, label: str, total: int | None = None, note: str = "") -> None:
        return None

    def advance(self, key: str, n: int = 1, note: str = "") -> None:
        return None

    def stage_end(self, key: str, status: StageStatus = "done", summary: str = "") -> None:
        return None

    def finding(self, severity: str) -> None:
        return None

    def tally(self, counts: dict[str, int]) -> None:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Recording implementation — for tests
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ProgressEvent:
    """
    One recorded call.

    kind "start":   data = {"label", "total", "note"}
    kind "advance": data = {"n", "note"}
    kind "end":     data = {"status", "summary"}
    kind "finding": key = None, data = {"severity"}
    kind "tally":   key = None, data = {"counts"}
    """
    kind: Literal["start", "advance", "end", "finding", "tally"]
    key:  str | None
    data: dict


class RecordingProgress:
    """Stores every event in `events`, in call order."""

    def __init__(self) -> None:
        self.events: list[ProgressEvent] = []

    # ── ProgressSink methods ─────────────────────────────────────────────────

    def stage_start(self, key: str, label: str, total: int | None = None, note: str = "") -> None:
        self.events.append(ProgressEvent("start", key, {"label": label, "total": total, "note": note}))

    def advance(self, key: str, n: int = 1, note: str = "") -> None:
        self.events.append(ProgressEvent("advance", key, {"n": n, "note": note}))

    def stage_end(self, key: str, status: StageStatus = "done", summary: str = "") -> None:
        self.events.append(ProgressEvent("end", key, {"status": status, "summary": summary}))

    def finding(self, severity: str) -> None:
        self.events.append(ProgressEvent("finding", None, {"severity": severity}))

    def tally(self, counts: dict[str, int]) -> None:
        self.events.append(ProgressEvent("tally", None, {"counts": dict(counts)}))

    # ── Query helpers ────────────────────────────────────────────────────────

    def stage_keys(self) -> list[str]:
        """Keys of "start" events, in the order the stages started."""
        return [e.key for e in self.events if e.kind == "start"]

    def advances(self, key: str) -> int:
        """Sum of n over the "advance" events of one stage."""
        return sum(e.data["n"] for e in self.events if e.kind == "advance" and e.key == key)

    def advance_notes(self, key: str) -> list[str]:
        """The note of every "advance" event of one stage, in order."""
        return [e.data["note"] for e in self.events if e.kind == "advance" and e.key == key]

    def end_status(self, key: str) -> str | None:
        """Status of the last "end" event of a stage, or None if it never ended."""
        ends = [e for e in self.events if e.kind == "end" and e.key == key]
        return ends[-1].data["status"] if ends else None

    def end_summary(self, key: str) -> str | None:
        """Summary of the last "end" event of a stage, or None if it never ended."""
        ends = [e for e in self.events if e.kind == "end" and e.key == key]
        return ends[-1].data["summary"] if ends else None

    def findings(self) -> list[str]:
        """Severity of every "finding" event, in order."""
        return [e.data["severity"] for e in self.events if e.kind == "finding"]


# ─────────────────────────────────────────────────────────────────────────────
# Summary helpers — short, single-line stage summaries
# ─────────────────────────────────────────────────────────────────────────────

SUMMARY_LIMIT = 80   # longest stage summary, in characters (one dashboard row)


def plural(n: int, noun: str) -> str:
    """'1 change', '3 changes', '1,024 rows' — the count with a thousands separator."""
    return f"{n:,} {noun}" if n == 1 else f"{n:,} {noun}s"


def error_summary(exc: BaseException, limit: int = SUMMARY_LIMIT) -> str:
    """
    The first line of str(exc), cut to `limit` characters, for a "failed"
    stage. Loader errors span several lines (a hint follows the message);
    a summary must stay on one line of the dashboard / plain output.
    """
    lines = str(exc).strip().splitlines()
    first = lines[0].strip() if lines else type(exc).__name__
    return first if len(first) <= limit else first[: limit - 3] + "..."
