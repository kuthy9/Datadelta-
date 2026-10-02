"""
findings.py — Cleaning actions as diff findings (`datadelta diff --clean`).

With --clean, both sides are cleaned with the same plan before they are
compared. What the cleaner did is part of the story ("before: 1,000
values parsed as numbers"), so every action becomes an INFO Finding in
its own "clean" layer. Scenario lenses never re-weight that layer: the
actions describe preparation, not change.

Titles and details are value-free: a title is built from the side, the
column, the count and a fixed phrase; the detail is the action note;
the metric is CleanAction.to_dict(include_examples=False).
"""

from __future__ import annotations

from typing import Literal

from ..differ import Finding
from .report  import CleanAction, CleanReport


# One phrase per action in report.ACTIONS, used after the count.
ACTION_LABELS: dict[str, str] = {
    "header_trimmed":  "column names trimmed",
    "trimmed":         "cells trimmed",
    "null_token":      "null tokens normalized",
    "parsed_bool":     "values parsed as booleans",
    "parsed_number":   "values parsed as numbers",
    "parsed_date":     "values parsed as dates",
    "coerced_to_null": "unparseable values set to null",
    "lowercased":      "cells re-cased",
    "uppercased":      "cells re-cased",
    "titlecased":      "cells re-cased",
    "dropped_rows":    "rows dropped",
    "imputed":         "cells imputed",
    "skipped":         "skipped",
    "ambiguous":       "ambiguous values (day/month order)",
}


def _title(action: CleanAction, side: str) -> str:
    column = action.column if action.column is not None else "table"
    if action.action == "skipped":
        return f"{side} · {column} · skipped: {action.note}"
    label = ACTION_LABELS.get(action.action, action.action)
    return f"{side} · {column} · {action.count:,} {label}"


def findings_from_report(report: CleanReport, side: Literal["before", "after"]) -> list[Finding]:
    """
    One INFO Finding (layer "clean") per action that changed something,
    plus every "skipped" action (so a refusal is never silent).
    """
    return [
        Finding(
            layer    = "clean",
            column   = action.column,
            severity = "INFO",
            title    = _title(action, side),
            detail   = action.note,
            metric   = action.to_dict(include_examples=False),
        )
        for action in report.actions
        if action.count > 0 or action.action == "skipped"
    ]
