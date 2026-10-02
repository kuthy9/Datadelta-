"""
report.py — What the cleaner did, in a form every renderer can use.

WHY A SEPARATE REPORT?
  Cleaning changes data, so it must be accountable: every change is
  recorded as a CleanAction (which step, which column, what kind of
  change, how many cells, a few before → after examples). CleanStats
  summarizes the whole run (rows, nulls, duplicates, retyped columns).

  The same CleanReport feeds the terminal report, `--json`, the HTML
  export and the diff "clean" layer.

PRIVACY
  `examples` hold real cell values. They are shown locally only:
  to_dict(include_examples=False) and summary_counts() never contain
  them, and step notes never quote cell values (only counts, configured
  null tokens and column names), so nothing value-level reaches an LLM.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing      import Any

import pandas as pd

from ..jsonutil import to_jsonable


# ── Action vocabulary ─────────────────────────────────────────────────────────

ACTIONS: frozenset[str] = frozenset({
    "header_trimmed", "trimmed", "null_token", "parsed_bool", "parsed_number",
    "parsed_date", "coerced_to_null", "lowercased", "uppercased", "titlecased",
    "dropped_rows", "imputed", "skipped", "ambiguous",
})

# Actions whose count is a number of cells whose value or type changed.
CELL_ACTIONS: frozenset[str] = frozenset({
    "trimmed", "null_token", "parsed_bool", "parsed_number", "parsed_date",
    "coerced_to_null", "lowercased", "uppercased", "titlecased", "imputed",
})

# Actions that give a column a new type, and the type name they give it.
RETYPE_ACTIONS: dict[str, str] = {
    "parsed_number": "number",
    "parsed_date":   "date",
    "parsed_bool":   "boolean",
}


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CleanAction:
    """
    One thing a step did (or deliberately did not do) to one column.

    step:     the step name ("numbers", "whitespace", ...)
    column:   the column it applies to (None for table-level actions)
    action:   one of ACTIONS
    count:    cells / rows / names affected; for "skipped", the values
              that blocked the change
    examples: up to 3 (before, after) pairs, local display only
    note:     short value-free explanation
    """
    step:     str
    column:   str | None
    action:   str
    count:    int
    examples: list[tuple[Any, Any]] = field(default_factory=list)
    note:     str = ""

    def to_dict(self, include_examples: bool = True) -> dict:
        out = {
            "step":   self.step,
            "column": self.column,
            "action": self.action,
            "count":  int(self.count),
            "note":   self.note,
        }
        if include_examples:
            out["examples"] = [to_jsonable([before, after]) for before, after in self.examples]
        return out


@dataclass
class CleanStats:
    """Whole-run numbers: rows, nulls per column, duplicates, retyped columns."""
    rows_before:     int
    rows_after:      int
    cells_changed:   int = 0                                          # cells of the result that differ from the input, once each
    columns_retyped: dict[str, str] = field(default_factory=dict)   # column -> "number" | "date" | "boolean"
    nulls_before:    dict[str, int] = field(default_factory=dict)
    nulls_after:     dict[str, int] = field(default_factory=dict)
    duplicate_rows:  int = 0
    duplicate_keys:  int | None = None

    def to_dict(self) -> dict:
        return {
            "rows_before":     int(self.rows_before),
            "rows_after":      int(self.rows_after),
            "cells_changed":   int(self.cells_changed),
            "columns_retyped": dict(self.columns_retyped),
            "nulls_before":    {k: int(v) for k, v in self.nulls_before.items()},
            "nulls_after":     {k: int(v) for k, v in self.nulls_after.items()},
            "duplicate_rows":  int(self.duplicate_rows),
            "duplicate_keys":  None if self.duplicate_keys is None else int(self.duplicate_keys),
        }


@dataclass
class CleanReport:
    """Every action, in execution order, plus the run statistics."""
    actions: list[CleanAction]
    stats:   CleanStats

    def to_dict(self, include_examples: bool = True) -> dict:
        return {
            "stats":   self.stats.to_dict(),
            "actions": [a.to_dict(include_examples=include_examples) for a in self.actions],
        }

    def summary_counts(self) -> dict:
        """Value-free totals — the only cleaning data the story payload carries."""
        return {
            "cells_changed":   int(self.stats.cells_changed),
            "rows_dropped":    int(self.stats.rows_before - self.stats.rows_after),
            "columns_retyped": dict(self.stats.columns_retyped),
            "actions":         sum(1 for a in self.actions if a.count > 0),
        }


@dataclass
class CleanResult:
    """The cleaned frame and the report that explains it."""
    df:     pd.DataFrame
    report: CleanReport
