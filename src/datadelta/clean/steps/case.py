"""
case.py — Re-case the text columns the user names.

"EMEA", "emea" and "Emea" are one region to a human but three categories
to the diff. Changing case does destroy information (an "ID" code may be
case-sensitive), so this step never guesses: it runs only for the
columns listed under `cleaning.case` (lower / upper / title).

A listed column that does not exist, or that is not text (e.g. it was
just parsed as numbers, or an object column holding no str cell at all),
is reported as "skipped" and left alone. In a mixed object column only
the str cells are re-cased.
"""

from __future__ import annotations

import pandas as pd

from ..plan   import CASE_MODES
from ..report import CleanAction
from .base    import StepContext, change_examples, is_text_column, replace_str_cells, str_mask


NAME = "case"

ACTION_FOR_MODE: dict[str, str] = {
    "lower": "lowercased",
    "upper": "uppercased",
    "title": "titlecased",
}


def _recase(s: pd.Series, mode: str) -> pd.Series:
    if mode == "lower":
        return s.str.lower()
    if mode == "upper":
        return s.str.upper()
    return s.str.title()


def _holds_text(s: pd.Series) -> bool:
    """A text dtype with at least one str cell (a column of only nulls has nothing to report)."""
    if not is_text_column(s):
        return False
    return bool(s.isna().all() or str_mask(s).any())


def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    columns = set(ctx.columns(df))
    excluded = set(ctx.plan.exclude_columns)
    out = None
    actions: list[CleanAction] = []

    for mode in CASE_MODES:
        for col in ctx.plan.case.get(mode, []):
            if col in excluded:
                continue                                # exclude_columns wins over every step
            if col not in columns:
                actions.append(CleanAction(NAME, col, "skipped", 0, note=f"case.{mode}: column not found"))
                continue
            s = (out if out is not None else df)[col]
            if not _holds_text(s):
                actions.append(CleanAction(NAME, col, "skipped", 0, note=f"case.{mode}: not a text column"))
                continue
            new, changed = replace_str_cells(s, lambda cells: _recase(cells, mode))
            count = int(changed.sum())
            if count == 0:
                continue
            if out is None:
                out = df.copy()
            out[col] = new
            actions.append(CleanAction(NAME, col, ACTION_FOR_MODE[mode], count,
                                       examples=change_examples(s, new, changed)))

    return (df if out is None else out), actions
