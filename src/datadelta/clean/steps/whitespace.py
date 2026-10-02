"""
whitespace.py — Trim text cells and remove invisible characters.

"NA " is not "NA", and "Acme" followed by a zero-width space is not
"Acme": stray spaces, non-breaking spaces (U+00A0) and zero-width
characters (U+200B, U+200C, U+200D, U+FEFF) silently split one category
into several and stop the null-token, boolean, number and date steps
from recognizing values.

Per text cell: zero-width characters are removed, U+00A0 becomes a
plain space, then leading/trailing whitespace is stripped. Inner
spaces are kept. Non-string cells in mixed object columns are untouched.
"""

from __future__ import annotations

import pandas as pd

from ..report import CleanAction
from .base    import StepContext, change_examples, is_text_column, replace_str_cells


NAME = "whitespace"

ZERO_WIDTH = "[\u200b\u200c\u200d\ufeff]"


def _normalize(s: pd.Series) -> pd.Series:
    return (
        s.str.replace(ZERO_WIDTH, "", regex=True)
         .str.replace("\u00a0", " ", regex=False)
         .str.strip()
    )


def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    out = None
    actions: list[CleanAction] = []

    for col in ctx.columns(df):
        s = df[col]
        if not is_text_column(s):
            continue
        new, changed = replace_str_cells(s, _normalize)
        count = int(changed.sum())
        if count == 0:
            continue
        if out is None:
            out = df.copy()
        out[col] = new
        actions.append(CleanAction(NAME, col, "trimmed", count, examples=change_examples(s, new, changed)))

    return (df if out is None else out), actions
