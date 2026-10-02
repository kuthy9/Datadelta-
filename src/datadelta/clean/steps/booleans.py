"""
booleans.py — Turn yes/no-style text columns into a real boolean dtype.

A column holding "yes" / "No" / "Y" / "true" / "是" / "否" is a boolean
in disguise; as text it shows up in the diff as a 3- or 4-way category.

Rules (all non-null values decide together):
  - every non-null value, stripped and casefolded, is one of
    true/false/yes/no/y/n/t/f/是/否/1/0, and
  - at least one value is a word, not 1/0.
A column of only "0"/"1" is left as text here; the numbers step turns
it into integers, which is the more faithful reading. Columns that are
already numeric or boolean are never touched, and the result uses the
nullable "boolean" dtype so missing values stay missing.
"""

from __future__ import annotations

import pandas as pd

from ..report import CleanAction
from .base    import StepContext, as_mask, change_examples, fold, is_text_column, str_mask


NAME = "booleans"

TRUE_TOKENS:  frozenset[str] = frozenset({"true", "yes", "y", "t", "是", "1"})
FALSE_TOKENS: frozenset[str] = frozenset({"false", "no", "n", "f", "否", "0"})
DIGIT_TOKENS: frozenset[str] = frozenset({"0", "1"})

_TO_BOOL: dict[str, bool] = {**{t: True for t in TRUE_TOKENS}, **{t: False for t in FALSE_TOKENS}}


def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    out = None
    actions: list[CleanAction] = []

    for col in ctx.parse_columns(df):
        s = df[col]
        if not is_text_column(s):
            continue
        nonnull = as_mask(s.notna())
        total = int(nonnull.sum())
        if total == 0 or int(str_mask(s).sum()) != total:
            continue                                    # empty, or mixes in non-text objects
        key = fold(s)
        if int(as_mask(key.isin(list(_TO_BOOL))).sum()) != total:
            continue                                    # some value is not a boolean token
        if as_mask(key.isin(list(DIGIT_TOKENS)))[nonnull].all():
            continue                                    # pure 0/1: left for the numbers step
        new = key.map(_TO_BOOL).astype("boolean")
        if out is None:
            out = df.copy()
        out[col] = new
        actions.append(CleanAction(NAME, col, "parsed_bool", total, examples=change_examples(s, new, nonnull)))

    return (df if out is None else out), actions
