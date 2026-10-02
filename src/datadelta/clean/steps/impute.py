"""
impute.py — Fill (or drop) missing values, only when asked to.

Imputation invents values, so it never runs by default; clean_frame()
reports nulls per column in the stats either way. Two sources of rules:

  cleaning.impute (per column, wins)
      median | mean      numeric columns only (booleans are not numeric)
      mode               any column; the first (smallest) mode
      {constant: value}  any column whose type accepts the value
                         (text columns receive it as text)
      drop_rows          drop the rows where this column is null
  --impute median | mode  (plan.impute_default, every other column)
      median             numeric columns only; other columns untouched
      mode               every column

Order: drop_rows rules run first, so the statistics used for filling
describe the rows that are kept. A median or mean is rounded when the
column holds integers, so the column keeps its integer type.

A rule that cannot apply (missing column, median of a text column, a
constant of the wrong type, a column with no values) is reported as
"skipped" and changes nothing. Notes never quote the fill value; it
appears only in the local examples as (None, value).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..plan   import ImputeRule
from ..report import CleanAction
from .base    import StepContext, change_examples, is_text_column


NAME = "impute"


# ─────────────────────────────────────────────────────────────────────────────
# Fill values
# ─────────────────────────────────────────────────────────────────────────────

def _is_numeric(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s.dtype) and not pd.api.types.is_bool_dtype(s.dtype)


def _constant_for(s: pd.Series, value: Any) -> tuple[Any, bool]:
    """(value converted for this column, whether it fits the column type)."""
    dtype = s.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return value, isinstance(value, bool)
    if is_text_column(s):
        return (value if isinstance(value, str) else str(value)), True
    if isinstance(value, bool):
        return value, False
    if pd.api.types.is_integer_dtype(dtype):
        return value, isinstance(value, int)
    if pd.api.types.is_numeric_dtype(dtype):
        return value, isinstance(value, (int, float))
    if pd.api.types.is_datetime64_any_dtype(dtype):
        try:
            stamp = pd.Timestamp(value)
        except (TypeError, ValueError):
            return value, False
        return stamp, (stamp.tzinfo is None) == (getattr(dtype, "tz", None) is None)
    return value, True


def _fill_value(s: pd.Series, rule: ImputeRule) -> tuple[Any, str]:
    """(value, problem); problem is "" when the value can be used."""
    if isinstance(rule, dict):
        value, fits = _constant_for(s, rule["constant"])
        return value, "" if fits else "the constant does not fit the column type"
    values = s.dropna()
    if values.empty:
        return None, "column has no values"
    if rule in ("median", "mean"):
        if not _is_numeric(s):
            return None, f"{rule} needs a numeric column"
        value = values.median() if rule == "median" else values.mean()
        if pd.api.types.is_integer_dtype(s.dtype):
            value = round(float(value))
        return value, ""
    return values.mode().iloc[0], ""


def _fill(col: str, s: pd.Series, rule: ImputeRule, label: str) -> tuple[pd.Series | None, CleanAction | None]:
    """(filled column or None, action or None) for one column."""
    missing = s.isna()
    count = int(missing.sum())
    if count == 0:
        return None, None
    value, problem = _fill_value(s, rule)
    if not problem:
        try:
            new = s.fillna(value)
        except (TypeError, ValueError):
            new = None
        if new is None or new.dtype != s.dtype:
            problem = "the constant does not fit the column type"
    if problem:
        return None, CleanAction(NAME, col, "skipped", 0, note=f"impute: {problem}")
    return new, CleanAction(NAME, col, "imputed", count, examples=change_examples(s, new, missing), note=label)


# ─────────────────────────────────────────────────────────────────────────────
# Step
# ─────────────────────────────────────────────────────────────────────────────

def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    rules    = ctx.plan.impute
    default  = ctx.plan.impute_default
    columns  = ctx.columns(df)
    excluded = set(ctx.plan.exclude_columns)
    out      = df
    actions: list[CleanAction] = []

    # ── Rules that name a column that does not exist ─────────────────────────
    for col in rules:
        if col not in df.columns and col not in excluded:
            actions.append(CleanAction(NAME, col, "skipped", 0, note="impute: column not found"))

    # ── drop_rows first: the fill statistics describe the kept rows ──────────
    for col, rule in rules.items():
        if rule != "drop_rows" or col not in columns:
            continue
        drop = out[col].isna().to_numpy()
        if drop.any():
            out = out[~drop].copy()
            actions.append(CleanAction(NAME, col, "dropped_rows", int(drop.sum()),
                                       note="rows where this column is null"))

    # ── Per-column fills, then the --impute default for every other column ──
    todo: list[tuple[str, ImputeRule, str]] = [
        (col, rule, rule if isinstance(rule, str) else "constant")
        for col, rule in rules.items() if rule != "drop_rows" and col in columns
    ]
    if default is not None:
        todo += [
            (col, default, f"{default} (--impute)")
            for col in columns
            if col not in rules and (default == "mode" or _is_numeric(out[col]))
        ]

    filled: dict[str, pd.Series] = {}
    for col, rule, label in todo:
        new, action = _fill(col, out[col], rule, label)
        if action is not None:
            actions.append(action)
        if new is not None:
            filled[col] = new

    if filled:
        out = out.copy() if out is df else out
        for col, new in filled.items():
            out[col] = new
    return out, actions
