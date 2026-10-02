"""
base.py — Shared pieces for clean steps.

THE STEP CONTRACT
  Every step module exposes NAME and

      apply(df, ctx) -> (new_df, [CleanAction, ...])

  apply() is pure: it never mutates `df` (it copies before changing a
  column) and reports every change it makes as a CleanAction. Rows keep
  their index labels (a step that drops rows drops their labels with
  them): clean_frame() matches the result to the input by label. Steps work
  column by column with vectorized pandas string operations, so 200k
  rows stay fast.

TEXT COLUMNS ACROSS PANDAS VERSIONS
  pandas 2 stores text as `object`; pandas 3 uses a "str" string dtype
  by default; the nullable "string" dtype exists in both. An object
  column may also mix str with numbers or dates (Excel does this).
  is_text_column() accepts object and string dtypes, and str_mask()
  says which cells actually hold a str, so steps never touch the rest.

  The `.str` accessor itself refuses an object column that holds no str
  cell at all (all ints, dates, dicts, bytes ...) and chains of it fail
  on cells that came back as NaN. So steps never call it on a whole
  column: fold() and replace_str_cells() run string operations on the
  str cells only, and a column without str cells is simply not theirs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing      import Any, Callable

import numpy as np
import pandas as pd

from ..plan   import CleanPlan
from ..report import CleanAction


# How many (before, after) pairs an action keeps, and how far change_examples()
# scans for distinct pairs before giving up.
EXAMPLE_LIMIT = 3
EXAMPLE_SCAN  = 200

# A parse step that cannot convert a column reports a "skipped" action only
# when at least this share of its non-null values did parse; below it the
# column is ordinary text and is left alone silently.
SKIP_REPORT_RATIO = 0.5


@dataclass
class StepContext:
    """What a step may read: the validated plan."""
    plan: CleanPlan

    def columns(self, df: pd.DataFrame) -> list[str]:
        """df columns not in plan.exclude_columns, in order."""
        excluded = set(self.plan.exclude_columns)
        return [c for c in df.columns if c not in excluded]

    def parse_columns(self, df: pd.DataFrame) -> list[str]:
        """The columns a parse step (booleans, numbers, dates) may retype: also not in plan.keep_text."""
        kept = set(self.plan.keep_text)
        return [c for c in self.columns(df) if c not in kept]


StepFn = Callable[[pd.DataFrame, StepContext], tuple[pd.DataFrame, list[CleanAction]]]


# ─────────────────────────────────────────────────────────────────────────────
# Column helpers
# ─────────────────────────────────────────────────────────────────────────────

def is_text_column(s: pd.Series) -> bool:
    """True for object and pandas string dtypes (pandas 2 and 3)."""
    return pd.api.types.is_object_dtype(s.dtype) or isinstance(s.dtype, pd.StringDtype)


def as_mask(values: pd.Series) -> pd.Series:
    """A plain numpy-bool Series with the same index; NA counts as False."""
    return pd.Series(values.to_numpy(dtype=bool, na_value=False), index=values.index)


def str_mask(s: pd.Series) -> pd.Series:
    """True where the cell holds a Python str (never for missing values)."""
    if isinstance(s.dtype, pd.StringDtype):
        return as_mask(s.notna())
    if not pd.api.types.is_object_dtype(s.dtype):
        return pd.Series(False, index=s.index)
    if pd.api.types.infer_dtype(s, skipna=True) == "string":
        return as_mask(s.notna())
    return as_mask(s.map(lambda v: isinstance(v, str)))


def fold(s: pd.Series) -> pd.Series:
    """Stripped, casefolded view for token matching (NaN where not a str)."""
    if isinstance(s.dtype, pd.StringDtype):
        return s.str.strip().str.casefold()
    mask = str_mask(s).to_numpy()
    values = np.full(len(s), np.nan, dtype=object)
    if mask.any():
        values[mask] = s[mask].str.strip().str.casefold().to_numpy(dtype=object)
    return pd.Series(values, index=s.index, name=s.name)


def replace_str_cells(
    s:  pd.Series,
    fn: Callable[[pd.Series], pd.Series],
) -> tuple[pd.Series, pd.Series]:
    """
    Apply `fn` to the str cells of `s` only. `fn` receives a Series holding
    nothing but str cells and returns one of the same length.

    Returns (new, changed): `new` is `s` with the str cells whose value fn
    changed replaced (every other cell untouched, index and dtype kept),
    `changed` a full-length mask of those cells. A column without any
    change comes back as `s` itself with an all-False mask.
    """
    nothing = pd.Series(False, index=s.index)
    mask = str_mask(s).to_numpy()
    if not mask.any():
        return s, nothing
    cells    = s[mask]
    replaced = fn(cells)
    differs  = (replaced != cells).to_numpy(dtype=bool)
    if not differs.any():
        return s, nothing
    positions = np.flatnonzero(mask)[differs]
    new = s.copy()
    new.iloc[positions] = replaced.to_numpy(dtype=object)[differs]
    changed = np.zeros(len(s), dtype=bool)
    changed[positions] = True
    return new, pd.Series(changed, index=s.index)


# ─────────────────────────────────────────────────────────────────────────────
# Examples
# ─────────────────────────────────────────────────────────────────────────────

def _plain(value: Any) -> Any:
    """numpy scalars → Python scalars; every missing marker → None."""
    if isinstance(value, (np.integer, np.floating, np.bool_)):
        value = value.item()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def change_examples(
    before: pd.Series,
    after:  pd.Series,
    mask:   pd.Series,
    limit:  int = EXAMPLE_LIMIT,
) -> list[tuple[Any, Any]]:
    """
    Up to `limit` distinct (before, after) pairs at the positions where
    `mask` is True. Works by position, so duplicate index labels are fine.
    """
    positions = np.flatnonzero(as_mask(mask).to_numpy())[:EXAMPLE_SCAN]
    examples: list[tuple[Any, Any]] = []
    seen:     set[str] = set()
    for pos in positions:
        pair = (_plain(before.iloc[pos]), _plain(after.iloc[pos]))
        marker = repr(pair)
        if marker in seen:
            continue
        seen.add(marker)
        examples.append(pair)
        if len(examples) >= limit:
            break
    return examples
