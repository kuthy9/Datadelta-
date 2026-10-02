"""
dedupe.py — Drop duplicate rows, only when asked to.

Duplicates inflate counts and sums, but deciding that two rows are "the
same" is a business decision, so this step runs only when configured
(`cleaning.dedupe` or `--dedupe`). Without it, clean_frame() still
reports duplicate_rows / duplicate_keys in the stats.

  mode exact  every column is equal (excluded columns are compared too:
              "exact" means exact). keep: first | last.
  mode key    the key columns are equal. Rows with a null in any key
              column are never treated as duplicates of each other: a
              missing key does not identify an entity.

A key column that does not exist makes the step skip (nothing dropped)
with a "skipped" action per missing column. The kept rows keep their
original index labels.
"""

from __future__ import annotations

import pandas as pd

from ..plan   import DedupeOptions
from ..report import CleanAction
from .base    import EXAMPLE_LIMIT, StepContext


NAME = "dedupe"


def _duplicated(df: pd.DataFrame, subset: list[str] | None, keep: str) -> pd.Series:
    frame = df if subset is None else df[subset]
    try:
        return frame.duplicated(keep=keep)
    except TypeError:                       # unhashable cells (e.g. dicts from JSON)
        return frame.astype(str).duplicated(keep=keep)


def _key_examples(dropped: pd.DataFrame) -> list[tuple]:
    """Up to EXAMPLE_LIMIT distinct dropped keys as (key, None) pairs."""
    try:
        keys = dropped.drop_duplicates().head(EXAMPLE_LIMIT).to_numpy(dtype=object).tolist()
    except TypeError:
        return []
    return [(key[0] if len(key) == 1 else key, None) for key in keys]


def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    options: DedupeOptions | None = ctx.plan.dedupe
    if options is None or df.empty:
        return df, []

    if options.mode == "exact":
        drop = _duplicated(df, None, options.keep)
        note = f"exact duplicate rows; kept the {options.keep} of each"
        examples: list = []
    else:
        missing = [k for k in options.keys if k not in df.columns]
        if missing:
            return df, [CleanAction(NAME, k, "skipped", 0, note="dedupe.keys: column not found") for k in missing]
        keyed = df[options.keys].notna().all(axis=1)
        drop = _duplicated(df, options.keys, options.keep) & keyed
        note = f"duplicate {', '.join(options.keys)}; kept the {options.keep} of each"
        examples = _key_examples(df.loc[drop.to_numpy(), options.keys])

    count = int(drop.sum())
    if count == 0:
        return df, []
    column = options.keys[0] if options.mode == "key" and len(options.keys) == 1 else None
    kept = df[~drop.to_numpy()].copy()              # a real copy: later steps may assign columns
    return kept, [CleanAction(NAME, column, "dropped_rows", count, examples=examples, note=note)]
