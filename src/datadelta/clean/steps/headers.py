"""
headers.py — Trim whitespace around column names.

" revenue " and "revenue" are the same column to a human, but not to
pandas: a stray space in a CSV header breaks --key, metrics.yaml column
references and before/after column matching.

Lossless rule: if trimming would make two columns share a name
("id" and " id"), no header is changed at all and one "skipped" action
lists the collisions, so no column ever disappears or gets shadowed.
"""

from __future__ import annotations

from collections import Counter

import pandas as pd

from ..report import CleanAction
from .base    import EXAMPLE_LIMIT, StepContext


NAME = "headers"


def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    excluded = set(ctx.plan.exclude_columns)
    old = list(df.columns)
    new = [c.strip() if isinstance(c, str) and c not in excluded else c for c in old]
    renamed = [(before, after) for before, after in zip(old, new) if before != after]
    if not renamed:
        return df, []

    counts = Counter(new)
    collisions = sorted({after for _before, after in renamed if counts[after] > 1})
    if collisions:
        listed = ", ".join(repr(name) for name in collisions)
        return df, [CleanAction(
            NAME, None, "skipped", len(renamed),
            examples = [pair for pair in renamed if pair[1] in collisions][:EXAMPLE_LIMIT],
            note     = f"trimming would duplicate column names {listed}; headers left unchanged",
        )]

    out = df.set_axis(new, axis=1)
    return out, [
        CleanAction(NAME, after, "header_trimmed", 1, examples=[(before, after)])
        for before, after in renamed
    ]
