"""
null_tokens.py — Turn placeholder text ("N/A", "null", "-", "") into real nulls.

Spreadsheets and exports spell "missing" in many ways. Left as text,
"N/A" counts as a category in the diff, blocks numeric parsing of an
otherwise numeric column, and hides the true null rate.

A text cell becomes null when its stripped, casefolded value is one of
plan.null_tokens (also casefolded). The list is configurable
(`cleaning.null_tokens` replaces it wholesale). The action note lists
which configured tokens matched; it never quotes other cell values.
"""

from __future__ import annotations

import pandas as pd

from ..report import CleanAction
from .base    import StepContext, as_mask, change_examples, fold, is_text_column, str_mask


NAME = "null_tokens"


def _quote(token: str) -> str:
    return f'"{token}"'


def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    tokens = {t.strip().casefold() for t in (ctx.plan.null_tokens or [])}
    out = None
    actions: list[CleanAction] = []
    if not tokens:
        return df, actions

    for col in ctx.columns(df):
        s = df[col]
        if not is_text_column(s):
            continue
        key = fold(s)
        hit = str_mask(s) & as_mask(key.isin(tokens))
        count = int(hit.sum())
        if count == 0:
            continue
        new = s.mask(hit)
        if out is None:
            out = df.copy()
        out[col] = new
        matched = sorted(set(key[hit]))
        actions.append(CleanAction(
            NAME, col, "null_token", count,
            examples = change_examples(s, new, hit),
            note     = "matched " + ", ".join(_quote(t) for t in matched),
        ))

    return (df if out is None else out), actions
