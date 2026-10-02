"""
clean — Lossless-by-default data cleaning.

    result = clean_frame(df, plan)      # CleanResult(df=..., report=...)
    before, after = clean_both(df_before, df_after, plan)     # diff --clean

clean_frame() runs the enabled steps in STEP_ORDER on a copy of `df`.
Each step is a pure function (see steps/base.py) that returns a new
frame plus the CleanActions describing what it changed; clean_frame()
collects those actions and computes CleanStats (rows, nulls per column,
duplicates, retyped columns, cells changed).

cells_changed counts the cells of the cleaned table whose value or type
differs from the input, each once: " 1" trimmed and then parsed is one
changed cell, and the cells of rows that dedupe or drop_rows removed are
not counted (those are rows). A per-action sum would count the first
twice and include the second.

duplicate_keys counts repeated values in the key column: plan.key_column
(--key / cleaning.key) when set, otherwise the first column the profiler
classifies as an id, the same rule `datadelta diff` uses without --key.

WHY THIS ORDER?
  headers first so every later step and every config entry sees the
  trimmed column names; whitespace before null_tokens so " NA " is
  recognized; null_tokens before the parsers so "N/A" does not block a
  numeric column; booleans before numbers so a yes/no/1/0 column becomes
  boolean rather than half-numeric; dedupe and impute last because they
  depend on the final values.
"""

from __future__ import annotations

import dataclasses
import warnings
from typing import Callable

import numpy as np
import pandas as pd

from ..profiler  import profile_columns
from ..progress  import NullProgress, ProgressSink, plural
from .plan       import (
    CleanConfigError, CleanPlan, DateOptions, DedupeOptions, ParseOptions,
    apply_cli_overrides, load_clean_plan, plan_from_config,
)
from .report     import (
    CELL_ACTIONS, RETYPE_ACTIONS, CleanAction, CleanReport, CleanResult, CleanStats,
)
from .steps      import STEP_FUNCS
from .steps.base import StepContext, str_mask


STEP_ORDER: list[str] = [
    "headers", "whitespace", "null_tokens", "booleans", "numbers",
    "dates", "case", "dedupe", "impute",
]

# Whether the plan turns a step on. Names without an entry (none in
# production) count as enabled.
_ENABLED: dict[str, Callable[[CleanPlan], bool]] = {
    "headers":     lambda plan: plan.headers,
    "whitespace":  lambda plan: plan.whitespace,
    "null_tokens": lambda plan: plan.null_tokens is not None,
    "booleans":    lambda plan: plan.booleans,
    "numbers":     lambda plan: plan.numbers is not None,
    "dates":       lambda plan: plan.dates is not None,
    "case":        lambda plan: any(plan.case.values()),
    "dedupe":      lambda plan: plan.dedupe is not None,
    "impute":      lambda plan: bool(plan.impute) or plan.impute_default is not None,
}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def enabled_steps(plan: CleanPlan) -> list[str]:
    """Step names in STEP_ORDER that the plan enables and that are registered."""
    return [
        name for name in STEP_ORDER
        if name in STEP_FUNCS and _ENABLED.get(name, lambda _plan: True)(plan)
    ]


def clean_frame(
    df:        pd.DataFrame,
    plan:      CleanPlan | None = None,
    progress:  ProgressSink | None = None,
    stage_key: str = "clean",
    label:     str = "clean",
) -> CleanResult:
    """
    Clean a copy of `df` according to `plan` (defaults when None).
    `df` itself is never modified.

    Progress: one stage `stage_key` with total = number of enabled steps,
    one advance per step (note = step name), and a "done" summary of the
    form "1,234 cells standardized" ("failed" if a step raises).
    """
    plan     = plan if plan is not None else CleanPlan()
    progress = progress if progress is not None else NullProgress()

    if df.columns.has_duplicates:
        dupes = sorted({str(c) for c in df.columns[df.columns.duplicated()]})
        raise ValueError(f"duplicate column names: {', '.join(dupes)}; rename them before cleaning")

    names   = enabled_steps(plan)
    ctx     = StepContext(plan)
    out     = df.copy()
    out.index = pd.RangeIndex(len(df))     # row positions while the steps run (they keep labels)
    actions = _missing_excludes(df, plan)

    progress.stage_start(stage_key, label, total=len(names))
    try:
        for name in names:
            out, step_actions = STEP_FUNCS[name](out, ctx)
            actions.extend(step_actions)
            progress.advance(stage_key, 1, note=name)
    except Exception as e:
        progress.stage_end(stage_key, "failed", summary=str(e))
        raise

    kept = out.index.to_numpy()            # positions of the input rows still present
    stats = _stats(df, out, actions, plan, kept)
    out.index = df.index[kept]
    progress.stage_end(stage_key, "done", summary=f"{stats.cells_changed:,} cells standardized")
    return CleanResult(df=out, report=CleanReport(actions=actions, stats=stats))


# The note of the "skipped" action clean_both() adds to the side it re-cleans.
KEPT_AS_TEXT_NOTE = "kept as text: the other side has values that do not parse"


def clean_both(
    before:   pd.DataFrame,
    after:    pd.DataFrame,
    plan:     CleanPlan | None = None,
    progress: ProgressSink | None = None,
) -> tuple[CleanResult, CleanResult]:
    """
    `diff --clean`: clean both sides with one plan, so that every shared
    column ends up with one logical type.

    Each side is cleaned on its own, so a parse step can convert a column
    on one side while a value on the other side blocks it (prices with one
    "TBD"): the diff would then compare numbers with text. Such a column
    stays text: the side that converted it is cleaned again with the parse
    steps told to leave the column alone (plan.keep_text), and its report
    gets a "skipped" action with KEPT_AS_TEXT_NOTE. A column that the other
    side's reader had already typed (Excel numbers against CSV currency
    text) is left converted: both sides then hold numbers.

    Progress: stages clean.before and clean.after, plus clean.keep_text
    when a side has to be cleaned again.
    """
    plan     = plan if plan is not None else CleanPlan()
    progress = progress if progress is not None else NullProgress()
    sides = {
        "before": (before, clean_frame(before, plan, progress, stage_key="clean.before", label="clean before")),
        "after":  (after,  clean_frame(after,  plan, progress, stage_key="clean.after",  label="clean after")),
    }
    keep = _one_sided_retypes(sides["before"][1], sides["after"][1])
    if not any(keep.values()):
        return sides["before"][1], sides["after"][1]

    progress.stage_start("clean.keep_text", "keep text", total=sum(1 for cols in keep.values() if cols))
    results = {}
    for side, (raw, first) in sides.items():
        results[side] = _keep_as_text(raw, plan, first, keep[side]) if keep[side] else first
        if keep[side]:
            progress.advance("clean.keep_text", 1, note=side)
    n_kept = sum(len(cols) for cols in keep.values())
    progress.stage_end("clean.keep_text", "done", summary=f"{plural(n_kept, 'column')} kept as text")
    return results["before"], results["after"]


def align_dtypes(before: pd.DataFrame, after: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    After `diff --clean` a shared column can hold one logical type in two
    storages: the side the cleaner parsed is nullable (Int64, Float64,
    boolean, datetime64[us]) while a side that arrived already typed keeps
    numpy dtypes (int64, float64, bool from DuckDB; datetime64[ns] from
    read_excel on pandas 2). Cast both sides of such a column to the
    cleaner's dtype so the schema layer compares types, not storage.

    Columns whose logical types differ (int vs float, number vs text,
    naive vs tz-aware datetimes) are left alone: that is a real type
    change. Columns on one side only are left alone. Neither input is
    modified.

    Integers try Int64, then UInt64: read_excel gives uint64 for values
    above 2**63, which Int64 cannot hold. When neither holds both sides
    (such a value against a negative one), the column is left alone.
    """
    before, after = before.copy(), after.copy()
    for col in before.columns.intersection(after.columns):
        if before[col].dtype == after[col].dtype:
            continue
        for target in _common_dtypes(before[col].dtype, after[col].dtype):
            try:
                cast_before, cast_after = before[col].astype(target), after[col].astype(target)
            except (TypeError, ValueError, OverflowError):     # values the target cannot hold
                continue
            before[col], after[col] = cast_before, cast_after
            break
    return before, after


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _missing_excludes(df: pd.DataFrame, plan: CleanPlan) -> list[CleanAction]:
    """A config entry naming a column that does not exist is skipped, visibly."""
    known = set(df.columns) | {c.strip() for c in df.columns if isinstance(c, str)}
    return [
        CleanAction("plan", name, "skipped", 0, note="exclude_columns: column not found")
        for name in plan.exclude_columns if name not in known
    ]


def _one_sided_retypes(before: CleanResult, after: CleanResult) -> dict[str, list]:
    """
    Per side, the shared columns a parse step retyped there while the other
    side still holds them as text (its parse step refused them). "Text"
    means at least one str cell: an object column of numbers, or of
    nulls only, holds no value that failed to parse.
    """
    retyped = {"before": before.report.stats.columns_retyped, "after": after.report.stats.columns_retyped}
    frames  = {"before": before.df, "after": after.df}
    shared  = before.df.columns.intersection(after.df.columns)
    keep: dict[str, list] = {"before": [], "after": []}
    for side, other in (("before", "after"), ("after", "before")):
        for col in shared:
            if col in retyped[side] and col not in retyped[other] and str_mask(frames[other][col]).any():
                keep[side].append(col)
    return keep


def _keep_as_text(raw: pd.DataFrame, plan: CleanPlan, first: CleanResult, columns: list) -> CleanResult:
    """
    Clean `raw` again with the parse steps leaving `columns` as text, and
    record why: one "skipped" action per column, in place of the parse
    action of the first pass (same step, same count).
    """
    again = clean_frame(raw, dataclasses.replace(plan, keep_text=[*plan.keep_text, *columns]))
    for col in columns:
        parsed = next(a for a in first.report.actions if a.column == col and a.action in RETYPE_ACTIONS)
        skipped = CleanAction(parsed.step, col, "skipped", parsed.count, note=KEPT_AS_TEXT_NOTE)
        actions = again.report.actions
        position = next(
            (i for i, a in enumerate(actions) if a.step in STEP_ORDER and STEP_ORDER.index(a.step) > STEP_ORDER.index(parsed.step)),
            len(actions),
        )
        actions.insert(position, skipped)
    return again


def _count_duplicate_rows(df: pd.DataFrame) -> int:
    try:
        return int(df.duplicated().sum())
    except TypeError:                       # unhashable cells (e.g. dicts from JSON)
        return int(df.astype(str).duplicated().sum())


def _detect_key(df: pd.DataFrame) -> str | None:
    """
    The first column the profiler classifies as an id (the rule differ.py
    applies when --key is not given), or None when there is none.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")     # the profiler's date sniffing warns on free text
        try:
            profile = profile_columns(df, df)
        except TypeError:                   # unhashable cells (e.g. dicts from JSON)
            return None
    ids = [col for col, cp in profile.columns.items() if cp.semantic_type == "id"]
    return ids[0] if ids else None


def _count_duplicate_keys(df: pd.DataFrame, key: str | None) -> int | None:
    if key is None or key not in df.columns:
        return None
    values = df[key].dropna()
    try:
        return int(values.duplicated().sum())
    except TypeError:                       # unhashable cells (e.g. dicts from JSON)
        return int(values.astype(str).duplicated().sum())


def _changed_cells(before: pd.DataFrame, after: pd.DataFrame, kept, actions: list[CleanAction]) -> int:
    """
    Cells of `after` whose value or type differs from the input cell in the
    same row (`kept`: input row positions) and column position (the headers
    step renames, it never moves). Missing stays missing: unchanged. Only
    columns some cell action touched are compared.
    """
    touched = {a.column for a in actions if a.action in CELL_ACTIONS and a.count > 0}
    changed = 0
    for i, col in enumerate(after.columns):
        if col not in touched:
            continue
        old = before.iloc[kept, i].to_numpy(dtype=object, na_value=None)
        new = after.iloc[:, i].to_numpy(dtype=object, na_value=None)
        try:
            same = np.asarray(old == new, dtype=bool)
        except (TypeError, ValueError):     # cells whose == is not a bool (arrays in cells)
            same = np.array([_same_cell(a, b) for a, b in zip(old, new)], dtype=bool)
        changed += int((~same).sum())
    return changed


def _same_cell(a, b) -> bool:
    """str "1" vs int 1 differ (a type change); None vs None is unchanged."""
    if a is b:
        return True
    try:
        return bool(a == b)
    except (TypeError, ValueError):
        return False


def _stats(before: pd.DataFrame, after: pd.DataFrame, actions: list[CleanAction], plan: CleanPlan, kept) -> CleanStats:
    retyped: dict[str, str] = {}
    for a in actions:
        if a.action in RETYPE_ACTIONS and a.column is not None and a.count > 0:
            retyped[a.column] = RETYPE_ACTIONS[a.action]

    key = plan.key_column
    if key is None:
        key = _detect_key(after)
    duplicate_keys = _count_duplicate_keys(after, key)

    return CleanStats(
        rows_before     = len(before),
        rows_after      = len(after),
        cells_changed   = _changed_cells(before, after, kept, actions),
        columns_retyped = retyped,
        nulls_before    = {col: int(n) for col, n in before.isna().sum().items()},
        nulls_after     = {col: int(n) for col, n in after.isna().sum().items()},
        duplicate_rows  = _count_duplicate_rows(after),
        duplicate_keys  = duplicate_keys,
    )


def _common_dtypes(left, right) -> tuple[str, ...]:
    """The cleaner's dtypes, in order of preference, for two storages of the same logical type."""
    t = pd.api.types
    if t.is_bool_dtype(left)       and t.is_bool_dtype(right):       return ("boolean",)
    if t.is_integer_dtype(left)    and t.is_integer_dtype(right):    return ("Int64", "UInt64")
    if t.is_float_dtype(left)      and t.is_float_dtype(right):      return ("Float64",)
    if t.is_datetime64_dtype(left) and t.is_datetime64_dtype(right): return ("datetime64[us]",)
    return ()


__all__ = [
    "STEP_ORDER", "enabled_steps", "clean_frame", "clean_both", "align_dtypes", "KEPT_AS_TEXT_NOTE",
    "CleanPlan", "ParseOptions", "DateOptions", "DedupeOptions", "CleanConfigError",
    "plan_from_config", "apply_cli_overrides", "load_clean_plan",
    "CleanAction", "CleanStats", "CleanReport", "CleanResult",
]
