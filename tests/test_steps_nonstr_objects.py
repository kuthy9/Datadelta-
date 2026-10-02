"""
test_steps_nonstr_objects.py — Object columns whose cells are not (all) str.

is_text_column() accepts every object column, but an object column can
hold ints, floats, bools, dates, Decimals, dicts, lists, bytes ... (JSON
and Excel loaders produce them). Every string operation of every step
must therefore touch only the cells str_mask() marks: a column without
str cells passes through untouched and unreported, and in a mixed column
only the str cells change. The one exception is a column of number cells
and nulls, which the numbers step types (NUMBER_CELLS below).
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from datadelta.clean            import clean_frame
from datadelta.clean.plan       import CASE_MODES, CleanPlan
from datadelta.clean.steps      import booleans, case, dates, null_tokens, numbers, whitespace
from datadelta.clean.steps.base import StepContext, fold


NON_STR_COLUMNS: dict[str, list] = {
    "ints":     [1, 2, 3],
    "floats":   [1.5, 2.5, 3.5],
    "bools":    [True, False, True],
    "dates":    [dt.date(2024, 1, 5), dt.date(2024, 1, 6), dt.date(2024, 1, 7)],
    "times":    [dt.time(10, 30), dt.time(11, 0), dt.time(12, 15)],
    "decimals": [Decimal("1.5"), Decimal("2"), Decimal("3.25")],
    "int_none": [1, None],
    "dicts":    [{"a": 1}, {"b": 2}, {"c": 3}],
    "lists":    [[1, 2], [3], [4, 5, 6]],
    "bytes":    [b"a", b" b ", b"N/A"],
    "arrays":   [np.array([1, 2]), np.array([3]), np.array([4, 5, 6])],
}

STEPS = [whitespace, null_tokens, case, booleans, numbers, dates]

# Columns of number cells and nulls are the numbers step's: it gives them a
# numeric dtype (re-review NB1: Excel number cells left after null_tokens;
# see test_steps_types.py). Every other step leaves them alone.
NUMBER_CELLS = {"ints", "floats", "int_none"}

LEFT_ALONE = [
    pytest.param(step, name, id=f"{name}-{step.NAME}")
    for step in STEPS for name in NON_STR_COLUMNS
    if not (step is numbers and name in NUMBER_CELLS)
]


def frame(values) -> pd.DataFrame:
    return pd.DataFrame({"v": pd.Series(values, dtype=object)})


def ctx(**plan_kwargs) -> StepContext:
    return StepContext(CleanPlan(**plan_kwargs))


def cells(s: pd.Series) -> list[str]:
    """repr of every cell: compares ndarray, dict and None cells without ambiguity."""
    return [repr(v) for v in s]


def summary(actions) -> list[tuple]:
    return [(a.step, a.column, a.action, a.count, a.note) for a in actions]


# ── Columns without a single str cell ─────────────────────────────────────────

@pytest.mark.parametrize("step, name", LEFT_ALONE)
def test_step_leaves_a_column_without_str_cells_alone(step, name):
    df = frame(NON_STR_COLUMNS[name])
    before = cells(df["v"])
    out, actions = step.apply(df, ctx(case={"lower": ["v"]}))

    assert cells(out["v"]) == before
    assert out["v"].dtype == object
    if step is case:
        assert summary(actions) == [("case", "v", "skipped", 0, "case.lower: not a text column")]
    else:
        assert actions == []


@pytest.mark.parametrize("mode", CASE_MODES)
@pytest.mark.parametrize("name", NON_STR_COLUMNS)
def test_case_skips_a_listed_column_without_str_cells(mode, name):
    df = frame(NON_STR_COLUMNS[name])
    out, actions = case.apply(df, ctx(case={mode: ["v"]}))
    assert cells(out["v"]) == cells(df["v"])
    assert summary(actions) == [("case", "v", "skipped", 0, f"case.{mode}: not a text column")]


@pytest.mark.parametrize("name", [name for name in NON_STR_COLUMNS if name not in NUMBER_CELLS])
def test_clean_frame_with_default_plan_leaves_the_column_alone(name):
    df = frame(NON_STR_COLUMNS[name])
    result = clean_frame(df)
    assert cells(result.df["v"]) == cells(df["v"])
    assert result.report.actions == []
    assert result.report.stats.cells_changed == 0
    assert result.report.stats.columns_retyped == {}


@pytest.mark.parametrize("name", sorted(NUMBER_CELLS))
def test_clean_frame_types_a_column_of_number_cells(name):
    df = frame(NON_STR_COLUMNS[name])
    result = clean_frame(df)
    assert result.df["v"].dropna().tolist() == [v for v in NON_STR_COLUMNS[name] if v is not None]
    assert str(result.df["v"].dtype) in ("Int64", "Float64")
    assert [(a.step, a.action) for a in result.report.actions] == [("numbers", "parsed_number")]
    assert result.report.stats.columns_retyped == {"v": "number"}


def test_an_all_null_object_column_is_not_reported_by_case():
    """No cell at all is not "not a text column": nothing to say."""
    out, actions = case.apply(frame([None, None]), ctx(case={"lower": ["v"]}))
    assert actions == []
    assert out["v"].isna().all()


# ── Mixed columns: only the str cells change ──────────────────────────────────

def test_whitespace_changes_only_the_str_cell_of_a_mixed_column():
    df = frame(["  a ", 5, None, Decimal("1.5")])
    out, actions = whitespace.apply(df, ctx())
    assert cells(out["v"]) == cells(pd.Series(["a", 5, None, Decimal("1.5")], dtype=object))
    assert [(a.action, a.count, a.examples) for a in actions] == [("trimmed", 1, [("  a ", "a")])]
    assert cells(df["v"])[0] == repr("  a ")                      # input untouched


def test_null_tokens_change_only_the_str_cell_of_a_mixed_column():
    df = frame([" N/A ", 5, None, Decimal("1.5"), "kept"])
    out, actions = null_tokens.apply(df, ctx())
    assert out["v"].isna().tolist() == [True, False, True, False, False]
    assert out["v"].tolist()[1] == 5 and out["v"].tolist()[3] == Decimal("1.5")
    assert out["v"].tolist()[4] == "kept"
    assert [(a.action, a.count, a.note) for a in actions] == [("null_token", 1, 'matched "n/a"')]


@pytest.mark.parametrize("mode, expected", [("lower", "abc"), ("upper", "ABC"), ("title", "Abc")])
def test_case_changes_only_the_str_cell_of_a_mixed_column(mode, expected):
    df = frame(["aBc", 5, None, Decimal("1.5"), b"X"])
    out, actions = case.apply(df, ctx(case={mode: ["v"]}))
    assert cells(out["v"]) == cells(pd.Series([expected, 5, None, Decimal("1.5"), b"X"], dtype=object))
    assert [(a.action, a.count) for a in actions] == [(case.ACTION_FOR_MODE[mode], 1)]


def test_mixed_column_with_containers_only_changes_the_str_cells():
    df = frame(["  a ", {"k": " v "}, [" x "], "b  ", b" y "])
    out, actions = whitespace.apply(df, ctx())
    assert cells(out["v"]) == cells(pd.Series(["a", {"k": " v "}, [" x "], "b", b" y "], dtype=object))
    assert [(a.action, a.count) for a in actions] == [("trimmed", 2)]


def test_clean_frame_changes_only_the_str_cell_of_a_mixed_column():
    df = frame(["  a ", 5, None, Decimal("1.5")])
    result = clean_frame(df)
    assert cells(result.df["v"]) == cells(pd.Series(["a", 5, None, Decimal("1.5")], dtype=object))
    assert [(a.step, a.action, a.count) for a in result.report.actions] == [("whitespace", "trimmed", 1)]


# ── fold() ────────────────────────────────────────────────────────────────────

def test_fold_is_nan_where_the_cell_is_not_a_str():
    folded = fold(frame(["  ÄB ", 5, None, b" x ", Decimal("1"), {"a": 1}])["v"])
    assert folded.iloc[0] == "äb"
    assert folded.iloc[1:].isna().all()


@pytest.mark.parametrize("name", NON_STR_COLUMNS)
def test_fold_of_a_column_without_str_cells_is_all_nan(name):
    folded = fold(frame(NON_STR_COLUMNS[name])["v"])
    assert len(folded) == len(NON_STR_COLUMNS[name])
    assert folded.isna().all()


def test_fold_keeps_the_index_and_works_on_string_dtype():
    s = pd.Series(["  A ", "b"], index=[7, 7], dtype="string")
    folded = fold(s)
    assert folded.tolist() == ["a", "b"] and list(folded.index) == [7, 7]
    mixed = pd.Series(["  A ", 1], index=[7, 7], dtype=object)
    assert fold(mixed).iloc[0] == "a" and list(fold(mixed).index) == [7, 7]
