"""
test_steps_basic.py — The headers, whitespace and null_tokens steps.

Each step is called directly through apply(df, StepContext(plan)); the
last section runs them through clean_frame() with the later parse steps
switched off, so these tests keep their meaning as more steps register.
"""

from __future__ import annotations

import pandas as pd
import pytest

from datadelta.clean            import clean_frame
from datadelta.clean.plan       import CleanPlan
from datadelta.clean.steps      import STEP_FUNCS, headers, null_tokens, whitespace
from datadelta.clean.steps.base import StepContext


def ctx(**plan_kwargs) -> StepContext:
    return StepContext(CleanPlan(**plan_kwargs))


def text(values) -> pd.DataFrame:
    return pd.DataFrame({"v": values}, dtype=object)


def test_steps_are_registered():
    assert STEP_FUNCS["headers"] is headers.apply
    assert STEP_FUNCS["whitespace"] is whitespace.apply
    assert STEP_FUNCS["null_tokens"] is null_tokens.apply
    assert (headers.NAME, whitespace.NAME, null_tokens.NAME) == ("headers", "whitespace", "null_tokens")


# ── headers ───────────────────────────────────────────────────────────────────

def test_headers_are_trimmed():
    df = pd.DataFrame(columns=[" id ", "name", "\tregion"])
    out, actions = headers.apply(df, ctx())
    assert list(out.columns) == ["id", "name", "region"]
    assert [(a.column, a.action, a.count, a.examples) for a in actions] == [
        ("id",     "header_trimmed", 1, [(" id ", "id")]),
        ("region", "header_trimmed", 1, [("\tregion", "region")]),
    ]
    assert list(df.columns) == [" id ", "name", "\tregion"]      # input untouched


def test_header_collision_leaves_every_name_unchanged():
    df = pd.DataFrame([[1, 2, 3]], columns=["id", " id", "x "])
    out, actions = headers.apply(df, ctx())
    assert list(out.columns) == ["id", " id", "x "]
    assert len(actions) == 1
    action = actions[0]
    assert (action.step, action.column, action.action, action.count) == ("headers", None, "skipped", 2)
    assert action.note == "trimming would duplicate column names 'id'; headers left unchanged"
    assert action.examples == [(" id", "id")]


def test_headers_respect_exclude_columns():
    df = pd.DataFrame(columns=[" keep ", " trim "])
    out, actions = headers.apply(df, ctx(exclude_columns=[" keep "]))
    assert list(out.columns) == [" keep ", "trim"]
    assert [a.column for a in actions] == ["trim"]


def test_clean_and_non_string_headers_are_left_alone():
    df = pd.DataFrame([[1, 2]], columns=[0, "a"])
    out, actions = headers.apply(df, ctx())
    assert out is df
    assert actions == []


# ── whitespace ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, cleaned", [
    (" a ",             "a"),
    ("\t x \n",         "x"),
    ("a\u00a0b",        "a b"),
    ("\u00a0a\u00a0",   "a"),
    ("ab\u200b",        "ab"),
    ("\ufeffid",        "id"),
    ("a\u200cb\u200d",  "ab"),
    ("a  b",            "a  b"),          # inner spaces are kept
])
def test_whitespace_normalizes_cells(raw, cleaned):
    out, actions = whitespace.apply(text([raw]), ctx())
    assert out["v"].tolist() == [cleaned]
    expected_count = int(raw != cleaned)
    assert sum(a.count for a in actions) == expected_count


def test_whitespace_counts_changed_cells_with_examples():
    df = text([" a", "a", "b ", None, " a"])
    out, actions = whitespace.apply(df, ctx())
    assert out["v"].tolist() == ["a", "a", "b", None, "a"]
    assert len(actions) == 1
    action = actions[0]
    assert (action.step, action.column, action.action, action.count) == ("whitespace", "v", "trimmed", 3)
    assert action.examples == [(" a", "a"), ("b ", "b")]
    assert df["v"].tolist() == [" a", "a", "b ", None, " a"]    # input untouched


def test_whitespace_leaves_non_strings_in_mixed_columns():
    out, actions = whitespace.apply(text([" a", 5, None, "b "]), ctx())
    assert out["v"].tolist() == ["a", 5, None, "b"]
    assert type(out["v"].iloc[1]) is int
    assert actions[0].count == 2


def test_whitespace_skips_numeric_columns_and_keeps_string_dtype():
    df = pd.DataFrame({
        "n": [1.5, 2.5],
        "s": pd.Series([" x", "y"], dtype="string"),
    })
    out, actions = whitespace.apply(df, ctx())
    assert out["n"].tolist() == [1.5, 2.5]
    assert str(out["s"].dtype) == "string"
    assert out["s"].tolist() == ["x", "y"]
    assert [a.column for a in actions] == ["s"]


def test_whitespace_respects_exclude_columns():
    df = pd.DataFrame({"keep": [" a "], "trim": [" b "]}, dtype=object)
    out, actions = whitespace.apply(df, ctx(exclude_columns=["keep"]))
    assert out["keep"].tolist() == [" a "]
    assert out["trim"].tolist() == ["b"]
    assert [a.column for a in actions] == ["trim"]


def test_whitespace_without_changes_returns_the_input_and_no_actions():
    df = text(["a", "b"])
    out, actions = whitespace.apply(df, ctx())
    assert out is df
    assert actions == []


# ── null_tokens ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("token", [
    "", "NA", "na", "N/A", "n/a", "NaN", "null", "NULL", "None", "-", "—", "--", "#N/A", " na ",
])
def test_default_null_tokens_become_null(token):
    out, actions = null_tokens.apply(text([token, "keep"]), ctx())
    assert out["v"].isna().tolist() == [True, False]
    assert actions[0].count == 1


@pytest.mark.parametrize("value", ["N.A.", "nan-x", "0", "none of them", "--x", "nul"])
def test_other_values_are_kept(value):
    out, actions = null_tokens.apply(text([value]), ctx())
    assert out["v"].tolist() == [value]
    assert actions == []


def test_null_token_action_lists_matched_tokens():
    df = text(["N/A", "n/a", "null", "x", None])
    out, actions = null_tokens.apply(df, ctx())
    assert out["v"].isna().tolist() == [True, True, True, False, True]
    assert len(actions) == 1
    action = actions[0]
    assert (action.step, action.column, action.action, action.count) == ("null_tokens", "v", "null_token", 3)
    assert action.note == 'matched "n/a", "null"'
    assert action.examples == [("N/A", None), ("n/a", None), ("null", None)]
    assert df["v"].tolist() == ["N/A", "n/a", "null", "x", None]  # input untouched


def test_configured_tokens_replace_the_defaults():
    out, actions = null_tokens.apply(text(["MISSING", " missing ", "N/A"]), ctx(null_tokens=["Missing"]))
    assert out["v"].isna().tolist() == [True, True, False]
    assert actions[0].note == 'matched "missing"'


def test_empty_token_list_changes_nothing():
    df = text(["N/A"])
    out, actions = null_tokens.apply(df, ctx(null_tokens=[]))
    assert out is df
    assert actions == []


def test_null_tokens_respect_exclude_columns_and_skip_non_text():
    df = pd.DataFrame({
        "notes": pd.Series(["N/A"], dtype=object),
        "code":  pd.Series(["N/A"], dtype=object),
        "qty":   [0],
    })
    out, actions = null_tokens.apply(df, ctx(exclude_columns=["notes"]))
    assert out["notes"].tolist() == ["N/A"]
    assert out["code"].isna().tolist() == [True]
    assert out["qty"].tolist() == [0]
    assert [a.column for a in actions] == ["code"]


def test_null_tokens_on_string_dtype():
    df = pd.DataFrame({"v": pd.Series(["NULL", "a"], dtype="string")})
    out, actions = null_tokens.apply(df, ctx())
    assert out["v"].isna().tolist() == [True, False]
    assert str(out["v"].dtype) == "string"


# ── Through clean_frame ───────────────────────────────────────────────────────

BASIC_ONLY = dict(booleans=False, numbers=None, dates=None)


def test_basic_steps_through_clean_frame():
    df = pd.DataFrame({
        " region ": [" EMEA", "N/A ", "APAC\u200b", "-"],
        "notes":    [" keep ", "N/A", "x", "y"],
    }, dtype=object)
    snapshot = df.copy()
    result = clean_frame(df, CleanPlan(exclude_columns=["notes"], **BASIC_ONLY))

    assert list(result.df.columns) == ["region", "notes"]
    assert result.df["region"].isna().tolist() == [False, True, False, True]
    assert [result.df["region"].iloc[0], result.df["region"].iloc[2]] == ["EMEA", "APAC"]
    assert result.df["notes"].tolist() == [" keep ", "N/A", "x", "y"]
    assert [(a.step, a.column, a.action, a.count) for a in result.report.actions] == [
        ("headers",     "region", "header_trimmed", 1),
        ("whitespace",  "region", "trimmed",        3),
        ("null_tokens", "region", "null_token",     2),
    ]
    assert result.report.stats.cells_changed == 4                      # "N/A " trimmed, then nulled: one cell
    assert result.report.stats.nulls_before == {" region ": 0, "notes": 0}
    assert result.report.stats.nulls_after == {"region": 2, "notes": 0}
    pd.testing.assert_frame_equal(df, snapshot)
