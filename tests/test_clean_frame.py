"""
test_clean_frame.py — clean_frame() orchestration, CleanReport and the
step helpers in steps/base.py.

The real steps arrive in later tasks, so these tests swap STEP_FUNCS for
small fake steps (monkeypatch restores the registry afterwards).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import datadelta.clean as clean
from datadelta.clean        import STEP_ORDER, clean_frame, enabled_steps
from datadelta.clean.plan   import CleanPlan, DedupeOptions
from datadelta.clean.report import (
    ACTIONS, CELL_ACTIONS, RETYPE_ACTIONS, CleanAction, CleanReport, CleanResult, CleanStats,
)
from datadelta.clean.steps  import STEP_FUNCS
from datadelta.clean.steps.base import (
    StepContext, change_examples, is_text_column, str_mask,
)
from datadelta.progress import RecordingProgress


# ── Fake steps ────────────────────────────────────────────────────────────────

def _fake_whitespace(df, ctx):
    out = df.copy()
    before = df["name"]
    after = before.str.strip()
    changed = (before != after) & before.notna()
    out["name"] = after
    return out, [CleanAction("whitespace", "name", "trimmed", int(changed.sum()),
                             examples=change_examples(before, after, changed))]


def _fake_numbers(df, ctx):
    out = df.copy()
    out["amount"] = pd.to_numeric(df["amount"]).astype("Int64")
    return out, [
        CleanAction("numbers", "amount", "parsed_number", len(df)),
        CleanAction("numbers", "code", "skipped", 1, note="leading zeros (codes)"),
    ]


def _fake_dedupe(df, ctx):
    out = df.drop_duplicates()                      # kept rows keep their labels, as in the real step
    return out, [CleanAction("dedupe", None, "dropped_rows", len(df) - len(out))]


def _noop(df, ctx):
    return df, []


@pytest.fixture
def fake_steps(monkeypatch):
    """Replace the registry with whitespace / numbers / dedupe fakes."""
    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    monkeypatch.setitem(STEP_FUNCS, "whitespace", _fake_whitespace)
    monkeypatch.setitem(STEP_FUNCS, "numbers", _fake_numbers)
    monkeypatch.setitem(STEP_FUNCS, "dedupe", _fake_dedupe)


@pytest.fixture
def frame():
    return pd.DataFrame({
        "name":   [" a", "b ", "c", "c", None],
        "amount": ["1", "2", "3", "3", "5"],
        "code":   ["01", "02", "03", "03", "05"],
    }, dtype=object)


# ── enabled_steps ─────────────────────────────────────────────────────────────

def test_step_order_is_the_spec_order():
    assert STEP_ORDER == ["headers", "whitespace", "null_tokens", "booleans", "numbers",
                          "dates", "case", "dedupe", "impute"]


def test_enabled_steps_follow_the_plan(monkeypatch):
    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    for name in STEP_ORDER:
        monkeypatch.setitem(STEP_FUNCS, name, _noop)

    assert enabled_steps(CleanPlan()) == ["headers", "whitespace", "null_tokens", "booleans",
                                          "numbers", "dates"]
    everything = CleanPlan(case={"lower": ["region"]}, dedupe=DedupeOptions("exact"),
                           impute={"revenue": "median"})
    assert enabled_steps(everything) == STEP_ORDER
    assert "impute" in enabled_steps(CleanPlan(impute_default="median"))
    assert "case" not in enabled_steps(CleanPlan(case={"lower": []}))

    nothing = CleanPlan(headers=False, whitespace=False, null_tokens=None, booleans=False,
                        numbers=None, dates=None)
    assert enabled_steps(nothing) == []


def test_enabled_steps_skip_unregistered_names(fake_steps):
    assert enabled_steps(CleanPlan()) == ["whitespace", "numbers"]
    assert enabled_steps(CleanPlan(dedupe=DedupeOptions("exact"))) == ["whitespace", "numbers", "dedupe"]


def test_steps_run_in_step_order(monkeypatch):
    calls = []

    def _recorder(name):
        def _step(df, ctx):
            calls.append(name)
            return df, []
        return _step

    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    monkeypatch.setitem(STEP_FUNCS, "alpha", _recorder("alpha"))
    monkeypatch.setitem(STEP_FUNCS, "beta", _recorder("beta"))
    monkeypatch.setattr(clean, "STEP_ORDER", ["beta", "alpha"])

    clean_frame(pd.DataFrame({"x": [1]}))
    assert calls == ["beta", "alpha"]


# ── clean_frame ───────────────────────────────────────────────────────────────

def test_clean_frame_never_modifies_its_input(fake_steps, frame):
    snapshot = frame.copy()
    result = clean_frame(frame)
    pd.testing.assert_frame_equal(frame, snapshot)
    assert result.df is not frame


def test_input_is_protected_even_from_a_mutating_step(monkeypatch, frame):
    def _bad_step(df, ctx):
        df["name"] = "overwritten"
        return df, []

    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    monkeypatch.setitem(STEP_FUNCS, "whitespace", _bad_step)
    snapshot = frame.copy()
    clean_frame(frame)
    pd.testing.assert_frame_equal(frame, snapshot)


def test_clean_frame_aggregates_actions_and_stats(fake_steps, frame):
    result = clean_frame(frame, CleanPlan(dedupe=DedupeOptions("exact"), key_column="code"))
    assert isinstance(result, CleanResult)

    assert [(a.step, a.column, a.action, a.count) for a in result.report.actions] == [
        ("whitespace", "name",   "trimmed",       2),
        ("numbers",    "amount", "parsed_number", 5),
        ("numbers",    "code",   "skipped",       1),
        ("dedupe",     None,     "dropped_rows",  1),
    ]
    assert result.report.actions[0].examples == [(" a", "a"), ("b ", "b")]

    stats = result.report.stats
    assert stats.rows_before == 5
    assert stats.rows_after == 4
    assert stats.cells_changed == 6                        # trimmed 2 + parsed 4: the parsed cell of the dropped row is gone
    assert stats.columns_retyped == {"amount": "number"}
    assert stats.nulls_before == {"name": 1, "amount": 0, "code": 0}
    assert stats.nulls_after == {"name": 1, "amount": 0, "code": 0}
    assert stats.duplicate_rows == 0                       # counted after cleaning
    assert stats.duplicate_keys == 0
    assert str(result.df["amount"].dtype) == "Int64"


def test_duplicate_stats_without_dedupe(fake_steps, frame):
    result = clean_frame(frame, CleanPlan(key_column="code"))
    assert result.report.stats.duplicate_rows == 1         # "c", 3, "03" appears twice
    assert result.report.stats.duplicate_keys == 1


def test_duplicate_keys_is_none_without_a_usable_key(fake_steps, frame):
    # `frame` has no id-like column (name is text, amount numeric, code not an id),
    # so nothing is auto-detected; an explicit key that does not exist stays None.
    assert clean_frame(frame).report.stats.duplicate_keys is None
    assert clean_frame(frame, CleanPlan(key_column="missing")).report.stats.duplicate_keys is None


def test_duplicate_keys_uses_an_auto_detected_key(monkeypatch):
    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    df = pd.DataFrame({
        "order_id": ["A1", "A2", "A3", "A1", None],
        "region":   ["EMEA", "APAC", "EMEA", "APAC", "LATAM"],
    }, dtype=object)

    stats = clean_frame(df).report.stats
    assert stats.duplicate_keys == 1                       # order_id detected as the key; nulls ignored
    assert stats.duplicate_rows == 0                       # the two A1 rows differ in region

    explicit = clean_frame(df, CleanPlan(key_column="region")).report.stats
    assert explicit.duplicate_keys == 2                    # an explicit key wins over detection


def test_nulls_after_reflect_the_cleaned_frame(monkeypatch):
    def _null_step(df, ctx):
        out = df.copy()
        out["x"] = df["x"].mask(df["x"] == "N/A")
        return out, [CleanAction("null_tokens", "x", "null_token", 2)]

    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    monkeypatch.setitem(STEP_FUNCS, "null_tokens", _null_step)
    df = pd.DataFrame({"x": ["N/A", "1", "N/A", None]}, dtype=object)
    stats = clean_frame(df).report.stats
    assert stats.nulls_before == {"x": 1}
    assert stats.nulls_after == {"x": 3}
    assert stats.cells_changed == 2


def test_no_registered_steps_returns_an_equal_copy(monkeypatch, frame):
    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    result = clean_frame(frame)
    pd.testing.assert_frame_equal(result.df, frame)
    assert result.report.actions == []
    assert result.report.stats.cells_changed == 0


def test_missing_exclude_column_is_reported_as_skipped(fake_steps, frame):
    result = clean_frame(frame, CleanPlan(exclude_columns=["code", "ghost"]))
    skipped = [a for a in result.report.actions if a.step == "plan"]
    assert [(a.column, a.action, a.count, a.note) for a in skipped] == [
        ("ghost", "skipped", 0, "exclude_columns: column not found"),
    ]


def test_duplicate_column_names_are_rejected():
    df = pd.DataFrame([[1, 2]], columns=["a", "a"])
    with pytest.raises(ValueError, match="duplicate column names: a"):
        clean_frame(df)


# ── Progress ──────────────────────────────────────────────────────────────────

def test_progress_is_one_stage_with_one_advance_per_step(fake_steps, frame):
    progress = RecordingProgress()
    clean_frame(frame, progress=progress)
    assert progress.stage_keys() == ["clean"]
    start = progress.events[0]
    assert (start.kind, start.data["label"], start.data["total"]) == ("start", "clean", 2)
    assert progress.advances("clean") == 2
    assert progress.advance_notes("clean") == ["whitespace", "numbers"]
    assert progress.end_status("clean") == "done"
    assert progress.end_summary("clean") == "7 cells standardized"


def test_progress_stage_key_and_label_are_configurable(fake_steps, frame):
    progress = RecordingProgress()
    clean_frame(frame, progress=progress, stage_key="clean.before", label="clean before")
    assert progress.stage_keys() == ["clean.before"]
    assert progress.events[0].data["label"] == "clean before"
    assert progress.end_status("clean.before") == "done"


def test_progress_summary_uses_thousands_separators(monkeypatch):
    def _big_step(df, ctx):
        out = df.copy()
        out["x"] = df["x"].str.strip()
        return out, [CleanAction("whitespace", "x", "trimmed", len(df))]

    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    monkeypatch.setitem(STEP_FUNCS, "whitespace", _big_step)
    progress = RecordingProgress()
    clean_frame(pd.DataFrame({"x": [" a"] * 1234}, dtype=object), progress=progress)
    assert progress.end_summary("clean") == "1,234 cells standardized"


def test_a_failing_step_ends_the_stage_as_failed(monkeypatch):
    def _boom(df, ctx):
        raise RuntimeError("boom")

    for name in list(STEP_FUNCS):
        monkeypatch.delitem(STEP_FUNCS, name)
    monkeypatch.setitem(STEP_FUNCS, "headers", _boom)
    progress = RecordingProgress()
    with pytest.raises(RuntimeError, match="boom"):
        clean_frame(pd.DataFrame({"x": [1]}), progress=progress)
    assert progress.end_status("clean") == "failed"
    assert progress.end_summary("clean") == "boom"


# ── CleanReport ───────────────────────────────────────────────────────────────

def _report() -> CleanReport:
    return CleanReport(
        actions=[
            CleanAction("numbers", "revenue", "parsed_number", 3,
                        examples=[("1,200", np.int64(1200)), ("N/A", float("nan"))],
                        note="currency symbol $ removed"),
            CleanAction("numbers", "zip", "skipped", 2, examples=[("01234", "01234")],
                        note="leading zeros (codes)"),
            CleanAction("plan", "ghost", "skipped", 0, note="exclude_columns: column not found"),
        ],
        stats=CleanStats(rows_before=10, rows_after=8, cells_changed=3,
                         columns_retyped={"revenue": "number"},
                         nulls_before={"revenue": 1}, nulls_after={"revenue": 1},
                         duplicate_rows=0, duplicate_keys=None),
    )


def test_action_to_dict_includes_jsonable_examples():
    assert _report().actions[0].to_dict() == {
        "step": "numbers", "column": "revenue", "action": "parsed_number", "count": 3,
        "note": "currency symbol $ removed",
        "examples": [["1,200", 1200], ["N/A", None]],
    }


def test_action_to_dict_can_omit_examples():
    out = _report().actions[0].to_dict(include_examples=False)
    assert "examples" not in out
    assert out["count"] == 3


def test_report_to_dict_shape():
    out = _report().to_dict(include_examples=False)
    assert set(out) == {"stats", "actions"}
    assert out["stats"] == {
        "rows_before": 10, "rows_after": 8, "cells_changed": 3,
        "columns_retyped": {"revenue": "number"},
        "nulls_before": {"revenue": 1}, "nulls_after": {"revenue": 1},
        "duplicate_rows": 0, "duplicate_keys": None,
    }
    assert [a["action"] for a in out["actions"]] == ["parsed_number", "skipped", "skipped"]
    assert all("examples" not in a for a in out["actions"])


def test_action_vocabulary_is_consistent():
    assert CELL_ACTIONS <= ACTIONS
    assert RETYPE_ACTIONS == {"parsed_number": "number", "parsed_date": "date", "parsed_bool": "boolean"}
    assert set(RETYPE_ACTIONS) <= CELL_ACTIONS
    assert {"skipped", "ambiguous", "dropped_rows", "header_trimmed"}.isdisjoint(CELL_ACTIONS)


def test_summary_counts_are_value_free():
    assert _report().summary_counts() == {
        "cells_changed":   3,
        "rows_dropped":    2,
        "columns_retyped": {"revenue": "number"},
        "actions":         2,          # the count-0 "plan" action is not counted
    }


# ── steps/base.py helpers ─────────────────────────────────────────────────────

def test_step_context_columns_respect_exclude_columns():
    ctx = StepContext(CleanPlan(exclude_columns=["b"]))
    assert ctx.columns(pd.DataFrame(columns=["a", "b", "c"])) == ["a", "c"]


@pytest.mark.parametrize("series, expected", [
    (pd.Series(["a", None], dtype=object),   True),
    (pd.Series(["a", None], dtype="string"), True),
    (pd.Series(["a", "b"]),                  True),       # "str" on pandas 3, object on pandas 2
    (pd.Series([1, 2]),                      False),
    (pd.Series([1.5, None]),                 False),
    (pd.Series([True, False]),               False),
    (pd.Series(pd.to_datetime(["2024-01-01"])), False),
])
def test_is_text_column(series, expected):
    assert is_text_column(series) is expected


def test_str_mask_marks_only_real_strings():
    s = pd.Series(["a", None, 3, float("nan"), "b"], dtype=object)
    assert str_mask(s).tolist() == [True, False, False, False, True]
    assert str_mask(pd.Series(["a", None], dtype="string")).tolist() == [True, False]
    assert str_mask(pd.Series([1, 2])).tolist() == [False, False]


def test_change_examples_are_distinct_positional_and_plain():
    before = pd.Series([" a", " a", "b ", "c", " d"], index=[7, 7, 8, 9, 9])
    after = pd.Series(["a", "a", "b", "c", "d"], index=[7, 7, 8, 9, 9])
    mask = pd.Series([True, True, True, False, True], index=[7, 7, 8, 9, 9])
    assert change_examples(before, after, mask) == [(" a", "a"), ("b ", "b"), (" d", "d")]
    assert change_examples(before, after, mask, limit=1) == [(" a", "a")]

    numbers = pd.Series(["1", "x"])
    parsed = pd.Series([1, None], dtype="Int64")
    examples = change_examples(numbers, parsed, pd.Series([True, True]))
    assert examples == [("1", 1), ("x", None)]
    assert type(examples[0][1]) is int


# ── cells_changed counts each cell of the cleaned table once (review minor) ───

def test_a_cell_changed_by_two_steps_counts_once():
    """Was 7 for these 6 cells: " 1" is trimmed and then parsed, one changed cell."""
    df = pd.DataFrame({"a": [" 1", " 2", " 3"], "b": [" x", "y", "z"]}, dtype=object)
    result = clean_frame(df)
    assert sorted((a.column, a.action, a.count) for a in result.report.actions) == [
        ("a", "parsed_number", 3), ("a", "trimmed", 3), ("b", "trimmed", 1),
    ]
    assert result.report.stats.cells_changed == 4


def test_cells_of_dropped_rows_are_not_cells_changed():
    """Row 1 duplicates row 0 and is dropped: its parsed and nulled cells are not in the result."""
    df = pd.DataFrame({"k": ["1", "1", "2"], "v": [" NA ", " NA ", "x"]}, dtype=object)
    result = clean_frame(df, CleanPlan(dedupe=DedupeOptions("exact")))
    assert result.df["k"].tolist() == [1, 2]
    assert result.report.stats.cells_changed == 3                     # k: 2 parsed, v: 1 trimmed then nulled


def test_clean_frame_keeps_the_input_index():
    df = pd.DataFrame({"k": ["1", "1", "2"], "v": ["a", "a", "b"]}, index=[10, 10, 30], dtype=object)
    result = clean_frame(df, CleanPlan(dedupe=DedupeOptions("exact")))
    assert list(result.df.index) == [10, 30]
    assert list(clean_frame(df).df.index) == [10, 10, 30]
