"""
test_steps_modify.py — The case, dedupe and impute steps.

These steps change or remove information, so they run only when the
plan asks for them; the last section checks that the default plan never
runs them while the stats still report duplicates and nulls.
"""

from __future__ import annotations

import pandas as pd
import pytest

from datadelta.clean            import clean_frame, enabled_steps
from datadelta.clean.plan       import CleanPlan, DedupeOptions
from datadelta.clean.steps      import STEP_FUNCS, case, dedupe, impute
from datadelta.clean.steps.base import StepContext


def ctx(**plan_kwargs) -> StepContext:
    return StepContext(CleanPlan(**plan_kwargs))


def test_steps_are_registered():
    assert STEP_FUNCS["case"] is case.apply
    assert STEP_FUNCS["dedupe"] is dedupe.apply
    assert STEP_FUNCS["impute"] is impute.apply
    assert (case.NAME, dedupe.NAME, impute.NAME) == ("case", "dedupe", "impute")


# ── case ──────────────────────────────────────────────────────────────────────

def test_case_changes_only_the_listed_columns():
    df = pd.DataFrame({
        "region": ["EMEA", "emea", "Apac", None],
        "status": ["Shipped", "PENDING", "shipped", "x"],
        "name":   ["ann lee", "BO KIM", "Cy", "dee"],
        "other":  ["KEEP", "Keep", "keep", "k"],
    }, dtype=object)
    plan = {"lower": ["region"], "upper": ["status"], "title": ["name"]}
    out, actions = case.apply(df, ctx(case=plan))

    assert out["region"].tolist() == ["emea", "emea", "apac", None]
    assert out["status"].tolist() == ["SHIPPED", "PENDING", "SHIPPED", "X"]
    assert out["name"].tolist() == ["Ann Lee", "Bo Kim", "Cy", "Dee"]
    assert out["other"].tolist() == ["KEEP", "Keep", "keep", "k"]
    assert [(a.step, a.column, a.action, a.count) for a in actions] == [
        ("case", "region", "lowercased", 2),
        ("case", "status", "uppercased", 3),
        ("case", "name",   "titlecased", 3),
    ]
    assert actions[0].examples == [("EMEA", "emea"), ("Apac", "apac")]
    assert df["region"].tolist() == ["EMEA", "emea", "Apac", None]   # input untouched


def test_case_skips_missing_and_non_text_columns():
    df = pd.DataFrame({"qty": [1, 2], "region": pd.Series(["A", "b"], dtype=object)})
    out, actions = case.apply(df, ctx(case={"lower": ["regoin", "region"], "upper": ["qty"]}))
    assert out["region"].tolist() == ["a", "b"]
    assert out["qty"].tolist() == [1, 2]
    assert [(a.column, a.action, a.count, a.note) for a in actions] == [
        ("regoin", "skipped",    0, "case.lower: column not found"),
        ("region", "lowercased", 1, ""),
        ("qty",    "skipped",    0, "case.upper: not a text column"),
    ]


def test_case_keeps_non_strings_and_string_dtype():
    mixed = pd.DataFrame({"v": ["ABC", 5, None]}, dtype=object)
    out, actions = case.apply(mixed, ctx(case={"lower": ["v"]}))
    assert out["v"].tolist() == ["abc", 5, None]
    assert type(out["v"].iloc[1]) is int
    assert actions[0].count == 1

    typed = pd.DataFrame({"v": pd.Series(["ABC", None], dtype="string")})
    out, _actions = case.apply(typed, ctx(case={"lower": ["v"]}))
    assert str(out["v"].dtype) == "string"
    assert out["v"].iloc[0] == "abc"


def test_case_respects_exclude_columns_and_returns_input_when_unchanged():
    df = pd.DataFrame({"region": ["EMEA"], "code": ["abc"]}, dtype=object)
    out, actions = case.apply(df, ctx(case={"lower": ["region", "code"]}, exclude_columns=["region"]))
    assert out is df
    assert actions == []


# ── dedupe ────────────────────────────────────────────────────────────────────

@pytest.fixture
def orders() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": [1, 2, 2, 3, 3, None, None],
        "amount":   [10.0, 20.0, 20.0, 30.0, 31.0, 5.0, 5.0],
    })


def test_exact_dedupe_drops_identical_rows_keeping_the_first(orders):
    out, actions = dedupe.apply(orders, ctx(dedupe=DedupeOptions("exact")))
    assert list(out.index) == [0, 1, 3, 4, 5]
    assert [(a.step, a.column, a.action, a.count) for a in actions] == [("dedupe", None, "dropped_rows", 2)]
    assert actions[0].note == "exact duplicate rows; kept the first of each"
    assert len(orders) == 7                                          # input untouched


def test_exact_dedupe_can_keep_the_last(orders):
    out, _actions = dedupe.apply(orders, ctx(dedupe=DedupeOptions("exact", keep="last")))
    assert list(out.index) == [0, 2, 3, 4, 6]


def test_key_dedupe_ignores_rows_without_a_key(orders):
    out, actions = dedupe.apply(orders, ctx(dedupe=DedupeOptions("key", keys=["order_id"])))
    assert list(out.index) == [0, 1, 3, 5, 6]                        # null keys are never duplicates
    assert [(a.column, a.action, a.count) for a in actions] == [("order_id", "dropped_rows", 2)]
    assert actions[0].note == "duplicate order_id; kept the first of each"
    assert actions[0].examples == [(2.0, None), (3.0, None)]


def test_key_dedupe_keep_last_and_several_keys():
    df = pd.DataFrame({"a": [1, 1, 1], "b": ["x", "x", "y"], "v": [1, 2, 3]})
    out, actions = dedupe.apply(df, ctx(dedupe=DedupeOptions("key", keys=["a", "b"], keep="last")))
    assert out["v"].tolist() == [2, 3]
    assert (actions[0].column, actions[0].count) == (None, 1)
    assert actions[0].examples == [([1, "x"], None)]
    assert type(actions[0].examples[0][0][0]) is int


def test_missing_key_column_skips_the_step(orders):
    out, actions = dedupe.apply(orders, ctx(dedupe=DedupeOptions("key", keys=["order_id", "sku"])))
    assert out is orders
    assert [(a.column, a.action, a.count, a.note) for a in actions] == [
        ("sku", "skipped", 0, "dedupe.keys: column not found"),
    ]


def test_dedupe_without_duplicates_returns_the_input():
    df = pd.DataFrame({"a": [1, 2]})
    out, actions = dedupe.apply(df, ctx(dedupe=DedupeOptions("exact")))
    assert out is df
    assert actions == []


def test_exact_dedupe_handles_unhashable_cells():
    df = pd.DataFrame({"payload": [{"a": 1}, {"a": 1}, {"a": 2}]})
    out, actions = dedupe.apply(df, ctx(dedupe=DedupeOptions("exact")))
    assert len(out) == 2
    assert actions[0].count == 1


# ── impute ────────────────────────────────────────────────────────────────────

def test_per_column_rules():
    df = pd.DataFrame({
        "revenue":  [10.0, None, 30.0, 40.0],
        "score":    [1.0, 2.0, None, 6.0],
        "region":   pd.Series(["EMEA", "APAC", "EMEA", None], dtype=object),
        "discount": [None, 0.5, None, 0.25],
        "tier":     pd.Series([None, "gold", None, "gold"], dtype=object),
    })
    rules = {"revenue": "median", "score": "mean", "region": "mode",
             "discount": {"constant": 0}, "tier": {"constant": 1}}
    out, actions = impute.apply(df, ctx(impute=rules))

    assert out["revenue"].tolist() == [10.0, 30.0, 30.0, 40.0]
    assert out["score"].tolist() == [1.0, 2.0, 3.0, 6.0]
    assert out["region"].tolist() == ["EMEA", "APAC", "EMEA", "EMEA"]
    assert out["discount"].tolist() == [0.0, 0.5, 0.0, 0.25]
    assert out["tier"].tolist() == ["1", "gold", "1", "gold"]          # text columns get text
    assert [(a.column, a.action, a.count, a.note) for a in actions] == [
        ("revenue",  "imputed", 1, "median"),
        ("score",    "imputed", 1, "mean"),
        ("region",   "imputed", 1, "mode"),
        ("discount", "imputed", 2, "constant"),
        ("tier",     "imputed", 2, "constant"),
    ]
    assert actions[0].examples == [(None, 30.0)]
    assert df["revenue"].isna().sum() == 1                             # input untouched


def test_integer_columns_keep_their_type():
    df = pd.DataFrame({"qty": pd.Series([1, 2, None], dtype="Int64")})
    out, actions = impute.apply(df, ctx(impute={"qty": "median"}))
    assert str(out["qty"].dtype) == "Int64"
    assert out["qty"].tolist() == [1, 2, 2]                            # median 1.5 rounds to 2
    assert actions[0].examples == [(None, 2)]


def test_drop_rows_runs_before_the_fills():
    df = pd.DataFrame({"amount": [1.0, None, 3.0, 100.0], "customer": ["a", "b", "c", None]})
    out, actions = impute.apply(df, ctx(impute={"amount": "median", "customer": "drop_rows"}))
    assert list(out.index) == [0, 1, 2]
    assert out["amount"].tolist() == [1.0, 2.0, 3.0]                   # median of the kept rows
    assert [(a.column, a.action, a.count) for a in actions] == [
        ("customer", "dropped_rows", 1),
        ("amount",   "imputed",      1),
    ]


@pytest.mark.parametrize("column, rule, note", [
    ("region", "median",          "impute: median needs a numeric column"),
    ("flag",   "mean",            "impute: mean needs a numeric column"),
    ("qty",    {"constant": 1.5}, "impute: the constant does not fit the column type"),
    ("flag",   {"constant": 1},   "impute: the constant does not fit the column type"),
    ("empty",  "mode",            "impute: column has no values"),
    ("ghost",  "median",          "impute: column not found"),
])
def test_rules_that_cannot_apply_are_skipped(column, rule, note):
    df = pd.DataFrame({
        "region": pd.Series(["EMEA", None], dtype=object),
        "flag":   pd.Series([True, None], dtype="boolean"),
        "qty":    pd.Series([1, None], dtype="Int64"),
        "empty":  pd.Series([None, None], dtype=object),
    })
    out, actions = impute.apply(df, ctx(impute={column: rule}))
    assert out is df
    assert [(a.column, a.action, a.count, a.note) for a in actions] == [(column, "skipped", 0, note)]


def test_impute_median_default_fills_numeric_columns_only():
    df = pd.DataFrame({
        "revenue": [10.0, None, 30.0],
        "units":   [1.0, 5.0, None],
        "region":  pd.Series(["EMEA", None, "APAC"], dtype=object),
    })
    out, actions = impute.apply(df, ctx(impute={"units": "mean"}, impute_default="median"))
    assert out["revenue"].tolist() == [10.0, 20.0, 30.0]
    assert out["units"].tolist() == [1.0, 5.0, 3.0]                    # the per-column rule wins
    assert out["region"].isna().tolist() == [False, True, False]       # text untouched by median
    assert [(a.column, a.note) for a in actions] == [("units", "mean"), ("revenue", "median (--impute)")]


def test_impute_mode_default_fills_every_column_but_excluded_ones():
    df = pd.DataFrame({
        "region": pd.Series(["EMEA", "EMEA", None], dtype=object),
        "units":  [2.0, None, 2.0],
        "notes":  pd.Series([None, "x", "x"], dtype=object),
    })
    out, actions = impute.apply(df, ctx(impute_default="mode", exclude_columns=["notes"]))
    assert out["region"].tolist() == ["EMEA", "EMEA", "EMEA"]
    assert out["units"].tolist() == [2.0, 2.0, 2.0]
    assert out["notes"].isna().tolist() == [True, False, False]
    assert [(a.column, a.note) for a in actions] == [("region", "mode (--impute)"), ("units", "mode (--impute)")]


def test_impute_without_nulls_returns_the_input():
    df = pd.DataFrame({"a": [1.0, 2.0]})
    out, actions = impute.apply(df, ctx(impute={"a": "median"}, impute_default="mode"))
    assert out is df
    assert actions == []


# ── Defaults and clean_frame ──────────────────────────────────────────────────

def test_default_plan_never_runs_modifying_steps_but_reports_stats():
    df = pd.DataFrame({
        "order_id": ["1", "2", "2", "3", "3"],
        "region":   ["EMEA", "emea", "emea", None, None],
        "amount":   ["10", "20", "20", None, None],
    })
    assert not {"case", "dedupe", "impute"} & set(enabled_steps(CleanPlan()))

    result = clean_frame(df, CleanPlan(key_column="order_id"))
    assert len(result.df) == 5                                          # duplicates kept
    assert result.df["region"].tolist()[:3] == ["EMEA", "emea", "emea"] # case kept
    assert result.df["amount"].isna().sum() == 2                        # nulls kept
    stats = result.report.stats
    assert stats.duplicate_rows == 2
    assert stats.duplicate_keys == 2
    assert stats.nulls_after == {"order_id": 0, "region": 2, "amount": 2}
    assert not {"case", "dedupe", "impute"} & {a.step for a in result.report.actions}


def test_modifying_steps_through_clean_frame():
    df = pd.DataFrame({
        "order_id": ["1", "2", "2", "3", "4"],
        "region":   ["EMEA", "emea", "emea", None, "APAC"],
        "amount":   ["10", "20", "20", None, "40"],
    })
    plan = CleanPlan(case={"lower": ["region"]}, dedupe=DedupeOptions("exact"),
                     impute={"amount": "median"}, impute_default="mode")
    result = clean_frame(df, plan)

    assert result.df["region"].tolist() == ["emea", "emea", "emea", "apac"]
    assert result.df["amount"].tolist() == [10, 20, 20, 40]
    stats = result.report.stats
    assert (stats.rows_before, stats.rows_after) == (5, 4)
    assert stats.nulls_after == {"order_id": 0, "region": 0, "amount": 0}
    assert [(a.step, a.column, a.action, a.count) for a in result.report.actions] == [
        ("numbers", "order_id", "parsed_number", 5),
        ("numbers", "amount",   "parsed_number", 4),
        ("case",    "region",   "lowercased",    2),
        ("dedupe",  None,       "dropped_rows",  1),
        ("impute",  "amount",   "imputed",       1),
        ("impute",  "region",   "imputed",       1),
    ]
    # Cells of the 4 kept rows: order_id 4 parsed, amount 3 parsed + 1 imputed,
    # region 2 lowercased + 1 imputed (the dropped duplicate row is not counted).
    assert stats.cells_changed == 4 + 4 + 3
    assert result.report.summary_counts()["rows_dropped"] == 1
