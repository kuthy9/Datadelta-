"""
test_steps_types.py — The booleans and numbers steps.

Covers the spec 3.5 cases: leading zeros, full-width digits, accounting
negatives, percent signs, currency symbols, pure 0/1 columns, null
tokens in front of numbers, and values lost under min_parse_ratio < 1.
Review Focus 1 (a mixed-junk column is never partially converted) is
test_mixed_junk_column_is_untouched_and_reported.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from datadelta.clean            import clean_frame
from datadelta.clean.plan       import CleanPlan, ParseOptions
from datadelta.clean.steps      import STEP_FUNCS, booleans, numbers
from datadelta.clean.steps.base import StepContext, is_text_column


def ctx(**plan_kwargs) -> StepContext:
    return StepContext(CleanPlan(**plan_kwargs))


def text(values) -> pd.DataFrame:
    return pd.DataFrame({"v": values}, dtype=object)


def test_steps_are_registered():
    assert STEP_FUNCS["booleans"] is booleans.apply
    assert STEP_FUNCS["numbers"] is numbers.apply
    assert (booleans.NAME, numbers.NAME) == ("booleans", "numbers")


# ── booleans ──────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    (["yes", "no", "Y", "n"],         [True, False, True, False]),
    (["TRUE", "false", " True "],     [True, False, True]),
    (["t", "F"],                      [True, False]),
    (["是", "否", "是"],               [True, False, True]),
    (["yes", "0", "1", "no"],         [True, False, True, False]),
    (["yes", None, "No"],             [True, None, False]),
])
def test_boolean_columns_are_converted(raw, expected):
    df = text(raw)
    out, actions = booleans.apply(df, ctx())
    assert str(out["v"].dtype) == "boolean"
    assert [None if pd.isna(v) else bool(v) for v in out["v"]] == expected
    assert len(actions) == 1
    action = actions[0]
    assert (action.step, action.column, action.action) == ("booleans", "v", "parsed_bool")
    assert action.count == sum(v is not None for v in expected)
    assert df["v"].tolist() == raw                        # input untouched


def test_boolean_examples_pair_text_with_bools():
    _out, actions = booleans.apply(text(["yes", "yes", "No"]), ctx())
    assert actions[0].examples == [("yes", True), ("No", False)]


@pytest.mark.parametrize("raw", [
    ["0", "1", "1"],                    # pure 0/1: left for the numbers step
    ["yes", "no", "maybe"],             # not every value is a token
    ["y", "n", 5],                      # mixes in a non-text object
    [None, None],                       # nothing to decide on
])
def test_non_boolean_text_columns_are_left_alone(raw):
    df = text(raw)
    out, actions = booleans.apply(df, ctx())
    assert out is df
    assert actions == []


def test_typed_columns_are_never_touched_by_booleans():
    df = pd.DataFrame({"i": [0, 1, 1], "b": [True, False, True], "f": [1.0, 0.0, 1.0]})
    out, actions = booleans.apply(df, ctx())
    assert out is df
    assert actions == []


def test_booleans_respect_exclude_columns():
    df = pd.DataFrame({"flag": ["yes", "no"], "keep": ["yes", "no"]}, dtype=object)
    out, actions = booleans.apply(df, ctx(exclude_columns=["keep"]))
    assert str(out["flag"].dtype) == "boolean"
    assert out["keep"].tolist() == ["yes", "no"]
    assert [a.column for a in actions] == ["flag"]


# ── numbers: accepted spellings ───────────────────────────────────────────────

@pytest.mark.parametrize("raw, value", [
    ("1,200",        1200),
    ("1 200",        1200),
    ("1 200",   1200),                # narrow no-break space as thousands separator
    ("1 200",   1200),                # thin space
    ("1,234,567",    1234567),
    ("１２３",        123),                 # full-width digits
    ("１，２００",     1200),                # full-width comma
    ("$1,200",       1200),
    ("1200$",        1200),
    ("€5",           5),
    ("£ 5",          5),
    ("¥5",           5),
    ("￥5",          5),                   # full-width yen sign
    ("(1,000)",      -1000),
    ("-5",           -5),
    ("+5",           5),
    ("−5",      -5),                  # Unicode minus sign
    ("$-5",          -5),
    ("-$5",          -5),
    ("0",            0),
])
def test_integers_parse_to_int64(raw, value):
    out, actions = numbers.apply(text([raw]), ctx())
    assert str(out["v"].dtype) == "Int64"
    assert out["v"].tolist() == [value]
    assert [a.action for a in actions] == ["parsed_number"]


@pytest.mark.parametrize("raw, value", [
    ("1,200.50",     1200.5),
    ("$1,200.50",    1200.5),
    ("(3.5)",        -3.5),
    ("($1,200.00)",  -1200.0),
    (".5",           0.5),
    ("0.25",         0.25),
    ("1e3",          1000.0),
    ("2.5E-1",       0.25),
    ("1200.",        1200.0),
])
def test_decimals_parse_to_float64(raw, value):
    out, _actions = numbers.apply(text([raw]), ctx())
    assert str(out["v"].dtype) == "Float64"
    assert out["v"].tolist() == [pytest.approx(value)]


def test_percent_column_is_divided_by_100():
    out, actions = numbers.apply(text(["12.5%", "50%", "100 %", "７％"]), ctx())
    assert str(out["v"].dtype) == "Float64"
    assert out["v"].tolist() == [pytest.approx(0.125), pytest.approx(0.5), pytest.approx(1.0), pytest.approx(0.07)]
    assert actions[0].note == "percent values divided by 100"


def test_currency_note_names_the_symbol():
    _out, actions = numbers.apply(text(["$5", "$1,200"]), ctx())
    assert actions[0].note == "currency symbol $ removed"
    assert actions[0].examples == [("$5", 5), ("$1,200", 1200)]


def test_nulls_stay_null_and_are_not_counted():
    out, actions = numbers.apply(text(["1", None, "3"]), ctx())
    assert out["v"].isna().tolist() == [False, True, False]
    assert actions[0].count == 2


def test_string_dtype_column_is_parsed():
    df = pd.DataFrame({"v": pd.Series(["1", "2", None], dtype="string")})
    out, actions = numbers.apply(df, ctx())
    assert str(out["v"].dtype) == "Int64"
    assert actions[0].count == 2


def test_mixed_object_column_with_real_numbers():
    out, actions = numbers.apply(text([1200, "1,300", None, 7]), ctx())
    assert str(out["v"].dtype) == "Int64"
    assert out["v"].tolist()[:2] == [1200, 1300]
    assert actions[0].count == 3


def test_mixed_object_column_with_floats_becomes_float64():
    out, _actions = numbers.apply(text([1.5, "2"]), ctx())
    assert str(out["v"].dtype) == "Float64"
    assert out["v"].tolist() == [1.5, 2.0]


@pytest.mark.parametrize("big", [
    12345678901234567,                  # the reproduced case: float64 would give ...568
    10 ** 15,                           # first 16-digit value: the text path refuses it too
    -(10 ** 15),
    np.int64(10 ** 15),
    2 ** 63 - 1,
])
def test_python_ints_beyond_15_digits_leave_the_column_untouched(big):
    """A float64 round trip would silently change the integer; the column stays as it is."""
    df = text([big, "1", "2"])
    out, actions = numbers.apply(df, ctx())
    assert out is df
    assert out["v"].tolist()[0] == big and out["v"].tolist()[1:] == ["1", "2"]
    assert [(a.step, a.column, a.action, a.count, a.note, a.examples) for a in actions] == [
        ("numbers", "v", "skipped", 1, "numbers: values beyond 15 significant digits", [(big, big)]),
    ]


def test_ints_below_the_limit_are_still_converted_exactly():
    out, actions = numbers.apply(text([10 ** 15 - 1, "1", None]), ctx())
    assert str(out["v"].dtype) == "Int64"
    assert out["v"].tolist()[:2] == [999999999999999, 1]
    assert [a.action for a in actions] == ["parsed_number"]


def test_huge_ints_are_reported_by_clean_frame_and_not_retyped():
    df = text([12345678901234567, "1", "2"])
    result = clean_frame(df)
    assert result.df["v"].tolist() == [12345678901234567, "1", "2"]
    assert result.report.stats.columns_retyped == {}
    assert [(a.step, a.action) for a in result.report.actions] == [("numbers", "skipped")]


def test_float_cells_are_exact_in_float64_so_large_floats_still_convert():
    out, actions = numbers.apply(text([1.5e20, "2"]), ctx())
    assert str(out["v"].dtype) == "Float64"
    assert out["v"].tolist() == [1.5e20, 2.0]
    assert [a.action for a in actions] == ["parsed_number"]


# ── numbers: object columns of number cells (no text left) ────────────────────
#
# The lossless Excel read keeps "N/A" as text; once null_tokens has nulled
# it, the column holds number cells and nulls only (re-review NB1).

@pytest.mark.parametrize("cells, dtype, expected", [
    ([101, None, 103, np.nan],          "Int64",   [101, None, 103, None]),
    ([100.5, None, 2, np.float64(3.0)], "Float64", [100.5, None, 2.0, 3.0]),
    ([np.int64(5), None, 7],            "Int64",   [5, None, 7]),
    ([12345678901234567, None, 1],      "Int64",   [12345678901234567, None, 1]),     # exact: never via float
    ([2 ** 63 + 5, None, 7],            "UInt64",  [2 ** 63 + 5, None, 7]),
    ([-(2 ** 63), 2 ** 63 - 1],         "Int64",   [-(2 ** 63), 2 ** 63 - 1]),
])
def test_object_column_of_number_cells_gets_a_numeric_dtype(cells, dtype, expected):
    df = text(cells)
    out, actions = numbers.apply(df, ctx())
    assert str(out["v"].dtype) == dtype
    assert [None if pd.isna(v) else v for v in out["v"].tolist()] == expected
    n = sum(v is not None for v in expected)
    assert [(a.step, a.column, a.action, a.count) for a in actions] == [("numbers", "v", "parsed_number", n)]
    assert df["v"].dtype == object                                     # input untouched


@pytest.mark.parametrize("cells, offending", [
    ([12345678901234567, 1.5, None], 12345678901234567),               # Float64 would round the int
    ([2 ** 64 + 1, 1, None],         2 ** 64 + 1),                     # no 64-bit integer holds it
    ([-1, 2 ** 63 + 5],              2 ** 63 + 5),                     # neither Int64 nor UInt64 holds both
])
def test_number_cells_no_dtype_holds_exactly_leave_the_column_alone(cells, offending):
    df = text(cells)
    out, actions = numbers.apply(df, ctx())
    assert out is df
    assert [(a.action, a.note) for a in actions] == [("skipped", "numbers: values beyond 15 significant digits")]
    assert offending in [before for before, _after in actions[0].examples]


@pytest.mark.parametrize("cells", [
    [True, False, None],                                               # the booleans step's
    [pd.Timestamp("2024-01-01"), None],                                # the dates step's
    [1, True, None],                                                   # mixed kinds: left alone
    [None, None],
])
def test_object_columns_of_other_cells_are_not_numbers(cells):
    df = text(cells)
    out, actions = numbers.apply(df, ctx())
    assert out is df
    assert actions == []


def test_clean_frame_types_number_cells_left_after_null_tokens():
    df = pd.DataFrame({"amount": pd.Series(["N/A", 101, 102.5, " null "], dtype=object)})
    result = clean_frame(df)
    assert str(result.df["amount"].dtype) == "Float64"
    assert result.report.stats.columns_retyped == {"amount": "number"}
    assert [(a.step, a.action, a.count) for a in result.report.actions] == [
        ("whitespace", "trimmed", 1), ("null_tokens", "null_token", 2), ("numbers", "parsed_number", 2),
    ]


def test_index_is_preserved():
    df = pd.DataFrame({"v": ["1", "2", "x"]}, index=[10, 10, 30], dtype=object)
    out, _actions = numbers.apply(df, ctx(numbers=ParseOptions(0.5)))
    assert list(out.index) == [10, 10, 30]
    assert out["v"].tolist()[:2] == [1, 2]


# ── numbers: lossless refusals ────────────────────────────────────────────────

def test_mixed_junk_column_is_untouched_and_reported():
    """Review Focus 1: "N/A" becomes null, "1.2.3" blocks the whole column."""
    df = pd.DataFrame({"amount": ["1,200", "N/A", "1.2.3", "$5"]})
    snapshot = df.copy()
    result = clean_frame(df)

    column = result.df["amount"]
    assert is_text_column(column)                                   # still text
    assert column.tolist()[0] == "1,200"
    assert column.isna().tolist() == [False, True, False, False]
    assert column.tolist()[2:] == ["1.2.3", "$5"]                   # no partial conversion
    assert result.report.stats.columns_retyped == {}

    number_actions = [a for a in result.report.actions if a.step == "numbers"]
    assert len(number_actions) == 1
    skipped = number_actions[0]
    assert (skipped.column, skipped.action, skipped.count) == ("amount", "skipped", 1)
    assert skipped.examples == [("1.2.3", "1.2.3")]
    assert skipped.note == "1 of 3 values are not numbers"
    assert "1.2.3" not in skipped.note                              # notes stay value-free
    pd.testing.assert_frame_equal(df, snapshot)


def test_min_parse_ratio_below_one_coerces_failures_to_null():
    df = pd.DataFrame({"amount": ["1,200", "N/A", "1.2.3", "$5"]})
    result = clean_frame(df, CleanPlan(numbers=ParseOptions(min_parse_ratio=0.6)))

    column = result.df["amount"]
    assert str(column.dtype) == "Int64"
    assert column.isna().tolist() == [False, True, True, False]
    assert [column.iloc[0], column.iloc[3]] == [1200, 5]
    assert result.report.stats.columns_retyped == {"amount": "number"}

    by_action = {a.action: a for a in result.report.actions if a.step == "numbers"}
    assert by_action["parsed_number"].count == 2
    coerced = by_action["coerced_to_null"]
    assert coerced.count == 1
    assert coerced.examples == [("1.2.3", None)]
    assert coerced.note == "1 of 3 values are not numbers (min_parse_ratio 0.6)"
    assert result.report.stats.nulls_after["amount"] == 2
    assert result.report.stats.cells_changed >= 4                   # null token + 2 parsed + 1 coerced


def test_ratio_not_met_even_when_lowered():
    df = text(["1", "2", "x", "y"])
    out, actions = numbers.apply(df, ctx(numbers=ParseOptions(min_parse_ratio=0.9)))
    assert out is df
    assert [(a.action, a.count) for a in actions] == [("skipped", 2)]


@pytest.mark.parametrize("raw, offending", [
    (["00123", "123", "4"],  [("00123", "00123")]),
    (["-012", "5"],          [("-012", "-012")]),
    (["$007", "$5"],         [("$007", "$007")]),
])
def test_leading_zeros_keep_the_column_as_codes(raw, offending):
    df = text(raw)
    out, actions = numbers.apply(df, ctx())
    assert out["v"].tolist() == raw
    assert [(a.action, a.note, a.examples) for a in actions] == [("skipped", "leading zeros (codes)", offending)]


def test_zero_point_values_are_not_codes():
    out, _actions = numbers.apply(text(["0.5", "0", "10"]), ctx())
    assert out["v"].tolist() == [0.5, 0.0, 10.0]


def test_mixed_percent_and_plain_numbers_are_skipped():
    df = text(["12%", "5", "7", "8%", "9"])
    out, actions = numbers.apply(df, ctx())
    assert out["v"].tolist() == ["12%", "5", "7", "8%", "9"]
    assert len(actions) == 1
    assert (actions[0].action, actions[0].count, actions[0].note) == ("skipped", 2, "mixed percent and plain numbers")
    assert actions[0].examples == [("12%", "12%"), ("8%", "8%")]


def test_mixed_currency_symbols_are_skipped():
    out, actions = numbers.apply(text(["$5", "€6", "$7"]), ctx())
    assert out["v"].tolist() == ["$5", "€6", "$7"]
    assert [(a.action, a.count, a.note, a.examples) for a in actions] == [
        ("skipped", 1, "mixed currency symbols", [("€6", "€6")]),
    ]


@pytest.mark.parametrize("bad", [
    "1.2.3",                 # two decimal points
    "1,2,3",                 # not 3-digit groups
    "0,123",                 # a thousands group never starts with 0
    "1,200 300",             # separators must be consistent
    "1,5",                   # decimal comma is ambiguous: left alone
    "(5",                    # unbalanced parenthesis
    "-(5)",                  # sign and parentheses together
    "--5",                   # two signs
    "$5€",                   # two currency symbols
    "5 USD",                 # currency words are not stripped
    "12345678901234567",     # more than 15 significant digits
    "1e999",                 # overflows float64
    "١٢٣",                   # Arabic-Indic digits are not folded to ASCII
])
def test_values_that_are_not_numbers_block_the_column(bad):
    df = text([bad, "1", "2"])
    out, actions = numbers.apply(df, ctx())
    assert out["v"].tolist() == [bad, "1", "2"]
    assert [(a.action, a.count, a.examples) for a in actions] == [("skipped", 1, [(bad, bad)])]


def test_plain_text_columns_are_left_alone_silently():
    df = pd.DataFrame({
        "region": ["EMEA", "APAC", "NA", "LATAM"],
        "sku":    ["ORD-1", "ORD-2", "ORD-3", "7"],
    }, dtype=object)
    out, actions = numbers.apply(df, ctx())
    assert out is df
    assert actions == []


def test_typed_columns_are_never_touched_by_numbers():
    df = pd.DataFrame({
        "f": [1.5, None],
        "i": pd.Series([1, None], dtype="Int64"),
        "d": pd.to_datetime(["2024-01-01", None]),
        "b": [True, False],
    })
    out, actions = numbers.apply(df, ctx())
    assert out is df
    assert actions == []


def test_object_column_with_dates_is_left_alone():
    df = text([pd.Timestamp("2024-01-01"), "1"])
    out, actions = numbers.apply(df, ctx())
    assert out is df
    assert actions == []


def test_numbers_respect_exclude_columns():
    df = pd.DataFrame({"zip": ["123", "456"], "qty": ["1", "2"]}, dtype=object)
    out, actions = numbers.apply(df, ctx(exclude_columns=["zip"]))
    assert out["zip"].tolist() == ["123", "456"]
    assert str(out["qty"].dtype) == "Int64"
    assert [a.column for a in actions] == ["qty"]


def test_numbers_never_modify_the_input():
    df = text(["1,200", "$5", "(3)"])
    snapshot = df.copy()
    numbers.apply(df, ctx())
    pd.testing.assert_frame_equal(df, snapshot)


# ── booleans + numbers through clean_frame ────────────────────────────────────

def test_zero_one_column_becomes_integers_and_yes_no_becomes_boolean():
    df = pd.DataFrame({
        "active": ["yes", "No", "N/A", "1"],
        "flag":   ["0", "1", "1", "0"],
        "price":  [" $1,200 ", "-", "（５）", "7"],
    })
    result = clean_frame(df, CleanPlan(dates=None))
    assert str(result.df["active"].dtype) == "boolean"
    assert str(result.df["flag"].dtype) == "Int64"
    assert str(result.df["price"].dtype) == "Int64"
    assert result.df["price"].tolist()[0] == 1200
    assert result.df["price"].iloc[2] == -5
    assert result.df["price"].isna().tolist() == [False, True, False, False]
    assert result.report.stats.columns_retyped == {"active": "boolean", "flag": "number", "price": "number"}


# ── numbers: only full-width forms are folded, never other digit look-alikes ──

@pytest.mark.parametrize("bad", [
    "5²",                    # superscript two: NFKC would read 52
    "10³",                   # NFKC: 103
    "①②",                    # circled digits: NFKC 12
    "𝟏𝟐",                    # mathematical bold digits: NFKC 12
    "½",                     # vulgar fraction: NFKC "1⁄2"
])
def test_digit_look_alikes_are_not_numbers(bad):
    out, actions = numbers.apply(text([bad, "1", "2"]), ctx())
    assert out["v"].tolist() == [bad, "1", "2"]
    assert [(a.action, a.count, a.examples) for a in actions] == [("skipped", 1, [(bad, bad)])]


def test_superscripts_never_change_a_value_silently():
    """Review: ['5²', '10³', '7'] used to become [52, 103, 7] as parsed_number."""
    result = clean_frame(text(["5²", "10³", "7"]))
    assert result.df["v"].tolist() == ["5²", "10³", "7"]
    assert result.report.stats.columns_retyped == {}


@pytest.mark.parametrize("raw, value", [
    ("（１，２００）",   -1200),              # full-width parentheses and comma
    ("－５",          -5),                 # full-width hyphen-minus
    ("＄５",          5),                  # full-width dollar sign
    ("￡５",          5),                  # full-width pound sign
    ("１　２００",  1200),               # ideographic space as thousands separator
    ("1 200",    1200),               # no-break space
])
def test_full_width_forms_and_spaces_are_folded(raw, value):
    out, actions = numbers.apply(text([raw]), ctx())
    assert out["v"].tolist() == [value]
    assert [a.action for a in actions] == ["parsed_number"]
