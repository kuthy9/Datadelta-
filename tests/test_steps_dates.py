"""
test_steps_dates.py — The dates step.

Covers the spec 3.2 / 3.5 cases: every accepted spelling, several
spellings mixed in one column, day/month order inferred from the data,
undecidable order (the "ambiguous" action and plan.dates.dayfirst),
contradicting order, time zones, and values lost under
min_parse_ratio < 1. Review Focus 5 (an impossible calendar date blocks
the column; datetime columns are never re-processed) is
test_invalid_calendar_date_leaves_the_column_as_text and
test_datetime_columns_are_not_reprocessed.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pytest

from datadelta.clean            import clean_frame
from datadelta.clean.plan       import CleanPlan, DateOptions
from datadelta.clean.steps      import STEP_FUNCS, dates
from datadelta.clean.steps.base import StepContext, is_text_column


def ctx(**plan_kwargs) -> StepContext:
    return StepContext(CleanPlan(**plan_kwargs))


def text(values) -> pd.DataFrame:
    return pd.DataFrame({"v": values}, dtype=object)


def ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value)


def test_step_is_registered():
    assert STEP_FUNCS["dates"] is dates.apply
    assert dates.NAME == "dates"


def test_candidate_formats_cover_the_spec_list():
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y年%m月%d日", "%d/%m/%Y", "%m/%d/%Y",
                "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "ISO8601"):
        assert fmt in dates.CANDIDATE_FORMATS


# ── Accepted spellings ────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("2024-01-05",               "2024-01-05"),
    ("2024-1-5",                 "2024-01-05"),
    ("2024/01/05",               "2024-01-05"),
    ("2024.01.05",               "2024-01-05"),
    ("2024年1月5日",              "2024-01-05"),
    ("2024-01-05 09:30",         "2024-01-05 09:30:00"),
    ("2024/01/05 09:30:15",      "2024-01-05 09:30:15"),
    ("2024年01月05日 18:00",      "2024-01-05 18:00:00"),
    ("2024-01-05T09:30",         "2024-01-05 09:30:00"),
    ("2024-01-05T09:30:15.250",  "2024-01-05 09:30:15.250"),
    ("25/12/2024",               "2024-12-25"),
    ("12/25/2024",               "2024-12-25"),
    ("25.12.2024 08:00",         "2024-12-25 08:00:00"),
])
def test_each_spelling_parses(raw, expected):
    out, actions = dates.apply(text([raw]), ctx())
    assert str(out["v"].dtype) == "datetime64[us]"
    assert out["v"].iloc[0] == ts(expected)
    assert [(a.action, a.count) for a in actions] == [("parsed_date", 1)]


def test_mixed_spellings_in_one_column():
    df = text(["2024-01-05", "2024/01/06", "2024年1月7日", "25/01/2024", None, "2024-01-09T10:00"])
    out, actions = dates.apply(df, ctx())
    assert out["v"].tolist()[:4] == [ts("2024-01-05"), ts("2024-01-06"), ts("2024-01-07"), ts("2024-01-25")]
    assert pd.isna(out["v"].iloc[4])
    assert out["v"].iloc[5] == ts("2024-01-09 10:00")
    assert len(actions) == 1
    action = actions[0]
    assert (action.step, action.column, action.action, action.count) == ("dates", "v", "parsed_date", 5)
    assert action.note == ("formats %Y-%m-%d, %Y/%m/%d, %Y年%m月%d日, %d/%m/%Y, ISO 8601; "
                           "day/month order inferred: day first")
    assert action.examples == [("2024-01-05", ts("2024-01-05")), ("2024/01/06", ts("2024-01-06")),
                               ("2024年1月7日", ts("2024-01-07"))]
    assert df["v"].tolist()[0] == "2024-01-05"                   # input untouched


def test_string_dtype_and_index_are_kept():
    df = pd.DataFrame({"v": pd.Series(["2024-01-05", None, "2024-01-07"], dtype="string", index=[10, 10, 30])})
    out, actions = dates.apply(df, ctx())
    assert list(out.index) == [10, 10, 30]
    assert out["v"].tolist()[0] == ts("2024-01-05")
    assert actions[0].count == 2


# ── Day/month order ───────────────────────────────────────────────────────────

def test_day_first_is_inferred_from_a_first_field_above_12():
    out, actions = dates.apply(text(["03/04/2024", "25/04/2024"]), ctx())
    assert out["v"].tolist() == [ts("2024-04-03"), ts("2024-04-25")]
    assert [a.action for a in actions] == ["parsed_date"]
    assert actions[0].note == "formats %d/%m/%Y; day/month order inferred: day first"


def test_month_first_is_inferred_from_a_second_field_above_12():
    out, actions = dates.apply(text(["03/04/2024", "04/25/2024"]), ctx())
    assert out["v"].tolist() == [ts("2024-03-04"), ts("2024-04-25")]
    assert actions[0].note == "formats %m/%d/%Y; day/month order inferred: month first"


def test_undecidable_order_uses_the_default_and_reports_ambiguous_values():
    out, actions = dates.apply(text(["03/04/2024", "04/04/2024", "05/06/2024"]), ctx())
    assert out["v"].tolist() == [ts("2024-03-04"), ts("2024-04-04"), ts("2024-05-06")]
    assert [(a.action, a.count) for a in actions] == [("parsed_date", 3), ("ambiguous", 2)]
    ambiguous = actions[1]
    assert ambiguous.note == ("day/month order cannot be told from the data; read as month/day "
                              "(cleaning.dates.dayfirst: false)")
    assert ambiguous.examples == [("03/04/2024", ts("2024-03-04")), ("05/06/2024", ts("2024-05-06"))]


def test_dayfirst_option_decides_undecidable_columns():
    out, actions = dates.apply(text(["03/04/2024", "05.06.2024"]), ctx(dates=DateOptions(dayfirst=True)))
    assert out["v"].tolist() == [ts("2024-04-03"), ts("2024-06-05")]
    assert actions[1].action == "ambiguous"
    assert actions[1].note.endswith("read as day/month (cleaning.dates.dayfirst: true)")


def test_inconsistent_day_month_order_is_skipped():
    df = text(["25/12/2024", "13/01/2024", "12/25/2024"])
    out, actions = dates.apply(df, ctx())
    assert out is df
    assert [(a.action, a.count, a.note, a.examples) for a in actions] == [
        ("skipped", 1, "inconsistent day/month order", [("12/25/2024", "12/25/2024")]),
    ]


# ── Lossless refusals ─────────────────────────────────────────────────────────

def test_invalid_calendar_date_leaves_the_column_as_text():
    """Review Focus 5: "2024-02-30" blocks the whole column and is listed."""
    df = pd.DataFrame({"order_date": ["2024-01-05", "2024-02-30", "2024-03-01", None]})
    snapshot = df.copy()
    result = clean_frame(df)

    column = result.df["order_date"]
    assert is_text_column(column)
    assert column.tolist()[:3] == ["2024-01-05", "2024-02-30", "2024-03-01"]
    assert result.report.stats.columns_retyped == {}

    date_actions = [a for a in result.report.actions if a.step == "dates"]
    assert [(a.column, a.action, a.count, a.examples) for a in date_actions] == [
        ("order_date", "skipped", 1, [("2024-02-30", "2024-02-30")]),
    ]
    assert date_actions[0].note == "1 of 3 values are not valid dates"
    assert "2024-02-30" not in date_actions[0].note                 # notes stay value-free
    pd.testing.assert_frame_equal(df, snapshot)


def test_datetime_columns_are_not_reprocessed():
    """Review Focus 5: typed columns keep their exact dtype and get no action."""
    df = pd.DataFrame({
        "naive": pd.to_datetime(["2024-01-05", None]).as_unit("ns"),
        "aware": pd.to_datetime(["2024-01-05 10:00", None]).tz_localize("Europe/Berlin"),
    })
    out, actions = dates.apply(df, ctx())
    assert out is df
    assert actions == []

    result = clean_frame(df)
    pd.testing.assert_frame_equal(result.df, df)
    assert str(result.df["naive"].dtype) == "datetime64[ns]"
    assert not [a for a in result.report.actions if a.step == "dates"]


def test_object_column_with_timestamps_is_left_alone():
    df = text([pd.Timestamp("2024-01-05"), "2024-01-06"])
    out, actions = dates.apply(df, ctx())
    assert out is df
    assert actions == []


def test_min_parse_ratio_below_one_coerces_bad_values_to_null():
    df = text(["2024-01-05", "2024-02-30", "2024-03-01", "2024-03-02"])
    out, actions = dates.apply(df, ctx(dates=DateOptions(min_parse_ratio=0.7)))
    assert str(out["v"].dtype) == "datetime64[us]"
    assert out["v"].isna().tolist() == [False, True, False, False]
    by_action = {a.action: a for a in actions}
    assert by_action["parsed_date"].count == 3
    coerced = by_action["coerced_to_null"]
    assert (coerced.count, coerced.examples) == (1, [("2024-02-30", None)])
    assert coerced.note == "1 of 4 values are not valid dates (min_parse_ratio 0.7)"


def test_ratio_not_met_even_when_lowered():
    df = text(["2024-01-05", "soon", "later", "2024-01-06"])
    out, actions = dates.apply(df, ctx(dates=DateOptions(min_parse_ratio=0.9)))
    assert out is df
    assert [(a.action, a.count, a.note) for a in actions] == [("skipped", 2, "2 of 4 values are not valid dates")]


@pytest.mark.parametrize("raw", [
    ["EMEA", "APAC", "2024-01-05"],          # mostly not dates
    ["2024", "2025", "2026"],                # years alone are not dates
    ["1/2/24", "3/4/24"],                    # two-digit years are not accepted
    ["10:30", "11:45"],                      # times alone are not dates
])
def test_plain_text_columns_are_left_alone_silently(raw):
    df = text(raw)
    out, actions = dates.apply(df, ctx())
    assert out is df
    assert actions == []


def test_typed_and_excluded_columns_are_never_touched():
    df = pd.DataFrame({
        "n":    [20240105, 20240106],
        "b":    [True, False],
        "keep": pd.Series(["2024-01-05", "2024-01-06"], dtype=object),
        "d":    pd.Series(["2024-01-05", "2024-01-06"], dtype=object),
    })
    out, actions = dates.apply(df, ctx(exclude_columns=["keep"]))
    assert out["n"].tolist() == [20240105, 20240106]
    assert out["keep"].tolist() == ["2024-01-05", "2024-01-06"]
    assert str(out["d"].dtype) == "datetime64[us]"
    assert [a.column for a in actions] == ["d"]


def test_dates_never_modify_the_input():
    df = text(["2024-01-05", "25/12/2024", "2024年1月7日"])
    snapshot = df.copy()
    dates.apply(df, ctx())
    pd.testing.assert_frame_equal(df, snapshot)


# ── Time zones ────────────────────────────────────────────────────────────────

def test_one_utc_offset_gives_a_tz_aware_column():
    out, _actions = dates.apply(text(["2024-01-05T10:00:00+02:00", "2024-01-06T11:30:00+0200"]), ctx())
    column = out["v"]
    assert column.dt.tz is not None
    assert column.iloc[0] == pd.Timestamp("2024-01-05T10:00:00+02:00")
    assert column.iloc[0].utcoffset() == timedelta(hours=2)
    assert column.iloc[1] == pd.Timestamp("2024-01-06T09:30:00Z")


def test_z_and_zero_offset_are_the_same_zone():
    out, _actions = dates.apply(text(["2024-01-05T10:00:00Z", "2024-01-05T12:00:00+00:00"]), ctx())
    assert str(out["v"].dtype) == "datetime64[us, UTC]"


@pytest.mark.parametrize("raw, odd", [
    (["2024-01-05T10:00:00+02:00", "2024-01-06T10:00:00+02:00", "2024-01-07T10:00:00-05:00"],
     "2024-01-07T10:00:00-05:00"),
    (["2024-01-05", "2024-01-06", "2024-01-07T10:00:00Z"],
     "2024-01-07T10:00:00Z"),
])
def test_mixed_offsets_are_skipped(raw, odd):
    df = text(raw)
    out, actions = dates.apply(df, ctx())
    assert out is df
    assert [(a.action, a.count, a.note, a.examples) for a in actions] == [
        ("skipped", 1, "mixed timezone offsets", [(odd, odd)]),
    ]


# ── Through clean_frame ───────────────────────────────────────────────────────

def test_dates_after_whitespace_and_null_tokens():
    df = pd.DataFrame({"shipped": [" 2024-01-05 ", "N/A", "2024/01/06", "-"]})
    result = clean_frame(df)
    column = result.df["shipped"]
    assert str(column.dtype) == "datetime64[us]"
    assert column.isna().tolist() == [False, True, False, True]
    assert result.report.stats.columns_retyped == {"shipped": "date"}
    assert [(a.step, a.action, a.count) for a in result.report.actions] == [
        ("whitespace",  "trimmed",     1),
        ("null_tokens", "null_token",  2),
        ("dates",       "parsed_date", 2),
    ]


# ── Sub-microsecond precision (correctness review I3) ─────────────────────────

def test_sub_microsecond_values_keep_the_column_as_text():
    """datetime64[us] would collapse .123456789 and .123456001 into one value."""
    raw = ["2024-01-05T09:30:15.123456789", "2024-01-05T09:30:15.123456001", "2024-01-06T10:00:00"]
    df = text(raw)
    out, actions = dates.apply(df, ctx())
    assert out is df
    assert [(a.step, a.action, a.count, a.note) for a in actions] == [
        ("dates", "skipped", 2, "dates: sub-microsecond precision"),
    ]
    assert [before for before, _after in actions[0].examples] == raw[:2]


@pytest.mark.parametrize("raw, expected", [
    ("2024-01-05T09:30:15.123456000", ts("2024-01-05 09:30:15.123456")),
    ("2024-01-05 09:30:15.120000000", ts("2024-01-05 09:30:15.12")),
    ("2024-01-05T09:30:15.1234567", None),
])
def test_zeros_beyond_microseconds_still_parse(raw, expected):
    df = text([raw, "2024-01-06T10:00:00"])
    out, actions = dates.apply(df, ctx())
    if expected is None:
        assert out is df and [a.action for a in actions] == ["skipped"]
        return
    assert out["v"].tolist()[0] == expected
    assert [a.action for a in actions] == ["parsed_date"]


def test_clean_frame_reports_sub_microsecond_columns_and_keeps_every_digit():
    df = pd.DataFrame({"t": ["2024-01-05T09:30:15.123456789", "2024-01-05T09:30:15.123456001"]})
    result = clean_frame(df)
    assert result.df["t"].tolist() == df["t"].tolist()
    assert result.report.stats.columns_retyped == {}
    assert [(a.step, a.action, a.note) for a in result.report.actions] == [
        ("dates", "skipped", "dates: sub-microsecond precision"),
    ]
