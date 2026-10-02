"""
test_jsonutil.py — to_jsonable() / dumps() produce strictly valid JSON.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from datadelta.jsonutil import dumps, to_jsonable


def _strict_loads(text: str):
    """json.loads that rejects NaN / Infinity / -Infinity like jq does."""
    def _reject(token: str):
        raise ValueError(f"non-standard JSON constant: {token}")
    return json.loads(text, parse_constant=_reject)


def test_numpy_scalars_become_python_natives():
    out = to_jsonable({"i": np.int64(3), "f": np.float32(1.5), "b": np.bool_(True)})
    assert out == {"i": 3, "f": 1.5, "b": True}
    assert type(out["i"]) is int
    assert type(out["f"]) is float
    assert type(out["b"]) is bool


@pytest.mark.parametrize("value", [
    float("nan"), float("inf"), -math.inf, np.float64("nan"), np.float32("inf"),
])
def test_non_finite_floats_become_none(value):
    assert to_jsonable(value) is None


def test_missing_value_singletons_become_none():
    assert to_jsonable(pd.NaT) is None
    assert to_jsonable(pd.NA) is None
    assert to_jsonable(np.datetime64("NaT", "ns")) is None


def test_dates_become_iso_strings():
    assert to_jsonable(pd.Timestamp("2024-01-02 03:04:05")) == "2024-01-02T03:04:05"
    assert to_jsonable(datetime(2024, 1, 2, 3, 4)) == "2024-01-02T03:04:00"
    assert to_jsonable(date(2024, 1, 2)) == "2024-01-02"
    assert to_jsonable(np.datetime64("2024-01-02")) == "2024-01-02T00:00:00"


def test_containers_are_converted_recursively():
    out = to_jsonable({
        "tags":   {"b", "a", "c"},
        "pair":   (1, np.int64(2)),
        "nested": [{"x": float("nan")}],
        "arr":    np.array([1.0, np.nan]),
    })
    assert out == {
        "tags":   ["a", "b", "c"],
        "pair":   [1, 2],
        "nested": [{"x": None}],
        "arr":    [1.0, None],
    }


def test_numpy_dict_keys_are_converted():
    assert to_jsonable({np.int64(1): "a"}) == {1: "a"}


def test_dumps_writes_null_for_nan():
    assert dumps(float("nan")) == "null"
    text = dumps({"value": np.float64("nan"), "count": np.int64(4)})
    assert _strict_loads(text) == {"value": None, "count": 4}


def test_dumps_keeps_non_ascii_and_indents_by_two():
    assert dumps({"region": "地区"}) == '{\n  "region": "地区"\n}'
    assert dumps([1], indent=None) == "[1]"


# ── Types clean examples can carry (DuckDB TIME / INTERVAL, Postgres NUMERIC) ─

def test_times_become_iso_strings():
    from datetime import time
    assert to_jsonable(time(8, 30)) == "08:30:00"
    assert to_jsonable(time(8, 30, 5, 250)) == "08:30:05.000250"


@pytest.mark.parametrize("value", [
    __import__("datetime").timedelta(days=1, hours=2, seconds=3),
    pd.Timedelta(np.timedelta64(93603, "s")),   # keyword construction warns on pandas 2 + numpy 2.5
    np.timedelta64(93603, "s"),
])
def test_durations_become_iso_8601_durations(value):
    assert to_jsonable(value) == "P1DT2H0M3S"


def test_missing_durations_become_none():
    assert to_jsonable(np.timedelta64("NaT", "s")) is None


def test_decimals_keep_every_digit_as_text():
    from decimal import Decimal
    assert to_jsonable(Decimal("12345678901234567890.123")) == "12345678901234567890.123"


@pytest.mark.parametrize("value, expected", [
    (b"ab\x00", "b'ab\\x00'"),
    (pd.Interval(0, 1), "(0, 1]"),
])
def test_anything_else_becomes_its_text(value, expected):
    assert to_jsonable(value) == expected


def test_dumps_never_raises_on_a_clean_example():
    """`clean --json` dumps CleanAction examples, which hold whatever the cells held."""
    from datetime import time, timedelta
    from decimal  import Decimal
    examples = [[None, time(8)], [timedelta(minutes=5), pd.Timedelta(0)], [Decimal("1.10"), b"x"]]
    assert _strict_loads(dumps({"examples": examples})) == {
        "examples": [[None, "08:00:00"], ["P0DT0H5M0S", "P0DT0H0M0S"], ["1.10", "b'x'"]],
    }
