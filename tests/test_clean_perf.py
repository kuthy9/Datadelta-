"""
test_clean_perf.py — Review Focus 2: every clean step is vectorized, so
200k rows x 10 columns of messy strings clean in well under 15 seconds.

Marked slow; deselect with `pytest -m "not slow"`.
"""

from __future__ import annotations

import time

import numpy as np
import pandas as pd
import pytest

from datadelta.clean      import clean_frame
from datadelta.clean.plan import CleanPlan, DedupeOptions


ROWS = 200_000
LIMIT_SECONDS = 15.0


def _messy_frame() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    pools = {
        "amount":   [" $1,200.50", "N/A", "$3,000", "($45.10)", "$42 ", "$7"],
        "quantity": ["１２", "3", " 4", "-", "15"],
        "active":   ["yes", "No", " y", "N/A", "n"],
        "region":   ["EMEA ", "apac", "N/A", "LATAM", "na"],
        "discount": ["12.5%", "50%", "-", "3%"],
        "zip_code": ["00123", "04567", "12345"],
        "ordered":  ["2024-01-05", "2024/02/03", "N/A", "2024年3月4日", "25/12/2024"],
        "notes":    ["foo", "bar baz", " qux", "1.2.3"],
    }
    df = pd.DataFrame({
        name: np.array(pool, dtype=object)[rng.integers(0, len(pool), ROWS)]
        for name, pool in pools.items()
    })
    # Two columns where every value is distinct: no help from repetition.
    df["price"] = [f"${i:,}.{i % 100:02d}" for i in range(ROWS)]
    df["stamp"] = (pd.Timestamp("2020-01-01") + pd.to_timedelta(np.arange(ROWS), unit="min")).strftime("%d/%m/%Y %H:%M")
    return df


@pytest.mark.slow
@pytest.mark.parametrize("plan", [
    CleanPlan(),
    CleanPlan(case={"lower": ["region"]}, dedupe=DedupeOptions("exact"), impute_default="mode"),
], ids=["defaults", "every-step"])
def test_clean_frame_on_200k_messy_rows_is_fast(plan):
    df = _messy_frame()
    assert df.shape == (ROWS, 10)

    start = time.perf_counter()
    result = clean_frame(df, plan)
    elapsed = time.perf_counter() - start

    assert elapsed < LIMIT_SECONDS, f"clean_frame took {elapsed:.1f}s"
    dtypes = {col: str(dtype) for col, dtype in result.df.dtypes.items()}
    assert dtypes["amount"] == "Float64"
    assert dtypes["quantity"] == "Int64"
    assert dtypes["active"] == "boolean"
    assert dtypes["discount"] == "Float64"
    assert dtypes["price"] == "Float64"
    assert dtypes["ordered"] == "datetime64[us]"
    assert dtypes["stamp"] == "datetime64[us]"
    assert pd.api.types.is_object_dtype(result.df["zip_code"]) or pd.api.types.is_string_dtype(result.df["zip_code"])
