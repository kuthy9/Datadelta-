"""
test_duckdb_stdout.py — DuckDB never draws its progress bar on stdout.

DuckDB prints its own progress bar to file descriptor 1 once a query has
run longer than `progress_bar_time` (2 s by default): a multi-GB CSV, or
a modest one on a slow CI runner. stdout belongs to the report or the
JSON document (spec 2.2), so every DuckDB connection datadelta opens
must switch the bar off.

The fixture below makes every connection draw the bar after 1 ms, which
simulates a slow load on a small file; capfd sees what reaches fd 1.
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import pytest

from datadelta.clean.io import write_frame
from datadelta.loader   import load_file


ROWS = 300_000          # large enough that a query takes a few progress ticks


def _draw_early(con) -> None:
    con.execute("SET enable_progress_bar = true")
    con.execute("SET enable_progress_bar_print = true")
    con.execute("SET progress_bar_time = 1")          # ms; a SET statement finishes well inside it


@pytest.fixture
def eager_progress_bar(monkeypatch):
    """Every DuckDB connection (the default one included) draws its bar after 1 ms."""
    real_connect = duckdb.connect

    def connect(*args, **kwargs):
        con = real_connect(*args, **kwargs)
        _draw_early(con)
        return con

    monkeypatch.setattr(duckdb, "connect", connect)
    _draw_early(duckdb.default_connection())
    yield
    for setting in ("enable_progress_bar", "enable_progress_bar_print", "progress_bar_time"):
        duckdb.default_connection().execute(f"RESET {setting}")


@pytest.fixture
def big_frame() -> pd.DataFrame:
    rng = np.random.default_rng(3)
    return pd.DataFrame({"id": np.arange(ROWS), "value": rng.random(ROWS)})


def test_the_fixture_does_make_duckdb_print(eager_progress_bar, big_frame, tmp_path, capfd):
    """Guard: without the setting, DuckDB really writes to stdout (else the tests below prove nothing)."""
    path = tmp_path / "big.csv"
    big_frame.to_csv(path, index=False)
    duckdb.connect().sql(f"SELECT * FROM read_csv_auto('{path}')").df()
    assert "%" in capfd.readouterr().out


@pytest.mark.parametrize("lossless", [False, True], ids=["typed", "lossless"])
def test_loading_a_csv_prints_nothing_on_stdout(eager_progress_bar, big_frame, tmp_path, capfd, lossless):
    path = tmp_path / "big.csv"
    big_frame.to_csv(path, index=False)
    capfd.readouterr()

    df = load_file(str(path), lossless=True) if lossless else load_file(str(path))

    assert len(df) == ROWS
    assert capfd.readouterr().out == ""


@pytest.mark.parametrize("suffix", [".json", ".parquet"])
def test_loading_other_duckdb_formats_prints_nothing_on_stdout(eager_progress_bar, big_frame, tmp_path, capfd, suffix):
    path = tmp_path / f"big{suffix}"
    write_frame(big_frame, path)
    capfd.readouterr()

    assert len(load_file(str(path))) == ROWS
    assert capfd.readouterr().out == ""


def test_writing_parquet_prints_nothing_on_stdout(eager_progress_bar, big_frame, tmp_path, capfd):
    capfd.readouterr()
    write_frame(big_frame, tmp_path / "out.parquet")
    assert capfd.readouterr().out == ""
