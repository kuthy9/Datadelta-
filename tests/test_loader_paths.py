"""
test_loader_paths.py — DuckDB reads take the path as data, never as SQL.

Security review I3: the CSV / JSON / Parquet readers built their query as
f"read_csv_auto('{path}')", so a single quote in a file name ended the
string literal and the rest of the name ran as SQL (a COPY ... TO wrote a
file, even under `clean --dry-run`), while a plain O'Brien.csv failed with
a parser traceback. Correctness review I2: DuckDB read errors (a CSV in
another encoding, a directory named like a file) escaped as tracebacks
instead of the one-line "Error loading data" that other load errors give.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from datadelta.clean.io import write_frame
from datadelta.loader   import load_file


INJECTED = "p'); COPY (SELECT 42 AS x) TO 'pwn.csv'; SELECT * FROM read_csv_auto('p.csv"


@pytest.mark.parametrize("lossless", [False, True])
@pytest.mark.parametrize("suffix", [".csv", ".json", ".parquet"])
def test_a_single_quote_in_the_file_name_is_read(tmp_path, suffix, lossless):
    df = pd.DataFrame({"name": ["O'Brien", "Smith"], "n": [1, 2]})
    path = tmp_path / f"O'Brien{suffix}"
    if suffix == ".csv":
        df.to_csv(path, index=False)
    elif suffix == ".json":
        df.to_json(path, orient="records", lines=True)
    else:
        write_frame(df, path)                                          # DuckDB's writer (no pyarrow)
    loaded = load_file(str(path), lossless=lossless)
    assert loaded["name"].tolist() == ["O'Brien", "Smith"]
    assert loaded["n"].tolist() == [1, 2]


def _crafted(folder) -> str:
    """The crafted file, plus the files "p" and "p.csv.csv" its first and last statements read."""
    for name in (f"{INJECTED}.csv", "p", "p.csv.csv"):
        (folder / name).write_text("a\n1\n", encoding="utf-8")
    return f"{INJECTED}.csv"


@pytest.mark.parametrize("lossless", [False, True])
def test_sql_in_a_file_name_is_never_run(isolated_cwd, lossless):
    loaded = load_file(_crafted(isolated_cwd), lossless=lossless)
    assert loaded["a"].tolist() == [1]
    assert not (isolated_cwd / "pwn.csv").exists()


def test_clean_dry_run_of_a_crafted_file_name_writes_nothing(cli, isolated_cwd):
    name   = _crafted(isolated_cwd)
    result = cli("clean", name, "--dry-run")
    assert result.exit_code == 0, result.stderr
    assert not (isolated_cwd / "pwn.csv").exists()
    assert sorted(p.name for p in isolated_cwd.iterdir()) == sorted([name, "p", "p.csv.csv"])


@pytest.fixture
def cp1252_csv(tmp_path):
    path = tmp_path / "latin.csv"
    path.write_bytes("name,n\ncaf\xe9,1\nna\xefve,2\n".encode("cp1252"))
    return path


@pytest.mark.parametrize("lossless", [False, True])
def test_duckdb_read_errors_are_one_line_value_errors(cp1252_csv, lossless):
    with pytest.raises(ValueError) as info:
        load_file(str(cp1252_csv), lossless=lossless)
    message = str(info.value)
    assert message.startswith(f"Failed to read {cp1252_csv}: ")
    assert "\n" not in message


def test_a_directory_named_like_a_csv_is_a_value_error(tmp_path):
    folder = tmp_path / "adir.csv"
    folder.mkdir()
    with pytest.raises(ValueError, match="Failed to read"):
        load_file(str(folder))


@pytest.mark.parametrize("command", [
    ["diff", "{bad}", "{good}"],
    ["diff", "{bad}", "{good}", "--clean", "--json"],
    ["clean", "{bad}", "--dry-run"],
])
def test_cli_prints_one_error_line_for_an_unreadable_csv(cli, write_csv, cp1252_csv, command):
    good = write_csv("good.csv", pd.DataFrame({"name": ["a"], "n": [1]}))
    result = cli(*[part.format(bad=cp1252_csv, good=good) for part in command])
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)                     # handled, not a traceback
    assert "Traceback" not in result.stderr
    with pytest.raises(ValueError) as info:
        load_file(str(cp1252_csv))
    [line] = [line for line in result.stderr.splitlines() if "Error loading data" in line]
    assert line.strip() == f"Error loading data: {info.value}"          # the whole message, unwrapped
    if "--json" in command:
        assert result.stdout == "" or json.loads(result.stdout)
