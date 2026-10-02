"""
test_loader_lossless.py — The cleaner's read (review C1).

`datadelta clean` and `diff --clean` load files with load_file(...,
lossless=True). The typed readers guess before the clean steps can see
the text: DuckDB sniffed "01/02/2024" as day-first and ignored
`dates: {dayfirst: false}`, rounded 20-digit ids to one float, and moved
"+02:00" timestamps into the machine's zone; pandas' Excel reader turned
the text "02134" into 2134. Each of those changed data while the report
said "nothing to change". Here the steps own every decision that needs
judgment, and a plain CSV still reads as typed, with nothing to report.
"""

from __future__ import annotations

import json
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from datadelta.loader import load_file


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def _xlsx(path: Path, rows: list[list]) -> Path:
    """A workbook whose cells keep exactly the Python types given (str stays a text cell)."""
    book = openpyxl.Workbook()
    for row in rows:
        book.active.append(row)
    book.save(path)
    return path


# ── load_file(lossless=True): CSV ─────────────────────────────────────────────

def test_plain_csv_columns_are_typed_on_reading(tmp_path):
    path = _write(tmp_path / "tidy.csv", (
        "id,amount,price,ratio,day,stamp,flag,qty\n"
        "1,10,1.50,2e-3,2024-01-05,2024-01-05 10:30:00,true,7\n"
        "2,-3,2.25,0.5,2024-01-06,2024-01-06T11:00:00.250,FALSE,\n"
        "3,0,0.30000000000000004,1,2024-02-29,2024-01-07 00:00:00,True,9\n"
    ))
    df = load_file(str(path), lossless=True)

    assert {col: str(dtype) for col, dtype in df.dtypes.items()} == {
        "id": "int64", "amount": "int64", "price": "float64", "ratio": "float64",
        "day": "datetime64[us]", "stamp": "datetime64[us]", "flag": "bool", "qty": "Int64",
    }
    assert df["price"].tolist() == [1.5, 2.25, 0.30000000000000004]
    assert df["stamp"].tolist()[1] == pd.Timestamp("2024-01-06 11:00:00.250")
    assert df["flag"].tolist() == [True, False, True]
    assert df["qty"].isna().tolist() == [False, True, False]


@pytest.mark.parametrize("values", [
    ["01/02/2024", "03/04/2024"],                          # day/month order is the dates step's call
    ["2024-01-05T10:00:00+02:00", "2024-01-06T10:00:00+02:00"],   # offsets are kept by the dates step
    ["02134", "10001"],                                    # leading zero: codes
    ["99999999999999999999", "1"],                         # beyond 64 bits
    ["0.12345678901234567890", "1"],                       # more digits than a float holds
    ["1e-400", "1"],                                       # underflows to 0.0
    ["$5", "6"], ["+5", "6"], [" 7", "8"], ["1,200", "3"],
    ["T", "F"],                                            # booleans step decides
    ["2024-01-05 10:30", "2024-01-06 11:00"],              # no seconds: the dates step reports it
    ["2024-02-30", "2024-03-01"],                          # not a calendar date
])
def test_values_that_need_a_decision_stay_text(tmp_path, values):
    path = _write(tmp_path / "t.csv", "v\n" + "\n".join(f'"{v}"' for v in values) + "\n")
    df = load_file(str(path), lossless=True)
    assert df["v"].tolist() == values


def test_ids_beyond_int64_stay_exact(tmp_path):
    path = _write(tmp_path / "ids.csv", "acct\n12345678901234567890\n12345678901234567891\n")
    df = load_file(str(path), lossless=True)
    assert df["acct"].tolist() == [12345678901234567890, 12345678901234567891]
    assert str(df["acct"].dtype) == "uint64"


def test_typed_read_is_unchanged_for_plain_diff(tmp_path):
    path = _write(tmp_path / "ids.csv", "acct\n12345678901234567890\n12345678901234567891\n")
    assert str(load_file(str(path))["acct"].dtype) == "float64"          # DuckDB's own typing


# ── load_file(lossless=True): JSON ────────────────────────────────────────────

@pytest.fixture
def events_json(tmp_path) -> Path:
    """DuckDB types JSON strings too: "+02:00" became naive UTC, 20-digit numbers one float."""
    return _write(tmp_path / "events.json", (
        '{"id": 1, "ts": "2024-01-05T10:00:00+02:00", "day": "2024-01-05", "big": 12345678901234567890, "n": {"a": 1}}\n'
        '{"id": 2, "ts": "2024-07-05T10:00:00+02:00", "day": "2024-01-06", "big": 12345678901234567891, "n": {"a": 2}}\n'
    ))


def test_json_strings_that_need_a_decision_stay_text(events_json):
    df = load_file(str(events_json), lossless=True)
    assert df["ts"].tolist() == ["2024-01-05T10:00:00+02:00", "2024-07-05T10:00:00+02:00"]
    assert str(df["day"].dtype) == "datetime64[us]"                    # plain ISO dates: typed as before
    assert df["big"].tolist() == [12345678901234567890, 12345678901234567891]
    assert df["id"].tolist() == [1, 2]
    assert df["n"].tolist() == [{"a": 1}, {"a": 2}]


@pytest.fixture
def lists_json(tmp_path) -> Path:
    """Arrays of date strings: DuckDB types them DATE[] / TIMESTAMP[], which are lists, not text."""
    return _write(tmp_path / "arr.json", (
        '{"id": 1, "tags": ["2024-01-01", "2024-02-01"], "seen": ["2024-01-05 10:30:00"], "day": "2024-01-05"}\n'
        '{"id": 2, "tags": ["2024-03-01"],               "seen": ["2024-01-06 10:30:00"], "day": "2024-01-06"}\n'
    ))


def test_json_list_of_dates_columns_stay_lists(lists_json):
    """Re-review NB2: "DATE[]".startswith("DATE") re-read the lists as JSON text."""
    df = load_file(str(lists_json), lossless=True)
    assert [[str(pd.Timestamp(v).date()) for v in cell] for cell in df["tags"]] == [["2024-01-01", "2024-02-01"], ["2024-03-01"]]
    assert all(not isinstance(cell, str) and len(cell) == 1 for cell in df["seen"])
    assert str(df["day"].dtype) == "datetime64[us]"                    # the scalar DATE column: as before


def test_clean_keeps_json_arrays_as_arrays(cli, lists_json, tmp_path):
    """Arrays stay arrays; DuckDB's list typing still writes the dates as ISO date-times (as before)."""
    out = tmp_path / "out.json"
    assert cli("clean", lists_json, "-o", out).exit_code == 0
    records = json.loads(out.read_text(encoding="utf-8"))
    assert [[v[:10] for v in r["tags"]] for r in records] == [["2024-01-01", "2024-02-01"], ["2024-03-01"]]
    assert all(isinstance(r["seen"], list) and len(r["seen"]) == 1 for r in records)


def test_clean_keeps_json_utc_offsets(cli, events_json, tmp_path):
    out = tmp_path / "out.json"
    assert cli("clean", events_json, "-o", out).exit_code == 0
    records = json.loads(out.read_text(encoding="utf-8"))
    assert [r["ts"] for r in records] == ["2024-01-05T10:00:00+02:00", "2024-07-05T10:00:00+02:00"]
    assert [r["big"] for r in records] == [12345678901234567890, 12345678901234567891]


# ── load_file(lossless=True): Excel ───────────────────────────────────────────

def test_excel_text_cells_stay_text(tmp_path):
    path = _xlsx(tmp_path / "z.xlsx", [
        ["code",  "region", "mixed", "n", "when"],
        ["02134", "NA",     10001,   1,   pd.Timestamp("2024-01-05").to_pydatetime()],
        ["10001", "EMEA",   "02134", 2,   None],
        ["00501", None,     5,       3,   pd.Timestamp("2024-01-07").to_pydatetime()],
    ])
    df = load_file(str(path), lossless=True)

    assert df["code"].tolist() == ["02134", "10001", "00501"]
    assert df["region"].tolist()[:2] == ["NA", "EMEA"]                  # "NA" is the null_tokens step's call
    assert df["region"].isna().tolist() == [False, False, True]
    assert df["mixed"].tolist() == [10001, "02134", 5]
    assert str(df["n"].dtype) == "int64"
    assert pd.api.types.is_datetime64_any_dtype(df["when"])

    typed = load_file(str(path))
    assert typed["code"].tolist() == [2134, 10001, 501]                 # what the typed reader does


# ── datadelta clean ───────────────────────────────────────────────────────────

@pytest.fixture
def monthly_us(tmp_path) -> Path:
    return _write(tmp_path / "monthly_us.csv", "month,sales\n01/01/2024,1\n02/01/2024,2\n03/01/2024,3\n04/01/2024,4\n")


def test_clean_reads_ambiguous_dates_month_first_and_counts_them(cli, monthly_us, tmp_path):
    out = tmp_path / "out.csv"
    result = cli("clean", monthly_us, "-o", out, "--json")
    assert result.exit_code == 0, result.stderr

    assert out.read_text(encoding="utf-8").splitlines() == [
        "month,sales", "2024-01-01,1", "2024-02-01,2", "2024-03-01,3", "2024-04-01,4",
    ]
    actions = {a["action"]: a for a in json.loads(result.stdout)["actions"]}
    assert actions["parsed_date"]["count"] == 4
    assert actions["ambiguous"]["count"] == 3                          # 01/01 reads the same both ways


def test_clean_honours_dayfirst(cli, monthly_us, tmp_path):
    config = _write(tmp_path / "rules.yaml", "cleaning:\n  dates: {dayfirst: true}\n")
    out = tmp_path / "out.csv"
    assert cli("clean", monthly_us, "-o", out, "--config", config).exit_code == 0
    assert out.read_text(encoding="utf-8").splitlines()[1:] == [
        "2024-01-01,1", "2024-01-02,2", "2024-01-03,3", "2024-01-04,4",
    ]


def test_clean_keeps_20_digit_ids(cli, tmp_path):
    source = _write(tmp_path / "zips.csv", "acct,n\n12345678901234567890,1\n12345678901234567891,2\n")
    out = tmp_path / "out.csv"
    assert cli("clean", source, "-o", out).exit_code == 0
    assert out.read_text(encoding="utf-8") == source.read_text(encoding="utf-8")


def test_clean_keeps_excel_text_codes(cli, tmp_path):
    source = _xlsx(tmp_path / "z.xlsx", [["zip"], ["02134"], ["10001"], ["00501"]])
    out = tmp_path / "z_out.csv"
    result = cli("clean", source, "-o", out, "--json")
    assert result.exit_code == 0, result.stderr
    assert out.read_text(encoding="utf-8").splitlines() == ["zip", "02134", "10001", "00501"]
    [skipped] = json.loads(result.stdout)["actions"]
    assert (skipped["action"], skipped["note"]) == ("skipped", "leading zeros (codes)")


def test_clean_keeps_utc_offsets(cli, tmp_path):
    source = _write(tmp_path / "tz.csv", "ts\n2024-01-05T10:00:00+02:00\n2024-07-05T10:00:00+02:00\n")
    out = tmp_path / "out.csv"
    assert cli("clean", source, "-o", out).exit_code == 0
    assert out.read_text(encoding="utf-8").splitlines() == ["ts", "2024-01-05T10:00:00+02:00", "2024-07-05T10:00:00+02:00"]


def test_clean_of_a_tidy_csv_has_nothing_to_report(cli, tmp_path):
    source = _write(tmp_path / "tidy.csv", "id,amount,day\n1,10.5,2024-01-05\n2,20.25,2024-01-06\n")
    result = cli("clean", source, "--dry-run", "--json")
    assert result.exit_code == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["actions"] == []
    assert report["stats"]["cells_changed"] == 0


# ── diff --clean: the same data as CSV and as Excel ───────────────────────────

def test_csv_vs_xlsx_with_offsets_codes_and_ambiguous_dates_match(cli, tmp_path, write_csv):
    df = pd.DataFrame({
        "id":    list(range(1, 41)),
        "ts":    [f"2024-01-{i % 28 + 1:02d}T10:00:00+02:00" for i in range(40)],
        "zip":   [f"{i * 37 % 1000:05d}" for i in range(40)],
        "month": [f"{i % 12 + 1:02d}/01/2024" for i in range(40)],
    })
    xlsx = tmp_path / "same.xlsx"
    df.to_excel(xlsx, index=False, engine="openpyxl")
    result = cli("diff", write_csv("same.csv", df), xlsx, "--clean", "--no-metrics", "--json")
    assert result.exit_code == 0, result.stderr

    payload = json.loads(result.stdout)
    assert [f["title"] for f in payload["findings"] if f["layer"] != "clean"] == []
    assert payload["cleaning"]["before"]["stats"]["columns_retyped"] == {"ts": "date", "month": "date"}
    assert payload["cleaning"]["after"]["stats"]["columns_retyped"] == {"ts": "date", "month": "date"}
