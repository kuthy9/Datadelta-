"""
test_cli_clean.py — The `datadelta clean` command.

stdout carries the report (or the JSON report with --json); "Saved ..."
and warnings go to stderr; config, load and write errors exit 1.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from datadelta.loader import load_file


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def messy(write_csv) -> Path:
    """8 rows: currency text, a null token, padded text, zip codes, 2 exact duplicates."""
    return write_csv("messy.csv", pd.DataFrame({
        "order_id": ["A1", "A2", "A3", "A4", "A5", "A6", "A1", "A2"],
        "amount":   ["$1,200.50", "N/A", "$30", "$4", "$5", "$6", "$1,200.50", "N/A"],
        "region":   [" EMEA", "APAC ", "EMEA", "LATAM", "APAC", "EMEA", " EMEA", "APAC "],
        "zip_code": ["N/A", "02134", "10001", "60601", "94105", "02139", "N/A", "02134"],
        "ordered":  ["2024-01-05", "2024/01/06", "25/01/2024", "2024-01-08", "2024-01-09",
                     "2024-01-10", "2024-01-05", "2024/01/06"],
    }))


# ── Writing ───────────────────────────────────────────────────────────────────

def test_writes_the_default_clean_csv_next_to_the_input(cli, messy):
    result = cli("clean", messy)
    assert result.exit_code == 0, result.stderr
    target = messy.with_name("messy.clean.csv")
    assert target.exists()
    assert "Saved" in result.stderr
    assert "messy.clean.csv" in result.stderr

    assert "parsed_number" in result.stdout
    assert "leading zeros (codes)" in result.stdout
    assert "Saved" not in result.stdout

    back = load_file(str(target))
    assert pd.api.types.is_numeric_dtype(back["amount"])
    assert back["amount"].iloc[0] == pytest.approx(1200.5)
    assert back["region"].tolist()[:2] == ["EMEA", "APAC"]
    assert len(back) == 8                                              # duplicates stay by default
    assert target.read_text(encoding="utf-8").splitlines()[1].endswith(",2024-01-05")


@pytest.mark.parametrize("suffix", [".parquet", ".xlsx", ".json"])
def test_output_format_follows_the_suffix(cli, messy, tmp_path, suffix):
    target = tmp_path / f"out{suffix}"
    result = cli("clean", messy, "-o", target)
    assert result.exit_code == 0, result.stderr
    back = load_file(str(target))
    assert len(back) == 8
    assert list(back.columns) == ["order_id", "amount", "region", "zip_code", "ordered"]
    assert pd.api.types.is_numeric_dtype(back["amount"])


def test_xlsx_keeps_codes_as_text_cells(cli, messy, tmp_path):
    target = tmp_path / "out.xlsx"
    assert cli("clean", messy, "-o", target).exit_code == 0
    sheet = openpyxl.load_workbook(target).active
    header = [cell.value for cell in sheet[1]]
    zip_cell = sheet.cell(row=3, column=header.index("zip_code") + 1)
    assert (zip_cell.value, zip_cell.data_type) == ("02134", "s")


def test_dry_run_writes_nothing(cli, messy):
    result = cli("clean", messy, "--dry-run")
    assert result.exit_code == 0, result.stderr
    assert not messy.with_name("messy.clean.csv").exists()
    assert "dry run · nothing written" in result.stdout
    assert "Saved" not in result.stderr


def test_json_report_on_stdout(cli, messy):
    result = cli("clean", messy, "--json", "--dry-run")
    assert result.exit_code == 0, result.stderr
    report = json.loads(result.stdout)
    assert set(report) == {"stats", "actions"}
    assert report["stats"]["rows_before"] == 8
    assert report["stats"]["duplicate_rows"] == 2
    assert report["stats"]["columns_retyped"] == {"amount": "number", "ordered": "date"}
    skipped = [a for a in report["actions"] if a["action"] == "skipped"]
    assert [(a["column"], a["note"]) for a in skipped] == [("zip_code", "leading zeros (codes)")]


def test_column_names_and_values_are_printed_literally(cli, write_csv):
    path = write_csv("markup.csv", pd.DataFrame({"[bold]x": [" a", "b"], "地区": [" 北", "南"]}))
    result = cli("clean", path, "--dry-run")
    assert result.exit_code == 0, result.stderr
    assert "[bold]x" in result.stdout
    assert "地区" in result.stdout
    assert "' a' → 'a'" in result.stdout


def test_quiet_hides_the_saved_line(cli, messy):
    result = cli("clean", messy, "--quiet")
    assert result.exit_code == 0
    assert messy.with_name("messy.clean.csv").exists()
    assert "Saved" not in result.stderr


# ── Flags and config ──────────────────────────────────────────────────────────

def test_dedupe_exact_and_impute_flags(cli, messy, tmp_path):
    target = tmp_path / "out.csv"
    result = cli("clean", messy, "-o", target, "--dedupe", "exact", "--impute", "median", "--json")
    assert result.exit_code == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["stats"]["rows_after"] == 6
    back = load_file(str(target))
    assert len(back) == 6
    assert back["amount"].isna().sum() == 0                            # median filled the N/A


def test_dedupe_key_uses_the_key_option(cli, messy):
    result = cli("clean", messy, "--dedupe", "key", "--key", "order_id", "--json", "--dry-run")
    assert result.exit_code == 0, result.stderr
    report = json.loads(result.stdout)
    dropped = [a for a in report["actions"] if a["action"] == "dropped_rows"]
    assert [(a["column"], a["count"]) for a in dropped] == [("order_id", 2)]
    assert report["stats"]["duplicate_keys"] == 0


def test_invalid_flag_value_is_a_usage_error(cli, messy):
    result = cli("clean", messy, "--dedupe", "fuzzy")
    assert result.exit_code == 2
    assert "'fuzzy' is not one of 'exact', 'key'" in result.stderr


def test_cleaning_section_of_metrics_yaml_is_auto_discovered(cli, messy, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  numbers: false\n", encoding="utf-8")
    result = cli("clean", messy, "--json", "--dry-run")
    assert result.exit_code == 0, result.stderr
    assert "amount" not in json.loads(result.stdout)["stats"]["columns_retyped"]


def test_explicit_config_file(cli, messy, tmp_path):
    config = tmp_path / "rules.yaml"
    config.write_text("cleaning:\n  case: {lower: [region]}\n", encoding="utf-8")
    result = cli("clean", messy, "--config", config, "--json", "--dry-run")
    assert result.exit_code == 0, result.stderr
    lower = [a for a in json.loads(result.stdout)["actions"] if a["action"] == "lowercased"]
    assert [(a["column"], a["count"]) for a in lower] == [("region", 8)]


def test_invalid_config_exits_1_with_the_key_path(cli, messy, tmp_path):
    config = tmp_path / "bad.yaml"
    config.write_text("cleaning:\n  numbers:\n    min_parse_ratio: 2\n", encoding="utf-8")
    result = cli("clean", messy, "--config", config)
    assert result.exit_code == 1
    assert "cleaning.numbers.min_parse_ratio" in result.stderr
    assert result.stdout == ""
    assert not messy.with_name("messy.clean.csv").exists()


def test_config_naming_a_missing_column_warns_and_continues(cli, messy, tmp_path):
    config = tmp_path / "rules.yaml"
    config.write_text("cleaning:\n  case: {lower: [regoin]}\n", encoding="utf-8")
    result = cli("clean", messy, "--config", config, "--dry-run")
    assert result.exit_code == 0, result.stderr
    assert "Warning" in result.stderr
    assert "'regoin'" in result.stderr
    assert "case.lower" in result.stderr


# ── Errors ────────────────────────────────────────────────────────────────────

def test_database_source_without_output_exits_1(cli, tmp_path):
    db = tmp_path / "shop.db"
    with sqlite3.connect(db) as conn:
        pd.DataFrame({"amount": ["$1", "$2"]}).to_sql("orders", conn, index=False)
    source = f"sqlite:///{db}::orders"

    result = cli("clean", source)
    assert result.exit_code == 1
    assert "-o" in result.stderr

    target = tmp_path / "orders.csv"
    result = cli("clean", source, "-o", target)
    assert result.exit_code == 0, result.stderr
    assert load_file(str(target))["amount"].tolist() == [1, 2]


def test_missing_input_exits_1(cli, tmp_path):
    result = cli("clean", tmp_path / "nope.csv")
    assert result.exit_code == 1
    assert "Error loading data" in result.stderr


@pytest.mark.parametrize("name", ["out.txt", "messy.csv"])
def test_bad_output_path_exits_1_before_loading(cli, messy, name):
    original = messy.read_text(encoding="utf-8")
    result = cli("clean", messy, "-o", messy.with_name(name))
    assert result.exit_code == 1
    assert "Error" in result.stderr
    assert messy.read_text(encoding="utf-8") == original               # never overwrites the input


def test_hard_link_to_the_input_is_refused(cli, messy, tmp_path):
    """A hard link has another name, so only samefile() sees that it is the input."""
    link = tmp_path / "hard.csv"
    try:
        os.link(messy, link)
    except (OSError, NotImplementedError):
        pytest.skip("hard links are not available on this filesystem")
    original = messy.read_bytes()

    result = cli("clean", messy, "-o", link)

    assert result.exit_code == 1
    assert "would overwrite the input" in result.stderr
    assert messy.read_bytes() == original and link.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["hard.csv", "messy.csv"]


def test_case_variant_of_the_input_is_refused(cli, write_csv, tmp_path):
    """On a case-insensitive filesystem (macOS, Windows) CASEIN.csv is casein.csv."""
    source = write_csv("casein.csv", pd.DataFrame({"x": [" a", "b "]}))
    if not (tmp_path / "CASEIN.csv").exists():
        pytest.skip("case-sensitive filesystem: CASEIN.csv is another file")
    original = source.read_bytes()

    result = cli("clean", source, "-o", tmp_path / "CASEIN.csv")

    assert result.exit_code == 1
    assert "would overwrite the input" in result.stderr
    assert source.read_bytes() == original


def test_symlink_to_the_input_is_refused(cli, messy, tmp_path):
    link = tmp_path / "alias.csv"
    try:
        link.symlink_to(messy)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available on this filesystem")
    original = messy.read_bytes()

    result = cli("clean", messy, "-o", link)

    assert result.exit_code == 1
    assert "would overwrite the input" in result.stderr
    assert messy.read_bytes() == original and link.is_symlink()


def test_another_existing_file_is_still_overwritten(cli, messy, tmp_path):
    other = tmp_path / "previous.csv"
    other.write_text("stale\n", encoding="utf-8")
    result = cli("clean", messy, "-o", other)
    assert result.exit_code == 0, result.stderr
    assert load_file(str(other)).shape[0] == 8


def test_unwritable_output_exits_1(cli, messy, tmp_path):
    result = cli("clean", messy, "-o", tmp_path / "missing_dir" / "out.csv")
    assert result.exit_code == 1
    assert "could not write" in result.stderr


@pytest.mark.parametrize("suffix", [".csv", ".json", ".xlsx", ".parquet"])
def test_write_failure_is_a_one_line_error_not_a_traceback(cli, messy, tmp_path, suffix):
    """DuckDB's IOException (parquet) used to escape as a raw traceback."""
    target = tmp_path / "nodir" / f"out{suffix}"
    result = cli("clean", messy, "-o", target)
    assert not isinstance(result.exception, Exception)              # typer.Exit, not a crash
    assert result.exit_code == 1
    assert result.stderr.splitlines()[-1].startswith("Error: could not write")    # after the stage lines
    assert f"out{suffix}" in result.stderr
    assert "Traceback" not in result.stderr + result.stdout
    assert "Saved" not in result.stderr
    assert not (tmp_path / "nodir").exists()


def test_xlsx_write_failure_keeps_the_previous_output(cli, write_csv, tmp_path):
    """A control character is rejected by openpyxl mid-save; the earlier good file must survive."""
    good = write_csv("good.csv", pd.DataFrame({"note": ["fine", "also fine"]}))
    target = tmp_path / "out.xlsx"
    assert cli("clean", good, "-o", target).exit_code == 0
    kept = target.read_bytes()
    before = sorted(p.name for p in tmp_path.iterdir())

    bad = write_csv("bad.csv", pd.DataFrame({"note": ["fine", "bell\x01char"]}))
    result = cli("clean", bad, "-o", target)

    assert not isinstance(result.exception, Exception)
    assert result.exit_code == 1
    assert result.stderr.splitlines()[-1].startswith("Error: could not write")    # after the stage lines
    assert "Saved" not in result.stderr
    assert target.read_bytes() == kept
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(before + ["bad.csv"])   # no temp file left


# ── Demo data ─────────────────────────────────────────────────────────────────

def test_messy_demo_data_exercises_every_default_step(cli, tmp_path):
    spec = importlib.util.spec_from_file_location("generate_demo_data", PROJECT_ROOT / "examples" / "generate_demo_data.py")
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    demo.messy_scenario(tmp_path)

    result = cli("clean", tmp_path / "messy_orders.csv", "--json", "--dry-run")
    assert result.exit_code == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["stats"]["rows_before"] == 305
    assert report["stats"]["duplicate_rows"] == 5
    assert report["stats"]["columns_retyped"] == {
        "paid": "boolean", "amount": "number", "quantity": "number", "order_date": "date",
    }
    actions = {(a["column"], a["action"]) for a in report["actions"]}
    assert {("region", "trimmed"), ("amount", "null_token"), ("zip_code", "skipped")} <= actions


def test_demo_regions_survive_the_default_null_tokens(cli, tmp_path):
    """Final review F2: the region code "NA" is a default null token, so --clean erased North America."""
    from datadelta.clean.plan import DEFAULT_NULL_TOKENS
    spec = importlib.util.spec_from_file_location("generate_demo_data", PROJECT_ROOT / "examples" / "generate_demo_data.py")
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    demo.main(tmp_path)

    for name in ("etl_before.csv", "etl_after.csv", "logistics_before.csv", "logistics_after.csv"):
        regions = set(pd.read_csv(tmp_path / name, dtype=str, keep_default_na=False)["region"])
        assert "NAM" in regions, name
        assert not {r.strip().casefold() for r in regions} & set(DEFAULT_NULL_TOKENS), name

    result = cli("diff", tmp_path / "etl_before.csv", tmp_path / "etl_before.xlsx", "--clean", "--json")
    assert result.exit_code == 0, result.stderr
    cleaning = json.loads(result.stdout)["cleaning"]
    assert [a for report in cleaning.values() for a in report["actions"] if a["column"] == "region"] == []


# ── Final review fixes ────────────────────────────────────────────────────────

def test_json_report_with_time_of_day_examples(cli, tmp_path):
    """--impute mode fills a TIME column; its examples hold datetime.time values (was: TypeError)."""
    import duckdb
    source = tmp_path / "times.parquet"
    duckdb.connect().execute(
        "COPY (SELECT * FROM (VALUES (TIME '08:00:00'), (NULL), (TIME '08:00:00'), (TIME '09:30:00')) v(t)) "
        f"TO '{source}' (FORMAT parquet)"
    )
    result = cli("clean", source, "--dry-run", "--impute", "mode", "--json")
    assert result.exit_code == 0, result.stderr
    [imputed] = [a for a in json.loads(result.stdout)["actions"] if a["action"] == "imputed"]
    assert imputed["examples"] == [[None, "08:00:00"]]


@pytest.mark.parametrize("kind", ["directory", "gbk"])
def test_unreadable_config_is_a_one_line_error(cli, messy, tmp_path, kind):
    if kind == "directory":
        config = tmp_path / "rules"
        config.mkdir()
    else:
        config = tmp_path / "rules.yaml"
        config.write_bytes("cleaning:\n  exclude_columns: [备注]\n".encode("gbk"))
    result = cli("clean", messy, "--config", config)
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)                  # handled, not a traceback
    assert result.stderr.startswith("Error: ")
    assert "rules" in result.stderr
