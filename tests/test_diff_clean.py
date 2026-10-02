"""
test_diff_clean.py — `datadelta diff --clean`.

Regression LPD-03 / E2E-08: the same data saved as CSV and as XLSX used
to differ by a false "Type changed" finding, because DuckDB types the CSV
date column while openpyxl leaves it as text. --clean runs the same
CleanPlan on both sides first, and align_dtypes() then gives a column
that holds one logical type in two storages (int64 / Int64, float64 /
Float64, bool / boolean, datetime64[ns] / [us]) the cleaner's dtype on
both sides, so the types agree. Boolean columns (the booleans step turns
Y/N into them) are categories, never numbers. The cleaning actions
appear as INFO findings in a "clean" layer that no scenario re-weights,
and the story payload carries only cleaning totals.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from datadelta.clean          import CleanPlan, align_dtypes, clean_frame
from datadelta.clean.findings import ACTION_LABELS, findings_from_report
from datadelta.clean.report   import ACTIONS, CleanAction, CleanReport, CleanStats
from datadelta.differ         import DiffResult, Finding
from datadelta.profiler       import profile_columns
from datadelta.scenarios      import SCENARIO_PROMOTIONS, apply_scenario_lens
from datadelta.story          import story_payload


def _layer(payload: dict, layer: str) -> list[dict]:
    return [f for f in payload["findings"] if f["layer"] == layer]


@pytest.fixture
def csv_and_xlsx(tmp_path, write_csv):
    """One DataFrame (int, float, date strings, category) saved as CSV and as XLSX."""
    df = pd.DataFrame({
        "order_id":   list(range(1, 41)),
        "revenue":    [100.5 + i for i in range(40)],
        "order_date": [f"2024-01-{(i % 28) + 1:02d}" for i in range(40)],
        "region":     [["North", "EMEA", "APAC", "LATAM"][i % 4] for i in range(40)],
    })
    xlsx = tmp_path / "orders.xlsx"
    df.to_excel(xlsx, index=False, engine="openpyxl")
    return write_csv("orders.csv", df), xlsx


@pytest.fixture
def messy_pair(write_csv):
    """before: 40 orders with currency text and padded regions; after: the first 30."""
    regions = [" EMEA", "APAC", "LATAM", "North"]
    before = pd.DataFrame({
        "order_id": list(range(1, 41)),
        "amount":   [f"${1000 + 25 * i:,}.50" for i in range(40)],
        "region":   [regions[i % 4] for i in range(40)],
    })
    after = before.iloc[:30]
    return write_csv("before.csv", before), write_csv("after.csv", after)


# ── Regression: CSV vs XLSX ───────────────────────────────────────────────────

def test_csv_vs_xlsx_reports_a_false_type_change_without_clean(cli, csv_and_xlsx):
    """Documents the bug --clean fixes."""
    csv, xlsx = csv_and_xlsx
    result = cli("diff", csv, xlsx, "--json")
    schema = _layer(json.loads(result.stdout), "schema")
    assert [f["title"].split(" (")[0] for f in schema] == ["Type changed: 'order_date'"]


def test_csv_vs_xlsx_has_no_type_change_with_clean(cli, csv_and_xlsx):
    csv, xlsx = csv_and_xlsx
    result = cli("diff", csv, xlsx, "--clean", "--json")
    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    assert _layer(payload, "schema") == []
    assert payload["summary"]["severity"] == "PASS"
    after_actions = payload["cleaning"]["after"]["actions"]
    assert [(a["column"], a["action"]) for a in after_actions] == [("order_date", "parsed_date")]


def test_csv_vs_xlsx_with_real_date_cells_has_no_type_change_with_clean(cli, tmp_path, write_csv):
    """Excel date cells: pandas 2 reads them as datetime64[ns], DuckDB's CSV column is datetime64[us]."""
    df = pd.DataFrame({
        "order_id":   list(range(1, 41)),
        "order_date": pd.to_datetime([f"2024-01-{(i % 28) + 1:02d}" for i in range(40)]),
    })
    xlsx = tmp_path / "dated.xlsx"
    df.to_excel(xlsx, index=False, engine="openpyxl")
    result = cli("diff", write_csv("dated.csv", df), xlsx, "--clean", "--json")
    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    assert _layer(payload, "schema") == []
    assert payload["summary"]["severity"] == "PASS"


# ── Same logical type, different storage (align_dtypes) ───────────────────────

def test_clean_reports_no_type_change_between_tidy_and_messy_numbers(cli, write_csv):
    """DuckDB types the tidy file (float64, int64); the cleaner types the messy one (Float64, Int64)."""
    tidy  = pd.DataFrame({"amount": [10.5, 20.0, 30.25, 40, 50], "qty": [1, 2, 3, 4, 5]})
    messy = pd.DataFrame({"amount": ["10.5", "N/A", "$30.25", "40", "50"], "qty": ["1", "2", "-", "4", "5"]})
    result = cli("diff", write_csv("tidy.csv", tidy), write_csv("messy.csv", messy), "--clean", "--json")
    payload = json.loads(result.stdout)
    assert [f["title"] for f in payload["findings"] if f["title"].startswith("Type changed")] == []
    assert _layer(payload, "schema") == []
    assert payload["cleaning"]["after"]["stats"]["columns_retyped"] == {"amount": "number", "qty": "number"}


_SAME_VALUES = {
    "int":      [1, 2],
    "float":    [1.5, 2.25],
    "bool":     [True, False],
    "datetime": ["2024-01-02", "2024-03-04"],
}


@pytest.mark.parametrize("kind, left, right, expected", [
    ("int",      "int64",          "Int64",          "Int64"),
    ("float",    "float64",        "Float64",        "Float64"),
    ("bool",     "bool",           "boolean",        "boolean"),
    ("datetime", "datetime64[ns]", "datetime64[us]", "datetime64[us]"),
])
def test_align_dtypes_gives_both_sides_the_cleaners_dtype(kind, left, right, expected):
    values = pd.Series(_SAME_VALUES[kind])
    if kind == "datetime":
        values = pd.to_datetime(values)
    before = pd.DataFrame({"c": values.astype(left)})
    after  = pd.DataFrame({"c": values.astype(right)})

    new_before, new_after = align_dtypes(before, after)
    assert str(new_before["c"].dtype) == expected
    assert str(new_after["c"].dtype) == expected
    assert new_before["c"].tolist() == new_after["c"].tolist() == values.tolist()
    assert str(before["c"].dtype) == left                              # inputs untouched
    assert str(after["c"].dtype) == right


def test_align_dtypes_leaves_real_type_changes_alone():
    dates  = pd.to_datetime(pd.Series(["2024-01-02", "2024-01-03"]))
    before = pd.DataFrame({"n": [1, 2],     "s": [1, 2],     "tz": dates,                     "gone": [1, 2]})
    after  = pd.DataFrame({"n": [1.5, 2.5], "s": ["a", "b"], "tz": dates.dt.tz_localize("UTC"), "new": [1, 2]})

    new_before, new_after = align_dtypes(before, after)
    assert dict(new_before.dtypes) == dict(before.dtypes)              # int vs float, number vs text,
    assert dict(new_after.dtypes) == dict(after.dtypes)                # naive vs tz-aware: real changes
    assert list(new_before.columns) == ["n", "s", "tz", "gone"]
    assert list(new_after.columns) == ["n", "s", "tz", "new"]


def test_align_dtypes_never_raises_on_unsigned_against_signed_integers():
    """Review minor (T13): uint64 above 2**63 vs int64 raised "cannot safely cast" out of diff --clean."""
    big    = [2 ** 63 + 5, 2 ** 63 + 6]
    before = pd.DataFrame({"acct": pd.Series(big, dtype="uint64"),     "n": pd.Series([1, 2], dtype="uint64")})
    after  = pd.DataFrame({"acct": pd.Series([1, -2], dtype="int64"),  "n": pd.Series([1, 2], dtype="int64")})

    new_before, new_after = align_dtypes(before, after)
    assert new_before["acct"].tolist() == big                          # no common integer type: left alone
    assert new_after["acct"].tolist() == [1, -2]
    assert str(new_before["n"].dtype) == str(new_after["n"].dtype)     # small values still align


def test_align_dtypes_uses_unsigned_storage_when_no_side_is_negative():
    big    = [2 ** 63 + 5, 7]
    before = pd.DataFrame({"acct": pd.Series(big, dtype="uint64")})
    after  = pd.DataFrame({"acct": pd.Series([7, 8], dtype="int64")})

    new_before, new_after = align_dtypes(before, after)
    assert str(new_before["acct"].dtype) == str(new_after["acct"].dtype) == "UInt64"
    assert new_before["acct"].tolist() == big


def test_clean_diff_of_unsigned_xlsx_against_signed_csv_is_not_a_crash(cli, tmp_path, write_csv):
    """read_excel gives uint64 for ids above 2**63; the CSV side holds int64 ids."""
    xlsx = tmp_path / "u_before.xlsx"
    pd.DataFrame({"acct": [2 ** 63 + i for i in range(20)], "v": range(20)}).to_excel(xlsx, index=False)
    csv  = write_csv("u_after.csv", pd.DataFrame({"acct": [-i for i in range(20)], "v": range(20)}))
    result = cli("diff", xlsx, csv, "--clean", "--no-metrics", "--json")
    assert result.exit_code in (0, 1), result.stderr
    assert isinstance(result.exception, SystemExit) or result.exception is None
    json.loads(result.stdout)


# ── A column that converts on one side only (review C2) ──────────────────────

@pytest.fixture
def one_sided_pair(write_csv):
    """price is "$1,000"-style text on both sides; after also holds one "TBD", which blocks parsing."""
    prices = [f"${(i % 8 + 1) * 1000:,}" for i in range(100)]
    before = pd.DataFrame({"id": range(100), "price": prices})
    after  = pd.DataFrame({"id": range(100), "price": prices[:99] + ["TBD"]})
    return write_csv("asym_before.csv", before), write_csv("asym_after.csv", after)


def test_plain_diff_of_a_one_sided_value_change_is_not_a_crash(cli, one_sided_pair):
    before, after = one_sided_pair
    result = cli("diff", before, after, "--no-metrics", "--json")
    assert result.exit_code == 0, result.stderr
    titles = [f["title"] for f in json.loads(result.stdout)["findings"]]
    assert "New categories in 'price': ['TBD']" in titles


@pytest.mark.parametrize("mode", [[], ["--json"]], ids=["terminal", "json"])
def test_clean_keeps_a_column_as_text_when_the_other_side_cannot_convert(cli, one_sided_pair, mode):
    """Was: before became Int64, after stayed text, and _numeric_diff raised TypeError (no report)."""
    before, after = one_sided_pair
    result = cli("diff", before, after, "--clean", "--no-metrics", *mode)
    assert result.exit_code == 0, result.stderr
    assert isinstance(result.exception, SystemExit) or result.exception is None
    if not mode:
        assert "New categories in 'price': ['TBD']" in result.stdout
        return

    payload  = json.loads(result.stdout)
    titles   = [f["title"] for f in payload["findings"]]
    cleaning = payload["cleaning"]
    assert _layer(payload, "schema") == []                             # one plan, one type: both text
    assert "New categories in 'price': ['TBD']" in titles
    assert cleaning["before"]["stats"]["columns_retyped"] == {}
    [kept] = [a for a in cleaning["before"]["actions"] if a["column"] == "price" and a["action"] == "skipped"]
    assert kept["step"] == "numbers"
    assert kept["count"] == 100
    assert kept["note"] == "kept as text: the other side has values that do not parse"
    assert "before · price · skipped: kept as text: the other side has values that do not parse" in titles
    assert [a["action"] for a in cleaning["after"]["actions"] if a["column"] == "price"] == ["skipped"]


def test_clean_still_aligns_a_column_the_other_reader_already_typed(cli, tmp_path, write_csv):
    """One side parsed by the cleaner, the other typed on load: the same logical type, not a revert."""
    tidy  = pd.DataFrame({"id": range(40), "amount": [1000.0 + i for i in range(40)]})
    messy = pd.DataFrame({"id": range(40), "amount": [f"${1000 + i:,}.00" for i in range(40)]})
    result = cli("diff", write_csv("tidy.csv", tidy), write_csv("messy.csv", messy), "--clean", "--no-metrics", "--json")
    payload = json.loads(result.stdout)
    assert _layer(payload, "schema") == []
    assert payload["cleaning"]["after"]["stats"]["columns_retyped"] == {"amount": "number"}


@pytest.mark.parametrize("before, after", [
    (pd.Series([1, 2, 3, 4, 5, 6] * 5, dtype="Int64"), pd.Series(["1", "2", "3", "4", "5", "TBD"] * 5, dtype=object)),
    (pd.Series([1.5, 2.5] * 15),                       pd.Series([1.5, "n/a"] * 15, dtype=object)),
    (pd.Series([True, False] * 15),                    pd.Series(["yes", "no"] * 15, dtype=object)),
])
def test_distribution_layer_skips_type_specific_checks_across_kinds(before, after):
    """The schema layer reports the type change; numbers and categories of two kinds are not compared."""
    from datadelta.differ import compute_diff
    df_before, df_after = pd.DataFrame({"c": before}), pd.DataFrame({"c": after})
    result = compute_diff(df_before, df_after, profile_columns(df_before, df_after), key_column=None)
    layers = [(f.layer, f.title.split(":")[0]) for f in result.findings]
    assert ("schema", "Type changed") in layers
    assert [f for f in result.findings if f.layer == "distribution"] == []


def test_object_columns_of_one_kind_are_still_compared():
    """Excel gives object for booleans or integers with blanks: same kind, so the checks still run."""
    from datadelta.differ import compute_diff
    df_before = pd.DataFrame({"flag": pd.Series([True] * 80 + [False] * 20, dtype="boolean")})
    df_after  = pd.DataFrame({"flag": pd.Series([True] * 20 + [False] * 79 + [None], dtype=object)})
    result = compute_diff(df_before, df_after, profile_columns(df_before, df_after), key_column=None)
    assert any(f.layer == "distribution" and "share" in f.title for f in result.findings)


# ── Excel null-token text in a number column (re-review NB1) ─────────────────
#
# The lossless Excel read keeps the text "N/A"; null_tokens nulls it, which
# left an object column of numbers and None. The numbers step skipped it (no
# text), the profiler sniffed the numbers as epoch dates, and the numeric
# checks never ran: FAIL findings vanished (exit 1 → 0). In mixed-format
# diffs the CSV side's parse was also reverted ("kept as text").

def _na_orders(shift: float = 0.0, scale: int = 1) -> pd.DataFrame:
    """70 orders; every 7th amount and qty cell is the text "N/A"."""
    return pd.DataFrame({
        "id":     range(1, 71),
        "amount": ["N/A" if i % 7 == 0 else 100.5 + i + shift for i in range(70)],
        "qty":    ["N/A" if i % 7 == 3 else (i % 10 + 1) * scale for i in range(70)],
    })


def _xlsx(tmp_path, name: str, df: pd.DataFrame):
    path = tmp_path / name
    df.to_excel(path, index=False, engine="openpyxl")
    return path


def _mean_shift_fails(payload: dict) -> list[str]:
    return sorted(f["column"] for f in payload["findings"]
                  if f["severity"] == "FAIL" and f["title"].startswith("Mean shifted"))


def test_clean_diff_of_xlsx_with_null_token_text_keeps_the_mean_shift_fail(cli, tmp_path):
    before = _xlsx(tmp_path, "x_before.xlsx", _na_orders())
    after  = _xlsx(tmp_path, "x_after.xlsx",  _na_orders(shift=500, scale=3))
    result = cli("diff", before, after, "--clean", "--no-metrics", "--json")
    assert result.exit_code == 1, result.stderr
    payload = json.loads(result.stdout)
    assert _mean_shift_fails(payload) == ["amount", "qty"]
    assert not [f for f in payload["findings"] if f["title"].startswith("Date range changed")]
    assert payload["cleaning"]["before"]["stats"]["columns_retyped"] == {"amount": "number", "qty": "number"}


def test_clean_diff_of_the_same_data_as_csv_and_xlsx_has_no_type_change(cli, tmp_path, write_csv):
    df = _na_orders()
    result = cli("diff", write_csv("na.csv", df), _xlsx(tmp_path, "na.xlsx", df), "--clean", "--no-metrics", "--json")
    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    assert _layer(payload, "schema") == []
    assert [f["title"] for f in _layer(payload, "clean") if "kept as text" in f["title"]] == []
    assert payload["summary"]["severity"] == "PASS"


def test_clean_diff_of_a_plain_csv_against_xlsx_with_null_token_text_keeps_the_fail(cli, tmp_path, write_csv):
    plain = _na_orders().replace("N/A", None).astype({"amount": "float64", "qty": "Int64"})
    plain["amount"] = plain["amount"].fillna(1.5)
    plain["qty"]    = plain["qty"].fillna(1)
    before = write_csv("x_before_plain.csv", plain)
    after  = _xlsx(tmp_path, "x_after.xlsx", _na_orders(shift=500, scale=3))
    result = cli("diff", before, after, "--clean", "--no-metrics", "--json")
    assert result.exit_code == 1, result.stderr
    payload = json.loads(result.stdout)
    assert _mean_shift_fails(payload) == ["amount", "qty"]
    assert [f["title"] for f in _layer(payload, "schema")] == []


def test_profiler_never_reads_number_or_boolean_cells_as_dates():
    """pd.to_datetime takes numbers for epoch offsets: an object column of numbers is not a date column."""
    df = pd.DataFrame({
        "ints":   pd.Series([101, None, 103, 104] * 10, dtype=object),
        "floats": pd.Series([100.5, 101.5, None, 3.0] * 10, dtype=object),
        "bools":  pd.Series([True, False, None, True] * 10, dtype=object),
        "dashes": pd.Series([101, 102, 103, 104, "-"] * 10, dtype=object),
    })
    profile = profile_columns(df, df)
    assert {col: profile.get(col).semantic_type for col in df.columns if profile.get(col).semantic_type == "datetime"} == {}


def test_clean_both_keeps_a_parse_when_the_other_side_has_no_text_cell():
    """An all-null column on the other side holds no value that fails to parse: no revert."""
    from datadelta.clean import KEPT_AS_TEXT_NOTE, clean_both
    before = pd.DataFrame({"amount": ["$1", "$2", "$3"]})
    after  = pd.DataFrame({"amount": pd.Series([None, None, None], dtype=object)})
    cleaned_before, cleaned_after = clean_both(before, after)
    assert str(cleaned_before.df["amount"].dtype) == "Int64"
    assert cleaned_before.report.stats.columns_retyped == {"amount": "number"}
    assert [a for a in cleaned_before.report.actions if a.note == KEPT_AS_TEXT_NOTE] == []


# ── Boolean columns are categories ────────────────────────────────────────────

@pytest.mark.parametrize("dtype", ["bool", "boolean"])
def test_profiler_treats_boolean_columns_as_categories(dtype):
    df = pd.DataFrame({"flag": pd.Series([True, False] * 50).astype(dtype)})
    assert profile_columns(df, df).get("flag").semantic_type == "category"


def test_clean_boolean_share_shift_is_reported_not_a_crash(cli, write_csv):
    """The booleans step turns Y/N into the boolean dtype; quantiles of booleans raise TypeError."""
    before = pd.DataFrame({"id": range(200), "flag": ["Y"] * 160 + ["N"] * 40})
    after  = pd.DataFrame({"id": range(200), "flag": ["Y"] * 40 + ["N"] * 160})
    result = cli("diff", write_csv("b.csv", before), write_csv("a.csv", after), "--clean", "--json")
    assert result.stdout.startswith("{"), repr(result.exception)
    payload = json.loads(result.stdout)
    assert sorted(f["title"] for f in payload["findings"] if f["title"].startswith("'flag=")) == [
        "'flag=False' share grew: 20.0% → 80.0% (+60.0%)",
        "'flag=True' share shrank: 80.0% → 20.0% (-60.0%)",
    ]
    assert payload["cleaning"]["before"]["stats"]["columns_retyped"] == {"flag": "boolean"}
    assert result.exit_code == 0                                       # share shifts are WARN


# ── The clean layer ───────────────────────────────────────────────────────────

def test_clean_findings_are_info_and_survive_the_etl_lens(cli, messy_pair):
    before, after = messy_pair
    result = cli("diff", before, after, "--clean", "--scenario", "etl", "--json")
    assert result.exit_code == 1                                       # 25% of keys deleted
    payload = json.loads(result.stdout)

    clean = _layer(payload, "clean")
    assert [f["title"] for f in clean] == [
        "before · region · 10 cells trimmed",
        "before · amount · 40 values parsed as numbers",
        "after · region · 8 cells trimmed",
        "after · amount · 30 values parsed as numbers",
    ]
    assert {f["severity"] for f in clean} == {"INFO"}
    assert clean[1]["detail"] == "currency symbol $ removed"
    assert clean[1]["metric"] == {"step": "numbers", "column": "amount", "action": "parsed_number",
                                  "count": 40, "note": "currency symbol $ removed"}

    info = [f for f in payload["findings"] if f["severity"] == "INFO"]
    assert info[:4] == clean                                           # clean sorts first among INFO


def test_scenario_lens_never_reweights_the_clean_layer(monkeypatch):
    monkeypatch.setitem(SCENARIO_PROMOTIONS, "etl", {"promote": ["clean", "schema"], "demote": []})
    finding = Finding("clean", "amount", "INFO", "before · amount · 3 values parsed as numbers", "", {})
    schema  = Finding("schema", "amount", "INFO", "x", "", {})
    result = DiffResult(rows_before=1, rows_after=1, row_delta=0, row_delta_pct=0.0, findings=[schema, finding])
    adjusted = apply_scenario_lens(result, "etl")
    by_layer = {f.layer: f.severity for f in adjusted.findings}
    assert by_layer == {"clean": "INFO", "schema": "WARN"}


def test_terminal_report_shows_the_clean_layer(cli, messy_pair):
    before, after = messy_pair
    result = cli("diff", before, after, "--clean")
    lines = result.stdout.splitlines()
    assert "clean" in lines                                            # lowercase layer title
    assert lines.index("clean") < lines.index("schema")                # listed before every other layer
    assert "values parsed as numbers" in result.stdout


def test_without_clean_there_is_no_clean_layer_or_cleaning_key(cli, messy_pair):
    before, after = messy_pair
    payload = json.loads(cli("diff", before, after, "--json").stdout)
    assert _layer(payload, "clean") == []
    assert "cleaning" not in payload


def test_json_carries_both_cleaning_reports(cli, messy_pair):
    before, after = messy_pair
    payload = json.loads(cli("diff", before, after, "--clean", "--json").stdout)
    cleaning = payload["cleaning"]
    assert set(cleaning) == {"before", "after"}
    assert cleaning["before"]["stats"]["rows_before"] == 40
    assert cleaning["after"]["stats"]["rows_before"] == 30
    assert cleaning["before"]["stats"]["columns_retyped"] == {"amount": "number"}
    parsed = [a for a in cleaning["before"]["actions"] if a["action"] == "parsed_number"][0]
    assert parsed["examples"][0] == ["$1,000.50", 1000.5]             # local JSON keeps examples


# ── Config ────────────────────────────────────────────────────────────────────

def test_cleaning_rules_come_from_the_metrics_file(cli, messy_pair, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  numbers: false\n", encoding="utf-8")
    before, after = messy_pair
    payload = json.loads(cli("diff", before, after, "--clean", "--json").stdout)
    assert payload["cleaning"]["before"]["stats"]["columns_retyped"] == {}

    explicit = isolated_cwd / "rules.yaml"
    explicit.write_text("cleaning:\n  whitespace: false\n", encoding="utf-8")
    payload = json.loads(cli("diff", before, after, "--clean", "-m", explicit, "--json").stdout)
    assert payload["cleaning"]["before"]["stats"]["columns_retyped"] == {"amount": "number"}
    assert not [a for a in payload["cleaning"]["before"]["actions"] if a["action"] == "trimmed"]


def test_no_metrics_without_a_path_uses_default_rules(cli, messy_pair, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  numbers: false\n", encoding="utf-8")
    before, after = messy_pair
    payload = json.loads(cli("diff", before, after, "--clean", "--no-metrics", "--json").stdout)
    assert payload["cleaning"]["before"]["stats"]["columns_retyped"] == {"amount": "number"}


def test_invalid_cleaning_config_exits_1_only_with_clean(cli, messy_pair, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  dedupe: {mode: fuzzy}\n", encoding="utf-8")
    before, after = messy_pair

    result = cli("diff", before, after, "--clean", "--json")
    assert result.exit_code == 1
    assert "cleaning.dedupe.mode" in result.stderr
    assert result.stdout == ""

    without = cli("diff", before, after, "--json")
    assert "cleaning" not in json.loads(without.stdout)


def test_dedupe_or_impute_prints_a_warning(cli, messy_pair, isolated_cwd):
    before, after = messy_pair
    result = cli("diff", before, after, "--clean", "--json")
    assert "reflect cleaned data" not in result.stderr

    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  dedupe: {mode: exact}\n", encoding="utf-8")
    result = cli("diff", before, after, "--clean", "--json")
    assert "integrity and null-rate findings reflect cleaned data" in result.stderr
    assert "reflect cleaned data" not in result.stdout


def test_missing_column_in_cleaning_config_warns_per_side(cli, messy_pair, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  case: {lower: [regoin]}\n", encoding="utf-8")
    before, after = messy_pair
    result = cli("diff", before, after, "--clean", "--json")
    assert "before: case.lower names column 'regoin'" in result.stderr
    assert "after: case.lower names column 'regoin'" in result.stderr
    titles = [f["title"] for f in _layer(json.loads(result.stdout), "clean")]
    assert "before · regoin · skipped: case.lower: column not found" in titles


# ── findings_from_report ──────────────────────────────────────────────────────

def test_every_action_has_a_label():
    assert set(ACTION_LABELS) == ACTIONS


def test_findings_titles_details_and_metrics():
    report = CleanReport(
        actions=[
            CleanAction("numbers", "amount", "parsed_number", 1200, examples=[("$1", 1)], note="currency symbol $ removed"),
            CleanAction("dedupe", None, "dropped_rows", 5, note="exact duplicate rows; kept the first of each"),
            CleanAction("numbers", "zip", "skipped", 3, examples=[("012", "012")], note="leading zeros (codes)"),
            CleanAction("plan", "ghost", "skipped", 0, note="exclude_columns: column not found"),
            CleanAction("whitespace", "region", "trimmed", 0),
            CleanAction("dates", "d", "ambiguous", 2, note="day/month order cannot be told from the data"),
        ],
        stats=CleanStats(rows_before=10, rows_after=5),
    )
    findings = findings_from_report(report, "after")
    assert [f.title for f in findings] == [
        "after · amount · 1,200 values parsed as numbers",
        "after · table · 5 rows dropped",
        "after · zip · skipped: leading zeros (codes)",
        "after · ghost · skipped: exclude_columns: column not found",
        "after · d · 2 ambiguous values (day/month order)",
    ]
    assert {(f.layer, f.severity) for f in findings} == {("clean", "INFO")}
    assert findings[0].detail == "currency symbol $ removed"
    assert findings[0].metric == report.actions[0].to_dict(include_examples=False)
    assert "examples" not in findings[2].metric
    assert [f.column for f in findings] == ["amount", None, "zip", "ghost", "d"]


# ── Story payload ─────────────────────────────────────────────────────────────

def test_story_payload_carries_cleaning_counts_only():
    before = clean_frame(pd.DataFrame({"amount": ["$1,200", "N/A", "$5"]}), CleanPlan())
    after  = clean_frame(pd.DataFrame({"amount": ["$7", "$8"]}), CleanPlan())
    result = DiffResult(rows_before=3, rows_after=2, row_delta=-1, row_delta_pct=-1 / 3, findings=[
        *findings_from_report(before.report, "before"),
        *findings_from_report(after.report, "after"),
    ])
    result.cleaning = {"before": before.report.to_dict(), "after": after.report.to_dict()}

    payload = story_payload(result)
    assert payload["cleaning"] == {
        "before": before.report.summary_counts(),
        "after":  after.report.summary_counts(),
    }
    assert payload["cleaning"]["before"] == {
        "cells_changed": 3, "rows_dropped": 0, "columns_retyped": {"amount": "number"}, "actions": 2,
    }
    text = json.dumps(payload, ensure_ascii=False)
    assert "examples" not in text
    assert "$1,200" not in text
    assert "examples" in json.dumps(result.cleaning)                   # the result itself is untouched


def test_story_sent_with_clean_has_counts_and_no_examples(cli, messy_pair, fake_anthropic):
    before, after = messy_pair
    cli("diff", before, after, "--clean", "--story")
    sent = fake_anthropic.user_message
    assert '"cleaning"' in sent
    assert '"cells_changed": 50' in sent
    assert "examples" not in sent
    assert "$1,000.50" not in sent
