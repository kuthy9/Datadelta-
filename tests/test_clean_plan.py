"""
test_clean_plan.py — CleanPlan defaults, YAML validation and precedence:
built-in defaults < `cleaning:` in metrics.yaml < CLI flags.
"""

from __future__ import annotations

import textwrap

import pytest

from datadelta.clean.plan import (
    DEFAULT_NULL_TOKENS, CleanConfigError, CleanPlan, DateOptions, DedupeOptions,
    ParseOptions, apply_cli_overrides, load_clean_plan, plan_from_config,
)


SPEC_EXAMPLE = textwrap.dedent("""\
    metrics: []
    cleaning:
      exclude_columns: [notes]
      null_tokens: ["", "NA", "N/A", "null", "-"]
      numbers: {min_parse_ratio: 1.0}
      dates:   {dayfirst: false, min_parse_ratio: 1.0}
      booleans: true
      case:    {lower: [region, status]}
      dedupe:  {mode: key, keys: [order_id], keep: first}
      impute:  {revenue: median, region: mode, discount: {constant: 0}, customer_id: drop_rows}
""")


# ── Defaults ──────────────────────────────────────────────────────────────────

def test_defaults_are_lossless():
    plan = CleanPlan()
    assert plan.exclude_columns == []
    assert plan.headers is True
    assert plan.whitespace is True
    assert plan.null_tokens == list(DEFAULT_NULL_TOKENS)
    assert plan.booleans is True
    assert plan.numbers == ParseOptions(min_parse_ratio=1.0)
    assert plan.dates == DateOptions(min_parse_ratio=1.0, dayfirst=False)
    assert plan.case == {}
    assert plan.dedupe is None
    assert plan.impute == {}
    assert plan.impute_default is None
    assert plan.key_column is None


def test_default_null_tokens_are_casefolded_and_cover_the_spec_list():
    assert DEFAULT_NULL_TOKENS == ("", "na", "n/a", "nan", "null", "none", "-", "—", "--", "#n/a")
    assert all(t == t.strip().casefold() for t in DEFAULT_NULL_TOKENS)


def test_each_plan_gets_its_own_lists():
    a, b = CleanPlan(), CleanPlan()
    a.null_tokens.append("x")
    a.exclude_columns.append("y")
    assert b.null_tokens == list(DEFAULT_NULL_TOKENS)
    assert b.exclude_columns == []


@pytest.mark.parametrize("raw", [None, {}])
def test_missing_or_empty_section_gives_defaults(raw):
    assert plan_from_config(raw) == CleanPlan()


# ── YAML parsing ──────────────────────────────────────────────────────────────

def test_full_spec_example(tmp_path):
    path = tmp_path / "cfg.yaml"
    path.write_text(SPEC_EXAMPLE, encoding="utf-8")
    plan = load_clean_plan(path)
    assert plan == CleanPlan(
        exclude_columns = ["notes"],
        null_tokens     = ["", "NA", "N/A", "null", "-"],
        numbers         = ParseOptions(1.0),
        dates           = DateOptions(1.0, dayfirst=False),
        booleans        = True,
        case            = {"lower": ["region", "status"]},
        dedupe          = DedupeOptions("key", keys=["order_id"], keep="first"),
        impute          = {"revenue": "median", "region": "mode",
                           "discount": {"constant": 0}, "customer_id": "drop_rows"},
    )


def test_false_disables_optional_steps():
    plan = plan_from_config({"null_tokens": False, "numbers": False, "dates": False,
                             "dedupe": False, "headers": False, "whitespace": False,
                             "booleans": False})
    assert plan.null_tokens is None
    assert plan.numbers is None
    assert plan.dates is None
    assert plan.dedupe is None
    assert (plan.headers, plan.whitespace, plan.booleans) == (False, False, False)


def test_true_keeps_defaults_for_switchable_steps():
    plan = plan_from_config({"null_tokens": True, "numbers": True, "dates": True})
    assert plan.null_tokens == list(DEFAULT_NULL_TOKENS)
    assert plan.numbers == ParseOptions()
    assert plan.dates == DateOptions()


def test_partial_mappings_keep_other_defaults():
    plan = plan_from_config({"numbers": {"min_parse_ratio": 0.9}, "dates": {"dayfirst": True}, "key": "order_id"})
    assert plan.numbers == ParseOptions(0.9)
    assert plan.dates == DateOptions(min_parse_ratio=1.0, dayfirst=True)
    assert plan.key_column == "order_id"
    assert plan.dedupe is None


def test_exact_dedupe_and_keep_last():
    plan = plan_from_config({"dedupe": {"mode": "exact", "keep": "last"}})
    assert plan.dedupe == DedupeOptions("exact", keys=[], keep="last")


@pytest.mark.parametrize("raw, message", [
    (["headers"],                                   "cleaning: expected a mapping, got a list"),
    ({"nul_tokens": []},                            "cleaning.nul_tokens: unknown key"),
    ({"headers": "yes please"},                     "cleaning.headers: expected true or false, got 'yes please'"),
    ({"whitespace": 1},                             "cleaning.whitespace: expected true or false, got 1"),
    ({"booleans": None},                            "cleaning.booleans: expected true or false, got null"),
    ({"exclude_columns": "notes"},                  "cleaning.exclude_columns: expected a list of column names, got 'notes'"),
    ({"exclude_columns": ["ok", 7]},                "cleaning.exclude_columns[1]: expected a column name (string), got 7"),
    ({"null_tokens": "NA"},                         "cleaning.null_tokens: expected a list of strings or false, got 'NA'"),
    ({"null_tokens": ["NA", None]},                 "cleaning.null_tokens[1]: expected a string, got null"),
    ({"numbers": {"min_parse_ratio": 1.5}},         "cleaning.numbers.min_parse_ratio: expected a number in (0, 1], got 1.5"),
    ({"numbers": {"min_parse_ratio": 0}},           "cleaning.numbers.min_parse_ratio: expected a number in (0, 1], got 0"),
    ({"numbers": {"min_parse_ratio": True}},        "cleaning.numbers.min_parse_ratio: expected a number in (0, 1], got true"),
    ({"numbers": {"ratio": 0.5}},                   "cleaning.numbers.ratio: unknown key"),
    ({"numbers": "strict"},                         "cleaning.numbers: expected a mapping, got 'strict'"),
    ({"dates": {"dayfirst": "no way"}},             "cleaning.dates.dayfirst: expected true or false, got 'no way'"),
    ({"dates": {"min_parse_ratio": -0.1}},          "cleaning.dates.min_parse_ratio: expected a number in (0, 1], got -0.1"),
    ({"dates": {"format": "%d/%m"}},                "cleaning.dates.format: unknown key"),
    ({"case": {"lower": "region"}},                 "cleaning.case.lower: expected a list of column names, got 'region'"),
    ({"case": {"snake": ["region"]}},               "cleaning.case.snake: unknown key"),
    ({"case": {"lower": ["a"], "upper": ["a"]}},    "cleaning.case: column 'a' is listed under both lower and upper"),
    ({"dedupe": True},                              "cleaning.dedupe: expected a mapping, got true"),
    ({"dedupe": {"keys": ["id"]}},                  "cleaning.dedupe.mode: required (exact or key)"),
    ({"dedupe": {"mode": "fuzzy"}},                 "cleaning.dedupe.mode: expected exact or key, got 'fuzzy'"),
    ({"dedupe": {"mode": "key"}},                   "cleaning.dedupe.keys: required when mode is key"),
    ({"dedupe": {"mode": "key", "keys": []}},       "cleaning.dedupe.keys: required when mode is key"),
    ({"dedupe": {"mode": "exact", "keys": ["id"]}}, "cleaning.dedupe.keys: only allowed when mode is key"),
    ({"dedupe": {"mode": "key", "keys": ["id"], "keep": "middle"}}, "cleaning.dedupe.keep: expected first or last, got 'middle'"),
    ({"dedupe": {"mode": "exact", "by": "id"}},     "cleaning.dedupe.by: unknown key"),
    ({"impute": ["revenue"]},                       "cleaning.impute: expected a mapping, got a list"),
    ({"impute": {"revenue": "average"}},            "cleaning.impute.revenue: expected median, mean, mode, drop_rows or {constant: value}, got 'average'"),
    ({"impute": {"revenue": {"value": 0}}},         "cleaning.impute.revenue.value: unknown key"),
    ({"impute": {"revenue": {}}},                   "cleaning.impute.revenue.constant: required"),
    ({"impute": {"revenue": {"constant": None}}},   "cleaning.impute.revenue.constant: expected a number, string or boolean, got null"),
    ({"impute": {"revenue": 0}},                    "cleaning.impute.revenue: expected median, mean, mode, drop_rows or {constant: value}, got 0"),
    ({"impute": {2024: "median"}},                  "cleaning.impute: column name 2024 must be a string (quote it in YAML)"),
    ({"key": ""},                                   "cleaning.key: expected a column name (string), got ''"),
    ({"key": ["order_id"]},                         "cleaning.key: expected a column name (string), got a list"),
])
def test_invalid_config_names_the_key_path(raw, message):
    with pytest.raises(CleanConfigError) as excinfo:
        plan_from_config(raw)
    assert str(excinfo.value).startswith(message)


def test_clean_config_error_is_a_value_error():
    assert issubclass(CleanConfigError, ValueError)


# ── CLI overrides ─────────────────────────────────────────────────────────────

def test_overrides_return_a_new_plan():
    plan = plan_from_config({"dedupe": {"mode": "key", "keys": ["order_id"]}})
    out = apply_cli_overrides(plan, dedupe="exact", key="sku", impute="mode")
    assert out is not plan
    assert plan.dedupe == DedupeOptions("key", keys=["order_id"])
    assert plan.key_column is None
    assert plan.impute_default is None


def test_no_overrides_keep_the_plan():
    plan = plan_from_config({"dedupe": {"mode": "key", "keys": ["order_id"], "keep": "last"}})
    assert apply_cli_overrides(plan) == plan


def test_dedupe_exact_replaces_yaml_dedupe():
    plan = plan_from_config({"dedupe": {"mode": "key", "keys": ["order_id"]}})
    assert apply_cli_overrides(plan, dedupe="exact").dedupe == DedupeOptions("exact")


def test_dedupe_key_prefers_the_cli_key_and_keeps_yaml_keep():
    plan = plan_from_config({"dedupe": {"mode": "key", "keys": ["order_id"], "keep": "last"}})
    out = apply_cli_overrides(plan, dedupe="key", key="sku")
    assert out.dedupe == DedupeOptions("key", keys=["sku"], keep="last")
    assert out.key_column == "sku"


def test_dedupe_key_falls_back_to_yaml_keys_then_key_column():
    from_yaml_keys = plan_from_config({"dedupe": {"mode": "key", "keys": ["a", "b"]}})
    assert apply_cli_overrides(from_yaml_keys, dedupe="key").dedupe == DedupeOptions("key", keys=["a", "b"])

    from_key_column = plan_from_config({"key": "order_id"})
    assert apply_cli_overrides(from_key_column, dedupe="key").dedupe == DedupeOptions("key", keys=["order_id"])


def test_dedupe_key_without_any_key_is_an_error():
    with pytest.raises(CleanConfigError, match="--dedupe key needs --key or cleaning.dedupe.keys"):
        apply_cli_overrides(CleanPlan(), dedupe="key")


def test_key_alone_sets_key_column_only():
    plan = plan_from_config({"dedupe": {"mode": "key", "keys": ["order_id"]}})
    out = apply_cli_overrides(plan, key="sku")
    assert out.key_column == "sku"
    assert out.dedupe == DedupeOptions("key", keys=["order_id"])


def test_impute_override_sets_the_default_and_keeps_column_rules():
    plan = plan_from_config({"impute": {"revenue": "mean"}})
    out = apply_cli_overrides(plan, impute="median")
    assert out.impute_default == "median"
    assert out.impute == {"revenue": "mean"}


@pytest.mark.parametrize("kwargs, message", [
    ({"dedupe": "fuzzy"}, "--dedupe: expected exact or key, got 'fuzzy'"),
    ({"impute": "mean"},  "--impute: expected median or mode, got 'mean'"),
])
def test_invalid_cli_values(kwargs, message):
    with pytest.raises(CleanConfigError) as excinfo:
        apply_cli_overrides(CleanPlan(), **kwargs)
    assert str(excinfo.value) == message


# ── load_clean_plan ───────────────────────────────────────────────────────────

def test_no_config_file_gives_defaults(isolated_cwd):
    assert load_clean_plan() == CleanPlan()


def test_auto_discovers_metrics_yaml_in_cwd(isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  whitespace: false\n", encoding="utf-8")
    assert load_clean_plan().whitespace is False


def test_metrics_yaml_without_cleaning_key_gives_defaults(isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("business_context: shop\nmetrics: []\n", encoding="utf-8")
    assert load_clean_plan() == CleanPlan()


def test_empty_cleaning_key_and_empty_file_give_defaults(tmp_path):
    empty_section = tmp_path / "a.yaml"
    empty_section.write_text("cleaning:\n", encoding="utf-8")
    empty_file = tmp_path / "b.yaml"
    empty_file.write_text("", encoding="utf-8")
    assert load_clean_plan(empty_section) == CleanPlan()
    assert load_clean_plan(empty_file) == CleanPlan()


def test_cli_flags_win_over_yaml(tmp_path):
    path = tmp_path / "cfg.yaml"
    path.write_text(SPEC_EXAMPLE, encoding="utf-8")
    plan = load_clean_plan(path, dedupe="exact", key="sku", impute="mode")
    assert plan.dedupe == DedupeOptions("exact")
    assert plan.key_column == "sku"
    assert plan.impute_default == "mode"
    assert plan.impute["revenue"] == "median"          # per-column YAML rules stay
    assert plan.exclude_columns == ["notes"]           # untouched YAML settings stay


def test_missing_explicit_config_is_an_error(tmp_path):
    with pytest.raises(CleanConfigError, match="config file not found"):
        load_clean_plan(tmp_path / "nope.yaml")


def test_invalid_yaml_is_an_error(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("cleaning: [unclosed\n", encoding="utf-8")
    with pytest.raises(CleanConfigError, match="invalid YAML"):
        load_clean_plan(path)


def test_top_level_must_be_a_mapping(tmp_path):
    path = tmp_path / "list.yaml"
    path.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(CleanConfigError, match="expected a YAML mapping at the top level"):
        load_clean_plan(path)


def test_invalid_cleaning_section_in_file_reports_the_key_path(tmp_path):
    path = tmp_path / "cfg.yaml"
    path.write_text("cleaning:\n  numbers:\n    min_parse_ratio: 2\n", encoding="utf-8")
    with pytest.raises(CleanConfigError, match=r"^cleaning\.numbers\.min_parse_ratio: "):
        load_clean_plan(path)


# ── Unreadable config files are config errors, not tracebacks ─────────────────

def test_a_directory_as_config_is_an_error(tmp_path):
    with pytest.raises(CleanConfigError, match=r"cannot read the config file"):
        load_clean_plan(tmp_path)


@pytest.mark.parametrize("encoding", ["gbk", "latin-1"])
def test_a_config_that_is_not_utf8_is_an_error(tmp_path, encoding):
    path = tmp_path / "rules.yaml"
    path.write_bytes("cleaning:\n  exclude_columns: [备注, café]\n".encode(encoding, errors="replace"))
    with pytest.raises(CleanConfigError, match=r"not UTF-8 text"):
        load_clean_plan(path)
