"""
test_readme.py — README claims that must stay true.

The README is the first thing a user runs commands from. These guards pin
the install name, clone URL, extras, scenario rules and privacy statement
to what the code actually does, so a later rewrite cannot reintroduce the
v0.2 mismatches. The v0.3 rewrite adds: the terminal sample is the real
generated output, every CLI flag is documented, and the YAML examples,
the cleaning steps and the custom-expression whitelist match the code.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from datadelta.clean      import STEP_ORDER
from datadelta.clean.plan import plan_from_config
from datadelta.metrics    import _parse_metrics_config
from datadelta.safe_eval  import ALLOWED_ATTRS, ALLOWED_BUILTINS, ALLOWED_NP, ALLOWED_PD
from datadelta.scenarios  import SCENARIO_PROMOTIONS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEXT         = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
LINES        = [line.strip() for line in TEXT.splitlines()]
ASSETS       = PROJECT_ROOT / "docs" / "assets"


# ── Install / clone ───────────────────────────────────────────────────────────

def test_install_uses_the_distribution_name():
    assert "pip install datadelta-cli" in TEXT
    assert "pip install datadelta" not in LINES          # that PyPI name is someone else's
    assert '"datadelta[' not in TEXT                     # old extras spelling


def test_no_pypi_version_badge_before_release():
    assert "img.shields.io/pypi/v/" not in TEXT
    assert "pypi.org/project/datadelta)" not in TEXT     # badge links to the other project


def test_clone_url_is_the_real_repository():
    assert "your-username" not in TEXT
    assert "git clone https://github.com/kuthy9/Datadelta-.git" in LINES
    assert "cd Datadelta-" in LINES


def test_every_extra_in_pyproject_is_documented():
    tomllib   = pytest.importorskip("tomllib")           # stdlib from Python 3.11
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extras    = set(pyproject["project"]["optional-dependencies"]) - {"dev"}
    assert extras == {"postgres", "mysql", "mssql", "watch", "openai", "gemini", "all"}
    for extra in sorted(extras):
        assert f'"datadelta-cli[{extra}]"' in TEXT, extra


def test_mssql_odbc_requirement_is_stated():
    mssql_lines = [line for line in LINES if "datadelta-cli[mssql]" in line]
    assert mssql_lines and all("ODBC" in line for line in mssql_lines)


# ── Scenarios ─────────────────────────────────────────────────────────────────

def _rule_line(scenario: str) -> str:
    """The commented `--scenario <name>  # Promotes: ...` line of the scenario section."""
    lines = [
        line for line in LINES
        if line.startswith(f"--scenario {scenario} ") and "#" in line
    ]
    assert len(lines) == 1, f"expected one '--scenario {scenario}  # ...' rule line, got {lines}"
    return lines[0]


@pytest.mark.parametrize("scenario", sorted(SCENARIO_PROMOTIONS))
def test_scenario_lines_match_the_scenario_rules(scenario):
    line  = _rule_line(scenario)
    rules = SCENARIO_PROMOTIONS[scenario]

    if rules["promote"]:
        assert "Promotes: " + " + ".join(rules["promote"]) in line
    else:
        assert "Promotes" not in line
    if rules["demote"]:
        assert "Demotes: " + " + ".join(rules["demote"]) in line
    else:
        assert "Demotes" not in line


def test_etl_does_not_claim_to_demote_distribution():
    assert "Demotes: distribution" not in _rule_line("etl")


# ── LLM features ──────────────────────────────────────────────────────────────

def test_privacy_statement_matches_what_is_sent():
    assert "never sent to any API" not in TEXT
    assert "--no-samples" in TEXT
    assert "10 category labels" in TEXT
    assert "+k more" in TEXT
    assert "5 sample values" in TEXT


def test_llm_provider_extras_are_named():
    assert "`--llm openai`" in TEXT and "`--llm deepseek`" in TEXT and "`--llm gemini`" in TEXT
    provider_rows = [line for line in LINES if line.startswith("| ") and "--llm " in line]
    rows = {row.split("`--llm ")[1].split("`")[0]: row for row in provider_rows}
    assert "`openai`" in rows["openai"]
    assert "`openai`" in rows["deepseek"]
    assert "`gemini`" in rows["gemini"]


def test_ci_exit_code_claim_is_present():
    # True since the --json contract task: --json exits 1 on FAIL too.
    assert "Exit code is `1` when any FAIL finding exists" in TEXT


# ── v0.3: the real terminal sample ────────────────────────────────────────────

def test_sample_images_are_referenced_and_exist():
    for name in ("diff.svg", "clean.svg"):
        assert f"docs/assets/{name}" in TEXT, name
        assert (ASSETS / name).is_file(), name


def test_text_sample_is_the_generated_output():
    # scripts/render_readme_assets.py writes docs/assets/diff.txt; the README quotes it verbatim.
    sample = (ASSETS / "diff.txt").read_text(encoding="utf-8").strip("\n")
    assert sample.startswith("data ▲")
    assert sample in TEXT


# ── v0.3: every CLI flag is documented ────────────────────────────────────────

OPTION_ROW = re.compile(r"^│\s*(?:\*\s*)?(--[a-z][a-z0-9-]*)(?:\s+(-[a-zA-Z]))?\s")


def _help_flags(cli, *command: str) -> list[tuple[str, str | None]]:
    """(long, short) pairs from the Options panel of `datadelta <command> --help`."""
    result = cli(*command, "--help")
    assert result.exit_code == 0, result.stdout
    flags, in_options = [], False
    for line in result.stdout.splitlines():
        if line.startswith("╭─ Options"):
            in_options = True
        elif line.startswith("╰"):
            in_options = False
        elif in_options:
            match = OPTION_ROW.match(line)
            if match and match.group(1) != "--help":
                flags.append((match.group(1), match.group(2)))
    return flags


@pytest.mark.parametrize("command, anchor", [
    ((),        ("--version", None)),
    (("diff",),  ("--quiet", "-q")),
    (("clean",), ("--dry-run", None)),
    (("init",),  ("--no-samples", None)),
])
def test_every_cli_flag_is_documented(cli, command, anchor):
    flags = _help_flags(cli, *command)
    assert anchor in flags                                 # the help parser found the panel
    for long, short in flags:
        assert re.search(rf"(?<![\w-]){re.escape(long)}(?![\w-])", TEXT), long
        if short is not None:
            assert f"{short}, {long}" in TEXT, f"{short}, {long}"


# ── v0.3: examples and tables match the code ──────────────────────────────────

def _yaml_blocks() -> list[str]:
    return re.findall(r"```yaml\n(.*?)```", TEXT, flags=re.S)


def test_cleaning_yaml_example_is_valid():
    block = next(b for b in _yaml_blocks() if b.startswith("cleaning:"))
    plan  = plan_from_config(yaml.safe_load(block)["cleaning"])
    assert plan.dedupe is not None and plan.dedupe.keys == ["order_id"]
    assert plan.impute["discount"] == {"constant": 0}


def test_metrics_yaml_example_is_valid():
    block  = next(b for b in _yaml_blocks() if b.startswith("business_context:"))
    config = _parse_metrics_config(yaml.safe_load(block))
    assert {m.type for m in config.metrics} == {"staleness", "rate", "ratio", "completeness", "custom"}


def test_cleaning_table_lists_every_step():
    for step in STEP_ORDER:
        assert any(line.startswith(f"| `{step}` |") for line in LINES), step


def test_custom_expression_whitelist_is_documented():
    names  = [f"`{name}`" for name in sorted(ALLOWED_ATTRS | set(ALLOWED_BUILTINS))]
    names += [f"`np.{name}`" for name in sorted(ALLOWED_NP)]
    names += [f"`pd.{name}`" for name in sorted(ALLOWED_PD)]
    missing = [name for name in names if name not in TEXT]
    assert missing == []


def test_known_limitations_are_listed():
    section = TEXT.split("## Known limitations", 1)[1].split("\n## ", 1)[0]
    for phrase in ("first sheet", "`.xls`", "first table", "dayfirst", "`--watch`"):
        assert phrase in section, phrase


def test_story_privacy_describes_the_clean_findings():
    # story_payload (Task 13) keeps one clean finding per cleaning action and
    # reduces only the two cleaning reports to totals; the README must say so.
    section = TEXT.split("### What is sent to the LLM", 1)[1].split("\n## ", 1)[0]
    assert "one clean finding per cleaning action" in section
    assert "reduced to their totals" in section
    assert "only the cleaning totals are sent" not in section


def test_json_date_reading_is_described_as_it_behaves(tmp_path):
    # Re-review NB4: the README said "JSON strings are not read as dates",
    # but plain ISO date strings in JSON are typed as dates on reading.
    from datadelta.loader import load_file
    path = tmp_path / "d.json"
    path.write_text('{"day": "2024-01-05"}\n{"day": "2024-01-06"}\n', encoding="utf-8")
    assert str(load_file(str(path), lossless=True)["day"].dtype).startswith("datetime64")

    assert "JSON strings are not read as dates" not in TEXT
    assert "JSON strings follow the same rule" in TEXT


def test_quickstart_clean_line_carries_the_na_caveat():
    # Final review F2: "NA" is a default null token; the Quickstart --clean
    # example says so right there, with the override.
    line = LINES.index("datadelta diff examples/etl_before.csv examples/etl_before.xlsx --clean")
    caveat = " ".join(LINES[line - 2:line])
    assert "NA" in caveat and "null_tokens" in caveat
