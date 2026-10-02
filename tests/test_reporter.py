"""
test_reporter.py — The ink & vermilion terminal reports.

Every report is recorded with Console(record=True, width=80,
color_system=None) and checked as plain text: layout, conditional hints,
no emoji, and Review Focus 4 — column names containing Rich markup or
HTML, and CJK names, print literally and keep their alignment.
"""

from __future__ import annotations

import io
import re
from datetime import datetime
from pathlib  import Path

import pytest
from rich.cells   import cell_len
from rich.console import Console

from datadelta.clean.report import CleanAction, CleanReport, CleanStats
from datadelta.differ       import DiffResult, Finding
from datadelta.reporter     import print_clean_report, print_report, print_story
from datadelta.story        import StoryResult


NOW = datetime(2026, 10, 1, 14, 2)

# Emoji and pictographs: U+1F300–U+1FAFF, U+2600–U+27BF (minus ✓ U+2713,
# the dashboard's "done" glyph) and the emoji variation selector U+FE0F.
EMOJI = re.compile("[\U0001F300-\U0001FAFF\u2600-\u2712\u2714-\u27BF\uFE0F]")


def _console() -> Console:
    return Console(record=True, width=80, color_system=None, force_terminal=False, file=io.StringIO())


def _diff(*findings: Finding, before: int = 1000, after: int = 744) -> DiffResult:
    return DiffResult(
        rows_before   = before,
        rows_after    = after,
        row_delta     = after - before,
        row_delta_pct = (after - before) / max(before, 1),
        findings      = list(findings),
    )


def _report_text(result: DiffResult, scenario: str = "etl", **kwargs) -> str:
    console = _console()
    print_report(result, scenario, console=console, now=NOW, **kwargs)
    return console.export_text()


FAIL_KEYS = Finding("integrity",    "order_id", "FAIL", "256 key(s) deleted from 'order_id' (25.6% of original)", "", {})
WARN_GONE = Finding("distribution", "region",   "WARN", "Categories disappeared from 'region': ['APAC']", "", {})
INFO_NULL = Finding("distribution", "地区",      "INFO", "Null rate increased: '地区' (0.0% → 5.0%)", "", {})
INFO_CLEAN = Finding("clean",       "amount",   "INFO", "before · amount · 40 values parsed as numbers", "", {})
PASS_KPI  = Finding("custom",       None,       "PASS", "on_time_rate: 0.97 → 0.96", "", {})
WARN_TYPE = Finding("schema",       "revenue",  "WARN", "Type changed: 'revenue' (float64 → str)", "", {})


# ── Diff report layout ────────────────────────────────────────────────────────

def test_header_rule_and_rows_line():
    lines = _report_text(_diff(FAIL_KEYS)).splitlines()
    assert lines[0] == ""
    assert lines[1].startswith("data ▲   ETL Validation")
    assert lines[1].endswith("2026-10-01 14:02")
    assert cell_len(lines[1]) == 80
    assert lines[2] == "─" * 80
    assert lines[3].startswith("rows     1,000 → 744")
    assert lines[3].endswith("▼ 256   −25.6%")
    assert cell_len(lines[3]) == 80


@pytest.mark.parametrize("before, after, right", [
    (1000, 1012, "▲ 12   +1.2%"),
    (500,  500,  "unchanged"),
    (0,    5,    "▲ 5"),
])
def test_rows_line_variants(before, after, right):
    rows = _report_text(_diff(before=before, after=after)).splitlines()[3]
    assert rows.startswith(f"rows     {before:,} → {after:,}")
    assert rows.endswith(right)


def test_layers_in_order_with_lowercase_titles():
    lines = _report_text(_diff(PASS_KPI, INFO_NULL, WARN_GONE, FAIL_KEYS, WARN_TYPE, INFO_CLEAN)).splitlines()
    titles = [line for line in lines if line in ("clean", "schema", "integrity", "distribution", "custom metrics")]
    assert titles == ["clean", "schema", "integrity", "distribution", "custom metrics"]
    assert "  ▲  fail   256 key(s) deleted from 'order_id' (25.6% of original)" in lines
    assert "  ●  warn   Categories disappeared from 'region': ['APAC']" in lines
    assert "  ·  info   before · amount · 40 values parsed as numbers" in lines
    assert "  ○  pass   on_time_rate: 0.97 → 0.96" in lines


def test_no_schema_changes_prints_a_pass_line():
    lines = _report_text(_diff(FAIL_KEYS)).splitlines()
    schema = lines.index("schema")
    assert lines[schema + 1] == "  ○  pass   no schema changes"


def test_schema_findings_replace_the_pass_line():
    text = _report_text(_diff(WARN_TYPE))
    assert "no schema changes" not in text
    assert "  ●  warn   Type changed: 'revenue' (float64 → str)" in text.splitlines()


def test_empty_layers_are_not_printed():
    lines = _report_text(_diff(FAIL_KEYS)).splitlines()
    assert "integrity" in lines
    for title in ("clean", "distribution", "custom metrics"):
        assert title not in lines


def test_footer_counts_and_exit_code():
    lines = _report_text(_diff(FAIL_KEYS, WARN_GONE, INFO_NULL, INFO_CLEAN)).splitlines()
    footer = next(line for line in lines if line.startswith("  1 fail"))
    assert footer.startswith("  1 fail   1 warn   2 info")
    assert footer.endswith("exit 1")
    assert cell_len(footer) == 80

    clean = _report_text(_diff(WARN_GONE)).splitlines()
    assert any(line.startswith("  0 fail   1 warn   0 info") and line.endswith("exit 0") for line in clean)


def test_long_titles_wrap_under_themselves():
    long = Finding("distribution", "region", "WARN", "Categories disappeared from 'region': " + str([f"REGION-{i}" for i in range(12)]), "", {})
    lines = _report_text(_diff(long)).splitlines()
    first = next(i for i, line in enumerate(lines) if line.startswith("  ●  warn   Categories"))
    assert lines[first + 1].startswith(" " * 12)
    assert lines[first + 1][12] != " "
    assert all(cell_len(line) <= 80 for line in lines)


def test_no_trailing_whitespace():
    text = _report_text(_diff(FAIL_KEYS, WARN_GONE, INFO_NULL))
    assert all(line == line.rstrip() for line in text.splitlines())


# ── Hints ─────────────────────────────────────────────────────────────────────

def test_both_hints_by_default():
    text = _report_text(_diff(FAIL_KEYS))
    assert "--export report.html   shareable HTML report" in text
    assert "--story" in text and "narrative via an LLM" in text


def test_export_hint_hidden_when_exported():
    text = _report_text(_diff(FAIL_KEYS), exported=True)
    assert "--export" not in text and "shareable HTML report" not in text
    assert "narrative via an LLM" in text


def test_story_hint_hidden_when_storied():
    text = _report_text(_diff(FAIL_KEYS), storied=True)
    assert "--story" not in text and "narrative via an LLM" not in text
    assert "shareable HTML report" in text


def test_no_hints_when_both_requested():
    text = _report_text(_diff(FAIL_KEYS), exported=True, storied=True)
    assert "--export" not in text
    assert "--story" not in text


# ── Review Focus 4: markup, HTML and CJK in data-derived text ─────────────────

MARKUP_TITLES = [
    "Type changed: '[bold]x' (int64 → str)",
    "Column added: '<b>y</b>' (type: str)",
    "Column removed: '[/]' (was: int64)",           # a bare closing tag: MarkupError if parsed
    "New categories in 'mood': [':thumbs_up:']",    # an emoji code: must not become an emoji
]


def test_markup_and_html_in_titles_print_literally():
    findings = [Finding("schema", None, "WARN", title, "", {}) for title in MARKUP_TITLES]
    text = _report_text(_diff(*findings))
    for title in MARKUP_TITLES:
        assert f"  ●  warn   {title}" in text.splitlines()
    assert EMOJI.search(text) is None


def test_markup_in_scenario_label_prints_literally():
    text = _report_text(_diff(), scenario="[red]custom[/red]")
    assert text.splitlines()[1].startswith("data ▲   [red]custom[/red]")


def test_cjk_column_names_keep_alignment():
    cjk = Finding("distribution", "地区", "WARN",
                  "Categories disappeared from '地区': ['华东', '华南', '华北', '西南', '东北', '西北', '港澳台', '海外']",
                  "", {})
    lines = _report_text(_diff(cjk, INFO_NULL), scenario="地区 check").splitlines()
    assert lines[1].startswith("data ▲   地区 check")
    assert cell_len(lines[1]) == 80                     # timestamp still flush right
    assert "  ·  info   Null rate increased: '地区' (0.0% → 5.0%)" in lines
    assert all(cell_len(line) <= 80 for line in lines)
    first = next(i for i, line in enumerate(lines) if "Categories disappeared from '地区'" in line)
    assert lines[first + 1].startswith(" " * 12)


# ── No emoji anywhere ─────────────────────────────────────────────────────────

def test_reports_contain_no_emoji():
    console = _console()
    print_report(_diff(FAIL_KEYS, WARN_GONE, INFO_NULL, INFO_CLEAN, PASS_KPI), "general", console=console, now=NOW)
    print_story(StoryResult(text="Rows fell by a quarter.", label="Claude (Anthropic)"), console=console)
    print_clean_report(_clean_report(), "orders.csv", Path("orders.clean.csv"), console=console)
    text = console.export_text()
    assert EMOJI.search(text) is None
    assert "▲" in text and "●" in text and "○" in text


# ── Story ─────────────────────────────────────────────────────────────────────

def test_story_header_names_the_provider_and_text_is_indented():
    console = _console()
    words = " ".join(f"word{i}" for i in range(60))
    print_story(StoryResult(text=f"  {words}\n", label="DeepSeek Chat"), console=console)
    lines = console.export_text().splitlines()
    assert lines[0] == "story · DeepSeek Chat"
    body = [line for line in lines[1:] if line]
    assert len(body) > 1                                 # wrapped
    assert all(line.startswith("  ") and line[2] != " " for line in body)
    assert all(cell_len(line) <= 80 for line in body)
    assert " ".join(" ".join(line.split()) for line in body) == words


def test_story_text_is_printed_literally():
    console = _console()
    print_story(StoryResult(text="[bold red]alert[/bold red] <b>y</b> [/]", label="Claude (Anthropic)"), console=console)
    assert "  [bold red]alert[/bold red] <b>y</b> [/]" in console.export_text().splitlines()


# ── Clean report ──────────────────────────────────────────────────────────────

def _clean_report() -> CleanReport:
    return CleanReport(
        actions = [
            CleanAction("whitespace", "region", "trimmed", 10,
                        examples=[(" EMEA", "EMEA"), ("APAC ", "APAC"), (" LATAM", "LATAM"), ("NA ", "NA")]),
            CleanAction("numbers", "amount", "parsed_number", 40,
                        examples=[("$1,000.50", 1000.5)], note="currency symbol $ removed"),
            CleanAction("numbers", "zip_code", "skipped", 3,
                        examples=[("02134", "02134")], note="leading zeros (codes)"),
            CleanAction("dedupe", None, "dropped_rows", 2, note="exact duplicate rows; kept the first of each"),
        ],
        stats = CleanStats(
            rows_before     = 8,
            rows_after      = 6,
            cells_changed   = 50,
            columns_retyped = {"amount": "number"},
            nulls_before    = {"amount": 0, "region": 1},
            nulls_after     = {"amount": 2, "region": 1},
            duplicate_rows  = 0,
        ),
    )


def _clean_text(report: CleanReport, output: Path | None = Path("orders.clean.csv"), **kwargs) -> str:
    console = _console()
    print_clean_report(report, "data/orders.csv", output, console=console, **kwargs)
    return console.export_text()


def test_clean_header_and_rows():
    lines = _clean_text(_clean_report()).splitlines()
    assert lines[1] == "data ▲   clean · data/orders.csv"
    assert lines[2] == "─" * 80
    assert lines[3].startswith("rows     8 → 6")
    assert lines[3].endswith("▼ 2   −25.0%")


def test_clean_actions_grouped_by_step_with_counts():
    lines = _clean_text(_clean_report()).splitlines()
    steps = [line for line in lines if line in ("whitespace", "numbers", "dedupe")]
    assert steps == ["whitespace", "numbers", "dedupe"]
    rows = {line.split()[0]: line.split() for line in lines if line.startswith("  ") and len(line.split()) >= 3}
    assert rows["region"][:3] == ["region", "trimmed", "10"]
    assert rows["amount"] == ["amount", "parsed_number", "40", "currency", "symbol", "$", "removed"]
    assert rows["zip_code"][:3] == ["zip_code", "skipped", "3"]
    assert rows["(table)"][:3] == ["(table)", "dropped_rows", "2"]


def test_clean_examples_at_most_three_and_literal():
    text = _clean_text(_clean_report())
    assert "' EMEA' → 'EMEA'" in text
    assert "'APAC ' → 'APAC'" in text
    assert "' LATAM' → 'LATAM'" in text
    assert "'NA ' → 'NA'" not in text                   # fourth example is not shown
    assert "'$1,000.50' → 1000.5" in text
    assert "'02134'" in text and "'02134' →" not in text   # skipped: only the blocking value


def test_clean_stats_block():
    lines = _clean_text(_clean_report()).splitlines()
    assert "  cells changed     50" in lines
    assert "  columns retyped   amount → number" in lines
    assert "  nulls             1 → 3" in lines
    assert "  duplicate rows    0" in lines


def test_clean_duplicates_left_in_place_are_explained():
    report = _clean_report()
    report.stats.duplicate_rows = 5
    report.stats.duplicate_keys = 2
    lines = _clean_text(report).splitlines()
    assert "  duplicate rows    5 · left in place (--dedupe exact drops them)" in lines
    assert "  duplicate keys    2" in lines


def test_clean_saved_or_dry_run_line():
    # The destination is a status line: clean_cmd prints "Saved <path>" on
    # stderr, so the stdout report never repeats it (with or without --dry-run).
    written = _clean_text(_clean_report()).splitlines()
    assert not any(line.lstrip().startswith("saved") for line in written)
    assert "orders.clean.csv" not in "\n".join(written)
    assert "dry run" not in "\n".join(written)

    dry = _clean_text(_clean_report(), dry_run=True).splitlines()
    assert "  dry run · nothing written" in dry
    assert not any(line.lstrip().startswith("saved") for line in dry)


def test_clean_long_paths_are_not_wrapped():
    deep = "/".join(["very-long-directory-name"] * 6)
    console = _console()
    print_clean_report(_clean_report(), f"{deep}/orders.csv", Path(f"{deep}/orders.clean.csv"), console=console)
    lines = console.export_text().splitlines()
    assert f"data ▲   clean · {deep}/orders.csv" in lines


def test_clean_nothing_to_change():
    report = CleanReport(actions=[], stats=CleanStats(rows_before=3, rows_after=3))
    text = _clean_text(report, output=None, dry_run=True)
    assert "  nothing to change" in text.splitlines()
    assert "  columns retyped   none" in text.splitlines()


def test_clean_markup_html_and_cjk_columns_align():
    report = CleanReport(
        actions = [
            CleanAction("whitespace", "[bold]x",  "trimmed", 1, examples=[(" a", "a")]),
            CleanAction("whitespace", "<b>y</b>", "trimmed", 2, examples=[("[red]b ", "[red]b")]),
            CleanAction("whitespace", "地区",      "trimmed", 3, examples=[(" 北", "北")]),
        ],
        stats = CleanStats(rows_before=3, rows_after=3, cells_changed=6),
    )
    lines = _clean_text(report).splitlines()
    rows = [line for line in lines if line.rstrip().endswith(("trimmed  1", "trimmed  2", "trimmed  3"))]
    assert [line.split()[0] for line in rows] == ["[bold]x", "<b>y</b>", "地区"]
    # The action word starts at the same terminal cell on every row.
    starts = {cell_len(line[: line.index("trimmed")]) for line in rows}
    assert len(starts) == 1
    text = "\n".join(lines)
    assert "' a' → 'a'" in text
    assert "'[red]b ' → '[red]b'" in text
    assert "' 北' → '北'" in text


def test_footer_shows_the_exit_code_the_command_will_use():
    """An unwritable --export exits 1 even without a FAIL; the footer must say so (review minor)."""
    lines = _report_text(_diff(WARN_GONE), exit_code=1).splitlines()
    footer = next(line for line in lines if line.startswith("  0 fail"))
    assert footer.endswith("exit 1")
