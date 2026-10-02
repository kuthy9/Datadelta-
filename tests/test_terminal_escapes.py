"""
test_terminal_escapes.py — Control characters from data never reach the terminal raw.

rich.text.Text keeps data from being read as markup, but Rich drops only
a few control characters (BEL, BS, VT, FF, CR): ESC passes through. A CSV
header like "x\\x1b]0;PWNED\\x07" could set the terminal title, erase a
fail line (ESC[2K ESC[1A) or write the clipboard (OSC 52); so could an
LLM narrative steered by the data (CWE-150). Every data-derived string is
printed with its C0 / C1 control characters shown as visible escapes.
"""

from __future__ import annotations

import io
import sys
from datetime import datetime
from pathlib  import Path

import pandas as pd
import pytest
from rich.console import Console

from datadelta.clean.report import CleanAction, CleanReport, CleanStats
from datadelta.dashboard    import LiveDashboard, PlainProgress
from datadelta.differ       import DiffResult, Finding
from datadelta.reporter     import print_clean_report, print_report, print_story
from datadelta.safetext     import printable
from datadelta.story        import StoryResult


HOSTILE = "x\x1b]0;PWNED\x07\x1b[8mhidden\x9b2J"


def _console() -> Console:
    return Console(record=True, width=120, color_system=None, file=io.StringIO())


def _assert_escaped(text: str) -> None:
    assert "\x1b" not in text and "\x07" not in text and "\x9b" not in text
    assert "\\x1b]0;PWNED\\x07\\x1b[8mhidden\\x9b2J" in text


# ── printable() ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, shown", [
    ("plain 地区 [bold]", "plain 地区 [bold]"),
    ("a\x1bb",            "a\\x1bb"),
    ("bell\x07",          "bell\\x07"),
    ("del\x7f",           "del\\x7f"),
    ("csi\x9b",           "csi\\x9b"),
    ("cr\r",              "cr\\x0d"),
    ("line\nnext\ttab",   "line\nnext\ttab"),       # newline and tab are layout, not control
])
def test_printable(raw, shown):
    assert printable(raw) == shown


# ── Reports ───────────────────────────────────────────────────────────────────

def test_diff_report_escapes_finding_titles():
    result = DiffResult(rows_before=1, rows_after=1, row_delta=0, row_delta_pct=0.0,
                        findings=[Finding("schema", HOSTILE, "WARN", f"Column added: '{HOSTILE}'", "", {})])
    console = _console()
    print_report(result, "general", console=console, now=datetime(2026, 10, 1))
    _assert_escaped(console.export_text())


def test_story_text_is_escaped():
    console = _console()
    print_story(StoryResult(text=f"The data says {HOSTILE}.", label="Claude"), console=console)
    _assert_escaped(console.export_text())


def test_clean_report_escapes_columns_notes_examples_and_source():
    report = CleanReport(
        actions = [CleanAction("whitespace", HOSTILE, "trimmed", 1, examples=[(f" {HOSTILE}", HOSTILE)], note=HOSTILE)],
        stats   = CleanStats(rows_before=1, rows_after=1, cells_changed=1, columns_retyped={HOSTILE: "number"}),
    )
    console = _console()
    print_clean_report(report, f"{HOSTILE}.csv", Path("out.csv"), console=console)
    text = console.export_text()
    _assert_escaped(text)
    assert text.count("PWNED") >= 4                                  # column, note, retyped column, source


# ── Progress ──────────────────────────────────────────────────────────────────

def test_live_dashboard_escapes_title_notes_and_summaries():
    dash = LiveDashboard(f"clean · {HOSTILE}.csv", console=_console())
    dash.stage_start("distribution", "distribution", total=2, note=HOSTILE)
    dash.stage_start("load", "load", note="a.csv")
    dash.stage_end("load", "failed", summary=HOSTILE)
    console = _console()
    console.print(dash.render())
    text = console.export_text()
    _assert_escaped(text)
    assert text.count("PWNED") == 3


def test_plain_progress_escapes_its_line():
    console = _console()
    plain = PlainProgress(console=console)
    plain.stage_start("load", "load", note=HOSTILE)
    plain.stage_end("load", "failed", summary=HOSTILE)
    _assert_escaped(console.export_text())


# ── End to end ────────────────────────────────────────────────────────────────

def test_hostile_header_never_reaches_stdout_or_stderr_raw(cli, tmp_path):
    before = tmp_path / "b.csv"
    after  = tmp_path / "a.csv"
    pd.DataFrame({"id": [1, 2]}).to_csv(before, index=False)
    pd.DataFrame({"id": [1, 2], HOSTILE: ["p", "q"]}).to_csv(after, index=False)

    result = cli("diff", before, after, "--no-metrics")
    assert result.exit_code == 0, result.stderr
    _assert_escaped(result.stdout)
    assert "\x1b" not in result.stderr


def test_hostile_text_in_a_load_error_is_escaped(cli, tmp_path):
    missing = tmp_path / f"{HOSTILE}.csv"
    result = cli("clean", missing)
    assert result.exit_code == 1
    assert "\x1b" not in result.stderr and "\\x1b" in result.stderr


def test_init_escapes_the_streamed_reply(cli, fake_anthropic, write_csv, isolated_cwd):
    """The reply is streamed to stderr as it arrives; YAML then rejects the control characters."""
    fake_anthropic.reply = f"metrics:\n  - name: x  # {HOSTILE}\n    type: completeness\n    column: id\n"
    sample = write_csv("s.csv", pd.DataFrame({"id": [1, 2]}))
    result = cli("init", "--from", sample, "--business", "shop")
    assert result.exit_code == 1
    assert "\x1b" not in result.stderr and "\\x1b]0;PWNED" in result.stderr
    assert (isolated_cwd / "metrics_draft.yaml").read_text(encoding="utf-8") == fake_anthropic.reply
