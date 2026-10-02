"""
reporter.py — The terminal reports, in ink & vermilion.

Diff report (print_report, `datadelta diff`):

    data ▲   ETL Validation                              2026-10-01 14:02
    ──────────────────────────────────────────────────────────────────────
    rows     1,000 → 744                                  ▼ 256   −25.6%

    schema
      ○  pass   no schema changes
    integrity
      ▲  fail   256 key(s) deleted from 'order_id' (25.6% of original)

      1 fail   0 warn   0 info                                     exit 1

Clean report (print_clean_report, `datadelta clean`): the same header and
rows line, then the actions grouped by step (column, action, count, note,
up to three local before → after examples), then a stats block.

Story (print_story, `--story`): "story · <provider>" and the narrative,
wrapped to the terminal width and indented two spaces. The narrative is
collected in full first (story.py), so it never interleaves with the
report or the progress display.

DESIGN RULES
  - Colors and glyphs come only from theme.py. No background colors;
    body text uses the terminal's own foreground; secondary text is dim.
  - Severity is a glyph plus a word, never color alone (NO_COLOR safe).
  - Every data-derived string (column names, values, titles, paths, LLM
    output) is printed through rich.text.Text, which is never parsed as
    markup or emoji codes: a column named "[bold]x" prints literally.
    It passes through safetext.printable() first, so control characters
    (ESC sequences in a header or a narrative) show as "\x1b" instead of
    acting on the terminal.
  - Long lines wrap with a hanging indent computed in terminal cells, so
    CJK column names keep the alignment.
"""

from __future__ import annotations

import math
from datetime import datetime
from pathlib  import Path
from typing   import TYPE_CHECKING, Any

import pandas as pd
from rich.console import Console
from rich.text    import Text

from .             import theme
from .clean.report import CleanAction, CleanReport
from .differ       import DiffResult
from .safetext     import printable
from .scenarios    import SCENARIO_LABELS

if TYPE_CHECKING:
    from .story import StoryResult


# Display order of the finding layers, and their (lowercase) titles.
LAYER_ORDER: list[str] = ["clean", "schema", "integrity", "distribution", "custom"]
LAYER_LABELS: dict[str, str] = {
    "clean":        "clean",
    "schema":       "schema",
    "integrity":    "integrity",
    "distribution": "distribution",
    "custom":       "custom metrics",
}

# Hints shown under the diff report for the outputs the user did not ask for.
HINT_EXPORT = ("--export report.html", "shareable HTML report")
HINT_STORY  = ("--story",              "narrative via an LLM")

INDENT        = "  "
MIN_WRAP      = 20      # never wrap a body narrower than this many cells
ROWS_LABEL    = 9       # "rows     " — label column of the rows line
STAT_LABEL    = 18      # "cells changed     " — label column of the clean stats
MAX_COLUMN_W  = 24      # clean report: widest column-name cell before alignment gives up
EXAMPLE_CHARS = 40      # clean report: longest example value shown, in characters

_MUTED = theme.rich_style("muted")


# ─────────────────────────────────────────────────────────────────────────────
# Diff report — `datadelta diff`
# ─────────────────────────────────────────────────────────────────────────────

def print_report(
    result:   DiffResult,
    scenario: str = "general",
    *,
    console:   Console | None  = None,
    exported:  bool            = False,
    storied:   bool            = False,
    now:       datetime | None = None,
    exit_code: int | None      = None,
) -> None:
    """
    Print the diff report (stdout by default).

    exported / storied: the user already asked for --export / --story, so
    the matching hint is not shown. now: header timestamp (tests pin it).
    exit_code: the code the command will exit with, shown in the footer;
    None means "from the findings" (1 when there is a FAIL). The CLI
    passes it because an unwritable --export also exits 1.
    """
    out   = console if console is not None else Console()
    width = out.width
    stamp = (now or datetime.now()).strftime("%Y-%m-%d %H:%M")
    label = SCENARIO_LABELS.get(scenario, scenario)

    out.print()
    out.print(_spread(Text.assemble(_wordmark(), "   ", label), Text(stamp, style=_MUTED), width))
    out.print(_rule(width))
    out.print(_rows_line(result.rows_before, result.rows_after, width))

    out.print()
    for layer in LAYER_ORDER:
        findings = [f for f in result.findings if f.layer == layer]
        if not findings and layer != "schema":
            continue                                   # empty layers are not shown
        out.print(Text(LAYER_LABELS[layer], style=_MUTED))
        for finding in findings:
            out.print(_severity_line(out, finding.severity, Text(printable(finding.title))))
        if not findings:
            out.print(_severity_line(out, "PASS", Text("no schema changes")))

    out.print()
    out.print(_footer(result, width, exit_code))

    hints = [h for h, shown in ((HINT_EXPORT, exported), (HINT_STORY, storied)) if not shown]
    if hints:
        out.print()
        flag_w = max(len(flag) for flag, _ in hints)
        for flag, text in hints:
            out.print(Text.assemble(INDENT, flag.ljust(flag_w), "   ", (text, _MUTED)))
    out.print()


def _severity_line(console: Console, severity: str, body: Text) -> Text:
    """One finding: glyph and word in the severity style, then the body wrapped under itself."""
    style  = theme.severity_style(severity)
    prefix = Text.assemble(
        INDENT,
        (theme.SEVERITY_GLYPH[severity], style),
        "  ",
        (theme.SEVERITY_WORD[severity].ljust(4), style),
        "   ",
    )
    return _hang(console, prefix, body)


def _footer(result: DiffResult, width: int, exit_code: int | None = None) -> Text:
    """Severity counts on the left ("1 fail   2 warn   0 info"), the exit code on the right."""
    counts = Text(INDENT)
    for i, severity in enumerate(("FAIL", "WARN", "INFO")):
        n = sum(1 for f in result.findings if f.severity == severity)
        if i:
            counts.append("   ")
        style = theme.severity_style(severity) if n else _MUTED
        counts.append(f"{n} {theme.SEVERITY_WORD[severity]}", style=style)
    code = exit_code if exit_code is not None else (1 if result.has_failures else 0)
    exit_text = Text(f"exit {code}", style=theme.severity_style("FAIL") if code else _MUTED)
    return _spread(counts, exit_text, width)


# ─────────────────────────────────────────────────────────────────────────────
# Story — `--story`
# ─────────────────────────────────────────────────────────────────────────────

def print_story(story: "StoryResult", *, console: Console | None = None) -> None:
    """Print the collected narrative: a "story · <provider>" line, then the text indented two spaces."""
    out = console if console is not None else Console()
    out.print(Text.assemble(("story", _MUTED), (f" {theme.SEP} ", _MUTED), printable(story.label)))
    out.print()
    out.print(_hang(out, Text(INDENT), Text(printable(story.text.strip()))))
    out.print()


# ─────────────────────────────────────────────────────────────────────────────
# Clean report — `datadelta clean`
# ─────────────────────────────────────────────────────────────────────────────

def print_clean_report(
    report:  CleanReport,
    source:  str,
    output:  Path | None,
    *,
    dry_run: bool = False,
    console: Console | None = None,
) -> None:
    """
    Print what `datadelta clean` changed (stdout by default).

    `output` is where the result goes (None when there is no destination).
    It is not printed: the destination is a status line, and `datadelta
    clean` confirms it once on stderr ("Saved <path>"). With dry_run the
    report ends with "dry run · nothing written".
    """
    out   = console if console is not None else Console()
    width = out.width
    stats = report.stats

    out.print()
    # Paths are printed unwrapped (soft_wrap) so they stay copy-pasteable.
    header = Text.assemble(_wordmark(), "   ", ("clean", _MUTED), (f" {theme.SEP} ", _MUTED), printable(source))
    out.print(header, soft_wrap=True)
    out.print(_rule(width))
    out.print(_rows_line(stats.rows_before, stats.rows_after, width))

    # ── Actions, grouped by step in execution order ──────────────────────
    labels  = [_column_label(a) for a in report.actions]
    col_w   = min(max((Text(label).cell_len for label in labels), default=0), MAX_COLUMN_W)
    act_w   = max((len(a.action) for a in report.actions), default=0)
    count_w = max((len(f"{a.count:,}") for a in report.actions), default=0)

    out.print()
    if not report.actions:
        out.print(Text(f"{INDENT}nothing to change", style=_MUTED))
    for step in dict.fromkeys(a.step for a in report.actions):
        out.print(Text(step, style=_MUTED))
        for action in (a for a in report.actions if a.step == step):
            out.print(_action_line(out, action, col_w, act_w, count_w))
            example_pad = Text(" " * (len(INDENT) + col_w + 2))
            for before, after in action.examples[:3]:
                out.print(_hang(out, example_pad, _example(action, before, after)))

    # ── Stats ─────────────────────────────────────────────────────────────
    out.print()
    retyped = f" {theme.SEP} ".join(
        f"{printable(col)} {theme.ARROW} {kind}" for col, kind in stats.columns_retyped.items()
    ) or "none"
    nulls = (
        f"{sum(stats.nulls_before.values()):,} {theme.ARROW} "
        f"{sum(stats.nulls_after.values()):,}"
    )
    duplicates = f"{stats.duplicate_rows:,}"
    if stats.duplicate_rows:
        duplicates += f" {theme.SEP} left in place (--dedupe exact drops them)"

    out.print(_stat_line(out, "cells changed",   f"{stats.cells_changed:,}"))
    out.print(_stat_line(out, "columns retyped", retyped))
    out.print(_stat_line(out, "nulls",           nulls))
    out.print(_stat_line(out, "duplicate rows",  duplicates))
    if stats.duplicate_keys is not None:
        out.print(_stat_line(out, "duplicate keys", f"{stats.duplicate_keys:,}"))

    if dry_run:
        out.print(Text(f"{INDENT}dry run {theme.SEP} nothing written", style=_MUTED))
    out.print()


def _column_label(action: CleanAction) -> str:
    return printable(action.column) if action.column is not None else "(table)"


def _action_line(console: Console, action: CleanAction, col_w: int, act_w: int, count_w: int) -> Text:
    """One action: column, action, count, then the note; padding is measured in cells (CJK-aware)."""
    label  = Text(_column_label(action))
    style  = theme.rich_style("ochre") if action.action in ("skipped", "ambiguous", "coerced_to_null") else ""
    prefix = Text.assemble(
        INDENT,
        label,
        " " * (max(col_w - label.cell_len, 0) + 2),
        (action.action.ljust(act_w), style),
        "  ",
        f"{action.count:,}".rjust(count_w),
        "   ",
    )
    return _hang(console, prefix, Text(printable(action.note), style=_MUTED))


def _example(action: CleanAction, before: Any, after: Any) -> Text:
    """A before → after example, or just the blocking value for a skipped action."""
    if action.action == "skipped":
        return Text(printable(_show(before)))
    return Text(printable(f"{_show(before)} {theme.ARROW} {_show(after)}"))


def _show(value: Any) -> str:
    """How an example value is printed: quotes around text, ISO dates, null for missing."""
    if value is None or value is pd.NA or value is pd.NaT or (isinstance(value, float) and math.isnan(value)):
        return "null"
    if isinstance(value, str):
        text = value if len(value) <= EXAMPLE_CHARS else value[:EXAMPLE_CHARS] + "..."
        return repr(text)
    if isinstance(value, pd.Timestamp):
        if value.tz is None and value == value.normalize():
            return value.strftime("%Y-%m-%d")
        return value.isoformat()
    return str(value)


def _stat_line(console: Console, label: str, value: str) -> Text:
    return _hang(console, Text(f"{INDENT}{label.ljust(STAT_LABEL)}", style=_MUTED), Text(value))


# ─────────────────────────────────────────────────────────────────────────────
# Shared pieces
# ─────────────────────────────────────────────────────────────────────────────

def _wordmark() -> Text:
    """The wordmark: "data" in bold ink, the ▲ mark in vermilion."""
    word, mark = theme.BRAND.rsplit(" ", 1)
    return Text.assemble((word, "bold"), " ", (mark, theme.rich_style("vermilion", bold=True)))


def _rule(width: int) -> Text:
    return Text(theme.RULE_CHAR * width, style=theme.rich_style("rule"))


def _rows_line(before: int, after: int, width: int) -> Text:
    """Row counts on the left ("rows     1,000 → 744"), the change on the right ("▼ 256   −25.6%")."""
    left = Text.assemble(
        ("rows".ljust(ROWS_LABEL), _MUTED),
        f"{before:,} {theme.ARROW} {after:,}",
    )
    delta = after - before
    if delta == 0:
        return _spread(left, Text("unchanged", style=_MUTED), width)
    glyph = theme.DELTA_UP if delta > 0 else theme.DELTA_DOWN
    right = Text(f"{glyph} {abs(delta):,}")
    pct   = delta / before if before else math.nan
    if math.isfinite(pct):
        sign = "+" if delta > 0 else theme.MINUS
        right.append(f"   {sign}{abs(pct):.1%}", style=_MUTED)
    return _spread(left, right, width)


def _spread(left: Text, right: Text, width: int) -> Text:
    """`left` and `right` on one line, padded to the console width (at least three spaces apart)."""
    gap = max(width - left.cell_len - right.cell_len, 3)
    return Text.assemble(left, " " * gap, right)


def _hang(console: Console, prefix: Text, body: Text) -> Text:
    """
    prefix + body, with body wrapped to the remaining width and every
    continuation line indented to where the body started. Widths are
    measured in terminal cells (CJK characters count as two).
    """
    indent = prefix.cell_len
    lines  = body.wrap(console, max(console.width - indent, MIN_WRAP))
    out    = Text()
    for i, line in enumerate(lines):
        line.rstrip()
        if i:
            out.append("\n")
            if line.plain:
                out.append(" " * indent)
        else:
            out.append_text(prefix)
        out.append_text(line)
    out.rstrip()
    return out
