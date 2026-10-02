"""
test_dashboard.py — The live dashboard, its plain-text fallback,
make_progress(), and the CLI wiring: stage lines on stderr, stdout left
to the report or the JSON document, --quiet silent unless something fails.

CliRunner's stderr is not a terminal, so every CLI test here sees
PlainProgress lines ("[datadelta] <label> <status> ... <secs>s").
"""

from __future__ import annotations

import io
import json
import re
import warnings

import pandas as pd
import pytest
from rich.cells   import cell_len
from rich.console import Console

from datadelta           import dashboard, theme
from datadelta.dashboard import LiveDashboard, PlainProgress, make_progress
from datadelta.differ    import DiffResult, _datetime_diff
from datadelta.profiler  import profile_columns
from datadelta.progress  import NullProgress
from datadelta.story     import generate_story


PLAIN_LINE = re.compile(r"^\[datadelta\] (?P<label>.+?) (?P<status>done|skipped|failed)\b")


@pytest.fixture(autouse=True)
def _plain_environment(monkeypatch):
    """
    FORCE_COLOR or TTY_COMPATIBLE in the developer's shell would make Rich
    treat captured stderr as a terminal; TERM=dumb (some CI runners) would
    stop make_progress from choosing the live view. Tests that need one of
    these set it themselves.
    """
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.delenv("TTY_COMPATIBLE", raising=False)
    monkeypatch.setenv("TERM", "xterm-256color")


class FakeClock:
    """Replaces dashboard._now so timings and the spinner frame are deterministic."""
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(dashboard, "_now", fake)
    return fake


def _recording_console(width: int = 80) -> Console:
    return Console(record=True, width=width, color_system=None, file=io.StringIO())


def _render_lines(dash: LiveDashboard) -> list[str]:
    console = _recording_console()
    console.print(dash.render())
    return console.export_text().splitlines()


def _stage_labels(stderr: str) -> list[str]:
    return [m.group("label") for m in map(PLAIN_LINE.match, stderr.splitlines()) if m]


# ── LiveDashboard.render() ────────────────────────────────────────────────────

def test_render_shows_header_stages_bar_and_tally(clock):
    dash = LiveDashboard("diff · ETL Validation", console=_recording_console())
    dash.stage_start("load.before", "load before", note="etl_before.csv")
    clock.t += 0.2
    dash.stage_end("load.before", "done", summary="1,000 rows · 5 cols")
    dash.stage_start("distribution", "distribution", total=12, note="order_id")
    dash.advance("distribution", 3, note="revenue")
    dash.finding("FAIL")
    dash.finding("WARN")
    dash.finding("WARN")
    clock.t += 1.3

    lines = _render_lines(dash)
    assert lines[0].startswith("data ▲   diff · ETL Validation")
    assert lines[0].endswith("00:01.5")
    assert lines[1] == "─" * 80

    load = next(line for line in lines if "load before" in line)
    assert load.startswith("✓ load before")
    assert "1,000 rows · 5 cols" in load
    assert load.endswith("0.2s")

    dist = next(line for line in lines if "distribution" in line)
    assert dist[0] == "◐"                                   # int(1.5 s * 8) % 4 == 0
    assert "━" * 6 + "─" * 18 + " 3/12" in dist             # 3 of 12 → 6 of 24 cells
    assert "revenue" in dist                                # the latest note
    assert dist.endswith("1.3s")

    assert lines[-2] == "─" * 80
    assert lines[-1] == "▲ 1 fail   ● 2 warn   · 0 info"
    assert all(cell_len(line) <= 80 for line in lines)


def test_spinner_cycles_through_the_frames(clock):
    dash = LiveDashboard("t", console=_recording_console())
    dash.stage_start("schema", "schema")
    frames = []
    for _ in range(4):
        frames.append(next(line for line in _render_lines(dash) if "schema" in line)[0])
        clock.t += 1 / 8
    assert tuple(frames) == theme.SPINNER_FRAMES


def test_finished_failed_and_skipped_rows(clock):
    dash = LiveDashboard("t", console=_recording_console())
    for key, status, summary in [
        ("schema",    "done",    "0 changes"),
        ("integrity", "skipped", "no key column"),
        ("export",    "failed",  "Permission denied"),
    ]:
        dash.stage_start(key, key)
        dash.stage_end(key, status, summary=summary)
    lines = _render_lines(dash)
    assert next(line for line in lines if "schema" in line).startswith("✓ schema")
    assert next(line for line in lines if "integrity" in line).startswith("– integrity")
    assert "no key column" in next(line for line in lines if "integrity" in line)
    assert next(line for line in lines if "export" in line).startswith("× export")
    assert "Permission denied" in next(line for line in lines if "export" in line)


def test_tally_replaces_the_running_counts(clock):
    dash = LiveDashboard("t", console=_recording_console())
    for _ in range(3):
        dash.finding("FAIL")
    dash.tally({"FAIL": 1, "WARN": 0, "INFO": 4, "PASS": 2})
    assert _render_lines(dash)[-1] == "▲ 1 fail   ● 0 warn   · 4 info"


def test_markup_and_cjk_in_rows_print_literally_and_stay_aligned(clock):
    # Review Focus 4: data-derived notes and titles are never parsed as markup.
    dash = LiveDashboard("clean · [red]订单[/red].csv", console=_recording_console())
    dash.stage_start("distribution", "distribution", total=3)
    dash.advance("distribution", 1, note="[bold]x")
    dash.stage_start("load", "load", note="地区.csv")
    dash.stage_end("load", "done", summary="<b>y</b> 地区 · 华东")
    lines = _render_lines(dash)
    assert lines[0].startswith("data ▲   clean · [red]订单[/red].csv")
    assert "[bold]x" in next(line for line in lines if "distribution" in line)
    assert "<b>y</b> 地区 · 华东" in next(line for line in lines if line.startswith("✓ load"))
    rows = [line for line in lines if line.startswith(("◐", "✓"))]
    assert [cell_len(line) for line in rows] == [80, 80]    # the seconds column ends flush right


# ── LiveDashboard as a context manager ────────────────────────────────────────

def _terminal_console() -> tuple[Console, io.StringIO]:
    buffer = io.StringIO()
    return Console(file=buffer, force_terminal=True, color_system=None, width=80), buffer


def test_context_manager_collapses_into_one_line(clock):
    console, buffer = _terminal_console()
    with LiveDashboard("t", console=console) as dash:
        dash.stage_start("load", "load")
        dash.stage_end("load", "done")
        dash.stage_start("integrity", "integrity")
        dash.stage_end("integrity", "skipped", summary="no key column")
        clock.t += 1.5
    assert buffer.getvalue().rstrip().endswith("✓ 2 stages · 1.5s")


def test_context_manager_names_the_failed_stage(clock):
    console, buffer = _terminal_console()
    with pytest.raises(RuntimeError):
        with LiveDashboard("t", console=console) as dash:
            dash.stage_start("load.before", "load before")
            raise RuntimeError("boom")
    assert dash.stages["load.before"].status == "failed"
    assert dash.stages["load.before"].summary == "boom"
    assert buffer.getvalue().rstrip().endswith("× failed at load before")


# ── PlainProgress ─────────────────────────────────────────────────────────────

def test_plain_progress_prints_one_line_per_finished_stage(clock):
    console = _recording_console()
    with PlainProgress(console=console) as plain:
        plain.stage_start("load.before", "load before", note="a.csv")
        clock.t += 0.3
        plain.stage_end("load.before", "done", summary="100 rows · 3 cols")
        plain.stage_start("distribution", "distribution", total=12)
        plain.advance("distribution", 12, note="revenue")
        clock.t += 0.4
        plain.stage_end("distribution", "done")
        plain.stage_start("integrity", "integrity")
        plain.stage_end("integrity", "skipped", summary="no key column")
        plain.finding("FAIL")
        plain.tally({"FAIL": 1})
    assert console.export_text().splitlines() == [
        "[datadelta] load before done 100 rows · 3 cols 0.3s",
        "[datadelta] distribution done 12/12 0.4s",
        "[datadelta] integrity skipped no key column 0.0s",
    ]


def test_plain_progress_prints_markup_literally(clock):
    console = _recording_console()
    plain = PlainProgress(console=console)
    plain.stage_start("load", "load", note="[bold]x.csv")
    plain.stage_end("load", "failed", summary="File not found: [red]地区[/red].csv")
    assert console.export_text().splitlines() == [
        "[datadelta] load failed File not found: [red]地区[/red].csv 0.0s",
    ]


# ── make_progress ─────────────────────────────────────────────────────────────

def test_make_progress_quiet_is_a_null_sink():
    with make_progress("t", quiet=True) as sink:
        assert isinstance(sink, NullProgress)


def test_make_progress_without_a_terminal_is_plain():
    console = Console(file=io.StringIO())
    with make_progress("t", console=console) as sink:
        assert isinstance(sink, PlainProgress)
        assert sink.console is console


def test_make_progress_on_a_terminal_is_live():
    console = Console(file=io.StringIO(), force_terminal=True, width=80)
    progress = make_progress("diff · ETL Validation", console=console)
    assert isinstance(progress, LiveDashboard)
    assert progress.title == "diff · ETL Validation"
    assert progress.console is console


def test_make_progress_defaults_to_stderr():
    progress = make_progress("t")
    assert isinstance(progress, (PlainProgress, LiveDashboard))
    assert progress.console.stderr is True


@pytest.mark.parametrize("name", ["FORCE_COLOR", "TTY_COMPATIBLE"])
def test_forced_color_on_a_pipe_stays_plain(monkeypatch, name):
    # CI often forces color. Rich then calls a pipe a terminal, but a Live
    # view on a pipe writes cursor-control sequences into the log.
    monkeypatch.setenv(name, "1")
    console = Console(file=io.StringIO())
    assert console.is_terminal                              # what Rich alone concludes
    with make_progress("t", console=console) as sink:
        assert isinstance(sink, PlainProgress)


def test_a_dumb_terminal_stays_plain(monkeypatch):
    monkeypatch.setenv("TERM", "dumb")
    console = Console(file=io.StringIO(), force_terminal=True, width=80)
    assert isinstance(make_progress("t", console=console), PlainProgress)


# ── CLI: diff ─────────────────────────────────────────────────────────────────

DIFF_STAGES = ["load before", "load after", "profile", "schema", "distribution", "integrity"]


def test_json_stdout_stays_json_and_stages_go_to_stderr(cli, etl_pair):
    before, after = etl_pair
    result = cli("diff", before, after, "--json")
    assert result.exit_code == 1
    assert json.loads(result.stdout)["summary"]["severity"] == "FAIL"
    assert _stage_labels(result.stderr) == DIFF_STAGES
    assert "Loading" not in result.stderr
    assert "[datadelta] load before done 100 rows · 3 cols" in result.stderr
    assert "[datadelta] distribution done 3/3" in result.stderr


def test_terminal_report_follows_the_stages(cli, etl_pair, tmp_path):
    before, after = etl_pair
    out = tmp_path / "report.html"
    result = cli("diff", before, after, "--export", out)
    assert result.exit_code == 1
    assert _stage_labels(result.stderr) == DIFF_STAGES + ["export"]
    assert "[datadelta] export done" in result.stderr
    assert result.stderr.splitlines()[-1] == f"Report saved to {out}"
    assert result.stdout.lstrip().startswith("data ▲")
    assert "[datadelta]" not in result.stdout


def test_clean_and_metrics_add_their_stages(cli, etl_pair, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text(
        "metrics:\n"
        "  - name: big_orders\n"
        "    type: custom\n"
        "    expression: \"(df['revenue'] > 150).mean()\"\n",
        encoding="utf-8",
    )
    before, after = etl_pair
    result = cli("diff", before, after, "--clean", "--json")
    json.loads(result.stdout)
    assert _stage_labels(result.stderr) == [
        "load before", "load after", "clean before", "clean after",
        "profile", "schema", "distribution", "integrity", "metrics",
    ]
    assert "[datadelta] metrics done 1/1 1 metric" in result.stderr


def test_story_is_a_stage_and_prints_after_the_report(cli, etl_pair, fake_anthropic):
    before, after = etl_pair
    result = cli("diff", before, after, "--story")
    assert _stage_labels(result.stderr)[-1] == "story"
    assert "[datadelta] story done Claude (Anthropic)" in result.stderr
    assert result.stdout.index("data ▲") < result.stdout.index("A short narrative about the diff.")


def test_story_without_a_key_is_a_skipped_stage(cli, etl_pair, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    before, after = etl_pair
    result = cli("diff", before, after, "--story")
    assert "[datadelta] story skipped no story" in result.stderr
    assert "ANTHROPIC_API_KEY not set" in result.stderr


def test_story_warnings_go_to_the_console_passed_in(monkeypatch):
    # The CLI passes the dashboard's console, so a warning prints above the live view.
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    console = _recording_console()
    empty   = DiffResult(rows_before=1, rows_after=1, row_delta=0, row_delta_pct=0.0)
    assert generate_story(empty, provider="claude", console=console) is None
    assert "ANTHROPIC_API_KEY not set" in console.export_text()


def test_load_failure_is_a_failed_stage(cli, etl_pair):
    _, after = etl_pair
    result = cli("diff", "missing.csv", after)
    assert result.exit_code == 1
    assert result.stderr.splitlines()[0].startswith("[datadelta] load before failed File not found: missing.csv")
    assert "Error loading data" in result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("mode", [[], ["--json"]])
def test_unwritable_export_still_prints_the_output_then_exits_1(cli, etl_pair, tmp_path, mode):
    # The same file on both sides: no FAIL, so the exit code 1 comes from the export alone.
    before, _ = etl_pair
    out = tmp_path / "missing" / "r.html"
    result = cli("diff", before, before, *mode, "--export", out)
    assert result.exit_code == 1
    assert "[datadelta] export failed" in result.stderr
    assert "Could not write" in result.stderr
    assert "Report saved to" not in result.stderr
    assert not out.exists()
    if mode:
        assert json.loads(result.stdout)["summary"]["rows_before"] == 100
    else:
        assert result.stdout.lstrip().startswith("data ▲")


@pytest.mark.filterwarnings("error::UserWarning")
@pytest.mark.parametrize("mode", [[], ["--json"]])
def test_quiet_prints_nothing_on_stderr(cli, etl_pair, tmp_path, mode):
    before, after = etl_pair
    same = cli("diff", before, before, "--quiet", *mode)
    assert same.exit_code == 0
    assert same.stderr == ""

    out = tmp_path / "report.html"
    failing = cli("diff", before, after, "-q", "--export", out, *mode)
    assert failing.exit_code == 1                          # a FAIL is a result, not an error
    assert failing.stderr == ""
    assert out.exists()
    assert failing.stdout.strip()                          # the report / JSON is still printed


@pytest.mark.filterwarnings("error::UserWarning")
def test_quiet_drops_warnings_and_notes(cli, etl_pair, isolated_cwd):
    # Spec 4.4: --quiet prints no non-error output. A rule naming a missing
    # column and the dedupe/impute note are warnings, not errors.
    (isolated_cwd / "metrics.yaml").write_text(
        "cleaning:\n  case: {lower: [regoin]}\n  dedupe: {mode: exact}\n",
        encoding="utf-8",
    )
    before, after = etl_pair
    loud = cli("diff", before, after, "--clean", "--json")
    assert "before: case.lower names column 'regoin'" in loud.stderr     # what --quiet drops
    assert "reflect cleaned data" in loud.stderr

    for mode in ([], ["--json"]):
        quiet = cli("diff", before, after, "--clean", "--quiet", *mode)
        assert quiet.exit_code == 1                        # the integrity FAIL still sets it
        assert quiet.stderr == ""
        assert quiet.stdout.strip()


def test_quiet_drops_the_story_notices(cli, etl_pair, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    before, after = etl_pair

    terminal = cli("diff", before, after, "--story", "--quiet")
    assert terminal.exit_code == 1
    assert terminal.stderr == ""                           # no "ANTHROPIC_API_KEY not set" warning
    assert terminal.stdout.lstrip().startswith("data ▲")

    piped = cli("diff", before, after, "--json", "--story", "--quiet")
    assert piped.exit_code == 1
    assert piped.stderr == ""                              # no "--story is ignored in --json mode"
    json.loads(piped.stdout)


def test_quiet_still_reports_errors(cli, etl_pair):
    _, after = etl_pair
    result = cli("diff", "missing.csv", after, "--quiet")
    assert result.exit_code == 1
    assert "Error loading data" in result.stderr
    assert "[datadelta]" not in result.stderr


# ── CLI: clean ────────────────────────────────────────────────────────────────

@pytest.fixture
def messy(write_csv):
    return write_csv("messy.csv", pd.DataFrame({"amount": ["$1", "N/A", "$3"], "region": [" EMEA", "APAC", "NA "]}))


def test_clean_command_stages(cli, messy):
    result = cli("clean", messy)
    assert result.exit_code == 0, result.stderr
    assert _stage_labels(result.stderr) == ["load", "clean", "write"]
    assert "[datadelta] write done 3 rows" in result.stderr
    assert result.stderr.splitlines()[-1].startswith("Saved ")


def test_clean_dry_run_has_no_write_stage(cli, messy):
    result = cli("clean", messy, "--dry-run", "--json")
    assert result.exit_code == 0, result.stderr
    json.loads(result.stdout)
    assert _stage_labels(result.stderr) == ["load", "clean"]


def test_clean_quiet_prints_nothing_on_stderr(cli, messy, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text("cleaning:\n  case: {lower: [regoin]}\n", encoding="utf-8")
    loud = cli("clean", messy, "--dry-run")
    assert "names column 'regoin'" in loud.stderr          # the warning --quiet drops

    result = cli("clean", messy, "--quiet")
    assert result.exit_code == 0
    assert result.stderr == ""
    assert messy.with_name("messy.clean.csv").exists()


# ── pandas date probes stay off stderr (--quiet) ──────────────────────────────
# pandas warns "Could not infer format, so each element will be parsed
# individually" on stderr when the first value of a text column is not a
# date. The profiler and the datetime layer probe text columns with
# pd.to_datetime(errors="coerce"), so they must silence that warning.

def _text(values: list[str]) -> pd.Series:
    return pd.Series(values * 10, dtype=object)


def test_profiling_text_columns_raises_no_warning():
    df = pd.DataFrame({
        "region": _text(["NA", "EMEA", "APAC"]),
        "mixed":  _text(["x", "2024-01-05", "05/01/2024"]),
    })
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        profile = profile_columns(df, df)
    assert profile.columns["region"].semantic_type == "category"
    assert profile.columns["mixed"].semantic_type  == "category"    # 2 of 3 parse: below the 80% bar


def test_date_range_of_text_dates_raises_no_warning():
    before = _text(["x", "2024-01-05", "2024-03-01"])
    after  = _text(["x", "2024-01-05", "2024-06-30"])
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        findings = _datetime_diff("shipped", before, after)
    assert [f.title for f in findings] == ["Date range changed: 'shipped'"]
    assert findings[0].metric["max_after"] == "2024-06-30"
