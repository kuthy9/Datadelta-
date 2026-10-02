"""
test_cli_watch.py — what a `diff --watch` re-run does.

The watchdog thread calls the diff command again on every file change.
It must hand the command exactly the options of the first run (by
keyword: a positional call once put `export` into the `json_output`
slot and left `clean` at Typer's truthy OptionInfo default), and the
non-zero exit of a re-run (typer.Exit, which is not a SystemExit) must
not kill the thread.

No watchdog and no file events are involved: the re-run helper and the
call site that builds its arguments are exercised directly.
"""

from __future__ import annotations

import inspect

import pytest
import typer

from datadelta import cli as cli_module
from datadelta.cli import Scenario


# ── _rerun ────────────────────────────────────────────────────────────────────

def test_rerun_passes_every_option_by_keyword_with_watch_off():
    calls: list[dict] = []
    params = {
        "before": "a.csv", "after": "b.csv", "scenario": Scenario.etl, "key": None, "threshold": 0.1,
        "metrics_file": None, "no_metrics": True, "story": False, "llm": "openai",
        "export": None, "json_output": False, "clean": False,
    }

    def callback(*args, **kwargs):
        assert args == ()
        calls.append(kwargs)

    cli_module._rerun(callback, params)

    assert calls == [{**params, "watch": False}]
    assert calls[0]["clean"] is False and calls[0]["json_output"] is False
    assert calls[0]["llm"] == "openai"
    assert "watch" not in params                                    # the caller's dict is not modified


@pytest.mark.parametrize("exit_signal", [typer.Exit(code=1), typer.Exit(code=0), SystemExit(1)],
                         ids=["typer.Exit(1)", "typer.Exit(0)", "SystemExit(1)"])
def test_rerun_swallows_the_exit_of_the_diff_command(exit_signal):
    seen: list[dict] = []

    def callback(**kwargs):
        seen.append(kwargs)
        raise exit_signal

    cli_module._rerun(callback, {"before": "a.csv"})                # a FAIL finding must not end the watcher
    cli_module._rerun(callback, {"before": "a.csv"})                # ... and the next change still re-runs
    assert len(seen) == 2


# ── the call site in diff_cmd ─────────────────────────────────────────────────

def _watch_recorder(monkeypatch) -> dict:
    """Replace the watcher with a recorder of what diff_cmd hands it."""
    captured: dict = {}

    def fake(callback, params):
        captured.update(callback=callback, params=params)

    monkeypatch.setattr(cli_module, "_run_watch_mode", fake)
    return captured


def test_diff_cmd_hands_the_watcher_every_option_it_was_given(cli, etl_pair, monkeypatch, tmp_path):
    captured = _watch_recorder(monkeypatch)
    before, after = etl_pair
    report = tmp_path / "report.html"

    result = cli("diff", before, after, "--watch", "--scenario", "etl", "--key", "order_id",
                 "--threshold", "0.2", "--no-metrics", "--llm", "openai", "--export", report)
    assert result.exit_code == 1, result.stderr                     # etl_pair has a FAIL finding

    params = captured["params"]
    assert captured["callback"] is cli_module.diff_cmd
    assert set(params) == set(inspect.signature(cli_module.diff_cmd).parameters) - {"watch"}
    assert params["before"] == str(before) and params["after"] == str(after)
    assert params["scenario"] is Scenario.etl
    assert params["key"] == "order_id"
    assert params["threshold"] == 0.2
    assert params["no_metrics"] is True and params["metrics_file"] is None
    assert params["llm"] == "openai"
    assert params["export"] == report
    assert params["story"] is False and params["json_output"] is False and params["clean"] is False


def test_diff_cmd_hands_the_watcher_the_flags_that_were_set(cli, etl_pair, monkeypatch):
    captured = _watch_recorder(monkeypatch)
    before, after = etl_pair
    cli("diff", before, after, "--watch", "--clean", "--no-metrics", "--story")
    params = captured["params"]
    assert params["clean"] is True and params["story"] is True and params["json_output"] is False


def test_a_rerun_prints_the_same_report_as_the_first_run(cli, etl_pair, monkeypatch):
    """End to end without watchdog: the re-run is the first run again, not JSON and not an exit."""
    before, after = etl_pair
    args = ("diff", before, after, "--scenario", "etl", "--no-metrics")
    first = cli(*args)
    assert first.exit_code == 1, first.stderr                       # FAIL findings: the re-run exits 1 too
    assert not first.stdout.lstrip().startswith("{")

    monkeypatch.setattr(cli_module, "_run_watch_mode", lambda callback, params: cli_module._rerun(callback, params))
    watched = cli(*args, "--watch")

    assert watched.exit_code == 1, watched.stderr
    assert watched.stdout == first.stdout * 2                       # first run + one identical re-run


# ── Without watchdog ──────────────────────────────────────────────────────────

def test_missing_watchdog_is_one_themed_error_line(monkeypatch, capsys):
    """Final review F3: the message uses theme.ERROR_STYLE, not a [red] tag; exit 1."""
    import sys
    monkeypatch.setitem(sys.modules, "watchdog", None)                 # import watchdog → ImportError
    monkeypatch.setitem(sys.modules, "watchdog.observers", None)
    with pytest.raises(typer.Exit) as info:
        cli_module._run_watch_mode(lambda **kwargs: None, {"before": "a.csv", "after": "b.csv"})
    assert info.value.exit_code == 1
    err = capsys.readouterr().err
    assert "watchdog is required for --watch mode." in err
    assert 'pip install "datadelta-cli[watch]"' in err
