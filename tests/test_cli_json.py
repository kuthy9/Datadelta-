"""
test_cli_json.py — The `--json` contract.

stdout carries exactly one strictly valid JSON document; every status
line goes to stderr; the exit code is 1 whenever a FAIL finding exists,
exactly as in terminal mode.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest


def _strict_loads(text: str):
    """json.loads that rejects NaN / Infinity / -Infinity like jq does."""
    def _reject(token: str):
        raise ValueError(f"non-standard JSON constant: {token}")
    return json.loads(text, parse_constant=_reject)


# ── --json ────────────────────────────────────────────────────────────────────

def test_json_stdout_is_one_strict_json_document(cli, etl_pair):
    before, after = etl_pair
    result = cli("diff", before, after, "--json")
    payload = _strict_loads(result.stdout)
    assert payload["summary"]["severity"] == "FAIL"
    assert payload["summary"]["rows_before"] == 100
    assert payload["summary"]["rows_after"] == 70


def test_json_exits_1_when_a_fail_exists(cli, etl_pair):
    before, after = etl_pair
    result = cli("diff", before, after, "--json")
    assert result.exit_code == 1


def test_json_exits_0_without_fail(cli, etl_pair):
    before, _ = etl_pair
    result = cli("diff", before, before, "--json")
    assert result.exit_code == 0
    assert _strict_loads(result.stdout)["summary"]["severity"] == "PASS"


def test_json_status_lines_go_to_stderr(cli, etl_pair):
    before, after = etl_pair
    result = cli("diff", before, after, "--json")
    assert "[datadelta] load before done" in result.stderr
    assert "[datadelta] load after done" in result.stderr
    assert "[datadelta]" not in result.stdout


def test_json_with_export_still_writes_html(cli, etl_pair, tmp_path):
    before, after = etl_pair
    out = tmp_path / "report.html"
    result = cli("diff", before, after, "--json", "--export", out)
    assert result.exit_code == 1
    _strict_loads(result.stdout)
    assert out.exists()
    assert "<html" in out.read_text(encoding="utf-8")
    assert "Report saved to" in result.stderr


def test_json_with_story_is_ignored_with_a_notice(cli, etl_pair, monkeypatch):
    def _fail_if_called(*args, **kwargs):
        raise AssertionError("generate_story must not run in --json mode")
    monkeypatch.setattr("datadelta.story.generate_story", _fail_if_called)

    before, after = etl_pair
    result = cli("diff", before, after, "--json", "--story")
    assert result.exit_code == 1
    _strict_loads(result.stdout)
    assert "--story is ignored in --json mode" in result.stderr


def test_json_writes_null_for_nan_metric_values(cli, etl_pair, isolated_cwd):
    (isolated_cwd / "metrics.yaml").write_text(
        "metrics:\n"
        "  - name: always_nan\n"
        "    type: custom\n"
        "    expression: \"np.nan\"\n",
        encoding="utf-8",
    )
    before, after = etl_pair
    result = cli("diff", before, after, "--json")
    payload = _strict_loads(result.stdout)
    custom = [f for f in payload["findings"] if f["layer"] == "custom"]
    assert len(custom) == 1
    assert custom[0]["metric"]["value_before"] is None
    assert custom[0]["metric"]["value_after"] is None


# ── terminal mode ─────────────────────────────────────────────────────────────

def test_terminal_mode_exits_1_on_fail_and_keeps_status_off_stdout(cli, etl_pair):
    before, after = etl_pair
    result = cli("diff", before, after)
    assert result.exit_code == 1
    assert "deleted" in result.stdout
    assert "[datadelta] load before done" in result.stderr
    assert "[datadelta]" not in result.stdout


def test_load_error_goes_to_stderr_and_exits_1(cli, etl_pair):
    _, after = etl_pair
    result = cli("diff", "does_not_exist.csv", after)
    assert result.exit_code == 1
    assert "Error loading data" in result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("mode", [[], ["--json"]])
def test_unwritable_export_path_exits_1(cli, etl_pair, tmp_path, mode):
    before, after = etl_pair
    out = tmp_path / "missing_dir" / "report.html"
    result = cli("diff", before, after, *mode, "--export", out)
    assert result.exit_code == 1
    assert "Could not write" in result.stderr
    assert not out.exists()


def test_failed_export_footer_says_exit_1(cli, write_csv, tmp_path):
    """Review: the footer printed `exit 0` while the process exited 1."""
    same = write_csv("same.csv", pd.DataFrame({"id": [1, 2, 3], "v": [1.0, 2.0, 3.0]}))
    result = cli("diff", same, same, "--no-metrics", "--export", tmp_path / "missing_dir" / "r.html")
    assert result.exit_code == 1
    footer = next(line for line in result.stdout.splitlines() if line.startswith("  0 fail"))
    assert footer.rstrip().endswith("exit 1")
