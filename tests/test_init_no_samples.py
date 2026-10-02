"""
test_init_no_samples.py — What `datadelta init` sends to the LLM, and
how it fails.

By default each column summary carries its distinct count, null rate and
up to 5 sample values; with --no-samples only column names and dtypes
leave the machine. The anthropic SDK is replaced by the `fake_anthropic`
fixture, so nothing touches the network.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from datadelta.metrics_generator import INIT_SYSTEM_PROMPT, _summarize_columns


VALID_YAML = """business_context: |
  Orders placed through the web shop.

metrics:
  - name: code_completeness
    description: "code must always be present"
    type: completeness
    column: code
    warn_if_below: 0.99
"""


@pytest.fixture
def sample_csv(write_csv):
    return write_csv("sample.csv", pd.DataFrame({
        "code":   ["SECRET-VALUE-123", "SECRET-VALUE-456", "SECRET-VALUE-789"],
        "amount": [98765.4321, 12.5, 7.25],
        "day":    ["2024-01-05", "2024-01-06", "2024-01-07"],
    }))


def _column_summary(message: str) -> list[dict]:
    """Pull the JSON column list back out of the init user message."""
    body = message.split("Dataset columns:\n", 1)[1]
    body = body.split("\n\nGenerate the metrics.yaml file now.", 1)[0]
    return json.loads(body)


# ── CLI ───────────────────────────────────────────────────────────────────────

def test_no_samples_sends_no_cell_values(cli, fake_anthropic, sample_csv, isolated_cwd):
    fake_anthropic.reply = VALID_YAML
    result = cli("init", "--from", sample_csv, "--business", "We run a web shop.", "--no-samples")

    assert result.exit_code == 0, result.stderr
    assert len(fake_anthropic.calls) == 1
    sent = fake_anthropic.user_message
    assert "SECRET-VALUE" not in sent
    assert "98765" not in sent
    assert "2024-01-05" not in sent

    columns = _column_summary(sent)
    assert [c["name"] for c in columns] == ["code", "amount", "day"]
    # Spec 2.4: only column names and dtypes, no statistics computed from the values.
    assert [set(c) for c in columns] == [{"name", "dtype"}] * 3
    assert (isolated_cwd / "metrics.yaml").read_text(encoding="utf-8") == VALID_YAML


def test_default_still_sends_samples(cli, fake_anthropic, sample_csv):
    fake_anthropic.reply = VALID_YAML
    result = cli("init", "--from", sample_csv, "--business", "We run a web shop.")

    assert result.exit_code == 0, result.stderr
    columns = {c["name"]: c for c in _column_summary(fake_anthropic.user_message)}
    assert columns["code"]["samples"] == ["SECRET-VALUE-123", "SECRET-VALUE-456", "SECRET-VALUE-789"]
    assert columns["amount"]["samples"][0] == 98765.4321
    # Date columns load as datetime64; their samples must still serialize.
    assert columns["day"]["samples"][0].startswith("2024-01-05")


def test_init_status_goes_to_stderr(cli, fake_anthropic, sample_csv):
    fake_anthropic.reply = VALID_YAML
    result = cli("init", "--from", sample_csv, "--business", "We run a web shop.", "--no-samples")
    assert result.exit_code == 0
    assert result.stdout == ""
    # Final review F3: a plain confirmation line (as `clean` prints), not a green box.
    assert "Saved metrics.yaml" in result.stderr.splitlines()
    assert "datadelta diff before.csv after.csv" in result.stderr
    assert not any(glyph in result.stderr for glyph in "╭╮╰╯│")


def test_help_lists_no_samples(cli):
    result = cli("init", "--help")
    assert result.exit_code == 0
    assert "--no-samples" in result.stdout


# ── Failures exit 1 ───────────────────────────────────────────────────────────

def test_missing_api_key_exits_1(cli, fake_anthropic, sample_csv, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY")
    result = cli("init", "--from", sample_csv, "--business", "We run a web shop.")

    assert result.exit_code == 1
    assert "ANTHROPIC_API_KEY not set" in result.stderr
    assert fake_anthropic.calls == []


def test_unloadable_sample_exits_1(cli, fake_anthropic, isolated_cwd):
    result = cli("init", "--from", "missing.csv", "--business", "We run a web shop.")

    assert result.exit_code == 1
    assert "Failed to load file" in result.stderr
    assert fake_anthropic.calls == []
    assert list(isolated_cwd.iterdir()) == []


def test_invalid_generated_yaml_exits_1_and_keeps_a_draft(cli, fake_anthropic, sample_csv, isolated_cwd):
    fake_anthropic.reply = "Sorry, here are some thoughts instead of YAML."
    result = cli("init", "--from", sample_csv, "--business", "We run a web shop.")

    assert result.exit_code == 1
    assert "Generated YAML failed validation" in result.stderr
    assert not (isolated_cwd / "metrics.yaml").exists()
    draft = isolated_cwd / "metrics_draft.yaml"
    assert draft.read_text(encoding="utf-8") == fake_anthropic.reply


def _failing_sdk(monkeypatch, error: Exception, *, mid_stream: bool = False) -> None:
    """Replace the fake SDK's client with one whose request fails (a bad key, a network error)."""
    import sys

    class _Stream:
        def __enter__(self):
            if not mid_stream:
                raise error
            return self

        def __exit__(self, *exc):
            return False

        @property
        def text_stream(self):
            yield "metrics:\n"
            raise error

    class _Messages:
        def stream(self, **kwargs):
            return _Stream()

    class Anthropic:
        def __init__(self, **kwargs):
            self.messages = _Messages()

    monkeypatch.setattr(sys.modules["anthropic"], "Anthropic", Anthropic)


@pytest.mark.parametrize("mid_stream", [False, True], ids=["on-request", "mid-stream"])
def test_llm_request_failure_is_a_one_line_error(cli, fake_anthropic, sample_csv, isolated_cwd, monkeypatch, mid_stream):
    """Review: a wrong or expired key printed a 48-line traceback; --story handles the same error in one line."""
    class AuthenticationError(Exception):
        pass

    _failing_sdk(monkeypatch, AuthenticationError("invalid x-api-key"), mid_stream=mid_stream)
    result = cli("init", "--from", sample_csv, "--business", "We run a web shop.")

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)                 # handled, not a traceback
    assert "AuthenticationError: invalid x-api-key" in result.stderr
    assert sorted(p.name for p in isolated_cwd.iterdir()) == ["sample.csv"]


def test_unwritable_output_is_a_one_line_error(cli, fake_anthropic, sample_csv, isolated_cwd):
    fake_anthropic.reply = VALID_YAML
    result = cli("init", "--from", sample_csv, "--business", "We run a web shop.",
                 "--output", isolated_cwd / "missing-dir" / "metrics.yaml")

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "Could not write" in result.stderr
    assert "missing-dir" in result.stderr


# ── Unit level ────────────────────────────────────────────────────────────────

def test_summarize_columns_include_samples_flag():
    df = pd.DataFrame({"code": ["A1", None, "B2"], "n": [1, 2, 3]})

    with_samples = _summarize_columns(df)
    assert set(with_samples[0]) == {"name", "dtype", "n_unique", "null_pct", "samples"}
    assert with_samples[0]["n_unique"] == 2
    assert with_samples[0]["null_pct"] == 0.333
    assert with_samples[0]["samples"] == ["A1", "B2"]
    assert with_samples[1]["samples"] == [1, 2, 3]
    assert type(with_samples[1]["samples"][0]) is int

    without = _summarize_columns(df, include_samples=False)
    assert without == [
        {"name": "code", "dtype": str(df["code"].dtype)},
        {"name": "n",    "dtype": str(df["n"].dtype)},
    ]


def test_system_prompt_allows_missing_samples_and_states_expression_rules():
    assert "may have no `samples` field" in INIT_SYSTEM_PROMPT
    assert "np.log1p" in INIT_SYSTEM_PROMPT
    assert "pd.to_datetime" in INIT_SYSTEM_PROMPT
