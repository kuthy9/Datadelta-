"""
test_story_buffered.py — `--story` collects the whole narrative first.

generate_story() returns a StoryResult (text + provider label) and never
prints the narrative; warnings go to stderr; any provider failure is a
warning plus None, never a crash. The CLI prints the story after the
report and passes it to the HTML export. All LLM SDKs are in-memory
fakes installed with monkeypatch.setitem(sys.modules, ...): no network.
"""

from __future__ import annotations

import sys
import types

import pytest

from datadelta.differ import DiffResult, Finding
from datadelta.story  import StoryResult, generate_story


def _result() -> DiffResult:
    finding = Finding("integrity", "order_id", "FAIL", "30 key(s) deleted from 'order_id' (30.0% of original)", "", {})
    return DiffResult(rows_before=100, rows_after=70, row_delta=-30, row_delta_pct=-0.3, findings=[finding])


def _install_anthropic(monkeypatch, chunks: list[str] | None = None, error: Exception | None = None) -> list[dict]:
    """A fake `anthropic` module whose stream yields `chunks` or raises `error`."""
    calls: list[dict] = []

    class _Stream:
        def __enter__(self):
            if error is not None:
                raise error
            self.text_stream = iter(chunks or [])
            return self

        def __exit__(self, *exc):
            return False

    class _Messages:
        def stream(self, **kwargs):
            calls.append(kwargs)
            return _Stream()

    class Anthropic:
        def __init__(self, api_key=None, **kwargs):
            self.messages = _Messages()

    module = types.ModuleType("anthropic")
    module.Anthropic = Anthropic
    monkeypatch.setitem(sys.modules, "anthropic", module)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")
    return calls


# ── generate_story: buffered, quiet on stdout ─────────────────────────────────

def test_returns_the_full_text_and_prints_nothing_to_stdout(fake_anthropic, capsys):
    fake_anthropic.reply = "Thirty orders vanished between the two extracts."
    story = generate_story(_result(), provider="claude")
    assert story == StoryResult(text="Thirty orders vanished between the two extracts.", label="Claude (Anthropic)")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Thirty orders" not in captured.err


def test_streamed_chunks_are_joined(monkeypatch, capsys):
    _install_anthropic(monkeypatch, chunks=["Rows fell ", "by 30%. ", "Check the loader.\n"])
    story = generate_story(_result(), provider="claude")
    assert story.text == "Rows fell by 30%. Check the loader."
    assert capsys.readouterr().out == ""


def test_provider_exception_is_a_warning_and_none(monkeypatch, capsys):
    _install_anthropic(monkeypatch, error=RuntimeError("boom"))
    assert generate_story(_result(), provider="claude") is None
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "story failed: RuntimeError: boom" in captured.err


def test_empty_reply_is_a_warning_and_none(monkeypatch, capsys):
    _install_anthropic(monkeypatch, chunks=["  ", "\n"])
    assert generate_story(_result(), provider="claude") is None
    assert "story failed: the LLM returned no text" in capsys.readouterr().err


def test_missing_api_key(monkeypatch, capsys):
    _install_anthropic(monkeypatch, chunks=["unused"])
    monkeypatch.delenv("ANTHROPIC_API_KEY")
    assert generate_story(_result(), provider="claude") is None
    captured = capsys.readouterr()
    assert "ANTHROPIC_API_KEY not set" in captured.err
    assert captured.out == ""


def test_unknown_provider(capsys):
    assert generate_story(_result(), provider="llama") is None
    assert "Unknown LLM provider: 'llama'" in capsys.readouterr().err


def test_missing_sdk_names_the_extra_literally(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "openai", None)          # import openai → ImportError
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-not-real")
    assert generate_story(_result(), provider="openai") is None
    err = capsys.readouterr().err
    assert "story skipped" in err
    assert 'pip install "datadelta-cli[openai]"' in err        # [openai] not eaten as markup


# ── Other providers ───────────────────────────────────────────────────────────

def _install_openai(monkeypatch, chunks: list[str | None]) -> list[dict]:
    created: list[dict] = []

    def _chunk(text):
        delta = types.SimpleNamespace(content=text)
        return types.SimpleNamespace(choices=[types.SimpleNamespace(delta=delta)])

    class _Completions:
        def create(self, **kwargs):
            created.append(kwargs)
            return iter([_chunk(c) for c in chunks] + [types.SimpleNamespace(choices=[])])

    class OpenAI:
        def __init__(self, api_key=None, base_url=None):
            created.append({"base_url": base_url})
            self.chat = types.SimpleNamespace(completions=_Completions())

    module = types.ModuleType("openai")
    module.OpenAI = OpenAI
    monkeypatch.setitem(sys.modules, "openai", module)
    return created


@pytest.mark.parametrize("provider, env_key, label, base_url", [
    ("openai",   "OPENAI_API_KEY",   "GPT-4o mini (OpenAI)", None),
    ("deepseek", "DEEPSEEK_API_KEY", "DeepSeek Chat",        "https://api.deepseek.com"),
])
def test_openai_compatible_providers(monkeypatch, capsys, provider, env_key, label, base_url):
    created = _install_openai(monkeypatch, ["Thirty ", None, "keys are gone."])
    monkeypatch.setenv(env_key, "test-key-not-real")
    story = generate_story(_result(), provider=provider)
    assert story == StoryResult(text="Thirty keys are gone.", label=label)
    assert created[0] == {"base_url": base_url}
    assert created[1]["stream"] is True
    assert capsys.readouterr().out == ""


def test_gemini(monkeypatch, capsys):
    configured: list[str] = []

    class GenerativeModel:
        def __init__(self, model_name, system_instruction):
            self.system_instruction = system_instruction

        def generate_content(self, user, stream):
            return iter([types.SimpleNamespace(text="Keys "), types.SimpleNamespace(text="vanished.")])

    genai = types.ModuleType("google.generativeai")
    genai.configure = lambda api_key: configured.append(api_key)
    genai.GenerativeModel = GenerativeModel
    google = types.ModuleType("google")
    google.generativeai = genai
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.generativeai", genai)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key-not-real")

    story = generate_story(_result(), provider="gemini")
    assert story == StoryResult(text="Keys vanished.", label="Gemini 1.5 Flash (Google)")
    assert configured == ["test-key-not-real"]
    assert capsys.readouterr().out == ""


# ── CLI: printed after the report, passed to the export ───────────────────────

def test_cli_prints_the_story_after_the_report(cli, etl_pair, fake_anthropic):
    fake_anthropic.reply = "Thirty orders vanished between the two extracts."
    before, after = etl_pair
    result = cli("diff", before, after, "--story")
    assert result.exit_code == 1
    out = result.stdout
    assert out.index("exit 1") < out.index("story · Claude (Anthropic)") < out.index("Thirty orders vanished")
    assert "  Thirty orders vanished between the two extracts." in out.splitlines()
    assert "narrative via an LLM" not in out                  # --story hint hidden
    assert "shareable HTML report" in out                     # --export hint still shown
    assert "Thirty orders" not in result.stderr


def test_cli_story_markup_is_printed_literally(cli, etl_pair, fake_anthropic):
    fake_anthropic.reply = "[bold]Thirty[/bold] keys left. [/]"
    before, after = etl_pair
    result = cli("diff", before, after, "--story")
    assert "  [bold]Thirty[/bold] keys left. [/]" in result.stdout.splitlines()


def test_cli_story_failure_keeps_the_report(cli, etl_pair, monkeypatch):
    _install_anthropic(monkeypatch, error=RuntimeError("quota exceeded"))
    before, after = etl_pair
    result = cli("diff", before, after, "--story")
    assert result.exit_code == 1                              # still the FAIL exit code
    assert "deleted" in result.stdout
    assert "story ·" not in result.stdout
    assert "story failed: RuntimeError: quota exceeded" in result.stderr


def test_cli_story_reaches_the_html_export(cli, etl_pair, fake_anthropic, tmp_path):
    fake_anthropic.reply = "Thirty orders vanished <quietly>."
    before, after = etl_pair
    out = tmp_path / "report.html"
    result = cli("diff", before, after, "--story", "--export", out)
    assert result.exit_code == 1
    html = out.read_text(encoding="utf-8")
    assert "Thirty orders vanished &lt;quietly&gt;." in html
    assert "--export" not in result.stdout and "--story" not in result.stdout
