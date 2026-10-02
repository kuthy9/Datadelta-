"""
conftest.py — Shared pytest fixtures.

Every CLI test runs inside `isolated_cwd` (a fresh tmp_path) so that a
stray ./metrics.yaml in the developer's checkout is never auto-loaded,
and every file a test writes lands under pytest's tmp_path.

Tests import `datadelta` from the checkout (`pythonpath = ["src"]` in
pyproject.toml) and spawn child interpreters with `subprocess_env`, so none
of them depends on the editable-install .pth file.
"""

from __future__ import annotations

import os
import sys
import types
from dataclasses import dataclass, field
from pathlib     import Path
from typing      import Callable

import pandas as pd
import pytest
from typer.testing import CliRunner, Result


SRC_DIR = Path(__file__).resolve().parents[1] / "src"


# ── Subprocess environment ───────────────────────────────────────────────────

@pytest.fixture
def subprocess_env() -> dict[str, str]:
    """
    The process environment with the checkout's src/ prepended to PYTHONPATH,
    for tests that spawn `python`. pytest's own `pythonpath` setting only
    reaches the test process, not its children.
    """
    existing = os.environ.get("PYTHONPATH")
    parts    = [str(SRC_DIR), existing] if existing else [str(SRC_DIR)]
    return {**os.environ, "PYTHONPATH": os.pathsep.join(parts)}


# ── Working directory ────────────────────────────────────────────────────────

@pytest.fixture
def isolated_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run the test with tmp_path as the current directory."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


# ── Data files ───────────────────────────────────────────────────────────────

@pytest.fixture
def write_csv(tmp_path: Path) -> Callable[[str, pd.DataFrame], Path]:
    """Return a helper that writes a DataFrame to tmp_path/<name> as CSV."""
    def _write(name: str, df: pd.DataFrame) -> Path:
        path = tmp_path / name
        df.to_csv(path, index=False)
        return path
    return _write


@pytest.fixture
def etl_pair(write_csv: Callable[[str, pd.DataFrame], Path]) -> tuple[Path, Path]:
    """
    A before/after pair whose diff contains a FAIL under every scenario.

    before: 100 rows — order_id 1..100, region cycling NA/EMEA/APAC/LATAM,
            revenue 100.0 + i
    after:  the rows with order_id <= 70

    30% of the keys are deleted (> the 10% integrity FAIL line). The
    ab-test lens demotes integrity to WARN, but it promotes the revenue
    mean-shift WARN (-10.0%) to FAIL, so has_failures holds everywhere.
    """
    regions = ["NA", "EMEA", "APAC", "LATAM"]
    before = pd.DataFrame({
        "order_id": list(range(1, 101)),
        "region":   [regions[i % 4] for i in range(100)],
        "revenue":  [100.0 + i for i in range(100)],
    })
    after = before[before["order_id"] <= 70].reset_index(drop=True)
    return write_csv("before.csv", before), write_csv("after.csv", after)


# ── CLI runner ───────────────────────────────────────────────────────────────

def _make_runner() -> CliRunner:
    """
    A CliRunner that captures stdout and stderr separately.
    Click < 8.2 mixes them unless mix_stderr=False; newer Typer/Click
    versions always separate them and no longer accept the argument.
    """
    try:
        return CliRunner(mix_stderr=False)
    except TypeError:
        return CliRunner()


@pytest.fixture
def cli(isolated_cwd: Path) -> Callable[..., Result]:
    """Invoke the datadelta Typer app: cli("diff", a, b, "--json")."""
    from datadelta.cli import app

    runner = _make_runner()

    def _invoke(*args: object) -> Result:
        return runner.invoke(app, [str(a) for a in args])
    return _invoke


# ── Fake LLM SDK ─────────────────────────────────────────────────────────────

@dataclass
class FakeLLM:
    """What a fake LLM SDK received (`calls`, one kwargs dict per request) and answers (`reply`)."""
    reply: str        = "A short narrative about the diff."
    calls: list[dict] = field(default_factory=list)

    @property
    def user_message(self) -> str:
        """Text of the last user message of the most recent request."""
        return self.calls[-1]["messages"][-1]["content"]


@pytest.fixture
def fake_anthropic(monkeypatch: pytest.MonkeyPatch) -> FakeLLM:
    """
    Replace the `anthropic` SDK with an in-memory fake and set a dummy
    ANTHROPIC_API_KEY. Both client.messages.stream(...) (a context manager
    exposing .text_stream) and client.messages.create(...) are recorded in
    FakeLLM.calls and answered with FakeLLM.reply. Nothing touches the network.
    """
    recorder = FakeLLM()

    class _Stream:
        def __init__(self, text: str) -> None:
            self.text_stream = iter([text])

        def __enter__(self) -> "_Stream":
            return self

        def __exit__(self, *exc: object) -> bool:
            return False

    class _Messages:
        def stream(self, **kwargs: object) -> _Stream:
            recorder.calls.append(kwargs)
            return _Stream(recorder.reply)

        def create(self, **kwargs: object) -> types.SimpleNamespace:
            recorder.calls.append(kwargs)
            block = types.SimpleNamespace(type="text", text=recorder.reply)
            return types.SimpleNamespace(content=[block])

    class Anthropic:
        def __init__(self, api_key: str | None = None, **kwargs: object) -> None:
            self.messages = _Messages()

    module = types.ModuleType("anthropic")
    module.Anthropic = Anthropic
    monkeypatch.setitem(sys.modules, "anthropic", module)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")
    return recorder
