"""
test_packaging.py — The package is importable as `datadelta`, has one
version source, and ships the `datadelta` console script.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

import datadelta


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_version_is_single_sourced():
    assert datadelta.__version__ == "0.3.0"


def test_version_flag_prints_brand_and_version(cli):
    result = cli("--version")
    assert result.exit_code == 0
    assert result.stdout.strip() == f"data ▲ datadelta {datadelta.__version__}"


def test_help_names_the_new_command(cli):
    result = cli("--help")
    assert result.exit_code == 0
    assert "datadiff" not in result.stdout
    assert "diff" in result.stdout
    assert "init" in result.stdout


def test_diff_help_has_no_doubled_percent(cli):
    result = cli("diff", "--help")
    assert result.exit_code == 0
    # Rich wraps help text inside a box; flatten it before searching.
    flat = " ".join(result.stdout.replace("│", " ").split())
    assert "%%" not in flat
    assert "10 percent" in flat


def test_no_legacy_name_left_in_code_or_examples():
    files = sorted((PROJECT_ROOT / "src" / "datadelta").glob("*.py"))
    files += sorted((PROJECT_ROOT / "examples").glob("*.py"))
    files += sorted((PROJECT_ROOT / "examples").glob("*.yaml"))
    offenders = [p.name for p in files if "datadiff" in p.read_text(encoding="utf-8")]
    assert offenders == []


def test_python_dash_m_runs_the_cli(tmp_path, subprocess_env):
    proc = subprocess.run(
        [sys.executable, "-m", "datadelta", "--help"],
        capture_output = True,
        text           = True,
        cwd            = tmp_path,
        env            = subprocess_env,
        timeout        = 60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "datadelta" in proc.stdout


# ── uv offline build ─────────────────────────────────────────────────────────

def _backend_unavailable_offline(stderr: str) -> bool:
    """
    True only when `uv build --offline` failed because uv could not fetch the
    build backend (hatchling is not in its cache and the network is disabled).
    Both markers come from uv's resolver message; any other failure, including
    one raised by the backend itself, must stay a test failure.
    """
    return (
        "Failed to resolve requirements from `build-system.requires`" in stderr
        and "network was disabled" in stderr
    )


# What `uv build --offline` prints when its cache has no hatchling (captured
# with an empty UV_CACHE_DIR; the project path is generalized).
UV_OFFLINE_NO_BACKEND = """\
Building wheel...
  × Failed to build `/project`
  ├─▶ Failed to resolve requirements from `build-system.requires`
  ├─▶ No solution found when resolving: `hatchling`
  ╰─▶ Because hatchling was not found in the cache and you require hatchling,
      we can conclude that your requirements are unsatisfiable.

      hint: Packages were unavailable because the network was disabled. When
      the network is disabled, registry packages may only be read from the
      cache.
"""

# What a real backend failure prints (a `[tool.hatch.version] path` that does
# not exist); it mentions hatchling in the traceback and in the summary line.
UV_BACKEND_FAILURE = """\
Building wheel...
Traceback (most recent call last):
  File "<string>", line 11, in <module>
  File "/cache/builds/lib/python3.11/site-packages/hatchling/build.py", line 58, in build_wheel
    return os.path.basename(next(builder.build(directory=wheel_directory, versions=["standard"])))
  File "/cache/builds/lib/python3.11/site-packages/hatchling/metadata/core.py", line 1550, in cached
    raise type(e)(message) from None
OSError: Error getting the version from source `regex`: file does not exist: src/datadelta/missing.py
  × Failed to build `/project`
  ├─▶ The build backend returned an error
  ╰─▶ Call to `hatchling.build.build_wheel` failed (exit status: 1)
      hint: This usually indicates a problem with the package or the build
      environment.
"""


def test_offline_missing_backend_is_recognized():
    assert _backend_unavailable_offline(UV_OFFLINE_NO_BACKEND) is True


def test_backend_failure_is_not_mistaken_for_offline():
    assert _backend_unavailable_offline(UV_BACKEND_FAILURE) is False
    assert _backend_unavailable_offline("") is False


@pytest.mark.build
def test_wheel_contains_package_and_entry_point(tmp_path):
    if shutil.which("uv") is None:
        pytest.skip("uv is not installed")

    proc = subprocess.run(
        ["uv", "build", "--wheel", "--offline", "-o", str(tmp_path)],
        capture_output = True,
        text           = True,
        cwd            = PROJECT_ROOT,
        timeout        = 300,
    )
    if proc.returncode != 0 and _backend_unavailable_offline(proc.stderr):
        pytest.skip("hatchling is not in uv's cache; run `uv build` once with network")
    assert proc.returncode == 0, proc.stderr

    wheels = list(tmp_path.glob("datadelta_cli-*.whl"))
    assert len(wheels) == 1, [p.name for p in tmp_path.iterdir()]
    assert wheels[0].name.startswith(f"datadelta_cli-{datadelta.__version__}-")

    with zipfile.ZipFile(wheels[0]) as zf:
        names = zf.namelist()
        assert "datadelta/__init__.py" in names
        assert "datadelta/__main__.py" in names
        assert not any(n.startswith("datadiff/") for n in names)
        entry_points = next(n for n in names if n.endswith(".dist-info/entry_points.txt"))
        assert "datadelta = datadelta.cli:app" in zf.read(entry_points).decode("utf-8")
