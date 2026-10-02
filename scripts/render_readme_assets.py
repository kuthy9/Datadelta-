"""
render_readme_assets.py — Regenerate the terminal samples shown in README.md.

Run from the repository root (with datadelta installed, e.g. the .venv):

    .venv/bin/python scripts/render_readme_assets.py

WHY A SCRIPT?
  The README shows real output, not a hand-drawn mock-up. This script
  generates the demo datasets into a temporary directory (examples/ is not
  touched), runs the same pipeline as

      datadelta diff examples/etl_before.csv examples/etl_after.csv --scenario etl
      datadelta clean examples/messy_orders.csv --dry-run

  in-process, and records both reports with Rich:

      docs/assets/diff.svg    docs/assets/diff.txt
      docs/assets/clean.svg   docs/assets/clean.txt

  The SVGs use the paper-and-ink terminal theme from datadelta.theme. The
  .txt files hold the same output as plain text; README.md quotes
  docs/assets/diff.txt verbatim (tests/test_readme.py checks that).
  The report timestamp is pinned to SAMPLE_TIME so that re-running the
  script changes the files only when the output itself changes.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import tempfile
from datetime import datetime
from pathlib  import Path
from types    import ModuleType

from rich.console import Console

from datadelta           import theme
from datadelta.clean     import clean_frame
from datadelta.differ    import compute_diff
from datadelta.loader    import load_file
from datadelta.profiler  import profile_columns
from datadelta.reporter  import print_clean_report, print_report
from datadelta.scenarios import apply_scenario_lens


ROOT        = Path(__file__).resolve().parents[1]
ASSETS      = ROOT / "docs" / "assets"
DEMO_SCRIPT = ROOT / "examples" / "generate_demo_data.py"

WIDTH       = 88                              # terminal columns of the samples
SAMPLE_TIME = datetime(2026, 10, 1, 9, 30)    # header timestamp shown in the samples


# ── Helpers ───────────────────────────────────────────────────────────────────

def _demo_module() -> ModuleType:
    """Import examples/generate_demo_data.py (it is a script, not a package module)."""
    spec   = importlib.util.spec_from_file_location("generate_demo_data", DEMO_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _recording_console() -> Console:
    """A true-color terminal of WIDTH columns that records instead of printing."""
    return Console(
        record         = True,
        width          = WIDTH,
        force_terminal = True,
        color_system   = "truecolor",
        no_color       = False,          # the samples are in color even if NO_COLOR is set
        file           = io.StringIO(),
    )


def _save(console: Console, name: str) -> None:
    """Write docs/assets/<name>.txt (plain text) and docs/assets/<name>.svg (paper theme)."""
    ASSETS.mkdir(parents=True, exist_ok=True)
    (ASSETS / f"{name}.txt").write_text(console.export_text(clear=False), encoding="utf-8")
    console.save_svg(str(ASSETS / f"{name}.svg"), title=theme.BRAND, theme=theme.svg_terminal_theme())


# ── The two samples ───────────────────────────────────────────────────────────

def render_diff(demo_dir: Path) -> None:
    """`datadelta diff examples/etl_before.csv examples/etl_after.csv --scenario etl`."""
    before = load_file(str(demo_dir / "etl_before.csv"))
    after  = load_file(str(demo_dir / "etl_after.csv"))
    result = compute_diff(before, after, profile_columns(before, after), key_column=None)
    result = apply_scenario_lens(result, scenario="etl")

    console = _recording_console()
    print_report(result, "etl", console=console, now=SAMPLE_TIME)
    _save(console, "diff")


def render_clean(demo_dir: Path) -> None:
    """`datadelta clean examples/messy_orders.csv --dry-run` (default rules, the cleaner's lossless read)."""
    df     = load_file(str(demo_dir / "messy_orders.csv"), lossless=True)
    result = clean_frame(df)

    console = _recording_console()
    print_clean_report(
        result.report,
        "examples/messy_orders.csv",
        Path("examples/messy_orders.clean.csv"),
        dry_run = True,
        console = console,
    )
    _save(console, "clean")


def main() -> None:
    demo = _demo_module()
    with tempfile.TemporaryDirectory() as tmp:
        demo_dir = Path(tmp)
        with contextlib.redirect_stdout(io.StringIO()):     # the generator's status lines
            demo.main(demo_dir)
        render_diff(demo_dir)
        render_clean(demo_dir)

    for name in ("diff.svg", "diff.txt", "clean.svg", "clean.txt"):
        print(f"wrote {(ASSETS / name).relative_to(ROOT)}")


if __name__ == "__main__":
    main()
