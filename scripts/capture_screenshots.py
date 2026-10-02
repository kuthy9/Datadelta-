"""
capture_screenshots.py — Regenerate the screenshots in README.md from real runs.

Run from the repository root with the project venv (macOS, for the PNG step):

    .venv/bin/python scripts/capture_screenshots.py

WHY A SECOND SCRIPT?
  scripts/render_readme_assets.py renders the two README samples in-process
  with a pinned timestamp, so they only change when the output changes.
  The screenshots below are different: each one is the `datadelta` command
  itself, run as a subprocess inside a pseudo-terminal, exactly as a user
  would type it. That is the only way to catch the live progress view,
  which draws on the terminal and disappears when the run ends.

WHAT IT WRITES (docs/screenshots/)
  live-progress.png          a frame from the middle of a 700,000-row `diff --clean`
  diff-clean-finished.png    the same run once it has finished
  custom-metrics.png         `diff --clean` on the logistics demo with metrics.yaml
  html-report-light.png      the `--export` HTML page, light color scheme
  html-report-dark.png       the same page, dark color scheme

HOW
  1. The demo datasets (examples/generate_demo_data.py) and a large pair of
     files are written into a temporary directory; examples/ is not touched.
  2. Each command runs in a pseudo-terminal of WIDTH x ROWS. The captured
     bytes are split into the redraws of the live view; escape sequences
     that move the cursor are dropped and the colors are kept.
  3. Rich turns that colored text into an SVG with the paper-and-ink theme
     from datadelta.theme, and macOS Quick Look (qlmanage) turns the SVG,
     and the exported HTML page, into PNG. Without qlmanage the SVGs are
     kept instead and the script says so.

Pseudo-terminals need a POSIX system (macOS or Linux).
"""

from __future__ import annotations

import contextlib
import fcntl
import importlib.util
import io
import os
import pty
import re
import select
import shutil
import struct
import subprocess
import sys
import tempfile
import termios
import time
from pathlib import Path
from types   import ModuleType

import numpy  as np
import pandas as pd
from rich.console import Console
from rich.text    import Text

from datadelta import theme


ROOT        = Path(__file__).resolve().parents[1]
OUT         = ROOT / "docs" / "screenshots"
DEMO_SCRIPT = ROOT / "examples" / "generate_demo_data.py"

WIDTH, ROWS = 92, 34            # terminal size of every screenshot
PNG_WIDTH   = 1600              # pixel width of the terminal PNGs
LARGE_ROWS  = 700_000           # big enough for the live view to be caught mid-run

CURSOR_ESCAPES = re.compile(r"\x1b\[[0-9;?]*[A-Za-ln-z]")   # every CSI sequence except colors (m)
VIEWBOX        = re.compile(r'viewBox="0 0 ([\d.]+) ([\d.]+)"')


# ── Data ──────────────────────────────────────────────────────────────────────

def _demo_module() -> ModuleType:
    """Import examples/generate_demo_data.py (it is a script, not a package module)."""
    spec   = importlib.util.spec_from_file_location("generate_demo_data", DEMO_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_large_pair(work: Path) -> None:
    """A slightly messy 700,000-row orders table and a later copy with APAC gone and scores shifted."""
    rng = np.random.default_rng(7)
    n   = LARGE_ROWS
    before = pd.DataFrame({
        "order_id": np.arange(1, n + 1),
        "region":   rng.choice(["NAM", "EMEA", " APAC", "LATAM", "N/A"], n),
        "status":   rng.choice(["completed", "pending", "cancelled"], n, p=[.7, .2, .1]),
        "revenue":  [f"${v:,.2f}" for v in rng.lognormal(5.5, .8, n)],
        "units":    rng.integers(1, 50, n),
        "paid":     rng.choice(["yes", "no"], n),
        "ordered":  pd.date_range("2024-01-01", periods=n, freq="min").strftime("%Y/%m/%d"),
        "score":    np.round(rng.normal(70, 12, n), 1),
    })
    after = before[before["region"] != " APAC"].copy()
    after["score"] = after["score"] + 6
    before.to_csv(work / "big_before.csv", index=False)
    after.to_csv(work / "big_after.csv", index=False)


# ── Running the CLI in a pseudo-terminal ──────────────────────────────────────

def run_in_terminal(args: list[str], cwd: Path) -> tuple[int, str, float]:
    """Run `datadelta <args>` attached to a pseudo-terminal; return (exit code, output, seconds)."""
    env = dict(os.environ)
    env.update({
        "PYTHONPATH": str(ROOT / "src"),
        "TERM":       "xterm-256color",
        "COLORTERM":  "truecolor",
        "COLUMNS":    str(WIDTH),
        "LINES":      str(ROWS),
    })
    for name in ("FORCE_COLOR", "NO_COLOR", "TTY_COMPATIBLE"):
        env.pop(name, None)

    parent_fd, child_fd = pty.openpty()
    fcntl.ioctl(child_fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, WIDTH, 0, 0))
    started = time.time()
    proc = subprocess.Popen(
        [sys.executable, "-m", "datadelta", *args],
        stdin=child_fd, stdout=child_fd, stderr=child_fd, cwd=cwd, env=env,
    )
    os.close(child_fd)

    output = b""
    while True:
        ready, _, _ = select.select([parent_fd], [], [], 0.2)
        if parent_fd in ready:
            try:
                chunk = os.read(parent_fd, 65536)
            except OSError:              # the child closed the terminal
                break
            if not chunk:
                break
            output += chunk
        elif proc.poll() is not None:
            break
    os.close(parent_fd)
    return proc.wait(), output.decode("utf-8", "replace"), time.time() - started


def redraws(output: str) -> list[str]:
    """Split terminal output into the frames the live view drew, colors kept, cursor moves dropped."""
    segments = re.split(r"\r(?=\x1b\[2K)", output)
    return [CURSOR_ESCAPES.sub("", s).replace("\r", "") for s in segments]


def mid_run_frame(frames: list[str]) -> str:
    """A frame two thirds into the run that shows a stage in progress (spinner and progress bar)."""
    running = [f for f in frames if any(g in f for g in theme.SPINNER_FRAMES) and theme.BAR_FULL in f]
    if not running:
        raise RuntimeError("the live view was never caught mid-run; make the large pair larger")
    return running[len(running) * 2 // 3].strip("\n")


def what_stays_on_screen(frames: list[str]) -> str:
    """The live view is transient: only its one-line summary and the report remain."""
    lines = frames[-1].splitlines()
    start = max((i for i, line in enumerate(lines) if " stages · " in line), default=0)
    return "\n".join(lines[start:]).strip("\n")


# ── Rendering ─────────────────────────────────────────────────────────────────

def save_terminal_svg(name: str, title: str, command: str, body: str) -> Path:
    """Draw `$ command` and the captured body in the paper-and-ink terminal theme."""
    console = Console(record=True, width=WIDTH, force_terminal=True, color_system="truecolor",
                      no_color=False, file=io.StringIO())
    console.print(Text("$ ", style=theme.rich_style("muted")) + Text(command, style="bold"))
    console.print(Text.from_ansi(body))
    svg = OUT / f"{name}.svg"
    console.save_svg(str(svg), title=title, theme=theme.svg_terminal_theme())
    return svg


def svg_to_png(svg: Path) -> Path | None:
    """Quick Look renders the SVG into a square canvas; crop it back to the SVG's own aspect ratio."""
    if not shutil.which("qlmanage") or not shutil.which("sips"):
        return None
    subprocess.run(["qlmanage", "-t", "-s", str(PNG_WIDTH), "-o", str(OUT), str(svg)],
                   check=True, capture_output=True)
    square = OUT / f"{svg.name}.png"
    width, height = (float(v) for v in VIEWBOX.search(svg.read_text(encoding="utf-8")).groups())
    if height <= width:
        crop = [str(int(PNG_WIDTH * height / width) + 2), str(PNG_WIDTH)]
    else:
        crop = [str(PNG_WIDTH), str(int(PNG_WIDTH * width / height) + 2)]
    png = svg.with_suffix(".png")
    subprocess.run(["sips", "--cropToHeightWidth", *crop, str(square), "--out", str(png)],
                   check=True, capture_output=True)
    square.unlink()
    svg.unlink()
    return png


def html_to_png(html: Path, name: str, scheme: str) -> Path | None:
    """Render the exported report in one color scheme by making its dark-mode block always/never apply."""
    if not shutil.which("qlmanage"):
        return None
    forced = html.read_text(encoding="utf-8").replace(
        "@media (prefers-color-scheme: dark)", "@media all" if scheme == "dark" else "@media not all"
    )
    page = html.with_name(f"{name}.html")
    page.write_text(forced, encoding="utf-8")
    subprocess.run(["qlmanage", "-t", "-s", str(PNG_WIDTH), "-o", str(OUT), str(page)],
                   check=True, capture_output=True)
    png = OUT / f"{name}.png"
    (OUT / f"{page.name}.png").replace(png)
    return png


# ── The screenshots ───────────────────────────────────────────────────────────

def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        with contextlib.redirect_stdout(io.StringIO()):     # the generator's status lines
            _demo_module().main(work)
        write_large_pair(work)

        # 1 + 2: a large diff --clean, mid-run and finished
        args = ["diff", "big_before.csv", "big_after.csv", "--clean", "--scenario", "etl"]
        code, output, seconds = run_in_terminal(args, work)
        frames  = redraws(output)
        command = "datadelta " + " ".join(args)
        written.append(save_terminal_svg("live-progress", f"{theme.BRAND} live progress (mid-run)",
                                         command, mid_run_frame(frames)))
        written.append(save_terminal_svg("diff-clean-finished", f"{theme.BRAND} diff --clean",
                                         command, what_stays_on_screen(frames)
                                         + f"\n\n[exit {code} · {seconds:.1f}s]"))

        # 3: custom KPIs from metrics.yaml on the cleaned logistics demo
        (work / "metrics.yaml").write_text((ROOT / "examples" / "metrics_logistics.yaml").read_text())
        args = ["diff", "logistics_before.csv", "logistics_after.csv", "--clean"]
        code, output, _ = run_in_terminal(args, work)
        written.append(save_terminal_svg("custom-metrics", f"{theme.BRAND} custom metrics",
                                         "datadelta " + " ".join(args),
                                         what_stays_on_screen(redraws(output)) + f"\n\n[exit {code}]"))
        (work / "metrics.yaml").unlink()

        # 4 + 5: the exported HTML page in both color schemes
        run_in_terminal(["diff", "etl_before.csv", "etl_after.csv", "--scenario", "etl",
                         "--export", "report.html", "-q"], work)
        pages = [html_to_png(work / "report.html", f"html-report-{s}", s) for s in ("light", "dark")]

        pngs = [svg_to_png(svg) for svg in written]

    for path in [*pngs, *pages]:
        if path is not None:
            print(f"wrote {path.relative_to(ROOT)}")
    if None in pngs or None in pages:
        print("qlmanage/sips not found: terminal screenshots kept as SVG, HTML screenshots skipped")


if __name__ == "__main__":
    main()
