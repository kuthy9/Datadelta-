"""
test_no_emoji.py — No emoji anywhere in the shipped text.

The ink & vermilion system speaks in a few typographic glyphs (▲ ● · ○
✓ × – ◐ ◓ ◑ ◒ → ▼ ━ ─), never in emoji. This scans src/, examples/,
scripts/ and README.md with the same pattern as tests/test_reporter.py:
U+1F300–U+1FAFF, U+2600–U+27BF except ✓ (U+2713), and the emoji
variation selector U+FE0F. The pattern is written with escapes, so this
file contains no emoji-range character itself.
"""

from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

EMOJI    = re.compile("[\U0001F300-\U0001FAFF☀-✒✔-➿️]")
TEXT_EXT = {".py", ".md", ".yaml", ".yml", ".toml", ".txt", ".html", ".css", ".json"}


def _shipped_text_files() -> list[Path]:
    files = [PROJECT_ROOT / "README.md"]
    for folder in ("src", "examples", "scripts"):
        files += sorted(
            path for path in (PROJECT_ROOT / folder).rglob("*")
            if path.is_file() and path.suffix in TEXT_EXT and "__pycache__" not in path.parts
        )
    return files


def test_scan_covers_the_project():
    names = {path.relative_to(PROJECT_ROOT).as_posix() for path in _shipped_text_files()}
    assert {"README.md", "src/datadelta/cli.py", "examples/generate_demo_data.py",
            "scripts/render_readme_assets.py"} <= names


def test_no_emoji_in_shipped_text():
    offenders = []
    for path in _shipped_text_files():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for match in EMOJI.finditer(line):
                offenders.append(f"{path.relative_to(PROJECT_ROOT)}:{number}: U+{ord(match.group()):04X}")
    assert offenders == []


def test_the_allowed_glyphs_are_not_emoji():
    assert EMOJI.search("▲ ● · ○ ✓ × – ◐ ◓ ◑ ◒ → ▼ ━ ─ −") is None
