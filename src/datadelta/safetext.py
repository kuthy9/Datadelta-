"""
safetext.py — Text from data and LLMs, made safe to print on a terminal.

WHY?
  Column names, cell values, file names, error messages and LLM replies
  are printed through rich.text.Text (or with markup escaped), so they are
  never read as Rich markup. But Rich removes only a few control
  characters (BEL, BS, VT, FF, CR) and ESC passes through: a CSV header
  such as "x\\x1b]0;PWNED\\x07" would set the terminal title, and "ESC[2K
  ESC[1A" would erase a "▲ fail" line above it; OSC 52 writes the
  clipboard (CWE-150). An LLM narrative can be steered by the data, so it
  counts as data too.

  printable() shows every C0 and C1 control character, and DEL, as a
  visible escape ("\\x1b"), so the reader sees that something odd is in
  the data instead of the terminal acting on it. Newline and tab are
  layout and stay as they are.
"""

from __future__ import annotations

import re
from typing import Any


_CONTROL = re.compile(r"[\x00-\x08\x0b-\x1f\x7f-\x9f]")


def printable(text: Any) -> str:
    """str(text) with control characters (except newline and tab) shown as \\xNN."""
    return _CONTROL.sub(lambda m: f"\\x{ord(m.group()):02x}", str(text))
