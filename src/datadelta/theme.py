"""
theme.py — The "ink & vermilion" (墨与朱) visual system, in one place.

WHY ONE MODULE?
  The terminal report, the live dashboard, the HTML export and the README
  screenshots must look like one product. Every color, glyph and font
  stack they use is defined here and nowhere else: no other module in
  src/ contains a hex color literal.

THE SYSTEM
  - Paper and ink: a warm off-white page with near-black text (inverted
    for dark mode). In the terminal we never paint a background and print
    body text in the terminal's own foreground color.
  - One accent, vermilion, for what demands attention: the ▲ mark, FAIL,
    progress bars. Ochre marks WARN and sage marks PASS; secondary text
    is "muted" (the terminal's dim attribute).
  - Severity is never color alone: every severity has a glyph and a word
    (▲ fail · ● warn · · info · ○ pass), so the report reads the same
    with NO_COLOR set (Rich honors NO_COLOR on its own) or in grayscale.
  - The three accents are identical in light and dark mode; only paper,
    ink, muted and rule change.
"""

from __future__ import annotations

from typing import Literal

from rich.terminal_theme import TerminalTheme


# ── Brand ─────────────────────────────────────────────────────────────────────

BRAND: str = "data ▲"


# ── Palette ───────────────────────────────────────────────────────────────────

TOKENS: tuple[str, ...] = ("paper", "ink", "muted", "rule", "vermilion", "ochre", "sage")

# Accent tokens: identical in both modes, rendered as true color in the terminal.
ACCENTS: tuple[str, ...] = ("vermilion", "ochre", "sage")

PALETTE: dict[str, dict[str, str]] = {
    "light": {
        "paper":     "#F6F4EF",
        "ink":       "#22201C",
        "muted":     "#6B665E",
        "rule":      "#D9D4CA",
        "vermilion": "#C8442C",
        "ochre":     "#B8862F",
        "sage":      "#5E7D5A",
    },
    "dark": {
        "paper":     "#1A1917",
        "ink":       "#ECE8E0",
        "muted":     "#9A948A",
        "rule":      "#34312C",
        "vermilion": "#C8442C",
        "ochre":     "#B8862F",
        "sage":      "#5E7D5A",
    },
}


# ── Glyphs ────────────────────────────────────────────────────────────────────

SEVERITY_GLYPH: dict[str, str] = {"FAIL": "▲", "WARN": "●", "INFO": "·", "PASS": "○"}
SEVERITY_WORD:  dict[str, str] = {"FAIL": "fail", "WARN": "warn", "INFO": "info", "PASS": "pass"}
SEVERITY_TOKEN: dict[str, str] = {"FAIL": "vermilion", "WARN": "ochre", "INFO": "muted", "PASS": "sage"}

# Dashboard stage states; a running stage cycles through SPINNER_FRAMES.
STAGE_GLYPH: dict[str, str] = {"pending": "○", "done": "✓", "failed": "×", "skipped": "–"}
SPINNER_FRAMES: tuple[str, ...] = ("◐", "◓", "◑", "◒")

# Typography shared by the terminal report, the clean report and the HTML export.
ARROW:      str = "→"     # before → after
SEP:        str = "·"     # separator inside a line
RULE_CHAR:  str = "─"     # thin horizontal rule
DELTA_UP:   str = "▲"     # row count went up
DELTA_DOWN: str = "▼"     # row count went down
MINUS:      str = "−"     # typographic minus (U+2212) for negative percentages
BAR_FULL:   str = "━"     # progress bar, done part
BAR_EMPTY:  str = "─"     # progress bar, remaining part


# ── Fonts (HTML export) ───────────────────────────────────────────────────────
# System and locally installed fonts only: the report loads nothing from the network.

FONT_SERIF: str = '"Source Serif 4", "Source Serif Pro", Georgia, "Songti SC", serif'
FONT_SANS:  str = '-apple-system, "Segoe UI", "Helvetica Neue", "PingFang SC", sans-serif'
FONT_MONO:  str = 'ui-monospace, "SF Mono", Menlo, monospace'


# ─────────────────────────────────────────────────────────────────────────────
# Terminal styles
# ─────────────────────────────────────────────────────────────────────────────

def rich_style(token: str, bold: bool = False) -> str:
    """
    The Rich style string for a palette token.

      vermilion / ochre / sage → true color hex, e.g. "#C8442C"
      muted / rule             → "dim" (works on light and dark terminals)
      ink / paper              → "" (the terminal's own colors; never a background)

    bold=True prefixes "bold". Unknown tokens raise ValueError.
    """
    if token in ACCENTS:
        base = PALETTE["light"][token]
    elif token in ("muted", "rule"):
        base = "dim"
    elif token in ("ink", "paper"):
        base = ""
    else:
        raise ValueError(f"unknown theme token: {token!r} (expected one of {', '.join(TOKENS)})")
    return " ".join(part for part in ("bold" if bold else "", base) if part)


def severity_style(severity: str) -> str:
    """Rich style for a severity: FAIL bold vermilion, WARN ochre, INFO dim, PASS sage."""
    if severity not in SEVERITY_TOKEN:
        raise ValueError(f"unknown severity: {severity!r}")
    return rich_style(SEVERITY_TOKEN[severity], bold=(severity == "FAIL"))


# ── Message styles (stderr) ───────────────────────────────────────────────────
# Errors, warnings and confirmations use the same accents as the severities,
# as Rich markup tags: f"[{ERROR_STYLE}]Error:[/] {escape(message)}".

ERROR_STYLE:   str = rich_style("vermilion", bold=True)
WARNING_STYLE: str = rich_style("ochre")
OK_STYLE:      str = rich_style("sage")


# ─────────────────────────────────────────────────────────────────────────────
# HTML and SVG
# ─────────────────────────────────────────────────────────────────────────────

def css_variables(mode: Literal["light", "dark"]) -> str:
    """
    CSS custom properties for one mode, one per token:
      "--paper: #F6F4EF;\\n  --ink: #22201C;\\n  ..."
    Lines after the first are indented two spaces, ready to sit inside a
    `:root { ... }` block of the HTML template.
    """
    if mode not in PALETTE:
        raise ValueError(f"unknown color mode: {mode!r} (expected 'light' or 'dark')")
    return "\n  ".join(f"--{token}: {PALETTE[mode][token]};" for token in TOKENS)


def _rgb(hex_color: str) -> tuple[int, int, int]:
    """'#C8442C' → (200, 68, 44)."""
    value = hex_color.lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def svg_terminal_theme() -> TerminalTheme:
    """
    A light "paper" TerminalTheme for Console.save_svg / export_svg (README
    screenshots). ANSI red/yellow/green map to vermilion/ochre/sage; every
    other ANSI color maps to ink or muted, so nothing renders in a color
    outside the palette. True-color styles (our accents) are kept as-is.
    """
    p = {token: _rgb(PALETTE["light"][token]) for token in TOKENS}
    #        black       red             green      yellow      blue      magenta   cyan        white
    normal = [p["ink"],   p["vermilion"], p["sage"], p["ochre"], p["ink"], p["ink"], p["muted"], p["muted"]]
    bright = [p["muted"], p["vermilion"], p["sage"], p["ochre"], p["ink"], p["ink"], p["muted"], p["ink"]]
    return TerminalTheme(
        background = p["paper"],
        foreground = p["ink"],
        normal     = normal,
        bright     = bright,
    )
