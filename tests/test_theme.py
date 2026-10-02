"""
test_theme.py — The ink & vermilion palette, glyphs, Rich styles, CSS
variables and the SVG terminal theme all come from datadelta.theme.
"""

from __future__ import annotations

import re

import pytest
from rich.console import Console
from rich.style   import Style
from rich.text    import Text

from datadelta import theme


HEX = re.compile(r"^#[0-9A-F]{6}$")


# ── Palette ───────────────────────────────────────────────────────────────────

def test_palette_values_match_the_spec():
    assert theme.PALETTE["light"] == {
        "paper": "#F6F4EF", "ink": "#22201C", "muted": "#6B665E", "rule": "#D9D4CA",
        "vermilion": "#C8442C", "ochre": "#B8862F", "sage": "#5E7D5A",
    }
    assert theme.PALETTE["dark"] == {
        "paper": "#1A1917", "ink": "#ECE8E0", "muted": "#9A948A", "rule": "#34312C",
        "vermilion": "#C8442C", "ochre": "#B8862F", "sage": "#5E7D5A",
    }


def test_both_modes_define_every_token_as_uppercase_hex():
    for mode in ("light", "dark"):
        assert tuple(theme.PALETTE[mode]) == theme.TOKENS
        assert all(HEX.match(value) for value in theme.PALETTE[mode].values())


def test_accents_are_identical_in_both_modes():
    for token in theme.ACCENTS:
        assert theme.PALETTE["light"][token] == theme.PALETTE["dark"][token]
    assert theme.ACCENTS == ("vermilion", "ochre", "sage")


# ── Glyphs and words ──────────────────────────────────────────────────────────

def test_brand_and_severity_glyphs():
    assert theme.BRAND == "data ▲"
    assert theme.SEVERITY_GLYPH == {"FAIL": "▲", "WARN": "●", "INFO": "·", "PASS": "○"}
    assert theme.SEVERITY_WORD == {"FAIL": "fail", "WARN": "warn", "INFO": "info", "PASS": "pass"}
    assert theme.SEVERITY_TOKEN == {"FAIL": "vermilion", "WARN": "ochre", "INFO": "muted", "PASS": "sage"}


def test_stage_glyphs_and_spinner():
    assert theme.STAGE_GLYPH == {"pending": "○", "done": "✓", "failed": "×", "skipped": "–"}
    assert theme.SPINNER_FRAMES == ("◐", "◓", "◑", "◒")


def test_font_stacks_are_local_only():
    assert theme.FONT_SERIF == '"Source Serif 4", "Source Serif Pro", Georgia, "Songti SC", serif'
    assert theme.FONT_SANS == '-apple-system, "Segoe UI", "Helvetica Neue", "PingFang SC", sans-serif'
    assert theme.FONT_MONO == 'ui-monospace, "SF Mono", Menlo, monospace'


# ── Rich styles ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("token, plain, bold", [
    ("vermilion", "#C8442C", "bold #C8442C"),
    ("ochre",     "#B8862F", "bold #B8862F"),
    ("sage",      "#5E7D5A", "bold #5E7D5A"),
    ("muted",     "dim",     "bold dim"),
    ("rule",      "dim",     "bold dim"),
    ("ink",       "",        "bold"),
    ("paper",     "",        "bold"),
])
def test_rich_style(token, plain, bold):
    assert theme.rich_style(token) == plain
    assert theme.rich_style(token, bold=True) == bold
    Style.parse(plain)                      # every style string is valid Rich syntax
    Style.parse(bold)


def test_rich_style_never_paints_a_background():
    for token in theme.TOKENS:
        assert " on " not in f" {theme.rich_style(token)} "
        assert Style.parse(theme.rich_style(token)).bgcolor is None


def test_rich_style_rejects_unknown_tokens():
    with pytest.raises(ValueError, match="unknown theme token: 'red'"):
        theme.rich_style("red")


def test_severity_and_message_styles(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)           # the color checks below need color enabled
    assert theme.severity_style("FAIL") == "bold #C8442C"
    assert theme.severity_style("WARN") == "#B8862F"
    assert theme.severity_style("INFO") == "dim"
    assert theme.severity_style("PASS") == "#5E7D5A"
    with pytest.raises(ValueError, match="unknown severity"):
        theme.severity_style("ERROR")
    # stderr messages (errors, warnings, confirmations) use the same accents
    assert theme.ERROR_STYLE   == "bold #C8442C"
    assert theme.WARNING_STYLE == "#B8862F"
    assert theme.OK_STYLE      == "#5E7D5A"
    console = Console(force_terminal=True, color_system="truecolor", width=60)
    with console.capture() as captured:
        console.print(f"[{theme.ERROR_STYLE}]Error:[/] [{theme.WARNING_STYLE}]Warning:[/] [{theme.OK_STYLE}]ok[/]")
    out = captured.get()
    assert "Error: Warning: ok" in Text.from_ansi(out).plain     # the tags are valid markup, not literal text
    assert "38;2;200;68;44" in out and "38;2;184;134;47" in out and "38;2;94;125;90" in out


def test_no_color_environment_removes_all_color(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    console = Console(force_terminal=True, color_system="truecolor", width=40)
    with console.capture() as captured:
        console.print(Text("fail", style=theme.severity_style("FAIL")))
    assert "38;2;" not in captured.get()    # no true-color escape sequence
    assert "fail" in captured.get()


# ── CSS variables ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("mode", ["light", "dark"])
def test_css_variables_list_every_token_once(mode):
    css = theme.css_variables(mode)
    lines = css.split("\n  ")
    assert lines == [f"--{token}: {theme.PALETTE[mode][token]};" for token in theme.TOKENS]


def test_css_variables_light_and_dark_differ_only_in_base_tokens():
    light = theme.css_variables("light")
    dark  = theme.css_variables("dark")
    assert "--paper: #F6F4EF;" in light and "--paper: #1A1917;" in dark
    assert "--vermilion: #C8442C;" in light and "--vermilion: #C8442C;" in dark


def test_css_variables_rejects_unknown_mode():
    with pytest.raises(ValueError, match="unknown color mode"):
        theme.css_variables("sepia")


# ── SVG terminal theme ────────────────────────────────────────────────────────

def test_svg_terminal_theme_is_paper_and_ink():
    t = theme.svg_terminal_theme()
    assert tuple(t.background_color) == (0xF6, 0xF4, 0xEF)
    assert tuple(t.foreground_color) == (0x22, 0x20, 0x1C)
    assert tuple(t.ansi_colors[1]) == (0xC8, 0x44, 0x2C)     # red    → vermilion
    assert tuple(t.ansi_colors[2]) == (0x5E, 0x7D, 0x5A)     # green  → sage
    assert tuple(t.ansi_colors[3]) == (0xB8, 0x86, 0x2F)     # yellow → ochre
    palette = {tuple(int(h[i:i + 2], 16) for i in (1, 3, 5)) for h in theme.PALETTE["light"].values()}
    assert all(tuple(c) in palette for c in (t.ansi_colors[i] for i in range(16)))


def test_svg_export_uses_the_paper_background():
    console = Console(record=True, width=40, force_terminal=True, color_system="truecolor")
    console.print(Text("▲ fail", style=theme.severity_style("FAIL")))
    svg = console.export_svg(theme=theme.svg_terminal_theme(), title="data ▲").lower()
    assert "#f6f4ef" in svg
    assert "#c8442c" in svg


def test_src_uses_no_named_color_markup():
    """Final review F3: every color comes from theme (ERROR_STYLE, WARNING_STYLE, OK_STYLE ...)."""
    from pathlib import Path
    package = Path(theme.__file__).parent
    named   = re.compile(r"\[/?(?:bold )?(?:red|yellow|green)\]")
    hits = [
        f"{path.relative_to(package)}:{number}: {line.strip()}"
        for path in sorted(package.rglob("*.py"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if named.search(line)
    ]
    assert hits == []
