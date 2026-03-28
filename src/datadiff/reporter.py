"""
reporter.py — Terminal output using the Rich library.

Rich provides: colored text, tables, panels, markdown rendering.
No external CSS or browser needed — everything renders in the terminal.

Output layout:
  1. Header panel  — scenario label + overall verdict (PASS/WARN/FAIL)
  2. Row summary   — before vs after row counts + delta
  3. Findings      — grouped by layer (schema → integrity → distribution → custom)
  4. Footer        — counts summary + hints for --export and --story
"""

from rich.console import Console
from rich.table   import Table
from rich.panel   import Panel
from rich         import box
from rich.text    import Text

from .differ     import DiffResult, Finding, Severity
from .scenarios  import SCENARIO_LABELS

console = Console()

# Maps severity → (terminal icon, Rich style string)
SEVERITY_STYLE: dict[Severity, tuple[str, str]] = {
    "FAIL": ("❌", "bold red"),
    "WARN": ("⚠️ ", "bold yellow"),
    "INFO": ("ℹ️ ", "cyan"),
    "PASS": ("✅", "green"),
}

# Human-readable layer names
LAYER_LABELS = {
    "schema":       "Schema Changes",
    "distribution": "Distribution Changes",
    "integrity":    "Integrity Checks",
    "custom":       "Custom Metrics (metrics.yaml)",   # Layer 4
}

# Display order for layers in the output
LAYER_ORDER = ["schema", "integrity", "distribution", "custom"]


def print_report(result: DiffResult, scenario: str = "general") -> None:
    """Main entry point. Prints the full report to the terminal."""
    _print_header(result, scenario)
    _print_row_summary(result)

    for layer in LAYER_ORDER:
        findings = [f for f in result.findings if f.layer == layer]
        if findings:
            _print_layer(layer, findings)

    _print_footer(result)


def _print_header(result: DiffResult, scenario: str) -> None:
    scenario_label = SCENARIO_LABELS.get(scenario, scenario)
    icon, style    = SEVERITY_STYLE[result.summary_severity]

    n_custom = sum(1 for f in result.findings if f.layer == "custom")
    subtitle = f"{len(result.findings)} finding(s)"
    if n_custom:
        subtitle += f"  ·  {n_custom} custom metric(s)"

    console.print()
    console.print(Panel(
        Text(f"{icon}  {result.summary_severity}", style=style),
        title        = f"[bold]datadiff[/bold]  ·  {scenario_label}",
        subtitle     = subtitle,
        border_style = style.replace("bold ", ""),
        padding      = (0, 2),
    ))


def _print_row_summary(result: DiffResult) -> None:
    delta_str = f"{result.row_delta:+,}"
    delta_pct = f"({result.row_delta_pct:+.1%})"
    color = "red" if result.row_delta < 0 else "green" if result.row_delta > 0 else "white"

    console.print(
        f"\n  Rows  "
        f"[dim]{result.rows_before:,}[/dim] → [bold]{result.rows_after:,}[/bold]  "
        f"[{color}]{delta_str} {delta_pct}[/{color}]"
    )


def _print_layer(layer: str, findings: list[Finding]) -> None:
    label = LAYER_LABELS.get(layer, layer.title())
    console.print(f"\n  [bold dim]{label}[/bold dim]")

    table = Table(
        box         = box.SIMPLE,
        show_header = False,
        padding     = (0, 1),
        expand      = False,
    )
    table.add_column("icon",  width=3)
    table.add_column("title", ratio=1)

    for finding in findings:
        icon, style = SEVERITY_STYLE[finding.severity]
        title_style = style if finding.severity in ("FAIL", "WARN") else ""
        table.add_row(
            Text(icon),
            Text(finding.title, style=title_style),
        )

    console.print(table)


def _print_footer(result: DiffResult) -> None:
    console.print()
    fails = sum(1 for f in result.findings if f.severity == "FAIL")
    warns = sum(1 for f in result.findings if f.severity == "WARN")

    parts = []
    if fails: parts.append(f"[red]{fails} failure(s)[/red]")
    if warns: parts.append(f"[yellow]{warns} warning(s)[/yellow]")
    if not parts: parts.append("[green]All checks passed[/green]")

    console.print("  " + "  ·  ".join(parts) + "\n")
    console.print("  [dim]--export report.html[/dim]  shareable HTML report")
    console.print("  [dim]--story[/dim]              natural language summary via Claude\n")