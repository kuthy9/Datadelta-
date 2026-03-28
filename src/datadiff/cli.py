"""
cli.py — Command-line interface.

Two subcommands:
  datadiff diff <before> <after> [options]   → run a diff
  datadiff init --from <file>                → generate metrics.yaml with LLM

WHY TYPER?
  Typer is a thin wrapper around Click. It uses Python type annotations
  to automatically:
    - Parse command-line arguments and flags
    - Generate --help text
    - Validate input types

  This means we write normal Python function signatures and get a full
  CLI for free. No manual argparse setup needed.

AUTO-DETECTION OF metrics.yaml:
  On every `datadiff diff` run, we check for a metrics.yaml in the
  current directory. If found, it's loaded automatically. This means
  the user can set it up once (`datadiff init`) and forget about it —
  every subsequent diff run will apply their custom metrics.

  The user can override the path with --metrics /path/to/custom.yaml
  or disable it entirely with --no-metrics.
"""

from __future__ import annotations

import typer
from pathlib import Path
from typing  import Optional
from enum    import Enum

app = typer.Typer(
    name            = "datadiff",
    help            = "git diff, but for your data.",
    add_completion  = False,
    no_args_is_help = True,   # Show help when called with no arguments
)


class Scenario(str, Enum):
    etl       = "etl"
    migration = "migration"
    ab_test   = "ab-test"
    general   = "general"


# ─────────────────────────────────────────────────────────────────────────────
# `datadiff diff` — Main diff command
# ─────────────────────────────────────────────────────────────────────────────

@app.command("diff")
def diff_cmd(
    before: str = typer.Argument(
        ...,
        help=(
            "Path to the 'before' file (CSV, JSON, Parquet, XLSX, SQLite) "
            "or a database connection string (postgresql://user:pass@host/db::table)"
        )
    ),
    after: str = typer.Argument(
        ...,
        help="Path to the 'after' file, or a connection string"
    ),

    # ── Scenario ──────────────────────────────────────────────────────────
    scenario: Scenario = typer.Option(
        Scenario.general,
        "--scenario", "-s",
        help="Comparison context. Adjusts which findings are highlighted.",
    ),

    # ── Primary key ───────────────────────────────────────────────────────
    key: Optional[str] = typer.Option(
        None,
        "--key", "-k",
        help=(
            "Column to use as primary key for integrity checks. "
            "Auto-detected if not provided."
        ),
    ),

    # ── Sensitivity ───────────────────────────────────────────────────────
    threshold: float = typer.Option(
        0.10,
        "--threshold", "-t",
        help="Relative change threshold for WARN (0–1). Default: 0.10 = 10%%.",
    ),

    # ── Custom metrics ────────────────────────────────────────────────────
    metrics_file: Optional[str] = typer.Option(
        None,
        "--metrics", "-m",
        help="Path to metrics.yaml. Auto-detected from current directory if omitted.",
    ),
    no_metrics: bool = typer.Option(
        False,
        "--no-metrics",
        help="Disable automatic metrics.yaml detection.",
    ),

    # ── Output modes ──────────────────────────────────────────────────────
    story: bool = typer.Option(
        False,
        "--story",
        help="Generate a natural language summary via LLM.",
    ),
    llm: str = typer.Option(
        "claude",
        "--llm",
        help="LLM provider for --story: claude | openai | deepseek | gemini  [default: claude]",
    ),
    export: Optional[Path] = typer.Option(
        None,
        "--export", "-e",
        help="Export report to an HTML file.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Output raw diff result as JSON (useful for piping to other tools).",
    ),

    # ── Watch mode ────────────────────────────────────────────────────────
    watch: bool = typer.Option(
        False,
        "--watch", "-w",
        help="Re-run diff automatically whenever either file changes.",
    ),
):
    """
    Compare two datasets and report what changed semantically.

    BEFORE and AFTER can be:
      - File paths: orders.csv, data.xlsx, snapshot.parquet
      - Connection strings: postgresql://user:pass@host/db::table_name
    """
    from rich.console import Console
    console = Console()

    # ── Load files ────────────────────────────────────────────────────────
    from .loader import load_file
    try:
        console.print(f"[dim]Loading before: {before}[/dim]")
        df_before = load_file(before)
        console.print(f"[dim]Loading after:  {after}[/dim]")
        df_after  = load_file(after)
    except (FileNotFoundError, ValueError, ConnectionError) as e:
        console.print(f"\n[red]Error loading data:[/red] {e}\n")
        raise typer.Exit(code=1)

    # ── Load metrics.yaml ─────────────────────────────────────────────────
    from .metrics import load_metrics, MetricsConfig
    metrics_config: MetricsConfig | None = None

    if not no_metrics:
        try:
            metrics_config = load_metrics(metrics_file)  # None if not found
            if metrics_config:
                n = len(metrics_config.metrics)
                console.print(f"[dim]Loaded {n} custom metric(s) from metrics.yaml[/dim]")
        except Exception as e:
            console.print(f"[yellow]⚠️  metrics.yaml error: {e}[/yellow]")
            console.print("[yellow]   Custom metrics will be skipped.[/yellow]")

    # ── Profile columns ───────────────────────────────────────────────────
    from .profiler import profile_columns
    profile = profile_columns(df_before, df_after)

    # ── Run diff engine ───────────────────────────────────────────────────
    from .differ import compute_diff
    diff_result = compute_diff(
        df_before      = df_before,
        df_after       = df_after,
        profile        = profile,
        key_column     = key,
        threshold      = threshold,
        metrics_config = metrics_config,
    )

    # ── Apply scenario lens ───────────────────────────────────────────────
    from .scenarios import apply_scenario_lens
    diff_result = apply_scenario_lens(diff_result, scenario=scenario.value)

    # ── Output ────────────────────────────────────────────────────────────

    # JSON mode: dump raw result and exit (for scripting / CI pipelines)
    if json_output:
        import json
        typer.echo(json.dumps(diff_result.to_dict(), indent=2))
        raise typer.Exit()

    # Normal terminal report
    from .reporter import print_report
    print_report(diff_result, scenario=scenario.value)

    # Optional: natural language story
    if story:
        from .story import generate_story
        generate_story(diff_result, metrics_config=metrics_config, provider=llm)

    # Optional: HTML export
    if export:
        from .export import export_html
        export_html(diff_result, output_path=export)
        console.print(f"📄 Report saved to [bold]{export}[/bold]")

    # Optional: watch mode (re-run on file change)
    if watch:
        _run_watch_mode(before, after, diff_cmd, scenario, key, threshold,
                        metrics_file, no_metrics, story, export, json_output)

    # Exit with non-zero code if there are FAILs (useful for CI)
    if diff_result.has_failures:
        raise typer.Exit(code=1)


# ─────────────────────────────────────────────────────────────────────────────
# `datadiff init` — Generate metrics.yaml with LLM assistance
# ─────────────────────────────────────────────────────────────────────────────

@app.command("init")
def init_cmd(
    from_file: str = typer.Option(
        ...,
        "--from", "-f",
        help="A sample data file to analyze (used to extract column names and types).",
    ),
    business: str = typer.Option(
        ...,
        "--business", "-b",
        help=(
            'Describe your business and what metrics matter. '
            'Example: "We are a logistics company. Key metrics: dead stock rate, '
            'inventory turnover, cancellation rate."'
        ),
    ),
    output: str = typer.Option(
        "metrics.yaml",
        "--output", "-o",
        help="Where to save the generated metrics.yaml. Default: ./metrics.yaml",
    ),
):
    """
    Generate a starter metrics.yaml for your dataset using Claude.

    This command analyzes your data's columns and, combined with your
    business description, generates a customized metrics.yaml file.

    Requires ANTHROPIC_API_KEY to be set in your environment.

    Example:

      datadiff init --from orders.csv \\
        --business "We are a logistics company. Key metrics are dead stock
                    rate (no movement in 90 days) and cancellation rate."
    """
    from .metrics_generator import generate_metrics_yaml
    generate_metrics_yaml(
        sample_file          = from_file,
        business_description = business,
        output_path          = output,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Watch mode helper
# ─────────────────────────────────────────────────────────────────────────────

def _run_watch_mode(before, after, callback, *args):
    """
    Use the watchdog library to monitor files and re-run on change.
    watchdog is an optional dependency: pip install datadelta[watch]
    """
    from rich.console import Console
    console = Console()

    try:
        from watchdog.observers import Observer
        from watchdog.events    import FileSystemEventHandler
        import time
    except ImportError:
        console.print(
            "[red]watchdog is required for --watch mode.[/red]\n"
            "[dim]Install it: pip install datadelta[watch][/dim]"
        )
        raise typer.Exit(code=1)

    watched_files = {str(Path(before).resolve()), str(Path(after).resolve())}

    class _Handler(FileSystemEventHandler):
        def on_modified(self, event):
            if event.src_path in watched_files:
                console.clear()
                console.print("[dim]🔄 File changed — re-running diff...[/dim]\n")
                try:
                    callback(before, after, *args, watch=False)
                except SystemExit:
                    pass  # Ignore typer.Exit from the inner call

    observer = Observer()
    # Watch the directory containing the before file
    observer.schedule(_Handler(), path=str(Path(before).parent), recursive=False)
    observer.start()

    console.print(f"[dim]👀 Watching for changes. Press Ctrl+C to stop.[/dim]\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()