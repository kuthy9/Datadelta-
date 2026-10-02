"""
cli.py — Command-line interface.

Three subcommands:
  datadelta diff <before> <after> [options]   → run a diff
  datadelta clean <input> [options]           → standardize a dataset, write it back out
  datadelta init --from <file>                → generate metrics.yaml with LLM

Plus one root option:
  datadelta --version                         → print "data ▲ datadelta <version>"

WHY TYPER?
  Typer uses Python type annotations to automatically:
    - Parse command-line arguments and flags
    - Generate --help text
    - Validate input types

  This means we write normal Python function signatures and get a full
  CLI for free. No manual argparse setup needed.

AUTO-DETECTION OF metrics.yaml:
  On every `datadelta diff` run, we check for a metrics.yaml in the
  current directory. If found, it's loaded automatically. This means
  the user can set it up once (`datadelta init`) and forget about it —
  every subsequent diff run will apply their custom metrics.

  The user can override the path with --metrics /path/to/custom.yaml
  or disable it entirely with --no-metrics.
"""

from __future__ import annotations

import os
import typer
from pathlib import Path
from typing  import Optional, TYPE_CHECKING
from enum    import Enum

from .          import __version__
from .safetext  import printable     # data-derived text in status and error lines

if TYPE_CHECKING:
    from typing             import NoReturn
    from rich.console       import Console
    from .clean.report      import CleanReport
    from .differ            import DiffResult
    from .progress          import ProgressSink

app = typer.Typer(
    name            = "datadelta",
    help            = "data ▲ — what changed in your data, and why it matters.",
    add_completion  = False,
    no_args_is_help = True,   # Show help when called with no arguments
)


class Scenario(str, Enum):
    etl       = "etl"
    migration = "migration"
    ab_test   = "ab-test"
    general   = "general"


# ─────────────────────────────────────────────────────────────────────────────
# Root options — `datadelta --version`
# ─────────────────────────────────────────────────────────────────────────────

def _version_callback(value: Optional[bool]) -> None:
    """Eager: runs before any subcommand is resolved, prints, and exits 0."""
    if value:
        typer.echo(f"data ▲ datadelta {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback  = _version_callback,
        is_eager  = True,
        help      = "Show the version and exit.",
    ),
) -> None:
    """data ▲ — what changed in your data, and why it matters."""


# ─────────────────────────────────────────────────────────────────────────────
# `datadelta diff` — Main diff command
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
        help="Relative change threshold for WARN (0–1). Default: 0.10 = 10 percent.",
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

    # ── Cleaning ──────────────────────────────────────────────────────────
    clean: bool = typer.Option(
        False,
        "--clean",
        help=(
            "Clean both datasets with the same rules before comparing "
            "(rules: the cleaning: section of the metrics file; see `datadelta clean`)."
        ),
    ),

    # ── Progress ──────────────────────────────────────────────────────────
    quiet: bool = typer.Option(
        False,
        "--quiet", "-q",
        help="No progress display, status lines or warnings on stderr (errors still appear).",
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
    from rich.markup  import escape

    from .            import theme
    from .dashboard   import make_progress
    from .scenarios   import SCENARIO_LABELS

    # stdout carries only the report (or the JSON document); every status
    # line, warning, error and the progress display go to stderr, so
    # `--json | jq` stays clean. Inside the progress block every stderr
    # line is printed through `err`, the console the live view runs on.
    err = Console(stderr=True)

    # ── Cleaning rules (--clean): the cleaning: section of the same file
    #    the metrics come from; --no-metrics without -m means defaults ────
    clean_plan = None
    if clean:
        from .clean import CleanConfigError, CleanPlan, load_clean_plan
        try:
            if no_metrics and metrics_file is None:
                clean_plan = CleanPlan(key_column=key)
            else:
                clean_plan = load_clean_plan(metrics_file, key=key)
        except CleanConfigError as e:
            err.print(f"[{theme.ERROR_STYLE}]Cleaning config error:[/] {escape(printable(e))}", soft_wrap=True)
            raise typer.Exit(code=1)

    title     = f"diff {theme.SEP} {SCENARIO_LABELS.get(scenario.value, scenario.value)}"
    narrative = None

    # Everything that takes time runs inside the progress display; the
    # report is printed after it has closed.
    with make_progress(title, quiet=quiet, console=err) as progress:

        # ── Load files (--clean: the cleaner's lossless read, so the clean
        #    steps, not the readers' guesses, decide every conversion) ──────
        from .loader import load_file
        try:
            df_before = load_file(before, progress, stage_key="load.before", label="load before", lossless=clean)
            df_after  = load_file(after,  progress, stage_key="load.after",  label="load after",  lossless=clean)
        except (FileNotFoundError, ValueError, ConnectionError) as e:
            err.print(f"\n[{theme.ERROR_STYLE}]Error loading data:[/] {escape(printable(e))}\n", soft_wrap=True)
            raise typer.Exit(code=1)

        # ── Clean both sides with the same plan (--clean) ─────────────────
        cleaned = {}
        if clean_plan is not None:
            from .clean import align_dtypes, clean_both
            try:
                # one plan, one logical type per column (see clean_both)
                cleaned["before"], cleaned["after"] = clean_both(df_before, df_after, clean_plan, progress)
            except ValueError as e:
                err.print(f"[{theme.ERROR_STYLE}]Error cleaning data:[/] {escape(printable(e))}")
                raise typer.Exit(code=1)
            df_before, df_after = cleaned["before"].df, cleaned["after"].df
            df_before, df_after = align_dtypes(df_before, df_after)        # one logical type, one dtype
            # Warnings and notes, not errors: --quiet drops them (spec 4.4)
            if not quiet:
                for side, outcome in cleaned.items():
                    _warn_missing_columns(outcome.report, err, side=side)
                if clean_plan.dedupe is not None or clean_plan.impute or clean_plan.impute_default is not None:
                    err.print(
                        f"[{theme.WARNING_STYLE}]Note: --clean drops or fills values (dedupe/impute); "
                        "integrity and null-rate findings reflect cleaned data[/]",
                        soft_wrap = True,
                    )

        # ── Load metrics.yaml ─────────────────────────────────────────────
        from .metrics import load_metrics, MetricsConfig
        metrics_config: MetricsConfig | None = None

        if not no_metrics:
            try:
                metrics_config = load_metrics(metrics_file)  # None if not found
                if metrics_config and not quiet:
                    n = len(metrics_config.metrics)
                    err.print(f"[dim]Loaded {n} custom metric(s) from metrics.yaml[/dim]")
            except Exception as e:
                err.print(f"[{theme.WARNING_STYLE}]metrics.yaml error: {escape(printable(e))}[/]")
                err.print(f"[{theme.WARNING_STYLE}]   Custom metrics will be skipped.[/]")

        # ── Profile columns ───────────────────────────────────────────────
        from .profiler import profile_columns
        from .progress import plural
        progress.stage_start("profile", "profile")
        profile = profile_columns(df_before, df_after)
        progress.stage_end("profile", "done", summary=plural(len(profile.columns), "column"))

        # ── Run diff engine (stages schema, distribution, integrity, metrics)
        from .differ import compute_diff
        diff_result = compute_diff(
            df_before      = df_before,
            df_after       = df_after,
            profile        = profile,
            key_column     = key,
            threshold      = threshold,
            metrics_config = metrics_config,
            progress       = progress,
        )

        # ── Clean layer: what --clean did, as INFO findings + full reports
        if cleaned:
            from .clean.findings import findings_from_report
            diff_result.findings[:0] = [
                finding
                for side, outcome in cleaned.items()
                for finding in findings_from_report(outcome.report, side)
            ]
            diff_result.cleaning = {side: outcome.report.to_dict() for side, outcome in cleaned.items()}

        # ── Apply scenario lens; its counts are the authoritative tally ───
        from .scenarios import apply_scenario_lens
        diff_result = apply_scenario_lens(diff_result, scenario=scenario.value)
        progress.tally(_severity_counts(diff_result))

        # ── Optional: natural language story (collected, printed later).
        #    Skipped in --json mode: a narrative would corrupt the stream.
        #    Its warnings (missing key, SDK or reply) are not errors, so
        #    --quiet gives it a console that prints nothing.
        if story and not json_output:
            from .story import generate_story
            story_console = Console(stderr=True, quiet=True) if quiet else err
            progress.stage_start("story", "story", note=llm)
            narrative = generate_story(diff_result, metrics_config=metrics_config, provider=llm, console=story_console)
            if narrative is not None:
                progress.stage_end("story", "done", summary=narrative.label)
            else:
                progress.stage_end("story", "skipped", summary="no story")

        # ── Optional: HTML export (carries the story when there is one).
        #    A failed write is reported here; the output below still
        #    prints, and the command then exits 1. ─────────────────────────
        export_ok = True
        if export:
            export_ok = _write_export(
                diff_result, export, err,
                story    = narrative.text if narrative is not None else None,
                progress = progress,
            )

    # ── Output (the progress display has closed) ──────────────────────────

    # JSON mode: one strict JSON document on stdout (for scripting / CI).
    # Exit code matches terminal mode; an unwritable --export also gives 1.
    if json_output:
        from .jsonutil import dumps
        typer.echo(dumps(diff_result.to_dict()))
        if export and export_ok and not quiet:
            err.print(f"[dim]Report saved to {escape(printable(export))}[/dim]", soft_wrap=True)
        if story and not quiet:
            err.print(f"[{theme.WARNING_STYLE}]--story is ignored in --json mode[/]")
        raise typer.Exit(code=1 if (diff_result.has_failures or not export_ok) else 0)

    # Normal terminal report. The --export / --story hints are shown only
    # for the outputs the user did not ask for; the story follows the report.
    from .reporter import print_report, print_story
    print_report(
        diff_result,
        scenario  = scenario.value,
        exported  = export is not None,
        storied   = story,
        exit_code = 1 if (diff_result.has_failures or not export_ok) else 0,
    )
    if narrative is not None:
        print_story(narrative)

    if export and export_ok and not quiet:
        err.print(f"[dim]Report saved to {escape(printable(export))}[/dim]", soft_wrap=True)
    if not export_ok:
        raise typer.Exit(code=1)            # the "Could not write" error is already on stderr

    # Optional: watch mode (re-run on file change). Every parameter goes by
    # keyword: a re-run is a direct Python call, so anything left out would
    # fall back to Typer's OptionInfo default (a truthy object).
    if watch:
        _run_watch_mode(diff_cmd, dict(
            before       = before,
            after        = after,
            scenario     = scenario,
            key          = key,
            threshold    = threshold,
            metrics_file = metrics_file,
            no_metrics   = no_metrics,
            story        = story,
            llm          = llm,
            export       = export,
            json_output  = json_output,
            clean        = clean,
            quiet        = quiet,
        ))

    # Exit with non-zero code if there are FAILs (useful for CI)
    if diff_result.has_failures:
        raise typer.Exit(code=1)


def _severity_counts(result: "DiffResult") -> dict[str, int]:
    """Findings per severity after the scenario lens: {"FAIL": n, "WARN": n, "INFO": n, "PASS": n}."""
    counts = {"FAIL": 0, "WARN": 0, "INFO": 0, "PASS": 0}
    for finding in result.findings:
        counts[finding.severity] = counts.get(finding.severity, 0) + 1
    return counts


def _write_export(
    result:   "DiffResult",
    path:     Path,
    err:      "Console",
    story:    str | None = None,
    progress: "ProgressSink | None" = None,
) -> bool:
    """
    Write the HTML report (with the --story narrative, if any) as the
    "export" progress stage. Returns True when the file was written.

    An unwritable path is an IO error: the stage ends "failed", the error
    goes to stderr and this returns False instead of raising, so the caller
    still prints the report (or the JSON document) and then exits 1. The
    "Report saved to" status line is printed by the caller, after the report.
    """
    from rich.markup import escape
    from .           import theme
    from .export     import export_html
    from .progress   import NullProgress, error_summary

    progress = progress if progress is not None else NullProgress()
    progress.stage_start("export", "export", note=path.name)
    try:
        export_html(result, output_path=path, story=story)
    except OSError as e:
        progress.stage_end("export", "failed", summary=error_summary(e))
        err.print(f"[{theme.ERROR_STYLE}]Could not write {escape(printable(path))}:[/] {escape(printable(e))}")
        return False
    progress.stage_end("export", "done", summary=f"{path.stat().st_size / 1024:,.1f} KB")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# `datadelta init` — Generate metrics.yaml with LLM assistance
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
    no_samples: bool = typer.Option(
        False,
        "--no-samples",
        help="Send no sample values or statistics to the LLM: only column names and dtypes.",
    ),
):
    """
    Generate a starter metrics.yaml for your dataset using Claude.

    This command analyzes your data's columns and, combined with your
    business description, generates a customized metrics.yaml file.

    Sent to the LLM: your business description and, per column, its name,
    dtype, distinct count, null rate and up to 5 sample values.
    Use --no-samples to send only column names and dtypes.

    Requires ANTHROPIC_API_KEY to be set in your environment. Exits 1 when
    the key is missing, the sample file cannot be loaded, the LLM request
    fails, or the generated YAML is invalid (the raw reply is kept in
    metrics_draft.yaml).

    Example:

      datadelta init --from orders.csv \\
        --business "We are a logistics company. Key metrics are dead stock
                    rate (no movement in 90 days) and cancellation rate."
    """
    from .metrics_generator import MetricsGenerationError, generate_metrics_yaml
    try:
        generate_metrics_yaml(
            sample_file          = from_file,
            business_description = business,
            output_path          = output,
            include_samples      = not no_samples,
        )
    except MetricsGenerationError:
        raise typer.Exit(code=1)


# ─────────────────────────────────────────────────────────────────────────────
# `datadelta clean` — Standardize one dataset and write it back out
# ─────────────────────────────────────────────────────────────────────────────

class DedupeMode(str, Enum):
    exact = "exact"
    key   = "key"


class ImputeMode(str, Enum):
    median = "median"
    mode   = "mode"


@app.command("clean")
def clean_cmd(
    source: str = typer.Argument(
        ...,
        metavar = "INPUT",
        help    = (
            "File to clean (CSV, JSON, Parquet, XLSX, SQLite) or a database "
            "connection string (postgresql://user:pass@host/db::table, needs -o)"
        ),
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Where to write the result: .csv .parquet .xlsx or .json. Default: <input dir>/<stem>.clean<suffix>",
    ),
    config: Optional[str] = typer.Option(
        None,
        "--config",
        help="YAML file whose `cleaning:` section sets the rules. Default: ./metrics.yaml if it exists.",
    ),
    dedupe: Optional[DedupeMode] = typer.Option(
        None,
        "--dedupe",
        help="Drop duplicate rows: exact (identical rows) or key (same --key value).",
    ),
    key: Optional[str] = typer.Option(
        None,
        "--key", "-k",
        help="Key column, used by --dedupe key and for the duplicate-key count.",
    ),
    impute: Optional[ImputeMode] = typer.Option(
        None,
        "--impute",
        help="Fill missing values: median (numeric columns) or mode (every column). Per-column YAML rules win.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would change; write nothing.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Print the cleaning report as JSON on stdout.",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet", "-q",
        help="No progress display, status lines or warnings on stderr (errors still appear).",
    ),
):
    """
    Standardize a dataset without losing information, and write it back out.

    By default: trims headers and text, turns null tokens (N/A, null, -)
    into real nulls, and parses booleans, numbers and dates only when every
    value in a column parses. Re-casing, dedupe and imputation run only
    when configured (cleaning: in metrics.yaml, or --dedupe / --impute).
    """
    from rich.console import Console
    from rich.markup  import escape

    from .           import theme
    from .clean      import CleanConfigError, clean_frame, load_clean_plan
    from .clean.io   import SUPPORTED_OUTPUTS, WriteError, default_output_path, write_frame
    from .dashboard  import make_progress
    from .loader     import _mask_password, load_file, source_label
    from .progress   import error_summary, plural

    err = Console(stderr=True)

    # ── Where the result goes (database sources need -o) ──────────────────
    target: Path | None = output
    if target is None:
        try:
            target = default_output_path(source)
        except ValueError as e:
            if not dry_run:
                _fail(err, str(e))
    if target is not None:
        if target.suffix.lower() not in SUPPORTED_OUTPUTS:
            _fail(err, f"unsupported output format '{target.suffix}' (use .csv, .json, .parquet or .xlsx)")
        if not dry_run and _is_same_file(target, Path(source)):
            _fail(err, "the output would overwrite the input file; choose another path with -o")

    # ── Rules: defaults < cleaning: in the config file < flags ────────────
    try:
        plan = load_clean_plan(
            config,
            dedupe = dedupe.value if dedupe is not None else None,
            key    = key,
            impute = impute.value if impute is not None else None,
        )
    except CleanConfigError as e:
        _fail(err, str(e))

    # ── Load, clean, write: stages load, clean, write (not on --dry-run) ──
    title = f"clean {theme.SEP} {source_label(source)}"
    with make_progress(title, quiet=quiet, console=err) as progress:
        try:
            df = load_file(source, progress, lossless=True)     # the steps decide every conversion
        except (FileNotFoundError, ValueError, ConnectionError) as e:
            err.print(f"[{theme.ERROR_STYLE}]Error loading data:[/] {escape(printable(e))}", soft_wrap=True)
            raise typer.Exit(code=1)

        try:
            result = clean_frame(df, plan, progress)
        except ValueError as e:
            _fail(err, str(e))
        if not quiet:                       # a warning, not an error: --quiet drops it
            _warn_missing_columns(result.report, err)

        if not dry_run:
            progress.stage_start("write", "write", note=target.name)
            try:
                write_frame(result.df, target)
            except WriteError as e:
                progress.stage_end("write", "failed", summary=error_summary(e))
                _fail(err, str(e))                      # already "could not write <path>: <cause>"
            except (OSError, ValueError) as e:
                progress.stage_end("write", "failed", summary=error_summary(e))
                _fail(err, f"could not write {target}: {e}")
            progress.stage_end("write", "done", summary=plural(len(result.df), "row"))

    # ── Report: JSON or terminal on stdout, status on stderr ──────────────
    if json_output:
        from .jsonutil import dumps
        typer.echo(dumps(result.report.to_dict()))
    else:
        from .reporter import print_clean_report
        print_clean_report(result.report, _mask_password(source), target, dry_run=dry_run)

    if not dry_run and not quiet:
        err.print(f"[dim]Saved {escape(printable(target))}[/dim]", soft_wrap=True)


def _is_same_file(target: Path, source: Path) -> bool:
    """
    True when writing `target` would overwrite the existing file `source`.
    resolve() sees through symlinks and ".."; samefile() also sees what a
    path cannot show: hard links and, on case-insensitive filesystems
    (macOS, Windows), a different spelling of the same name. samefile()
    needs both files to exist; a target that does not exist yet cannot be
    the input, and the resolve() comparison still covers odd spellings.
    """
    if not source.exists():
        return False
    if target.resolve() == source.resolve():
        return True
    return target.exists() and os.path.samefile(target, source)


def _fail(err: "Console", message: str) -> "NoReturn":
    """Print an error on stderr and exit 1 (config and IO errors)."""
    from rich.markup import escape
    from .           import theme
    err.print(f"[{theme.ERROR_STYLE}]Error:[/] {escape(printable(message))}", soft_wrap=True)
    raise typer.Exit(code=1)


def _warn_missing_columns(report: "CleanReport", err: "Console", side: str = "") -> None:
    """Spec 3.3: a rule naming a column that does not exist is skipped, with a warning."""
    from rich.markup import escape
    from .           import theme
    for action in report.actions:
        if action.action == "skipped" and action.note.endswith("column not found"):
            rule = action.note.removesuffix(": column not found")
            where = f"{side}: " if side else ""
            err.print(
                f"[{theme.WARNING_STYLE}]Warning:[/] {where}{escape(printable(rule))} names column "
                f"{escape(repr(action.column))}, which does not exist; rule skipped",
                soft_wrap = True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Watch mode helper
# ─────────────────────────────────────────────────────────────────────────────

def _rerun(callback, params: dict) -> None:
    """
    Run the diff command again with the options of the first run. A re-run
    that ends with a non-zero exit (a FAIL finding, a file that is mid-write)
    must not end the watcher: typer.Exit is a RuntimeError, not a SystemExit,
    so both are caught.
    """
    try:
        callback(**params, watch=False)
    except (SystemExit, typer.Exit):
        pass


def _run_watch_mode(callback, params: dict):
    """
    Use the watchdog library to monitor files and re-run `callback(**params)`
    on change. `params` holds every parameter of the diff command; `before`
    and `after` name the files to watch.
    watchdog is an optional dependency: pip install "datadelta-cli[watch]"
    """
    from rich.console import Console

    from . import theme
    console = Console(stderr=True)

    try:
        from watchdog.observers import Observer
        from watchdog.events    import FileSystemEventHandler
        import time
    except ImportError:
        console.print(
            f"[{theme.ERROR_STYLE}]watchdog is required for --watch mode.[/]\n"
            "[dim]Install it: pip install \"datadelta-cli\\[watch]\"[/dim]"
        )
        raise typer.Exit(code=1)

    before, after = params["before"], params["after"]
    watched_files = {str(Path(before).resolve()), str(Path(after).resolve())}

    class _Handler(FileSystemEventHandler):
        def on_modified(self, event):
            if event.src_path in watched_files:
                console.clear()
                console.print("[dim]File changed — re-running diff...[/dim]\n")
                _rerun(callback, params)

    observer = Observer()
    # Watch the directory containing the before file
    observer.schedule(_Handler(), path=str(Path(before).parent), recursive=False)
    observer.start()

    console.print("[dim]Watching for changes. Press Ctrl+C to stop.[/dim]\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
