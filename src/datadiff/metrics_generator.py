"""
metrics_generator.py — LLM-assisted metrics.yaml generation.

This powers the `datadiff init` command. Given a sample data file and
a natural language description of the user's business, it calls Claude
to generate a starter metrics.yaml.

HOW IT WORKS (the "skill" mechanism):
  1. We load the sample file and extract column metadata (names, types, samples)
  2. We build a structured prompt describing: the data shape + the business context
  3. Claude generates valid YAML based on the prompt
  4. We validate the YAML can be parsed, then save it to disk

WHY THIS IS THE "CLAUDE SKILL" EQUIVALENT:
  Claude Skills inject pre-written context into an LLM call at runtime.
  Here, we do the same thing in reverse: we ask the LLM to *produce*
  the context (metrics.yaml) that will then be injected into future LLM
  calls (--story mode). The generated file becomes the persistent "skill".

  Think of it as: "teach the tool your business vocabulary once,
  and it will use that vocabulary in every future diff."
"""

import os
import json
import yaml
from pathlib import Path

import pandas as pd


INIT_SYSTEM_PROMPT = """You are a data engineer helping set up monitoring for a data pipeline.

The user will provide:
1. A description of their business and what metrics they care about
2. A summary of their dataset's columns (names, types, sample values)

Your job is to generate a valid metrics.yaml file.

STRICT RULES:
- Output ONLY valid YAML. No explanation, no markdown fences, no preamble.
- Start directly with `business_context:` on line 1.
- Only reference column names that exist in the provided column list.
- Use only these metric types: staleness, ratio, rate, completeness, custom
- Write 3–6 metrics that are genuinely useful for the stated business context.
- Keep descriptions concise and business-friendly (one sentence each).
- All threshold values must be floats between 0 and 1 (they are proportions/rates).

YAML SCHEMA:
business_context: |
  <2-3 sentences about what this dataset is and what matters>

metrics:
  - name: snake_case_metric_name
    description: "One sentence business description"
    type: staleness | ratio | rate | completeness | custom
    # staleness fields:
    column: column_name
    threshold_days: 90
    warn_if_above: 0.15
    fail_if_above: 0.30
    # ratio fields:
    numerator: col_a
    denominator: col_b
    warn_if_delta_pct: 0.10
    fail_if_delta_pct: 0.25
    # rate fields:
    column: column_name
    match_value: some_value
    warn_if_above: 0.05
    fail_if_above: 0.10
    # completeness fields:
    column: column_name
    warn_if_below: 0.99
    fail_if_below: 0.95
    # custom fields:
    expression: "(df['col'] > 1000).mean()"
    warn_if_delta_pct: 0.20
"""


def generate_metrics_yaml(
    sample_file: str,
    business_description: str,
    output_path: str = "metrics.yaml",
) -> None:
    """
    Main entry point for `datadiff init`.
    Loads the sample file, asks Claude to generate metrics.yaml, saves it.
    """
    from rich.console import Console
    from rich.panel import Panel
    console = Console()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        console.print("\n[red]Error: ANTHROPIC_API_KEY not set.[/red]")
        console.print("[dim]  export ANTHROPIC_API_KEY=your_key[/dim]\n")
        return

    # ── Step 1: Load the sample file and extract column metadata ─────────────
    console.print(f"\n[dim]Loading sample file: {sample_file}[/dim]")
    from .loader import load_file
    try:
        df = load_file(sample_file)
    except Exception as e:
        console.print(f"[red]Failed to load file: {e}[/red]")
        return

    column_summary = _summarize_columns(df)
    console.print(f"[dim]Found {len(df.columns)} columns, {len(df):,} rows[/dim]")

    # ── Step 2: Build the prompt ──────────────────────────────────────────────
    user_message = _build_user_message(business_description, column_summary)

    # ── Step 3: Call Claude ───────────────────────────────────────────────────
    console.print("\n[bold]Generating metrics.yaml...[/bold]\n")

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    raw_yaml = ""

    # Stream the response so the user sees progress
    with client.messages.stream(
        model      = "claude-sonnet-4-20250514",
        max_tokens = 1500,
        system     = INIT_SYSTEM_PROMPT,
        messages   = [{"role": "user", "content": user_message}],
    ) as stream:
        for text in stream.text_stream:
            console.print(text, end="")
            raw_yaml += text

    console.print("\n")

    # ── Step 4: Validate the YAML before saving ───────────────────────────────
    try:
        parsed = yaml.safe_load(raw_yaml)
        if not isinstance(parsed, dict) or "metrics" not in parsed:
            raise ValueError("Generated YAML is missing 'metrics' key")
    except Exception as e:
        console.print(f"[red]Generated YAML failed validation: {e}[/red]")
        console.print("[yellow]Raw output saved to metrics_draft.yaml for manual editing[/yellow]")
        Path("metrics_draft.yaml").write_text(raw_yaml, encoding="utf-8")
        return

    # ── Step 5: Save ──────────────────────────────────────────────────────────
    out = Path(output_path)
    if out.exists():
        console.print(f"[yellow]⚠️  {output_path} already exists. Saving as metrics_generated.yaml[/yellow]")
        out = Path("metrics_generated.yaml")

    out.write_text(raw_yaml, encoding="utf-8")
    console.print(Panel(
        f"[green]✅ Saved to [bold]{out}[/bold][/green]\n\n"
        f"Review and edit the file, then run:\n"
        f"  [bold]datadiff diff before.csv after.csv[/bold]\n\n"
        f"The tool will auto-detect metrics.yaml and apply your custom metrics.",
        title="metrics.yaml generated",
        border_style="green",
    ))


def _summarize_columns(df: pd.DataFrame) -> list[dict]:
    """
    Extract column metadata for the LLM prompt:
    name, dtype, and a few sample values (non-null).
    We don't send raw data rows to the API — just metadata.
    """
    summary = []
    for col in df.columns:
        series = df[col]
        samples = series.dropna().head(5).tolist()
        # Convert numpy types to Python native for JSON serialization
        samples = [_to_native(v) for v in samples]

        summary.append({
            "name":     col,
            "dtype":    str(series.dtype),
            "n_unique": int(series.nunique()),
            "null_pct": round(series.isna().mean(), 3),
            "samples":  samples,
        })
    return summary


def _build_user_message(business_description: str, column_summary: list[dict]) -> str:
    return (
        f"Business context:\n{business_description}\n\n"
        f"Dataset columns:\n{json.dumps(column_summary, indent=2)}\n\n"
        f"Generate the metrics.yaml file now."
    )


def _to_native(val):
    """Convert numpy scalar to Python native type for JSON serialization."""
    import numpy as np
    if isinstance(val, (np.integer,)):  return int(val)
    if isinstance(val, (np.floating,)): return float(val)
    if isinstance(val, (np.bool_,)):    return bool(val)
    return val