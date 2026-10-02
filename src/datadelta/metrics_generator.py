"""
metrics_generator.py — LLM-assisted metrics.yaml generation.

This powers the `datadelta init` command. Given a sample data file and
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

PRIVACY:
  By default, per column we send the name, dtype, distinct-value count,
  null rate and up to 5 sample values (the first non-null values). With
  `datadelta init --no-samples` (include_samples=False) we send only the
  column names and dtypes: no cell value, and no statistic computed from
  the values, leaves the machine.

OUTPUT:
  Every status line, the streamed YAML and every error go to stderr;
  `datadelta init` writes nothing to stdout. A failure (no API key, an
  unloadable sample file, a failed LLM request, invalid generated YAML,
  an unwritable output) is printed on one line and then raised as
  MetricsGenerationError, which the CLI turns into exit code 1.
"""

import os
import yaml
from pathlib import Path

import pandas as pd

from .          import theme
from .safetext  import printable
from .jsonutil  import dumps, to_jsonable
from .safe_eval import ALLOWED_ATTRS, ALLOWED_BUILTINS, ALLOWED_NP, ALLOWED_PD


# Number of non-null example values per column when samples are included.
SAMPLES_PER_COLUMN = 5

# Model used by `datadelta init`.
INIT_MODEL = "claude-sonnet-4-20250514"


class MetricsGenerationError(RuntimeError):
    """init could not produce metrics.yaml; the message has already been printed."""


# The custom-expression whitelist from safe_eval.py, spelled out for the
# LLM so generated `custom` metrics pass validation.
_EXPRESSION_METHODS   = ", ".join(sorted(ALLOWED_ATTRS))
_EXPRESSION_FUNCTIONS = ", ".join(
    [f"np.{name}" for name in sorted(ALLOWED_NP)]
    + [f"pd.{name}" for name in sorted(ALLOWED_PD)]
    + sorted(ALLOWED_BUILTINS)
)


INIT_SYSTEM_PROMPT = f"""You are a data engineer helping set up monitoring for a data pipeline.

The user will provide:
1. A description of their business and what metrics they care about
2. A summary of their dataset's columns: name and dtype, and usually the
   distinct count (n_unique), null rate (null_pct) and a few sample values

Your job is to generate a valid metrics.yaml file.

STRICT RULES:
- Output ONLY valid YAML. No explanation, no markdown fences, no preamble.
- Start directly with `business_context:` on line 1.
- Only reference column names that exist in the provided column list.
- Use only these metric types: staleness, ratio, rate, completeness, custom
- Write 3–6 metrics that are genuinely useful for the stated business context.
- Keep descriptions concise and business-friendly (one sentence each).
- All threshold values must be floats between 0 and 1 (they are proportions/rates).
- A column may have no `samples` field (the user chose to share only column names
  and dtypes, so n_unique and null_pct are absent too). Infer its meaning from its
  name and dtype only, and do not invent match_value literals you cannot see;
  prefer completeness, ratio and staleness metrics then.
- A `custom` expression is checked against a whitelist before it runs. Use only
  `df`, `df['column']`, comparisons, arithmetic and & | ~, plus
  these methods/attributes: {_EXPRESSION_METHODS};
  these functions and constants: {_EXPRESSION_FUNCTIONS}.
  No lambdas, comprehensions, imports, or names starting with "_".

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
    sample_file:          str,
    business_description: str,
    output_path:          str  = "metrics.yaml",
    include_samples:      bool = True,
) -> None:
    """
    Main entry point for `datadelta init`.
    Loads the sample file, asks Claude to generate metrics.yaml, saves it.

    include_samples=False (the --no-samples flag) sends only column names
    and dtypes — no cell values and no statistics computed from them.

    Raises MetricsGenerationError, after printing the reason to stderr,
    when ANTHROPIC_API_KEY is missing, the sample file cannot be loaded,
    the LLM request fails (a wrong or expired key, the network, a quota),
    the generated YAML fails validation (the raw reply is then saved to
    metrics_draft.yaml), or the result cannot be written.
    """
    from rich.console import Console
    from rich.markup  import escape
    console = Console(stderr=True)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        console.print(f"\n[{theme.ERROR_STYLE}]Error:[/] ANTHROPIC_API_KEY not set.")
        console.print("[dim]  export ANTHROPIC_API_KEY=your_key[/dim]\n")
        raise MetricsGenerationError("ANTHROPIC_API_KEY not set")

    # ── Step 1: Load the sample file and extract column metadata ─────────────
    from .loader import _mask_password, load_file
    shown = _mask_password(str(sample_file))           # a connection string: password masked
    console.print(f"\n[dim]Loading sample file: {escape(printable(shown))}[/dim]")
    try:
        df = load_file(sample_file)
    except Exception as e:
        console.print(f"[{theme.ERROR_STYLE}]Failed to load file:[/] {escape(printable(e))}")
        raise MetricsGenerationError(f"failed to load {shown}") from e

    column_summary = _summarize_columns(df, include_samples=include_samples)
    console.print(f"[dim]Found {len(df.columns)} columns, {len(df):,} rows[/dim]")
    if not include_samples:
        console.print("[dim]--no-samples: sending only column names and dtypes[/dim]")

    # ── Step 2: Build the prompt ──────────────────────────────────────────────
    user_message = _build_user_message(business_description, column_summary)

    # ── Step 3: Call Claude ───────────────────────────────────────────────────
    console.print("\n[bold]Generating metrics.yaml...[/bold]\n")

    import anthropic

    raw_yaml = ""

    # Stream the response so the user sees progress. markup=False: YAML
    # flow lists such as [region, status] must not be read as Rich tags.
    # Any SDK, network or quota error ends init with one line, as --story
    # does, instead of a traceback.
    try:
        client = anthropic.Anthropic(api_key=api_key)
        with client.messages.stream(
            model      = INIT_MODEL,
            max_tokens = 1500,
            system     = INIT_SYSTEM_PROMPT,
            messages   = [{"role": "user", "content": user_message}],
        ) as stream:
            for text in stream.text_stream:
                console.print(printable(text), end="", markup=False, highlight=False)
                raw_yaml += text
    except Exception as e:
        console.print()
        console.print(
            f"[{theme.ERROR_STYLE}]LLM request failed:[/] {escape(type(e).__name__)}: {escape(printable(e))}",
            soft_wrap = True,
        )
        raise MetricsGenerationError("LLM request failed") from e

    console.print("\n")

    # ── Step 4: Validate the YAML before saving ───────────────────────────────
    try:
        parsed = yaml.safe_load(raw_yaml)
        if not isinstance(parsed, dict) or "metrics" not in parsed:
            raise ValueError("Generated YAML is missing 'metrics' key")
    except Exception as e:
        console.print(f"[{theme.ERROR_STYLE}]Generated YAML failed validation:[/] {escape(printable(e))}")
        console.print(f"[{theme.WARNING_STYLE}]Raw output saved to metrics_draft.yaml for manual editing[/]")
        Path("metrics_draft.yaml").write_text(raw_yaml, encoding="utf-8")
        raise MetricsGenerationError("generated YAML failed validation") from e

    # ── Step 5: Save ──────────────────────────────────────────────────────────
    out = Path(output_path)
    if out.exists():
        console.print(
            f"[{theme.WARNING_STYLE}]{escape(printable(output_path))} already exists. "
            f"Saving as metrics_generated.yaml[/]"
        )
        out = Path("metrics_generated.yaml")

    try:
        out.write_text(raw_yaml, encoding="utf-8")
    except OSError as e:
        console.print(f"[{theme.ERROR_STYLE}]Could not write {escape(printable(out))}:[/] {escape(printable(e.strerror or e))}")
        raise MetricsGenerationError(f"could not write {out}") from e
    # One plain confirmation line, as `clean` prints, and the next step.
    # `diff` picks up ./metrics.yaml by itself; any other file needs -m.
    run = "datadelta diff before.csv after.csv"
    if out != Path("metrics.yaml"):
        run += f" -m {out}"
    console.print(f"Saved {escape(printable(out))}", soft_wrap=True)
    console.print(f"[dim]Review and edit it, then run: {escape(printable(run))}[/dim]", soft_wrap=True)


def _summarize_columns(df: pd.DataFrame, include_samples: bool = True) -> list[dict]:
    """
    Extract column metadata for the LLM prompt. Raw rows are never sent.

      include_samples=True   name, dtype, distinct count, null rate and up
                             to SAMPLES_PER_COLUMN non-null sample values
                             (JSON-ready: numpy → native, Timestamp → ISO)
      include_samples=False  name and dtype only (spec 2.4: --no-samples
                             sends nothing computed from the cell values)
    """
    summary = []
    for col in df.columns:
        series = df[col]
        entry  = {"name": str(col), "dtype": str(series.dtype)}
        if include_samples:
            entry["n_unique"] = int(series.nunique())
            entry["null_pct"] = round(float(series.isna().mean()), 3)
            entry["samples"]  = to_jsonable(
                series.dropna().head(SAMPLES_PER_COLUMN).tolist()
            )
        summary.append(entry)
    return summary


def _build_user_message(business_description: str, column_summary: list[dict]) -> str:
    return (
        f"Business context:\n{business_description}\n\n"
        f"Dataset columns:\n{dumps(column_summary)}\n\n"
        f"Generate the metrics.yaml file now."
    )
