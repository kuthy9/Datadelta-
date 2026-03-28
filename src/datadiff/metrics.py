"""
metrics.py — Custom business metrics engine.

This is the "skill" system. Users define their company-specific KPIs
in a metrics.yaml file, and this module evaluates them as an additional
diff layer (Layer 4) on top of the generic statistical checks.

WHY YAML?
  YAML is readable by non-engineers. A DA can hand this file to a
  business analyst or product manager and they can understand/edit it.
  It also means the "skill" is version-controllable (git commit it!).

HOW IT RELATES TO THE CLAUDE SKILL CONCEPT:
  A Claude "skill" is essentially: context injected into an LLM prompt
  at runtime. Our metrics.yaml serves a dual purpose:
    1. Structured rules → evaluated programmatically (no LLM needed)
    2. business_context → injected into --story LLM prompt so the
       narrative uses your company's vocabulary, not generic stats jargon

METRIC TYPES:
  staleness   → What % of rows have a date column older than N days?
                Example: dead stock rate (no movement in 90 days)

  ratio       → sum(col_a) / sum(col_b), check if delta exceeds threshold
                Example: revenue per unit = sum(revenue) / sum(units_sold)

  rate        → What % of rows match a condition (col == value)?
                Example: cancellation rate = (status == "cancelled").mean()

  completeness → What % of values are non-null?
                 Example: customer_id must always be present (> 99%)

  custom      → Arbitrary pandas expression returning a float 0–1 or scalar
                Example: (df['revenue'] > 1000).mean()
                Note: eval() is used. This is intentional for a developer tool.

SEVERITY LOGIC:
  For each metric, we compute the value for both before and after datasets,
  then check against user-defined thresholds:
    - fail_if_above / warn_if_above   → absolute threshold on the after value
    - fail_if_below / warn_if_below   → absolute threshold on the after value
    - fail_if_delta_pct / warn_if_delta_pct → relative change between before and after

  If no threshold is set, the metric is reported as INFO (informational only).
"""

from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import pandas as pd
import numpy as np


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MetricDefinition:
    """One metric entry from metrics.yaml."""
    name:        str
    description: str
    type:        str   # staleness | ratio | rate | completeness | custom

    # Column references
    column:      str | None = None   # used by: staleness, rate, completeness
    numerator:   str | None = None   # used by: ratio
    denominator: str | None = None   # used by: ratio

    # Condition (for rate type)
    match_value: Any        = None   # the value to count: status == match_value

    # Custom expression (for custom type)
    expression:  str | None = None   # pandas eval string, e.g. "(df['x'] > 0).mean()"

    # Staleness config
    threshold_days: int | None = None

    # Absolute thresholds (applied to the AFTER value)
    warn_if_above: float | None = None
    fail_if_above: float | None = None
    warn_if_below: float | None = None
    fail_if_below: float | None = None

    # Relative thresholds (applied to |after - before| / |before|)
    warn_if_delta_pct: float | None = None
    fail_if_delta_pct: float | None = None


@dataclass
class MetricsConfig:
    """
    The full parsed content of metrics.yaml.
    business_context is a free-text field injected into LLM prompts.
    """
    business_context: str                    = ""
    metrics:          list[MetricDefinition] = field(default_factory=list)

    @property
    def has_metrics(self) -> bool:
        return len(self.metrics) > 0


# ─────────────────────────────────────────────────────────────────────────────
# Loader
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_METRICS_FILE = "metrics.yaml"


def load_metrics(path: str | Path | None = None) -> MetricsConfig | None:
    """
    Load metrics.yaml from the given path, or auto-detect in the current directory.
    Returns None if no file is found (caller should handle gracefully).
    """
    if path is None:
        candidate = Path(DEFAULT_METRICS_FILE)
        if not candidate.exists():
            return None
        path = candidate

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Metrics file not found: {path}")

    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"metrics.yaml must be a YAML mapping, got: {type(raw)}")

    return _parse_metrics_config(raw)


def _parse_metrics_config(raw: dict) -> MetricsConfig:
    """Convert the raw YAML dict into a typed MetricsConfig."""
    config = MetricsConfig(
        business_context=raw.get("business_context", ""),
    )

    for entry in raw.get("metrics", []):
        try:
            metric = MetricDefinition(
                name        = entry["name"],
                description = entry.get("description", ""),
                type        = entry["type"],
                column      = entry.get("column"),
                numerator   = entry.get("numerator"),
                denominator = entry.get("denominator"),
                match_value = entry.get("match_value"),
                expression  = entry.get("expression"),
                threshold_days    = entry.get("threshold_days"),
                warn_if_above     = entry.get("warn_if_above"),
                fail_if_above     = entry.get("fail_if_above"),
                warn_if_below     = entry.get("warn_if_below"),
                fail_if_below     = entry.get("fail_if_below"),
                warn_if_delta_pct = entry.get("warn_if_delta_pct"),
                fail_if_delta_pct = entry.get("fail_if_delta_pct"),
            )
            config.metrics.append(metric)
        except KeyError as e:
            raise ValueError(f"Metric entry missing required field {e}: {entry}") from e

    return config


# ─────────────────────────────────────────────────────────────────────────────
# Evaluator
# ─────────────────────────────────────────────────────────────────────────────

# Importing here to avoid circular import (differ imports metrics)
from .differ import Finding, Severity


def evaluate_all_metrics(
    df_before: pd.DataFrame,
    df_after:  pd.DataFrame,
    config:    MetricsConfig,
) -> list[Finding]:
    """
    Run all metrics defined in the config against both DataFrames.
    Returns a list of Finding objects to be appended to the DiffResult.
    """
    findings = []
    for metric in config.metrics:
        try:
            finding = _evaluate_metric(metric, df_before, df_after)
            findings.append(finding)
        except Exception as e:
            # A broken metric definition should not crash the whole diff.
            # Report it as a WARN so the user can fix their metrics.yaml.
            findings.append(Finding(
                layer    = "custom",
                column   = metric.column,
                severity = "WARN",
                title    = f"[metrics.yaml error] '{metric.name}': {e}",
                detail   = f"Check your metrics.yaml definition for '{metric.name}'.",
                metric   = {},
            ))
    return findings


def _evaluate_metric(
    metric:    MetricDefinition,
    df_before: pd.DataFrame,
    df_after:  pd.DataFrame,
) -> Finding:
    """Evaluate a single metric and return a Finding."""
    value_before = _compute(metric, df_before)
    value_after  = _compute(metric, df_after)

    severity = _determine_severity(metric, value_before, value_after)

    # Format the values for display
    # If values look like rates/proportions (0–1), show as percentages
    is_rate = 0.0 <= value_before <= 1.0 and 0.0 <= value_after <= 1.0
    fmt = "{:.2%}" if is_rate else "{:.4f}"

    delta_pct = None
    if value_before != 0:
        delta_pct = (value_after - value_before) / abs(value_before)

    delta_str = f" ({delta_pct:+.1%})" if delta_pct is not None else ""

    return Finding(
        layer    = "custom",
        column   = metric.column,
        severity = severity,
        title    = (
            f"{metric.name}: "
            f"{fmt.format(value_before)} → {fmt.format(value_after)}"
            f"{delta_str}"
        ),
        detail   = metric.description,
        metric   = {
            "name":         metric.name,
            "type":         metric.type,
            "value_before": round(value_before, 6),
            "value_after":  round(value_after, 6),
            "delta_pct":    round(delta_pct, 4) if delta_pct is not None else None,
        },
    )


def _compute(metric: MetricDefinition, df: pd.DataFrame) -> float:
    """
    Dispatch to the right computation based on metric type.
    All methods return a float scalar.
    """
    if metric.type == "staleness":
        return _compute_staleness(metric, df)

    elif metric.type == "ratio":
        return _compute_ratio(metric, df)

    elif metric.type == "rate":
        return _compute_rate(metric, df)

    elif metric.type == "completeness":
        return _compute_completeness(metric, df)

    elif metric.type == "custom":
        return _compute_custom(metric, df)

    else:
        raise ValueError(
            f"Unknown metric type: '{metric.type}'. "
            f"Supported: staleness, ratio, rate, completeness, custom"
        )


def _compute_staleness(metric: MetricDefinition, df: pd.DataFrame) -> float:
    """
    What % of rows have a date column older than threshold_days?
    Returns a value 0.0–1.0 (proportion of stale rows).

    Example: dead_stock_rate — SKUs with no outbound movement in 90 days.
    We compare each date value against today's date.
    """
    if not metric.column or metric.column not in df.columns:
        raise ValueError(f"Column '{metric.column}' not found in dataset")
    if metric.threshold_days is None:
        raise ValueError("staleness metric requires 'threshold_days'")

    dates = pd.to_datetime(df[metric.column], errors="coerce")
    today = pd.Timestamp.now().normalize()  # midnight today

    # Days since last activity; NaT values are treated as infinitely stale
    days_since = (today - dates).dt.days
    stale_mask = days_since > metric.threshold_days
    return stale_mask.mean()  # proportion 0–1


def _compute_ratio(metric: MetricDefinition, df: pd.DataFrame) -> float:
    """
    sum(numerator_col) / sum(denominator_col).
    Returns 0.0 if denominator is zero to avoid ZeroDivisionError.

    Example: revenue_per_unit = sum(revenue) / sum(units_sold)
    Example: margin_rate = sum(profit) / sum(revenue)
    """
    for col in (metric.numerator, metric.denominator):
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in dataset")

    denom = df[metric.denominator].sum()
    if denom == 0:
        return 0.0
    return df[metric.numerator].sum() / denom


def _compute_rate(metric: MetricDefinition, df: pd.DataFrame) -> float:
    """
    What % of rows have column == match_value?
    Returns a value 0.0–1.0.

    Example: cancellation_rate = (status == "cancelled").mean()
    Example: high_value_flag_rate = (flag == 1).mean()
    """
    if not metric.column or metric.column not in df.columns:
        raise ValueError(f"Column '{metric.column}' not found in dataset")
    if metric.match_value is None:
        raise ValueError("rate metric requires 'match_value'")

    return (df[metric.column] == metric.match_value).mean()


def _compute_completeness(metric: MetricDefinition, df: pd.DataFrame) -> float:
    """
    What % of values in the column are non-null?
    Returns a value 0.0–1.0.

    Example: customer_id_completeness → must be > 0.99
    """
    if not metric.column or metric.column not in df.columns:
        raise ValueError(f"Column '{metric.column}' not found in dataset")

    return df[metric.column].notna().mean()


def _compute_custom(metric: MetricDefinition, df: pd.DataFrame) -> float:
    """
    Evaluate an arbitrary pandas expression.
    The expression has access to:
      df  → the full DataFrame
      pd  → pandas
      np  → numpy

    SECURITY NOTE:
      eval() executes arbitrary Python code. This is acceptable for a
      developer CLI tool where the user controls their own metrics.yaml.
      Do NOT use this in a web service that accepts user-provided expressions.

    Example: "(df['revenue'] > 1000).mean()"
    Example: "df['revenue'].sum() / df['order_count'].sum()"
    """
    if not metric.expression:
        raise ValueError("custom metric requires 'expression'")

    result = eval(metric.expression, {"df": df, "pd": pd, "np": np})  # noqa: S307

    # Coerce to float — the expression might return a numpy scalar
    return float(result)


def _determine_severity(
    metric:       MetricDefinition,
    value_before: float,
    value_after:  float,
) -> Severity:
    """
    Check the computed values against all configured thresholds.
    Priority: FAIL > WARN > INFO/PASS

    We check absolute thresholds first (is the after value dangerous?),
    then relative change (did it shift a lot from before to after?).
    """
    # ── Absolute thresholds on the after value ────────────────────────────────
    if metric.fail_if_above is not None and value_after > metric.fail_if_above:
        return "FAIL"
    if metric.warn_if_above is not None and value_after > metric.warn_if_above:
        return "WARN"
    if metric.fail_if_below is not None and value_after < metric.fail_if_below:
        return "FAIL"
    if metric.warn_if_below is not None and value_after < metric.warn_if_below:
        return "WARN"

    # ── Relative change threshold ─────────────────────────────────────────────
    has_delta_threshold = (
        metric.warn_if_delta_pct is not None or
        metric.fail_if_delta_pct is not None
    )
    if has_delta_threshold and value_before != 0:
        delta_pct = abs(value_after - value_before) / abs(value_before)
        if metric.fail_if_delta_pct is not None and delta_pct > metric.fail_if_delta_pct:
            return "FAIL"
        if metric.warn_if_delta_pct is not None and delta_pct > metric.warn_if_delta_pct:
            return "WARN"

    # ── No threshold configured → just informational ─────────────────────────
    has_any_threshold = any(v is not None for v in [
        metric.warn_if_above, metric.fail_if_above,
        metric.warn_if_below, metric.fail_if_below,
        metric.warn_if_delta_pct, metric.fail_if_delta_pct,
    ])
    return "PASS" if has_any_threshold else "INFO"