"""
differ.py — Core diff engine.

Runs four layers of analysis in sequence:

  Layer 1 — Schema Diff
    Columns added, removed, or type-changed between before and after.
    These are structural changes — the "shape" of the data changed.

  Layer 2 — Distribution Diff
    Statistical changes inside columns that exist in both datasets.
    Strategy adapts to semantic type (numeric vs categorical vs datetime).
    - Numeric:   mean shift + KS test (detects shape changes beyond mean)
    - Categorical: share shift per value, new/missing categories
    - Datetime:  range change
    All columns: null rate change

  Layer 3 — Integrity Diff
    Primary key health: duplicates, deletions, new keys.
    Uses the user-specified --key column, or auto-detects an ID column.

  Layer 4 — Custom Metrics (NEW in v0.2)
    User-defined KPIs from metrics.yaml.
    Only runs if a MetricsConfig is provided (i.e., metrics.yaml exists).

Each finding has a severity: FAIL / WARN / INFO / PASS
The `threshold` parameter (default 0.10 = 10%) controls sensitivity
for distribution shift warnings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TYPE_CHECKING
import pandas as pd
import numpy as np
from scipy import stats

from .profiler import DataProfile

# Avoid circular import: metrics.py imports Finding from differ.py
# We use TYPE_CHECKING to allow the type hint without runtime import
if TYPE_CHECKING:
    from .metrics import MetricsConfig


Severity = Literal["PASS", "WARN", "FAIL", "INFO"]


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Finding:
    """
    A single diff finding — one specific thing that changed (or didn't).

    layer:    Which analysis layer produced this (schema/distribution/integrity/custom)
    column:   Which column it relates to (None for table-level findings)
    severity: How serious is this? FAIL > WARN > INFO > PASS
    title:    One-line summary shown in the terminal
    detail:   Longer explanation shown in HTML report and --verbose mode
    metric:   Raw numbers for JSON export, HTML charts, and LLM context
    """
    layer:    Literal["schema", "distribution", "integrity", "custom"]
    column:   str | None
    severity: Severity
    title:    str
    detail:   str
    metric:   dict = field(default_factory=dict)


@dataclass
class DiffResult:
    """
    The complete result of a diff operation.
    This object is passed through scenarios.py (re-weighting),
    reporter.py (terminal output), story.py (LLM), and export.py (HTML).
    """
    rows_before:    int
    rows_after:     int
    row_delta:      int
    row_delta_pct:  float

    findings: list[Finding] = field(default_factory=list)
    scenario: str           = "general"

    @property
    def has_failures(self) -> bool:
        return any(f.severity == "FAIL" for f in self.findings)

    @property
    def has_warnings(self) -> bool:
        return any(f.severity == "WARN" for f in self.findings)

    @property
    def summary_severity(self) -> Severity:
        if self.has_failures: return "FAIL"
        if self.has_warnings: return "WARN"
        return "PASS"

    def to_dict(self) -> dict:
        """Serialize to dict for JSON output and LLM context."""
        return {
            "summary": {
                "severity":      self.summary_severity,
                "rows_before":   self.rows_before,
                "rows_after":    self.rows_after,
                "row_delta":     self.row_delta,
                "row_delta_pct": round(self.row_delta_pct, 4),
            },
            "findings": [
                {
                    "layer":    f.layer,
                    "column":   f.column,
                    "severity": f.severity,
                    "title":    f.title,
                    "detail":   f.detail,
                    "metric":   f.metric,
                }
                for f in self.findings
            ],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def compute_diff(
    df_before:      pd.DataFrame,
    df_after:       pd.DataFrame,
    profile:        DataProfile,
    key_column:     str | None,
    threshold:      float = 0.10,
    metrics_config: "MetricsConfig | None" = None,
) -> DiffResult:
    """
    Run all four diff layers and return a DiffResult.
    metrics_config is optional; if None, Layer 4 is skipped.
    """
    row_delta     = len(df_after) - len(df_before)
    row_delta_pct = row_delta / max(len(df_before), 1)

    result = DiffResult(
        rows_before   = len(df_before),
        rows_after    = len(df_after),
        row_delta     = row_delta,
        row_delta_pct = row_delta_pct,
    )

    # Run the four layers
    result.findings.extend(_schema_diff(df_before, df_after, profile))
    result.findings.extend(_distribution_diff(df_before, df_after, profile, threshold))
    result.findings.extend(_integrity_diff(df_before, df_after, profile, key_column))

    # Layer 4: only if metrics.yaml was loaded
    if metrics_config and metrics_config.has_metrics:
        from .metrics import evaluate_all_metrics
        result.findings.extend(evaluate_all_metrics(df_before, df_after, metrics_config))

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Layer 1: Schema Diff
# ─────────────────────────────────────────────────────────────────────────────

def _schema_diff(
    df_before: pd.DataFrame,
    df_after:  pd.DataFrame,
    profile:   DataProfile,
) -> list[Finding]:
    findings = []

    for col, cp in profile.columns.items():
        if not cp.exists_in_before:
            # Column appeared in after → newly added
            findings.append(Finding(
                layer    = "schema",
                column   = col,
                severity = "WARN",
                title    = f"Column added: '{col}' (type: {cp.dtype_after})",
                detail   = f"Column '{col}' does not exist in the before dataset.",
                metric   = {"column": col, "dtype": cp.dtype_after},
            ))

        elif not cp.exists_in_after:
            # Column was in before but gone from after → deletion is serious
            findings.append(Finding(
                layer    = "schema",
                column   = col,
                severity = "FAIL",
                title    = f"Column removed: '{col}' (was: {cp.dtype_before})",
                detail   = f"Column '{col}' existed in before but is missing from after.",
                metric   = {"column": col, "dtype": cp.dtype_before},
            ))

        elif cp.dtype_before != cp.dtype_after:
            # Type changed — could silently break downstream queries
            findings.append(Finding(
                layer    = "schema",
                column   = col,
                severity = "WARN",
                title    = f"Type changed: '{col}' ({cp.dtype_before} → {cp.dtype_after})",
                detail   = (
                    f"Data type of '{col}' changed from {cp.dtype_before} to {cp.dtype_after}. "
                    f"This may break downstream SQL casts or joins."
                ),
                metric   = {"column": col, "before": cp.dtype_before, "after": cp.dtype_after},
            ))

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2: Distribution Diff
# ─────────────────────────────────────────────────────────────────────────────

def _distribution_diff(
    df_before: pd.DataFrame,
    df_after:  pd.DataFrame,
    profile:   DataProfile,
    threshold: float,
) -> list[Finding]:
    findings = []

    for col, cp in profile.columns.items():
        # Skip columns that don't exist in both datasets
        # (already handled by schema diff)
        if not (cp.exists_in_before and cp.exists_in_after):
            continue

        s_before = df_before[col]
        s_after  = df_after[col]

        # Null rate applies to ALL column types
        null_finding = _check_null_rate(col, s_before, s_after, threshold)
        if null_finding:
            findings.append(null_finding)

        # Type-specific distribution checks
        if cp.semantic_type == "numeric":
            findings.extend(_numeric_diff(col, s_before.dropna(), s_after.dropna(), threshold))

        elif cp.semantic_type == "category":
            findings.extend(_category_diff(col, s_before.dropna(), s_after.dropna(), threshold))

        elif cp.semantic_type == "datetime":
            findings.extend(_datetime_diff(col, s_before.dropna(), s_after.dropna()))

    return findings


def _check_null_rate(
    col: str,
    s_before: pd.Series,
    s_after:  pd.Series,
    threshold: float,
) -> Finding | None:
    """Alert if the null rate changes significantly."""
    null_before = s_before.isna().mean()
    null_after  = s_after.isna().mean()
    delta       = null_after - null_before

    if abs(delta) <= threshold:
        return None

    # A jump beyond 2x the threshold is a FAIL
    severity  = "FAIL" if abs(delta) > threshold * 2 else "WARN"
    direction = "increased" if delta > 0 else "decreased"

    return Finding(
        layer    = "distribution",
        column   = col,
        severity = severity,
        title    = f"Null rate {direction}: '{col}' ({null_before:.1%} → {null_after:.1%})",
        detail   = f"Null rate changed by {delta:+.1%} in column '{col}'.",
        metric   = {
            "null_before": round(null_before, 4),
            "null_after":  round(null_after, 4),
            "delta":       round(delta, 4),
        },
    )


def _numeric_diff(
    col: str,
    s_before: pd.Series,
    s_after:  pd.Series,
    threshold: float,
) -> list[Finding]:
    findings = []

    if len(s_before) < 5 or len(s_after) < 5:
        return findings  # Too few rows for meaningful stats

    # ── Mean shift ────────────────────────────────────────────────────────────
    mean_before = s_before.mean()
    mean_after  = s_after.mean()

    if mean_before != 0:
        mean_delta_pct = (mean_after - mean_before) / abs(mean_before)
        if abs(mean_delta_pct) > threshold:
            severity = "FAIL" if abs(mean_delta_pct) > threshold * 2 else "WARN"
            findings.append(Finding(
                layer    = "distribution",
                column   = col,
                severity = severity,
                title    = (
                    f"Mean shifted: '{col}' "
                    f"({mean_before:.2f} → {mean_after:.2f}, {mean_delta_pct:+.1%})"
                ),
                detail   = f"The mean of '{col}' changed by {mean_delta_pct:+.1%}.",
                metric   = {
                    "mean_before":   round(mean_before, 4),
                    "mean_after":    round(mean_after, 4),
                    "median_before": round(s_before.median(), 4),
                    "median_after":  round(s_after.median(), 4),
                    "std_before":    round(s_before.std(), 4),
                    "std_after":     round(s_after.std(), 4),
                    "p25_before":    round(s_before.quantile(0.25), 4),
                    "p25_after":     round(s_after.quantile(0.25), 4),
                    "p75_before":    round(s_before.quantile(0.75), 4),
                    "p75_after":     round(s_after.quantile(0.75), 4),
                    "delta_pct":     round(mean_delta_pct, 4),
                },
            ))

    # ── KS test — detects distributional shape changes beyond just the mean ──
    # Example: mean stays the same but the distribution becomes bimodal.
    # The KS statistic measures the max gap between two cumulative distributions.
    # p_value < 0.05 means the difference is statistically significant.
    # ks_stat > 0.1 means the practical effect size is meaningful.
    ks_stat, p_value = stats.ks_2samp(
        s_before.astype(float).values,
        s_after.astype(float).values,
    )
    if p_value < 0.05 and ks_stat > 0.1:
        findings.append(Finding(
            layer    = "distribution",
            column   = col,
            severity = "WARN",
            title    = (
                f"Distribution shape shifted: '{col}' "
                f"(KS={ks_stat:.2f}, p={p_value:.3f})"
            ),
            detail   = (
                f"Kolmogorov-Smirnov test detects a statistically significant "
                f"distribution change in '{col}'. The mean may look similar "
                f"but the shape of the distribution has changed."
            ),
            metric   = {
                "ks_statistic": round(ks_stat, 4),
                "p_value":      round(p_value, 4),
            },
        ))

    return findings


def _category_diff(
    col: str,
    s_before: pd.Series,
    s_after:  pd.Series,
    threshold: float,
) -> list[Finding]:
    findings = []

    vals_before = set(s_before.unique())
    vals_after  = set(s_after.unique())

    # ── New categories appeared ───────────────────────────────────────────────
    new_vals = vals_after - vals_before
    if new_vals:
        findings.append(Finding(
            layer    = "distribution",
            column   = col,
            severity = "INFO",
            title    = f"New categories in '{col}': {sorted(str(v) for v in new_vals)}",
            detail   = f"{len(new_vals)} new value(s) appeared in column '{col}'.",
            metric   = {"new_values": [str(v) for v in new_vals]},
        ))

    # ── Categories disappeared ────────────────────────────────────────────────
    gone_vals = vals_before - vals_after
    if gone_vals:
        gone_ratio = len(gone_vals) / max(len(vals_before), 1)
        severity   = "FAIL" if gone_ratio > 0.5 else "WARN"
        findings.append(Finding(
            layer    = "distribution",
            column   = col,
            severity = severity,
            title    = f"Categories disappeared from '{col}': {sorted(str(v) for v in gone_vals)}",
            detail   = (
                f"{len(gone_vals)} category value(s) from the before dataset "
                f"are completely absent from after. "
                f"This represents {gone_ratio:.0%} of the original category set."
            ),
            metric   = {"missing_values": [str(v) for v in gone_vals]},
        ))

    # ── Share shift for each existing category ────────────────────────────────
    # "APAC was 30% of orders before, now it's 0%" is more actionable than
    # "APAC disappeared" — because the disappearance might be intentional,
    # but the share shift tells you the magnitude.
    dist_before = s_before.value_counts(normalize=True)
    dist_after  = s_after.value_counts(normalize=True)

    for val in set(dist_before.index) & set(dist_after.index):
        share_before = dist_before.get(val, 0.0)
        share_after  = dist_after.get(val, 0.0)
        delta        = share_after - share_before

        if abs(delta) > threshold:
            direction = "grew" if delta > 0 else "shrank"
            findings.append(Finding(
                layer    = "distribution",
                column   = col,
                severity = "WARN",
                title    = (
                    f"'{col}={val}' share {direction}: "
                    f"{share_before:.1%} → {share_after:.1%} ({delta:+.1%})"
                ),
                detail   = (
                    f"The proportion of '{val}' in column '{col}' "
                    f"changed by {delta:+.1%}."
                ),
                metric   = {
                    "value":  str(val),
                    "before": round(share_before, 4),
                    "after":  round(share_after, 4),
                    "delta":  round(delta, 4),
                },
            ))

    return findings


def _datetime_diff(
    col: str,
    s_before: pd.Series,
    s_after:  pd.Series,
) -> list[Finding]:
    """Report changes in the time range covered by the column."""
    findings = []
    try:
        dt_before = pd.to_datetime(s_before, errors="coerce").dropna()
        dt_after  = pd.to_datetime(s_after,  errors="coerce").dropna()

        if len(dt_before) == 0 or len(dt_after) == 0:
            return findings

        range_before = (dt_before.min(), dt_before.max())
        range_after  = (dt_after.min(),  dt_after.max())

        if range_before != range_after:
            findings.append(Finding(
                layer    = "distribution",
                column   = col,
                severity = "INFO",
                title    = f"Date range changed: '{col}'",
                detail   = (
                    f"Before: {range_before[0].date()} → {range_before[1].date()}\n"
                    f"After:  {range_after[0].date()} → {range_after[1].date()}"
                ),
                metric   = {
                    "min_before": str(range_before[0].date()),
                    "max_before": str(range_before[1].date()),
                    "min_after":  str(range_after[0].date()),
                    "max_after":  str(range_after[1].date()),
                },
            ))
    except Exception:
        pass  # Don't crash on unparseable dates

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3: Integrity Diff
# ─────────────────────────────────────────────────────────────────────────────

def _integrity_diff(
    df_before:  pd.DataFrame,
    df_after:   pd.DataFrame,
    profile:    DataProfile,
    key_column: str | None,
) -> list[Finding]:
    # Auto-detect a key column if the user didn't specify one
    if key_column is None:
        id_cols    = [col for col, cp in profile.columns.items() if cp.semantic_type == "id"]
        key_column = id_cols[0] if id_cols else None

    if key_column is None:
        return []  # No key to track

    if key_column not in df_before.columns or key_column not in df_after.columns:
        return []  # Key column missing from one side (already caught by schema diff)

    return _key_integrity(df_before, df_after, key_column)


def _key_integrity(
    df_before: pd.DataFrame,
    df_after:  pd.DataFrame,
    key_col:   str,
) -> list[Finding]:
    findings = []

    # ── Duplicate keys in after ───────────────────────────────────────────────
    dupe_count = int(df_after[key_col].duplicated().sum())
    if dupe_count > 0:
        dupes = df_after[df_after[key_col].duplicated(keep=False)][key_col].unique()
        sample = list(dupes)[:5]
        findings.append(Finding(
            layer    = "integrity",
            column   = key_col,
            severity = "FAIL",
            title    = f"Primary key duplicates: {dupe_count} duplicate(s) in '{key_col}'",
            detail   = (
                f"Column '{key_col}' has {dupe_count} duplicate values in the after dataset. "
                f"Sample duplicate keys: {sample}"
            ),
            metric   = {"duplicate_count": dupe_count, "sample": [str(k) for k in sample]},
        ))

    keys_before = set(df_before[key_col].dropna())
    keys_after  = set(df_after[key_col].dropna())

    # ── Deleted keys ──────────────────────────────────────────────────────────
    deleted = keys_before - keys_after
    if deleted:
        sample = list(deleted)[:5]
        delete_pct = len(deleted) / max(len(keys_before), 1)
        severity   = "FAIL" if delete_pct > 0.10 else "WARN"  # >10% deleted → FAIL
        findings.append(Finding(
            layer    = "integrity",
            column   = key_col,
            severity = severity,
            title    = (
                f"{len(deleted):,} key(s) deleted from '{key_col}' "
                f"({delete_pct:.1%} of original)"
            ),
            detail   = f"Keys present in before but not in after. Sample: {sample}",
            metric   = {
                "deleted_count": len(deleted),
                "delete_pct":    round(delete_pct, 4),
                "sample":        [str(k) for k in sample],
            },
        ))

    # ── New keys ──────────────────────────────────────────────────────────────
    added = keys_after - keys_before
    if added:
        sample = list(added)[:5]
        findings.append(Finding(
            layer    = "integrity",
            column   = key_col,
            severity = "INFO",
            title    = f"{len(added):,} new key(s) in '{key_col}'",
            detail   = f"New keys in after that weren't in before. Sample: {sample}",
            metric   = {
                "added_count": len(added),
                "sample":      [str(k) for k in sample],
            },
        ))

    return findings