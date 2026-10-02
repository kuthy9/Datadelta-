"""
test_progress_events.py — What load_file, compute_diff and
evaluate_all_metrics report to a ProgressSink.

The dashboard draws one row per stage key in the order stages start, a
bar from the advance events, and a live tally from the finding events,
so those sequences are the contract checked here with RecordingProgress.
"""

from __future__ import annotations

from collections import Counter

import pandas as pd
import pytest

from datadelta.differ   import compute_diff
from datadelta.loader   import load_file, source_label
from datadelta.metrics  import MetricDefinition, MetricsConfig, evaluate_all_metrics
from datadelta.profiler import profile_columns
from datadelta.progress import RecordingProgress, error_summary, plural


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    before: order_id, region, revenue, legacy_flag
    after:  order_id, region, revenue, channel   (legacy_flag removed, channel added)
    Common columns: order_id, region, revenue. 30% of the keys are deleted
    and APAC disappears, so every layer produces at least one finding.
    """
    regions = ["NA", "EMEA", "APAC", "LATAM"]
    before = pd.DataFrame({
        "order_id":    list(range(1, 101)),
        "region":      [regions[i % 4] for i in range(100)],
        "revenue":     [100.0 + i for i in range(100)],
        "legacy_flag": [i % 2 == 0 for i in range(100)],
    })
    after = before[(before["order_id"] <= 70) & (before["region"] != "APAC")].drop(columns="legacy_flag")
    after = after.assign(channel="web").reset_index(drop=True)
    return before, after


def _metrics() -> MetricsConfig:
    return MetricsConfig(metrics=[
        MetricDefinition(name="big_orders", description="", type="custom",
                         expression="(df['revenue'] > 150).mean()"),
        MetricDefinition(name="broken", description="", type="custom",
                         expression="df['no_such_column'].mean()"),
    ])


def _diff(progress=None, metrics_config=None):
    before, after = _frames()
    return compute_diff(
        before, after, profile_columns(before, after),
        key_column     = None,
        metrics_config = metrics_config,
        progress       = progress,
    )


# ── compute_diff ──────────────────────────────────────────────────────────────

def test_compute_diff_stage_order_without_metrics():
    rec = RecordingProgress()
    _diff(rec)
    assert rec.stage_keys() == ["schema", "distribution", "integrity"]
    assert [rec.end_status(k) for k in rec.stage_keys()] == ["done", "done", "done"]


def test_compute_diff_stage_order_with_metrics():
    rec = RecordingProgress()
    _diff(rec, metrics_config=_metrics())
    assert rec.stage_keys() == ["schema", "distribution", "integrity", "metrics"]
    assert rec.end_status("metrics") == "done"
    assert rec.end_summary("metrics") == "2 metrics"


def test_distribution_advances_once_per_common_column():
    rec = RecordingProgress()
    _diff(rec)
    start = next(e for e in rec.events if e.kind == "start" and e.key == "distribution")
    assert start.data["total"] == 3
    assert rec.advances("distribution") == 3
    assert rec.advance_notes("distribution") == ["order_id", "region", "revenue"]


def test_one_finding_event_per_finding():
    rec = RecordingProgress()
    result = _diff(rec, metrics_config=_metrics())
    assert len(rec.findings()) == len(result.findings)
    assert Counter(rec.findings()) == Counter(f.severity for f in result.findings)
    assert {f.layer for f in result.findings} >= {"schema", "distribution", "integrity", "custom"}


def test_schema_summary_counts_changes():
    rec = RecordingProgress()
    _diff(rec)
    assert rec.end_summary("schema") == "2 changes"        # legacy_flag removed, channel added


def test_integrity_notes_the_key_column():
    rec = RecordingProgress()
    _diff(rec)
    start = next(e for e in rec.events if e.kind == "start" and e.key == "integrity")
    assert start.data["note"] == "order_id"


def test_integrity_is_skipped_without_a_key_column():
    before = pd.DataFrame({"region": ["NA", "EMEA"] * 10, "revenue": [1.0] * 20})
    rec = RecordingProgress()
    result = compute_diff(before, before, profile_columns(before, before), key_column=None, progress=rec)
    assert rec.end_status("integrity") == "skipped"
    assert rec.end_summary("integrity") == "no key column"
    assert not [f for f in result.findings if f.layer == "integrity"]


def test_integrity_is_skipped_when_the_given_key_is_missing():
    before = pd.DataFrame({"region": ["NA", "EMEA"] * 10})
    rec = RecordingProgress()
    compute_diff(before, before, profile_columns(before, before), key_column="order_id", progress=rec)
    assert rec.end_status("integrity") == "skipped"
    assert rec.end_summary("integrity") == "key 'order_id' is not on both sides"


def test_progress_does_not_change_the_findings():
    silent   = _diff(None, metrics_config=_metrics())
    recorded = _diff(RecordingProgress(), metrics_config=_metrics())
    assert [f.title for f in silent.findings] == [f.title for f in recorded.findings]


def test_a_crashing_layer_ends_its_stage_failed(monkeypatch):
    def _boom(*args, **kwargs):
        raise RuntimeError("cannot compare\nsecond line")
    monkeypatch.setattr("datadelta.differ._numeric_diff", _boom)

    rec = RecordingProgress()
    with pytest.raises(RuntimeError, match="cannot compare"):
        _diff(rec)
    assert rec.end_status("schema") == "done"
    assert rec.end_status("distribution") == "failed"
    assert rec.end_summary("distribution") == "cannot compare"
    assert "integrity" not in rec.stage_keys()


# ── evaluate_all_metrics ──────────────────────────────────────────────────────

def test_metrics_advance_once_per_metric_with_its_name():
    before, after = _frames()
    rec = RecordingProgress()
    findings = evaluate_all_metrics(before, after, _metrics(), progress=rec)
    assert rec.stage_keys() == ["metrics"]
    start = rec.events[0]
    assert start.data["total"] == 2
    assert rec.advance_notes("metrics") == ["big_orders", "broken"]
    assert rec.findings() == [f.severity for f in findings]
    assert findings[1].severity == "WARN"                  # the broken metric still advances


# ── load_file ─────────────────────────────────────────────────────────────────

def test_load_file_reports_one_stage(write_csv):
    path = write_csv("orders.csv", pd.DataFrame({"a": range(1200), "b": "x", "c": 1.5}))
    rec = RecordingProgress()
    df = load_file(str(path), progress=rec, stage_key="load.before", label="load before")
    assert len(df) == 1200
    assert rec.stage_keys() == ["load.before"]
    assert rec.events[0].data == {"label": "load before", "total": None, "note": "orders.csv"}
    assert rec.end_status("load.before") == "done"
    assert rec.end_summary("load.before") == "1,200 rows · 3 cols"


def test_load_file_defaults_to_the_load_stage(write_csv):
    path = write_csv("t.csv", pd.DataFrame({"a": [1]}))
    rec = RecordingProgress()
    load_file(str(path), progress=rec)
    assert rec.stage_keys() == ["load"]
    assert rec.events[0].data["label"] == "load"


def test_load_file_failure_ends_the_stage_failed_and_reraises(tmp_path):
    rec = RecordingProgress()
    with pytest.raises(FileNotFoundError):
        load_file(str(tmp_path / "missing.csv"), progress=rec, stage_key="load.after", label="load after")
    assert rec.end_status("load.after") == "failed"
    summary = rec.end_summary("load.after")
    assert summary.startswith("File not found:")
    assert "\n" not in summary and len(summary) <= 80


def test_connection_strings_are_labelled_without_the_password(monkeypatch):
    def _refuse(source):
        raise ConnectionError("refused")
    monkeypatch.setattr("datadelta.loader._load_sql", _refuse)

    url = "postgresql://alice:s3cret@db.internal:5432/shop::orders"
    rec = RecordingProgress()
    with pytest.raises(ConnectionError):
        load_file(url, progress=rec)
    assert rec.events[0].data["note"] == "postgresql://alice:***@db.internal:5432/shop::orders"
    assert source_label(url) == rec.events[0].data["note"]
    assert source_label("data/dir/orders.csv") == "orders.csv"


# ── Summary helpers ───────────────────────────────────────────────────────────

def test_plural_and_error_summary():
    assert plural(1, "change") == "1 change"
    assert plural(0, "change") == "0 changes"
    assert plural(1200, "row") == "1,200 rows"
    assert error_summary(ValueError("first\nsecond")) == "first"
    assert error_summary(ValueError("x" * 200)) == "x" * 77 + "..."
    assert error_summary(KeyboardInterrupt()) == "KeyboardInterrupt"
