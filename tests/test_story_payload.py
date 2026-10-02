"""
test_story_payload.py — What `--story` sends to the LLM.

The payload is the diff result minus primary-key sample values, with every
finding's category-label list capped at 10 (the rest summarized as
"+k more"), and with metric-error findings reduced to the metric name and
the exception type (the exception text can quote a cell value). Counts,
aggregates and everything else are unchanged.
"""

from __future__ import annotations

import json

import pandas as pd

from datadelta.differ   import DiffResult, Finding, compute_diff
from datadelta.jsonutil import dumps
from datadelta.metrics  import MetricDefinition, MetricsConfig
from datadelta.profiler import profile_columns
from datadelta.story    import _build_user_message, generate_story, story_payload


NEW_LABELS = [f"NEWREGION-{i:02d}" for i in range(25)]


def _diff() -> DiffResult:
    """
    before: 100 orders ORD-0001..ORD-0100, 4 regions.
    after:  the first 70 orders (30 keys deleted) + 3 duplicated keys,
            and 25 brand-new region labels on the first 25 rows.
    """
    regions = ["NA", "EMEA", "APAC", "LATAM"]
    before = pd.DataFrame({
        "order_id": [f"ORD-{i:04d}" for i in range(1, 101)],
        "region":   [regions[i % 4] for i in range(100)],
        "revenue":  [100.0 + i for i in range(100)],
    })
    after = before.iloc[:70].copy()
    after.loc[:24, "region"] = NEW_LABELS
    after = pd.concat([after, after.iloc[:3]], ignore_index=True)
    return compute_diff(before, after, profile_columns(before, after), key_column=None)


def _by_metric_key(findings: list[dict], key: str) -> dict:
    matches = [f for f in findings if key in f["metric"]]
    assert len(matches) == 1, [f["title"] for f in findings]
    return matches[0]


# ── Key samples ───────────────────────────────────────────────────────────────

def test_integrity_findings_carry_counts_but_no_key_samples():
    result  = _diff()
    payload = story_payload(result)

    deleted = _by_metric_key(payload["findings"], "deleted_count")
    assert deleted["metric"] == {"deleted_count": 30, "delete_pct": 0.3}
    assert deleted["detail"] == "Keys present in before but not in after."

    dupes = _by_metric_key(payload["findings"], "duplicate_count")
    assert dupes["metric"] == {"duplicate_count": 3}
    assert dupes["detail"] == "Column 'order_id' has 3 duplicate value(s) in the after dataset."

    assert "ORD-" not in json.dumps(payload)


def test_new_key_detail_has_no_sample():
    finding = Finding(
        layer    = "integrity",
        column   = "user_id",
        severity = "INFO",
        title    = "2 new key(s) in 'user_id'",
        detail   = "New keys in after that weren't in before. Sample: ['U-77', 'U-78']",
        metric   = {"added_count": 2, "sample": ["U-77", "U-78"]},
    )
    result  = DiffResult(rows_before=1, rows_after=3, row_delta=2, row_delta_pct=2.0, findings=[finding])
    payload = story_payload(result)
    assert payload["findings"][0]["metric"] == {"added_count": 2}
    assert payload["findings"][0]["detail"] == "New keys in after that weren't in before."
    assert "U-7" not in json.dumps(payload)


# ── Category labels ───────────────────────────────────────────────────────────

def test_new_category_labels_are_capped_at_ten():
    payload = story_payload(_diff())
    new = _by_metric_key(payload["findings"], "new_values")

    assert new["metric"]["new_values"] == NEW_LABELS[:10] + ["+15 more"]
    assert new["title"] == f"New categories in 'region': {NEW_LABELS[:10] + ['+15 more']}"
    assert "NEWREGION-10" not in json.dumps(payload)
    assert new["detail"] == "25 new value(s) appeared in column 'region'."


def test_missing_category_labels_are_capped_and_sorted():
    gone = [f"SKU-{i:02d}" for i in range(12)]
    finding = Finding(
        layer    = "distribution",
        column   = "sku",
        severity = "FAIL",
        title    = f"Categories disappeared from 'sku': {sorted(gone)}",
        detail   = "12 category value(s) from the before dataset are completely absent from after.",
        metric   = {"missing_values": list(reversed(gone))},
    )
    result  = DiffResult(rows_before=12, rows_after=0, row_delta=-12, row_delta_pct=-1.0, findings=[finding])
    payload = story_payload(result)

    capped = gone[:10] + ["+2 more"]
    assert payload["findings"][0]["metric"]["missing_values"] == capped
    assert payload["findings"][0]["title"] == f"Categories disappeared from 'sku': {capped}"


def test_lists_within_the_cap_are_unchanged():
    finding = Finding(
        layer    = "distribution",
        column   = "tier",
        severity = "INFO",
        title    = "New categories in 'tier': ['gold', 'silver']",
        detail   = "2 new value(s) appeared in column 'tier'.",
        metric   = {"new_values": ["silver", "gold"]},
    )
    result  = DiffResult(rows_before=5, rows_after=5, row_delta=0, row_delta_pct=0.0, findings=[finding])
    payload = story_payload(result)
    assert payload["findings"][0]["metric"]["new_values"] == ["gold", "silver"]
    assert payload["findings"][0]["title"] == "New categories in 'tier': ['gold', 'silver']"


def test_max_labels_is_configurable():
    new = _by_metric_key(story_payload(_diff(), max_labels=3)["findings"], "new_values")
    assert new["metric"]["new_values"] == NEW_LABELS[:3] + ["+22 more"]


# ── Everything else ───────────────────────────────────────────────────────────

def test_summary_and_other_findings_are_unchanged():
    result   = _diff()
    original = result.to_dict()
    payload  = story_payload(result)

    assert payload["summary"] == original["summary"]
    assert len(payload["findings"]) == len(original["findings"])
    untouched = [
        (p, o) for p, o in zip(payload["findings"], original["findings"])
        if not ({"sample", "new_values", "missing_values"} & set(o["metric"]))
    ]
    assert untouched, "expected share-shift / mean-shift findings in the fixture"
    for p, o in untouched:
        assert p == o


def test_payload_does_not_mutate_the_result():
    result = _diff()
    story_payload(result)
    deleted = next(f for f in result.findings if "deleted_count" in f.metric)
    new     = next(f for f in result.findings if "new_values" in f.metric)
    assert len(deleted.metric["sample"]) == 5
    assert "Sample:" in deleted.detail
    assert len(new.metric["new_values"]) == 25


# ── Metric errors ─────────────────────────────────────────────────────────────

SECRET        = "SECRET-VALUE-123"
METRIC_NAME   = "avg_amount"
RAW_EXCEPTION = f"could not convert string to float: '{SECRET}'"


def _metric_error_diff() -> DiffResult:
    """
    A custom metric whose expression fails because one cell of `amount` is a
    string; numpy's exception text quotes that cell.
    """
    frame = pd.DataFrame({"id": [1, 2, 3], "amount": ["10.5", SECRET, "7.25"]})
    config = MetricsConfig(metrics=[MetricDefinition(
        name        = METRIC_NAME,
        description = "Mean order amount",
        type        = "custom",
        column      = "amount",
        expression  = "df['amount'].astype(float).mean()",
    )])
    return compute_diff(
        frame, frame, profile_columns(frame, frame),
        key_column=None, metrics_config=config,
    )


def _error_finding(findings: list[dict]) -> dict:
    matches = [f for f in findings if f["layer"] == "custom"]
    assert len(matches) == 1, [f["title"] for f in findings]
    return matches[0]


def test_metric_error_finding_keeps_the_full_message_locally():
    result  = _metric_error_diff()
    finding = _error_finding(result.to_dict()["findings"])   # what --json prints

    assert finding["title"] == f"[metrics.yaml error] '{METRIC_NAME}': {RAW_EXCEPTION}"
    assert finding["metric"] == {"name": METRIC_NAME, "error": True, "error_type": "ValueError"}
    assert SECRET in dumps(result.to_dict())


def test_story_payload_hides_the_metric_exception_text():
    result  = _metric_error_diff()
    payload = story_payload(result)
    finding = _error_finding(payload["findings"])

    assert finding["title"] == f"[metrics.yaml error] '{METRIC_NAME}': ValueError"
    assert finding["metric"] == {"name": METRIC_NAME, "error": True, "error_type": "ValueError"}
    assert finding["severity"] == "WARN"

    sent = dumps(payload)
    assert SECRET not in sent
    assert RAW_EXCEPTION not in sent
    assert "could not convert" not in sent
    assert METRIC_NAME in sent
    assert "ValueError" in sent

    # the result object itself still carries the full message for the terminal
    assert SECRET in _error_finding(result.to_dict()["findings"])["title"]


def test_metric_error_detail_is_replaced_with_a_fixed_sentence():
    finding = Finding(
        layer    = "custom",
        column   = "amount",
        severity = "WARN",
        title    = f"[metrics.yaml error] 'm': boom {SECRET}",
        detail   = f"Check this: {SECRET}",
        metric   = {"name": "m", "error": True, "error_type": "KeyError"},
    )
    result  = DiffResult(rows_before=1, rows_after=1, row_delta=0, row_delta_pct=0.0, findings=[finding])
    reduced = story_payload(result)["findings"][0]

    assert reduced["title"]  == "[metrics.yaml error] 'm': KeyError"
    assert reduced["detail"] == "Check your metrics.yaml definition for 'm'."
    assert SECRET not in dumps(story_payload(result))


# ── What actually reaches the LLM ─────────────────────────────────────────────

def test_user_message_is_built_from_the_payload():
    message = _build_user_message(_diff())
    assert "ORD-" not in message
    assert "+15 more" in message
    assert "NEWREGION-10" not in message


def test_generate_story_sends_no_key_samples(fake_anthropic):
    generate_story(_diff(), provider="claude")
    assert len(fake_anthropic.calls) == 1
    sent = fake_anthropic.user_message
    assert "ORD-" not in sent
    assert "NEWREGION-24" not in sent
    assert '"deleted_count": 30' in sent



def test_generate_story_sends_no_metric_exception_text(fake_anthropic):
    generate_story(_metric_error_diff(), provider="claude")
    assert len(fake_anthropic.calls) == 1
    sent = fake_anthropic.user_message
    assert SECRET not in sent
    assert "could not convert" not in sent
    assert f"[metrics.yaml error] '{METRIC_NAME}': ValueError" in sent


# ── Values of the key column (security review I2) ─────────────────────────────
#
# A non-unique --key (an orders table keyed by customer email) profiles as a
# category, so its deleted and new values came back as category labels and
# share-shift values, past the key-sample redaction of the integrity layer.

EMAIL_DOMAIN = "@example.com"


def _email_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    before: 30 customers, 3 orders each (unique ratio 1/3: a category).
    after:  customers 10..29, customer10 now with 30 orders (a share shift),
            plus one new customer.
    """
    emails = [f"customer{i:02d}{EMAIL_DOMAIN}" for i in range(30)]
    before_emails = [e for e in emails for _ in range(3)]
    after_emails  = [emails[10]] * 30 + [e for e in emails[11:] for _ in range(3)] + [f"newcomer{EMAIL_DOMAIN}"] * 3
    before = pd.DataFrame({"email": before_emails, "amount": [10.0 + i for i in range(len(before_emails))]})
    after  = pd.DataFrame({"email": after_emails,  "amount": [10.0 + i for i in range(len(after_emails))]})
    return before, after


def _email_diff() -> DiffResult:
    before, after = _email_frames()
    return compute_diff(before, after, profile_columns(before, after), key_column="email")


def test_diff_result_records_the_resolved_key_column():
    assert _email_diff().key_column == "email"
    assert _diff().key_column == "order_id"                            # auto-detected
    df = pd.DataFrame({"n": [1.5, 2.5]})
    assert compute_diff(df, df, profile_columns(df, df), key_column=None).key_column is None


def test_story_payload_sends_no_value_of_the_key_column():
    result = _email_diff()
    local  = dumps(result.to_dict())
    assert EMAIL_DOMAIN in local                                       # the local report keeps them
    for title in ("New categories in 'email'", "Categories disappeared from 'email'", "'email="):
        assert any(f.title.startswith(title) for f in result.findings), title

    sent = dumps(story_payload(result))
    assert EMAIL_DOMAIN not in sent
    assert "customer" not in sent and "newcomer" not in sent
    findings = {f["title"].split(":")[0]: f for f in story_payload(result)["findings"] if f["column"] == "email"}
    assert findings["New categories in 'email'"]["metric"] == {"new_value_count": 1}
    assert findings["Categories disappeared from 'email'"]["metric"] == {"missing_value_count": 10}
    [shift] = [f for f in story_payload(result)["findings"] if f["column"] == "email" and "share" in f["title"]]
    assert "value" not in shift["metric"] and shift["metric"]["delta"] > 0
    assert dumps(result.to_dict()) == local                            # the result is not modified


def test_story_sent_by_the_cli_has_no_key_values(cli, write_csv, fake_anthropic):
    before, after = _email_frames()
    result = cli("diff", write_csv("b.csv", before), write_csv("a.csv", after), "--key", "email", "--story")
    assert result.exit_code in (0, 1), result.stderr
    sent = fake_anthropic.user_message
    assert "'email'" in sent
    assert EMAIL_DOMAIN not in sent
