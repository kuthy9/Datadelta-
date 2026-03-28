"""
scenarios.py — Apply a scenario-specific lens to diff results.

The core diff engine treats all findings equally.
This module re-weights severities based on the use case, so the output
highlights what actually matters for the user's specific scenario.

HOW SEVERITY PROMOTION/DEMOTION WORKS:
  We maintain a severity scale: PASS → INFO → WARN → FAIL
  Promoting a finding moves it one step up the scale.
  Demoting a finding moves it one step down.

  Example — migration scenario:
    Distribution findings are DEMOTED (some drift is expected when migrating)
    Integrity findings are PROMOTED (data loss is the worst outcome)

  This means a distribution WARN becomes INFO (less alarming),
  and an integrity WARN becomes FAIL (more alarming) — without changing
  the underlying detection logic in differ.py.

  The custom metrics layer (Layer 4) is never demoted or promoted by scenarios
  — those thresholds are defined by the user in metrics.yaml and are assumed
  to be intentional.
"""

from .differ import DiffResult, Finding, Severity

SCENARIO_PROMOTIONS: dict[str, dict] = {
    "etl": {
        # ETL: schema and integrity changes are critical, distribution drift expected
        "promote": ["schema", "integrity"],
        "demote":  [],
    },
    "migration": {
        # Migration: getting all rows across is paramount; distribution shift is expected
        "promote": ["integrity", "schema"],
        "demote":  ["distribution"],
    },
    "ab-test": {
        # A/B test: distribution balance between groups is everything
        "promote": ["distribution"],
        "demote":  ["integrity"],
    },
    "general": {
        "promote": [],
        "demote":  [],
    },
}

SCENARIO_LABELS = {
    "etl":       "ETL Validation",
    "migration": "Data Migration",
    "ab-test":   "A/B Test Balance Check",
    "general":   "General Diff",
}

# Severity scale for promotion/demotion arithmetic
_SCALE: list[Severity] = ["PASS", "INFO", "WARN", "FAIL"]


def apply_scenario_lens(result: DiffResult, scenario: str) -> DiffResult:
    """
    Adjust finding severities and sort order based on the scenario.
    Returns the modified DiffResult (modifies in-place, also returns for chaining).
    """
    result.scenario = scenario
    rules = SCENARIO_PROMOTIONS.get(scenario, SCENARIO_PROMOTIONS["general"])

    promoted_layers = set(rules.get("promote", []))
    demoted_layers  = set(rules.get("demote",  []))

    adjusted = []
    for finding in result.findings:
        # Never adjust custom metrics — the user defined those thresholds explicitly
        if finding.layer == "custom":
            adjusted.append(finding)
            continue

        new_severity = _adjust_severity(
            finding.severity, finding.layer, promoted_layers, demoted_layers
        )
        adjusted.append(Finding(
            layer    = finding.layer,
            column   = finding.column,
            severity = new_severity,
            title    = finding.title,
            detail   = finding.detail,
            metric   = finding.metric,
        ))

    # Sort by severity (worst first), then by layer for grouping
    layer_order = {"schema": 0, "integrity": 1, "distribution": 2, "custom": 3}
    sev_order   = {"FAIL": 0, "WARN": 1, "INFO": 2, "PASS": 3}
    adjusted.sort(key=lambda f: (sev_order.get(f.severity, 4), layer_order.get(f.layer, 5)))

    result.findings = adjusted
    return result


def _adjust_severity(
    severity:         Severity,
    layer:            str,
    promoted_layers:  set,
    demoted_layers:   set,
) -> Severity:
    if layer in promoted_layers:
        idx = min(_SCALE.index(severity) + 1, len(_SCALE) - 1)
        return _SCALE[idx]
    if layer in demoted_layers:
        idx = max(_SCALE.index(severity) - 1, 0)
        return _SCALE[idx]
    return severity