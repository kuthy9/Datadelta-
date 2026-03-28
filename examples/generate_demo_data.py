"""
generate_demo_data.py — Creates test datasets for all scenarios.

Run: python examples/generate_demo_data.py

Outputs:
  CSV:   etl_before.csv / etl_after.csv
  XLSX:  etl_before.xlsx / etl_after.xlsx (same data, Excel format)
         → demonstrates Excel support
  CSV:   migration_before.csv / migration_after.csv
  CSV:   ab_control.csv / ab_treatment.csv
  CSV:   logistics_before.csv / logistics_after.csv
         → column names match metrics_logistics.yaml
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

rng = np.random.default_rng(42)
out = Path(__file__).parent


def etl_scenario():
    """
    Simulates a daily ETL run where the APAC region data went missing.
    Demonstrates: category disappearance, null rate increase, row loss.
    """
    regions = ["NA", "EMEA", "APAC", "LATAM"]
    n = 1000

    before = pd.DataFrame({
        "order_id":    range(1, n + 1),
        "region":      rng.choice(regions, n),
        "revenue":     np.round(rng.normal(250, 80, n), 2),
        "status":      rng.choice(["completed", "pending", "cancelled"], n, p=[0.7, 0.2, 0.1]),
        "created_at":  pd.date_range("2024-01-01", periods=n, freq="1h").astype(str),
    })

    # Simulate: APAC rows gone + some nulls in revenue
    after = before[before["region"] != "APAC"].copy().reset_index(drop=True)
    null_idx = rng.choice(len(after), 30, replace=False)
    after.loc[null_idx, "revenue"] = None

    before.to_csv(out / "etl_before.csv", index=False)
    after.to_csv(out / "etl_after.csv", index=False)

    # Also save as Excel — same data, different format
    before.to_excel(out / "etl_before.xlsx", index=False, engine="openpyxl")
    after.to_excel(out / "etl_after.xlsx",   index=False, engine="openpyxl")

    print("✅ ETL scenario:")
    print("   etl_before.csv / etl_after.csv")
    print("   etl_before.xlsx / etl_after.xlsx  (Excel format — same data)")


def migration_scenario():
    """
    Simulates a DB migration where some keys got duplicated and a column type changed.
    Demonstrates: key duplicates, type change.
    """
    n = 500
    before = pd.DataFrame({
        "user_id":  range(1, n + 1),
        "email":    [f"user{i}@example.com" for i in range(1, n + 1)],
        "plan":     rng.choice(["free", "pro", "enterprise"], n, p=[0.6, 0.3, 0.1]),
        "spend":    np.round(rng.exponential(100, n), 2),
    })

    after = before.copy()
    # Simulate: 10 duplicate IDs (migration bug)
    dupes = before.sample(10, random_state=1)
    after = pd.concat([after, dupes], ignore_index=True)
    # Simulate: spend column accidentally cast to string
    after["spend"] = after["spend"].astype(str)

    before.to_csv(out / "migration_before.csv", index=False)
    after.to_csv(out / "migration_after.csv", index=False)
    print("✅ Migration scenario: migration_before.csv / migration_after.csv")


def ab_test_scenario():
    """
    Simulates an A/B test where the treatment group is accidentally older and richer.
    Demonstrates: distribution imbalance (a real A/B test pitfall).
    """
    n = 800
    control = pd.DataFrame({
        "user_id": range(1, n + 1),
        "country": rng.choice(["US", "UK", "CA", "AU"], n),
        "age":     rng.integers(18, 65, n),
        "ltv":     np.round(rng.normal(200, 50, n), 2),
    })
    # Treatment group skews older and higher LTV — a pre-experiment bias
    treatment = pd.DataFrame({
        "user_id": range(n + 1, 2 * n + 1),
        "country": rng.choice(["US", "UK", "CA", "AU"], n),
        "age":     rng.integers(40, 70, n),            # older
        "ltv":     np.round(rng.normal(310, 50, n), 2), # higher LTV
    })

    control.to_csv(out / "ab_control.csv", index=False)
    treatment.to_csv(out / "ab_treatment.csv", index=False)
    print("✅ A/B test scenario: ab_control.csv / ab_treatment.csv")


def logistics_scenario():
    """
    Creates a logistics dataset whose columns match metrics_logistics.yaml.
    Simulates a bad day: APAC orders gone, cancellation rate spiked,
    some revenue nulls, and dead stock rate exceeded threshold.
    """
    n = 1000
    regions = ["NA", "EMEA", "APAC", "LATAM"]
    today = datetime.today()

    # "Before": a healthy day
    before = pd.DataFrame({
        "order_id":          range(1, n + 1),
        "region":            rng.choice(regions, n, p=[0.4, 0.3, 0.2, 0.1]),
        "status":            rng.choice(
            ["completed", "pending", "cancelled", "returned"],
            n, p=[0.72, 0.16, 0.07, 0.05]
        ),
        "revenue":           np.round(rng.lognormal(5.5, 0.8, n), 2),
        "units_shipped":     rng.integers(1, 50, n),
        "last_outbound_date": [
            (today - timedelta(days=int(d))).strftime("%Y-%m-%d")
            for d in rng.integers(1, 120, n)          # mix of fresh and stale
        ],
    })

    # "After": a bad day
    # 1. APAC rows missing
    after = before[before["region"] != "APAC"].copy().reset_index(drop=True)
    # 2. Cancellation rate spiked
    cancel_idx = rng.choice(len(after), 60, replace=False)
    after.loc[cancel_idx, "status"] = "cancelled"
    # 3. Revenue nulls introduced
    null_idx = rng.choice(len(after), 25, replace=False)
    after.loc[null_idx, "revenue"] = None
    # 4. Make more items stale (>90 days)
    stale_idx = rng.choice(len(after), 80, replace=False)
    after.loc[stale_idx, "last_outbound_date"] = (today - timedelta(days=100)).strftime("%Y-%m-%d")

    before.to_csv(out / "logistics_before.csv", index=False)
    after.to_csv(out / "logistics_after.csv", index=False)
    print("✅ Logistics scenario: logistics_before.csv / logistics_after.csv")
    print("   (column names match examples/metrics_logistics.yaml)")


if __name__ == "__main__":
    print("Generating demo datasets...\n")
    etl_scenario()
    migration_scenario()
    ab_test_scenario()
    logistics_scenario()

    print("\n── Quick start commands ──────────────────────────────────────────")
    print("# Basic ETL diff (CSV)")
    print("datadiff diff examples/etl_before.csv examples/etl_after.csv --scenario etl\n")

    print("# Same data from Excel files")
    print("datadiff diff examples/etl_before.xlsx examples/etl_after.xlsx --scenario etl\n")

    print("# Migration check with explicit key column")
    print("datadiff diff examples/migration_before.csv examples/migration_after.csv \\")
    print("  --scenario migration --key user_id\n")

    print("# A/B test balance check")
    print("datadiff diff examples/ab_control.csv examples/ab_treatment.csv --scenario ab-test\n")

    print("# Logistics diff with custom metrics (copy metrics.yaml first)")
    print("cp examples/metrics_logistics.yaml metrics.yaml")
    print("datadiff diff examples/logistics_before.csv examples/logistics_after.csv\n")

    print("# Export HTML report")
    print("datadiff diff examples/logistics_before.csv examples/logistics_after.csv \\")
    print("  --export report.html\n")

    print("# Generate natural language story (requires ANTHROPIC_API_KEY)")
    print("datadiff diff examples/logistics_before.csv examples/logistics_after.csv --story\n")

    print("# Generate metrics.yaml for a new dataset (requires ANTHROPIC_API_KEY)")
    print('datadiff init --from examples/logistics_before.csv \\')
    print('  --business "We are a logistics company. Key metrics: dead stock rate, cancellation rate."')