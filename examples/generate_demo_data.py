"""
generate_demo_data.py — Creates test datasets for all scenarios.

Run: python examples/generate_demo_data.py

Outputs (next to this script by default):
  CSV:   etl_before.csv / etl_after.csv
  XLSX:  etl_before.xlsx / etl_after.xlsx (same data, Excel format)
         → demonstrates Excel support
  CSV:   migration_before.csv / migration_after.csv
  CSV:   ab_control.csv / ab_treatment.csv
  CSV:   logistics_before.csv / logistics_after.csv
         → column names match metrics_logistics.yaml
  CSV:   messy_orders.csv
         → input for `datadelta clean` (mixed dates, currency text, null tokens, duplicates)

From Python, main(out_dir) writes the same files into another directory;
scripts/render_readme_assets.py uses that to build the README samples
without touching examples/. Every run with the same SEED produces the
same data (the logistics dates are relative to today).
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

SEED    = 42                      # one generator, shared by the scenarios in a fixed order
OUT_DIR = Path(__file__).parent   # default output directory


def etl_scenario(out_dir: Path, rng: np.random.Generator) -> None:
    """
    Simulates a daily ETL run where the APAC region data went missing.
    Demonstrates: category disappearance, null rate increase, row loss.
    """
    regions = ["NAM", "EMEA", "APAC", "LATAM"]     # not "NA": a default null token for `clean`
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

    before.to_csv(out_dir / "etl_before.csv", index=False)
    after.to_csv(out_dir / "etl_after.csv", index=False)

    # Also save as Excel — same data, different format
    before.to_excel(out_dir / "etl_before.xlsx", index=False, engine="openpyxl")
    after.to_excel(out_dir / "etl_after.xlsx",   index=False, engine="openpyxl")

    print("ETL scenario:")
    print("   etl_before.csv / etl_after.csv")
    print("   etl_before.xlsx / etl_after.xlsx  (Excel format — same data)")


def migration_scenario(out_dir: Path, rng: np.random.Generator) -> None:
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

    before.to_csv(out_dir / "migration_before.csv", index=False)
    after.to_csv(out_dir / "migration_after.csv", index=False)
    print("Migration scenario: migration_before.csv / migration_after.csv")


def ab_test_scenario(out_dir: Path, rng: np.random.Generator) -> None:
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

    control.to_csv(out_dir / "ab_control.csv", index=False)
    treatment.to_csv(out_dir / "ab_treatment.csv", index=False)
    print("A/B test scenario: ab_control.csv / ab_treatment.csv")


def logistics_scenario(out_dir: Path, rng: np.random.Generator) -> None:
    """
    Creates a logistics dataset whose columns match metrics_logistics.yaml.
    Simulates a bad day: APAC orders gone, cancellation rate spiked,
    some revenue nulls, and dead stock rate exceeded threshold.
    """
    n = 1000
    regions = ["NAM", "EMEA", "APAC", "LATAM"]     # not "NA": a default null token for `clean`
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

    before.to_csv(out_dir / "logistics_before.csv", index=False)
    after.to_csv(out_dir / "logistics_after.csv", index=False)
    print("Logistics scenario: logistics_before.csv / logistics_after.csv")
    print("   (column names match examples/metrics_logistics.yaml)")


def messy_scenario(out_dir: Path) -> None:
    """
    An order export that needs `datadelta clean` before it can be trusted.
    Demonstrates: four date spellings in one column (day-first d/m/Y among
    them), currency text with thousands separators, full-width digits,
    null tokens (N/A, -, null), padded text, zip codes with leading zeros
    (they must stay text) and 5 exact duplicate rows (reported, dropped
    only with --dedupe exact).
    """
    n = 300
    local = np.random.default_rng(7)                 # own generator: output does not depend on the other scenarios
    full_width = str.maketrans("0123456789", "０１２３４５６７８９")
    styles = ["%Y-%m-%d", "%Y/%m/%d", "%Y年%m月%d日", "%d/%m/%Y"]
    days = pd.date_range("2024-03-01", periods=n, freq="D")

    amounts    = np.round(local.gamma(2.0, 600.0, n), 2)
    quantities = local.integers(1, 40, n)
    zips       = local.integers(500, 99_999, n)

    messy = pd.DataFrame({
        "order_id":   [f"ORD-{i:05d}" for i in range(1, n + 1)],
        "order_date": [day.strftime(styles[i % 4]) for i, day in enumerate(days)],
        "amount":     ["N/A" if i % 25 == 0 else f"${a:,.2f}" for i, a in enumerate(amounts)],
        "quantity":   ["-" if i % 30 == 0 else (str(q).translate(full_width) if i % 3 == 0 else str(q))
                       for i, q in enumerate(quantities)],
        "region":     ["null" if i % 40 == 0 else r for i, r in
                       enumerate(local.choice([" EMEA", "APAC ", "AMER", "LATAM", "EMEA"], n))],
        "zip_code":   ["N/A" if i % 50 == 0 else f"{z:05d}" for i, z in enumerate(zips)],
        "paid":       local.choice(["yes", "no", "Y", "N"], n),
    })
    messy = pd.concat([messy, messy.iloc[:5]], ignore_index=True)   # 5 exact duplicates

    messy.to_csv(out_dir / "messy_orders.csv", index=False)
    print("Messy data scenario: messy_orders.csv")
    print("   (mixed dates, currency text, full-width digits, null tokens, 5 duplicate rows)")


def main(out_dir: Path = OUT_DIR) -> None:
    """Write every demo dataset into out_dir (created if missing)."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)

    print("Generating demo datasets...\n")
    etl_scenario(out_dir, rng)
    migration_scenario(out_dir, rng)
    ab_test_scenario(out_dir, rng)
    logistics_scenario(out_dir, rng)
    messy_scenario(out_dir)


def print_quickstart() -> None:
    """The commands to try next, for the datasets in examples/."""
    print("\n── Quick start commands ──────────────────────────────────────────")
    print("# Basic ETL diff (CSV)")
    print("datadelta diff examples/etl_before.csv examples/etl_after.csv --scenario etl\n")

    print("# Same data from Excel files")
    print("datadelta diff examples/etl_before.xlsx examples/etl_after.xlsx --scenario etl\n")

    print("# Migration check with explicit key column")
    print("datadelta diff examples/migration_before.csv examples/migration_after.csv \\")
    print("  --scenario migration --key user_id\n")

    print("# A/B test balance check")
    print("datadelta diff examples/ab_control.csv examples/ab_treatment.csv --scenario ab-test\n")

    print("# Logistics diff with custom metrics (copy metrics.yaml first)")
    print("cp examples/metrics_logistics.yaml metrics.yaml")
    print("datadelta diff examples/logistics_before.csv examples/logistics_after.csv\n")

    print("# Export HTML report")
    print("datadelta diff examples/logistics_before.csv examples/logistics_after.csv \\")
    print("  --export report.html\n")

    print("# Clean a messy export: preview, then write examples/messy_orders.clean.csv")
    print("datadelta clean examples/messy_orders.csv --dry-run")
    print("datadelta clean examples/messy_orders.csv --dedupe exact\n")

    print("# Generate natural language story (requires ANTHROPIC_API_KEY)")
    print("datadelta diff examples/logistics_before.csv examples/logistics_after.csv --story\n")

    print("# Generate metrics.yaml for a new dataset (requires ANTHROPIC_API_KEY)")
    print('datadelta init --from examples/logistics_before.csv \\')
    print('  --business "We are a logistics company. Key metrics: dead stock rate, cancellation rate."')


if __name__ == "__main__":
    main()
    print_quickstart()
