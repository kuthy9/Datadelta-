<div align="center">

# ▲ datadelta

**Your data changed. But what does it mean?**

`datadelta` is a zero-config CLI that compares two datasets and tells you not just *what* changed — but *why it matters*. Powered by statistical analysis, scenario-aware intelligence, and LLM-generated narratives.

[![PyPI version](https://img.shields.io/pypi/v/datadelta?color=black&style=flat-square)](https://pypi.org/project/datadelta)
[![Python](https://img.shields.io/badge/python-3.10%2B-black?style=flat-square)](https://pypi.org/project/datadelta)
[![License: MIT](https://img.shields.io/badge/license-MIT-black?style=flat-square)](LICENSE)

</div>

---

```
$ datadelta diff orders_yesterday.csv orders_today.csv --scenario etl --story

  Loading before: orders_yesterday.csv
  Loading after:  orders_today.csv
  Loaded 6 custom metric(s) from metrics.yaml

  ╭─────────────────────────────────────────────────────╮
  │  datadelta  ·  ETL Validation  ·  8 finding(s)      │
  │  ❌  FAIL                                           │
  ╰─────────────────────────────────────────────────────╯

  Rows  1,000 → 796  (-204, -20.4%)

  Schema Changes
  ✅  No schema changes

  Integrity Checks
  ⚠️   204 key(s) deleted from 'order_id' (20.4% of original)

  Distribution Changes
  ❌  Categories disappeared from 'region': ['APAC']
  ⚠️   Null rate increased: 'revenue' (0.0% → 3.1%)
  ⚠️   Mean shifted: 'revenue' (246.21 → 229.84, -6.6%)

  Custom Metrics  (metrics.yaml)
  ❌  cancellation_rate: 7.00% → 14.84%  (+111.8%)
  ⚠️   dead_stock_rate: 11.20% → 18.92%  (+68.9%)
  ✅  order_id_completeness: 100.00% → 99.60%

  📖 Story  via Claude

  Today's pipeline is missing the entire APAC region — 204 orders,
  20.4% of yesterday's volume — almost certainly a feed outage rather
  than real order loss. More urgently, the cancellation rate has doubled
  to 14.8%, breaching the failure threshold, suggesting a separate
  operational issue. Investigate the APAC pipeline first, then escalate
  the cancellation spike to the ops team.

  2 failure(s)  ·  3 warning(s)
  --export report.html   shareable HTML report
```

---

## Why datadelta?

Every data team asks the same questions, every single day:

> *"Did the ETL run correctly?"*  
> *"Did the migration lose any rows?"*  
> *"Are my A/B test groups actually balanced?"*  
> *"The upstream table changed — what broke?"*

The current answer is: open a BI tool, manually slice dimensions, hope you find the issue. **datadelta automates that entire process in one command.**

|  | `csv-diff` | Evidently AI | **datadelta** |
|--|--|--|--|
| What it compares | Row-by-row | ML feature distributions | **Semantic meaning** |
| Who it's for | Developers | ML engineers | **Data analysts** |
| Setup | None | Jupyter notebook | **None** |
| Custom KPIs | ✗ | ✗ | **✓ metrics.yaml** |
| LLM narrative | ✗ | ✗ | **✓ 4 providers** |
| Scenario-aware | ✗ | ✗ | **✓ ETL / migration / A/B** |
| Output | Text diff | HTML report | **Terminal + JSON + HTML** |

---

## Install

```bash
pip install datadelta
```

With optional database drivers:
```bash
pip install "datadelta[postgres]"   # PostgreSQL
pip install "datadelta[mysql]"      # MySQL
pip install "datadelta[watch]"      # --watch mode
pip install "datadelta[all]"        # everything
```

---

## Quickstart

```bash
# Compare two CSV files
datadelta diff before.csv after.csv

# Excel works too
datadelta diff before.xlsx after.xlsx

# Tell it what kind of comparison you're doing
datadelta diff snapshot.csv live.csv --scenario etl
datadelta diff old_db.csv new_db.csv --scenario migration --key user_id
datadelta diff control.csv treatment.csv --scenario ab-test

# Get an AI-generated narrative
export ANTHROPIC_API_KEY=sk-...
datadelta diff before.csv after.csv --story

# Use DeepSeek, OpenAI, or Gemini instead
datadelta diff before.csv after.csv --story --llm deepseek
datadelta diff before.csv after.csv --story --llm openai
datadelta diff before.csv after.csv --story --llm gemini

# Export a shareable HTML report
datadelta diff before.csv after.csv --export report.html

# Pipe JSON to your CI pipeline
datadelta diff before.csv after.csv --json | jq '.findings[] | select(.severity=="FAIL")'

# Compare directly from a database
datadelta diff "postgresql://user:pw@host/db::orders_jan" \
               "postgresql://user:pw@host/db::orders_feb"
```

---

## Four analysis layers

Every diff runs four layers in sequence. Each layer is independent — a finding in one doesn't affect the others.

```
Layer 1 — Schema
  Columns added, removed, or type-changed.
  A silently broken downstream join starts here.

Layer 2 — Distribution
  Per-column statistical analysis, adapted to semantic type.
  Numeric   → mean shift + KS test (detects shape changes beyond the mean)
  Category  → share shift per value + new / disappeared categories
  Datetime  → range change
  All types → null rate change

Layer 3 — Integrity
  Primary key health: duplicates, deletions, new keys.
  Auto-detected from column names and cardinality.

Layer 4 — Custom Metrics  (your metrics.yaml)
  Company-specific KPIs evaluated on top of the generic checks.
  Completely yours. Never affected by scenario re-weighting.
```

---

## Scenario-aware intelligence

Pass `--scenario` and datadelta re-weights findings based on what you're actually doing.

```bash
--scenario etl        # Promotes: schema + integrity   Demotes: distribution
--scenario migration  # Promotes: integrity            Demotes: distribution
--scenario ab-test    # Promotes: distribution         Demotes: integrity
--scenario general    # Everything equally (default)
```

Same detection logic. Different emphasis. The tool adapts to your context.

---

## Custom metrics: `metrics.yaml`

Define your company's KPIs once. datadelta evaluates them automatically on every run.

```yaml
business_context: |
  We operate a regional logistics network. Core KPIs: dead stock rate
  (idle inventory >90 days), cancellation rate, and revenue per unit.

metrics:

  - name: dead_stock_rate
    description: "% of SKUs with no outbound movement in 90 days"
    type: staleness
    column: last_outbound_date
    threshold_days: 90
    warn_if_above: 0.15
    fail_if_above: 0.30

  - name: cancellation_rate
    description: "% of orders cancelled"
    type: rate
    column: status
    match_value: cancelled
    warn_if_above: 0.05
    fail_if_above: 0.12

  - name: avg_revenue_per_unit
    description: "Revenue per shipped unit"
    type: ratio
    numerator: revenue
    denominator: units_shipped
    warn_if_delta_pct: 0.10

  - name: order_id_completeness
    description: "order_id must never be null"
    type: completeness
    column: order_id
    warn_if_below: 0.999

  - name: high_value_order_share
    description: "% of orders with revenue > $1,000"
    type: custom
    expression: "(df['revenue'] > 1000).mean()"
    warn_if_delta_pct: 0.20
```

Five metric types: `staleness` · `ratio` · `rate` · `completeness` · `custom`

**Don't want to write it yourself?** Generate it with Claude:

```bash
datadelta init \
  --from orders.csv \
  --business "We are a logistics company. Key metrics: dead stock rate, cancellation rate, revenue per unit."
```

The `business_context` field is also injected into `--story` mode — so the LLM narrative uses your company's vocabulary, not generic statistical jargon.

---

## Supported data sources

| Source | Format |
|--------|--------|
| CSV | `.csv` |
| Excel | `.xlsx`, `.xls` |
| JSON | `.json` |
| Parquet | `.parquet` |
| SQLite | `.sqlite`, `.db` |
| PostgreSQL | `postgresql://user:pw@host/db::table` |
| MySQL | `mysql+pymysql://user:pw@host/db::table` |
| SQL Server | `mssql+pyodbc://user:pw@host/db::table` |

Mix formats freely — `datadelta diff snapshot.parquet live_export.xlsx` works.

---

## LLM providers for `--story`

| Provider | Flag | API key env var |
|----------|------|----------------|
| Claude (default) | `--llm claude` | `ANTHROPIC_API_KEY` |
| OpenAI | `--llm openai` | `OPENAI_API_KEY` |
| DeepSeek | `--llm deepseek` | `DEEPSEEK_API_KEY` |
| Gemini | `--llm gemini` | `GEMINI_API_KEY` |

Raw data rows are **never sent to any API.** Only the structured diff findings (a few KB of JSON) are sent. Your data stays on your machine.

---

## CI / CD integration

Exit code is `1` when any FAIL finding exists.

```yaml
# .github/workflows/data-quality.yml
- name: Validate daily ETL output
  run: |
    datadelta diff data/yesterday.parquet data/today.parquet \
      --scenario etl \
      --json > diff_result.json
  # Build fails automatically if FAIL findings exist
```

---

## Try the demo

```bash
git clone https://github.com/your-username/datadelta
cd datadelta
pip install -e ".[all]"

python examples/generate_demo_data.py

# ETL scenario
datadelta diff examples/etl_before.csv examples/etl_after.csv --scenario etl

# With custom metrics
cp examples/metrics_logistics.yaml metrics.yaml
datadelta diff examples/logistics_before.csv examples/logistics_after.csv

# Export HTML
datadelta diff examples/logistics_before.csv examples/logistics_after.csv --export report.html
```

---

## Architecture

```
datadelta/
├── cli.py                 Entry point. Commands: diff + init.
├── loader.py              Unified source loader.
│                          CSV/JSON/Parquet → DuckDB
│                          Excel → openpyxl
│                          Databases → SQLAlchemy
├── profiler.py            Semantic type detection per column.
│                          id / numeric / category / datetime / text
├── differ.py              Four-layer diff engine.
├── metrics.py             Custom metrics engine (Layer 4).
│                          staleness / ratio / rate / completeness / custom
├── metrics_generator.py   LLM-assisted metrics.yaml generation.
├── scenarios.py           Scenario lens (severity re-weighting).
├── reporter.py            Rich terminal output.
├── story.py               LLM narrative. 4 providers supported.
└── export.py              Self-contained HTML report.
```

---

## Full CLI reference

```
datadelta diff <before> <after> [OPTIONS]

  -s, --scenario    etl | migration | ab-test | general   [default: general]
  -k, --key         Primary key column (auto-detected if omitted)
  -t, --threshold   Warn threshold for distribution shift  [default: 0.10]
  -m, --metrics     Path to metrics.yaml (auto-detected if omitted)
      --no-metrics  Disable metrics.yaml auto-detection
      --story       Generate LLM narrative
      --llm         claude | openai | deepseek | gemini   [default: claude]
  -e, --export      Export HTML report
      --json        Raw JSON output
  -w, --watch       Re-run on file change

datadelta init [OPTIONS]

  -f, --from        Sample data file (required)
  -b, --business    Business description (required)
  -o, --output      Output path                           [default: metrics.yaml]
```

---

<div align="center">

MIT License · Built with Python · Contributions welcome

</div>