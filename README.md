# datadelta Δ

> **git diff, but for your data.**

A lightweight CLI tool that compares two datasets and tells you not just _what_ changed, but _what it means_ — statistically, structurally, and in your own business vocabulary.

```
$ datadiff diff logistics_before.csv logistics_after.csv

╭──────────────────────────────────────────────────────╮
│  datadiff  ·  General Diff  ·  7 finding(s)          │
│  ❌  FAIL                                            │
╰──────────────────────────────────────────────────────╯

  Rows  1,000 → 796  (-204, -20.4%)

  Schema Changes
  ✅  No schema changes

  Integrity Checks
  ⚠️   204 key(s) deleted from 'order_id' (20.4% of original)

  Distribution Changes
  ❌  Categories disappeared from 'region': ['APAC']
  ⚠️   Null rate increased: 'revenue' (0.0% → 3.1%)
  ⚠️   Mean shifted: 'revenue' (246.21 → 229.84, -6.6%)

  Custom Metrics (metrics.yaml)
  ❌  cancellation_rate: 7.00% → 14.84% (+111.8%)
  ⚠️   dead_stock_rate: 11.20% → 18.92% (+68.9%)
```

## Why does this exist?

Every data team asks the same questions repeatedly:
- _"Did the ETL run correctly today?"_
- _"Did the migration lose any rows?"_
- _"Are my A/B test groups actually balanced?"_
- _"The upstream table changed — what broke downstream?"_

Current options are:
- **BI tools (PBI, Tableau)**: interactive, but require manual exploration. No one presses "diff".
- **Evidently AI / Great Expectations**: powerful, but aimed at ML model monitoring. Heavy setup.
- **csv-diff**: row-level only. Tells you _which_ rows changed, not _what it means_ statistically.

**datadelta Δ** fills the gap: a zero-config CLI tool for the daily data analyst workflow.

---

## Install

```bash
pip install datadelta
```

**With optional database drivers:**
```bash
pip install "datadelta[postgres]"   # PostgreSQL
pip install "datadelta[mysql]"      # MySQL
pip install "datadelta[watch]"      # --watch mode
pip install "datadelta[all]"        # everything
```

---

## Supported data sources

| Format | Example |
|--------|---------|
| CSV | `orders.csv` |
| Excel | `orders.xlsx`, `orders.xls` |
| JSON | `records.json` |
| Parquet | `snapshot.parquet` |
| SQLite | `local.db`, `local.sqlite` |
| PostgreSQL | `postgresql://user:pass@host:5432/db::table_name` |
| MySQL | `mysql+pymysql://user:pass@host/db::table_name` |
| SQL Server | `mssql+pyodbc://user:pass@host/db::table_name` |

For database connections, use the format `<connection_string>::<table_name>`.  
The `::` separator is unambiguous because connection strings already use every other delimiter character.

---

## Basic usage

```bash
# Basic diff (auto-detects metrics.yaml if present)
datadiff diff before.csv after.csv

# Excel files work exactly the same way
datadiff diff before.xlsx after.xlsx

# From a PostgreSQL table
datadiff diff "postgresql://alice:pw@localhost/warehouse::orders_2024" \
              "postgresql://alice:pw@localhost/warehouse::orders_2025"

# Mix formats freely
datadiff diff snapshot.parquet live_export.csv
```

---

## Scenarios

Use `--scenario` to tell the tool what kind of comparison you're doing.  
This re-weights which findings are emphasized (without changing the detection logic).

```bash
datadiff diff before.csv after.csv --scenario etl
datadiff diff before.csv after.csv --scenario migration --key user_id
datadiff diff control.csv treatment.csv --scenario ab-test
```

| Scenario | What it emphasizes |
|----------|-------------------|
| `general` | Everything equally (default) |
| `etl` | Schema changes + integrity promoted; distribution demoted |
| `migration` | Integrity (data loss) promoted; distribution demoted |
| `ab-test` | Distribution balance promoted; integrity demoted |

---

## All flags

```bash
datadiff diff <before> <after> [OPTIONS]

Arguments:
  before    Path or connection string for the reference dataset
  after     Path or connection string for the comparison dataset

Options:
  -s, --scenario   etl | migration | ab-test | general  [default: general]
  -k, --key        Primary key column (auto-detected if omitted)
  -t, --threshold  WARN threshold for distribution shift  [default: 0.10]
  -m, --metrics    Path to metrics.yaml  (auto-detected if omitted)
      --no-metrics Disable metrics.yaml auto-detection
      --story      Generate natural language summary via Claude API
  -e, --export     Export HTML report to file
      --json       Output raw JSON (for CI pipelines / scripting)
  -w, --watch      Re-run diff when files change
```

---

## Custom metrics: `metrics.yaml`

This is the most powerful feature. Define your company's KPIs in a `metrics.yaml` file,
and the tool will evaluate them automatically on every diff run.

**Create it automatically (requires `ANTHROPIC_API_KEY`):**
```bash
datadiff init \
  --from orders.csv \
  --business "We are a logistics company. Key metrics: dead stock rate (no movement in 90 days), cancellation rate, and average order value."
```

**Or write it manually.** Place `metrics.yaml` in the directory where you run `datadiff` and it will be detected automatically.

### Full metrics.yaml reference

```yaml
# Free-text business context.
# This is injected into the --story LLM prompt so the narrative
# uses your company's vocabulary instead of generic statistical jargon.
business_context: |
  We operate a regional logistics network. Core KPIs are dead stock rate
  (inventory idle >90 days), cancellation rate, and revenue per unit.
  An APAC data drop usually means a pipeline issue, not actual order loss.

metrics:

  # ── staleness ─────────────────────────────────────────────────────────
  # What % of rows have a date column older than N days?
  # Use case: dead stock, churned users, stale leads
  - name: dead_stock_rate
    description: "% of SKUs with no outbound movement in 90 days"
    type: staleness
    column: last_outbound_date     # must be a date/datetime column
    threshold_days: 90
    warn_if_above: 0.15            # 15% stale → WARN
    fail_if_above: 0.30            # 30% stale → FAIL

  # ── ratio ─────────────────────────────────────────────────────────────
  # sum(numerator_col) / sum(denominator_col)
  # Use case: revenue per unit, margin rate, conversion rate
  - name: avg_revenue_per_unit
    description: "Average revenue per shipped unit"
    type: ratio
    numerator: revenue
    denominator: units_shipped
    warn_if_delta_pct: 0.10        # >10% change from before → WARN
    fail_if_delta_pct: 0.25        # >25% change from before → FAIL

  # ── rate ──────────────────────────────────────────────────────────────
  # % of rows where column == match_value
  # Use case: cancellation rate, churn flag rate, defect rate
  - name: cancellation_rate
    description: "% of orders with status = 'cancelled'"
    type: rate
    column: status
    match_value: cancelled
    warn_if_above: 0.05
    fail_if_above: 0.12

  # ── completeness ──────────────────────────────────────────────────────
  # % of non-null values in a column
  # Use case: required fields that must never be empty
  - name: order_id_completeness
    description: "order_id must never be null"
    type: completeness
    column: order_id
    warn_if_below: 0.999
    fail_if_below: 0.990

  # ── custom ────────────────────────────────────────────────────────────
  # Arbitrary pandas expression, evaluated on the full DataFrame.
  # Variables available: df (the DataFrame), pd (pandas), np (numpy)
  # Must return a float scalar.
  - name: high_value_order_share
    description: "% of orders with revenue > $1,000"
    type: custom
    expression: "(df['revenue'] > 1000).mean()"
    warn_if_delta_pct: 0.20
```

### Threshold types explained

| Threshold | What it checks | Example |
|-----------|---------------|---------|
| `warn_if_above` | After value > threshold | cancellation > 5% |
| `fail_if_above` | After value > threshold | cancellation > 12% |
| `warn_if_below` | After value < threshold | completeness < 99.9% |
| `fail_if_below` | After value < threshold | completeness < 99.0% |
| `warn_if_delta_pct` | \|after - before\| / before > threshold | revenue dropped >10% |
| `fail_if_delta_pct` | \|after - before\| / before > threshold | revenue dropped >25% |

---

## Output modes

### Terminal (default)
Color-coded Rich terminal output with icons and tables.
```bash
datadiff diff before.csv after.csv
```

### HTML report
Self-contained HTML file, no external dependencies. Share via email, Slack, or GitHub PR.
```bash
datadiff diff before.csv after.csv --export report.html
```

### JSON output
Machine-readable output for CI pipelines or scripting.
```bash
datadiff diff before.csv after.csv --json | jq '.findings[] | select(.severity == "FAIL")'
```
Exit code is `1` if any FAIL findings exist — useful for breaking CI builds.

### Natural language story (requires `ANTHROPIC_API_KEY`)
Generates a 3–5 sentence narrative summary using Claude.
If `metrics.yaml` has a `business_context`, the narrative uses your company's vocabulary.
```bash
export ANTHROPIC_API_KEY=your_key
datadiff diff before.csv after.csv --story
```

### Watch mode
Re-runs the diff automatically whenever either file changes on disk.
```bash
datadiff diff before.csv after.csv --watch
```

---

## How it works: the four analysis layers

### Layer 1: Schema Diff
Detects structural changes: columns added, removed, or type-changed.

| Finding | Severity |
|---------|----------|
| Column added | WARN |
| Column removed | FAIL |
| Type changed (e.g. float → string) | WARN |

### Layer 2: Distribution Diff
Statistical analysis per column. Strategy adapts to the column's detected semantic type.

**Numeric columns:**
- Mean shift: is the average significantly different? (threshold: configurable, default 10%)
- KS test: has the shape of the distribution changed, beyond just the mean?

**Categorical columns:**
- New/missing categories (e.g. a region disappeared entirely)
- Share shift: a category's proportion changed significantly (e.g. APAC went from 30% to 2%)

**Datetime columns:**
- Date range change (min/max date shifted)

**All columns:**
- Null rate change

### Layer 3: Integrity Diff
Primary key health. Uses the `--key` column, or auto-detects a column with ID-like naming/cardinality.

- Duplicate keys in the after dataset
- Keys deleted (present in before, absent in after)
- New keys added

### Layer 4: Custom Metrics
Evaluates all metrics defined in `metrics.yaml`. This layer is company-specific and never affected by scenario re-weighting.

---

## Architecture overview

```
datadiff/
├── cli.py              Entry point. Two commands: diff + init.
│                       Wires together all modules. Handles flags.
│
├── loader.py           Unified data source loader.
│                       CSV/JSON/Parquet → DuckDB (native SQL engine, no setup)
│                       Excel → pandas + openpyxl
│                       SQLite → sqlite3 → pandas
│                       Databases → SQLAlchemy → pandas
│
├── profiler.py         Semantic type detection.
│                       int with >95% unique values → "id"
│                       string with <5% unique values → "category"
│                       parseable as dates → "datetime"
│                       continuous numbers → "numeric"
│
├── differ.py           Core diff engine. Runs 4 layers.
│                       Returns DiffResult with a list of Finding objects.
│
├── metrics.py          Custom metrics engine (Layer 4).
│                       Loads and validates metrics.yaml.
│                       Evaluates: staleness, ratio, rate, completeness, custom.
│
├── metrics_generator.py  LLM-assisted metrics.yaml generation.
│                          Powers `datadiff init`. Sends only column metadata
│                          (not raw data) to the API for privacy.
│
├── scenarios.py        Scenario lens. Promotes/demotes finding severities
│                       based on the --scenario flag. Never touches Layer 4.
│
├── reporter.py         Terminal output via Rich library.
├── story.py            --story flag. Injects business_context into LLM prompt.
└── export.py           --export flag. Self-contained HTML report via Jinja2.
```

**Data flow:**
```
Input (file / connection string)
  → loader.py         → DataFrame
  → profiler.py       → column semantic types
  → differ.py         → DiffResult (findings list)
  → metrics.py        → additional findings (Layer 4)
  → scenarios.py      → severity adjustments
  → reporter.py       → terminal output
  → story.py          → LLM narrative (optional)
  → export.py         → HTML report (optional)
```

---

## Why these technology choices?

**DuckDB** (not pandas, not sqlite for flat files):  
DuckDB reads CSV, JSON, and Parquet directly with a SQL query — no import step, no schema definition, no server. One line: `duckdb.query("SELECT * FROM read_csv_auto('file.csv')").df()`. It auto-detects delimiters, headers, and types. For flat files, it's the fastest and most convenient option.

**openpyxl** (for Excel):  
DuckDB doesn't have a native Excel reader. openpyxl is the standard Python Excel engine for .xlsx files. pandas wraps it: `pd.read_excel(path, engine="openpyxl")`.

**SQLAlchemy** (for databases):  
Provides a unified connection interface for every major SQL database. The user only needs to install the right driver (psycopg2, pymysql, etc.) and pass a standard connection string. We never wrote a MySQL connector or a PostgreSQL connector — SQLAlchemy handles all of that.

**Rich** (for terminal output):  
Rich makes terminals beautiful without any browser or CSS. Tables, panels, colors, and icons all work natively in any terminal.

**Jinja2** (for HTML export):  
The gold standard for Python HTML templating. The HTML template is embedded as a string in export.py — no external template files needed.

**YAML** (for metrics.yaml):  
Human-readable, version-controllable, editable by non-engineers. The format is simple enough that a business analyst can open it and understand what it does.

---

## Try the demo

```bash
# Generate all demo datasets
python examples/generate_demo_data.py

# ETL scenario with CSV
datadiff diff examples/etl_before.csv examples/etl_after.csv --scenario etl

# Same data from Excel
datadiff diff examples/etl_before.xlsx examples/etl_after.xlsx --scenario etl

# Logistics data with custom metrics
cp examples/metrics_logistics.yaml metrics.yaml
datadiff diff examples/logistics_before.csv examples/logistics_after.csv

# Export to HTML
datadiff diff examples/logistics_before.csv examples/logistics_after.csv --export report.html
```

---

## CI / CD integration

Exit code is `1` when any FAIL finding exists.

```yaml
# GitHub Actions example
- name: Validate ETL output
  run: |
    datadiff diff data/yesterday.csv data/today.csv \
      --scenario etl \
      --json > diff_result.json
  # Build fails if FAIL findings exist
```

---

## License

MIT