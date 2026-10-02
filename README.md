<div align="center">

# data ▲

**What changed in your data, and why it matters.**

[![Python](https://img.shields.io/badge/python-3.10%2B-black?style=flat-square)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-black?style=flat-square)](LICENSE)

</div>

`datadelta` reads two versions of a dataset (yesterday's export and today's, a table before and after a migration, a control group beside its treatment) and tells you, in a few quiet lines, what changed and how much it matters. It reads CSV, Excel, JSON, Parquet, SQLite and SQL databases. It can tidy a messy export before comparing, without throwing information away. It answers in the terminal, as JSON for a pipeline, or as one self-contained HTML page.

![datadelta diff on the demo ETL data](docs/assets/diff.svg)

The same report as plain text, from `datadelta diff examples/etl_before.csv examples/etl_after.csv --scenario etl`:

```text
data ▲   ETL Validation                                                 2026-10-01 09:30
────────────────────────────────────────────────────────────────────────────────────────
rows     1,000 → 744                                                      ▼ 256   −25.6%

schema
  ○  pass   no schema changes
integrity
  ▲  fail   256 key(s) deleted from 'order_id' (25.6% of original)
distribution
  ●  warn   Categories disappeared from 'region': ['APAC']
  ·  info   Date range changed: 'created_at'

  1 fail   1 warn   1 info                                                        exit 1

  --export report.html   shareable HTML report
  --story                narrative via an LLM
```

One mark carries the urgency: ▲ in vermilion, for what fails. The rest is ink on paper: ● for a warning, · for information, ○ for a pass. Every severity is a glyph and a word, so the report reads the same in color, in grayscale, or with `NO_COLOR` set.

---

## Install

Requires Python 3.10 or newer. The PyPI distribution is named `datadelta-cli` (the names `datadelta` and `datadiff` belong to other projects); the command and the Python package are both `datadelta`.

Install the wheel attached to the [v0.3.0 release](https://github.com/kuthy9/Datadelta-/releases/tag/v0.3.0):

```bash
pip install https://github.com/kuthy9/Datadelta-/releases/download/v0.3.0/datadelta_cli-0.3.0-py3-none-any.whl
```

Or, until the first PyPI release, install from a clone (for development, or for the optional extras below):

```bash
git clone https://github.com/kuthy9/Datadelta-.git
cd Datadelta-
pip install -e ".[all]"
```

Once it is released:

```bash
pip install datadelta-cli
```

Optional extras. From a clone, use `pip install -e ".[postgres]"` and so on.

| Extra | Install | Adds |
|-------|---------|------|
| postgres | `pip install "datadelta-cli[postgres]"` | PostgreSQL driver (psycopg2) |
| mysql | `pip install "datadelta-cli[mysql]"` | MySQL driver (PyMySQL) |
| mssql | `pip install "datadelta-cli[mssql]"` | SQL Server driver (pyodbc); also needs Microsoft's ODBC driver installed on the system |
| watch | `pip install "datadelta-cli[watch]"` | `--watch` mode (watchdog) |
| openai | `pip install "datadelta-cli[openai]"` | `--llm openai` and `--llm deepseek` |
| gemini | `pip install "datadelta-cli[gemini]"` | `--llm gemini` |
| all | `pip install "datadelta-cli[all]"` | everything above except mssql |

---

## Quickstart

```bash
# Write the demo datasets into examples/
python examples/generate_demo_data.py

# What changed between two snapshots?
datadelta diff examples/etl_before.csv examples/etl_after.csv --scenario etl

# Standardize a messy export: look first, then write examples/messy_orders.clean.csv
datadelta clean examples/messy_orders.csv --dry-run
datadelta clean examples/messy_orders.csv

# The same data saved as CSV and as Excel: clean both sides, then compare
# (the default null tokens include NA; if NA is a real value, set cleaning: {null_tokens: [...]} without it)
datadelta diff examples/etl_before.csv examples/etl_before.xlsx --clean

# JSON for scripts and CI
datadelta diff examples/etl_before.csv examples/etl_after.csv --json | jq '.findings[] | select(.severity == "FAIL")'

# One self-contained HTML page to share
datadelta diff examples/etl_before.csv examples/etl_after.csv --export report.html

# A short narrative from an LLM (needs ANTHROPIC_API_KEY; see "LLM providers and privacy")
datadelta diff examples/etl_before.csv examples/etl_after.csv --story

# Use DeepSeek, OpenAI, or Gemini instead (needs the openai / gemini extra)
datadelta diff examples/etl_before.csv examples/etl_after.csv --story --llm deepseek

# Your own KPIs: copy a metrics.yaml into the working directory
cp examples/metrics_logistics.yaml metrics.yaml
datadelta diff examples/logistics_before.csv examples/logistics_after.csv
```

---

## Screenshots

Real runs, captured from a terminal. Each image is the `datadelta` command itself, run in a pseudo-terminal on the demo data; `scripts/capture_screenshots.py` regenerates all of them. The two samples above and under [Cleaning](#cleaning) come from `scripts/render_readme_assets.py`.

### Live progress, mid-run

`datadelta diff big_before.csv big_after.csv --clean --scenario etl`, on two files of 700,000 and 560,164 rows.

![The live progress view on stderr, halfway through cleaning the second file](docs/screenshots/live-progress.png)

While it works, datadelta draws its progress on stderr: one line per stage with a vermilion bar, what the stage is doing right now, and how long it has taken. The tally at the bottom counts findings as they arrive. When the run ends the view folds into a single `✓ 8 stages · …` line, so only the report stays on screen. In a pipe or a CI log it prints one plain `[datadelta] …` line per stage instead, and `-q` silences it.

### The finished run

The same command, once it is done.

![The finished diff --clean report on 700,000 rows](docs/screenshots/diff-clean-finished.png)

The `clean` layer lists what was standardized on each side: padded region names trimmed, `N/A` read as empty, `yes`/`no` as booleans, `$1,234.56` as numbers, `2024/01/05` as dates. Because both sides are typed the same way, the schema layer reports no changes, and what remains are the real ones: 20% of the order ids are gone (fail), the APAC region disappeared and the score distribution moved (warn). The process exits with `1` because there is a fail.

### Your own KPIs

`cp examples/metrics_logistics.yaml metrics.yaml`, then `datadelta diff examples/logistics_before.csv examples/logistics_after.csv --clean`.

![Custom metrics from metrics.yaml evaluated on the logistics demo](docs/screenshots/custom-metrics.png)

The `custom metrics` layer evaluates the KPIs defined in `metrics.yaml` on both sides and checks them against their thresholds. Here the dead-stock rate and the cancellation rate cross their fail thresholds; the other four stay within bounds. These findings keep the severity you set: scenarios never re-weight them.

### The HTML report

`datadelta diff examples/etl_before.csv examples/etl_after.csv --scenario etl --export report.html`

<p>
  <img src="docs/screenshots/html-report-light.png" alt="The exported HTML report, light color scheme" width="49%">
  <img src="docs/screenshots/html-report-dark.png" alt="The exported HTML report, dark color scheme" width="49%">
</p>

One self-contained page to attach or share: the verdict first, then the row count and each layer's findings with their details. Nothing is loaded from the network, every value from the data is escaped, and the page follows the reader's light or dark setting.

---

## Cleaning

Real exports are rarely tidy: `" EMEA"` beside `"EMEA"`, `N/A` where a number should be, one date written four ways in a single column. `datadelta clean` standardizes a file and writes it back out. `datadelta diff --clean` does the same to both sides, in memory, before comparing them.

The defaults are lossless. A rule that could lose information (re-casing, dropping duplicates, filling gaps) runs only when you ask for it, and a column becomes numbers or dates only when every value in it parses. Whatever is left alone is reported, together with the values that stopped it.

For cleaning, files are read without guessing, so these rules make every decision. A CSV column is typed on reading only when every value is already in plain form: whole numbers without a leading zero, decimals that a float holds exactly, `true`/`false`, and ISO dates or date-times without a UTC offset (`2024-01-05`, `2024-01-05 10:30:00`). Excel text cells stay text. JSON strings follow the same rule: a plain ISO date or date-time string is typed as a date on reading, and any other date-like string stays text. Codes such as `02134`, day/month dates, `+02:00` offsets and ids too long for a float therefore reach the rules as written, never as a reader's guess.

![datadelta clean on the demo messy_orders.csv](docs/assets/clean.svg)

```bash
datadelta clean orders.csv --dry-run              # show what would change, write nothing
datadelta clean orders.csv                        # writes orders.clean.csv next to the input
datadelta clean orders.csv -o tidy.parquet        # the suffix picks the format: .csv .json .parquet .xlsx
datadelta clean orders.csv --dedupe exact --impute median
datadelta clean orders.csv --json --dry-run       # the cleaning report as JSON on stdout
```

The steps run in this order:

| Step | Default | What it does | Lossless rule |
|------|---------|--------------|---------------|
| `headers` | on | Trims whitespace around column names | If trimming would make two names collide, no header changes (`skipped`) |
| `whitespace` | on | Trims text cells, turns non-breaking spaces into spaces, removes zero-width characters | Inner spaces are kept |
| `null_tokens` | on | Turns `""`, `NA`, `N/A`, `NaN`, `null`, `None`, `-`, `—`, `--`, `#N/A` (any case) into real nulls | The list is yours to replace with `null_tokens:` |
| `booleans` | on | `true/false`, `yes/no`, `y/n`, `t/f`, `是/否`, `1/0` become booleans | Every value must be one of these and at least one must be a word; a column of only 0 and 1 is left to `numbers` |
| `numbers` | on | Full-width digits, thousands separators, `$ ¥ € £ ￥`, `(3.5)` as −3.5, `12%` as 0.12 | Converts only when every value parses; a leading zero (`00123`) marks codes, which stay text (`skipped`) |
| `dates` | on | `2024-01-05`, `2024/01/05`, `2024.01.05`, `2024年1月5日`, `05/01/2024`, each with or without a time, and ISO 8601 timestamps | Converts only when every value parses; when day and month cannot be told apart, `dayfirst` decides and the values are counted as `ambiguous` |
| `case` | off | `lower`, `upper` or `title` for the columns you list | Only the listed columns |
| `dedupe` | off | `exact`: identical rows; `key`: the same key value, keeping the `first` or `last` | Without it, duplicates are only counted |
| `impute` | off | Per column: `median`, `mean`, `mode`, `{constant: value}` or `drop_rows` | Without it, nulls are only counted |

Columns that already hold numbers, dates or booleans are never re-parsed. `exclude_columns` lists columns whose values no step changes.

Rules live in an optional `cleaning:` section of `metrics.yaml` (the same file `diff` already picks up), or in any YAML file passed with `--config`:

```yaml
cleaning:
  exclude_columns: [notes]
  null_tokens: ["", "NA", "N/A", "null", "-"]
  numbers: {min_parse_ratio: 1.0}
  dates:   {dayfirst: false, min_parse_ratio: 1.0}
  booleans: true
  case:    {lower: [region, status]}
  dedupe:  {mode: key, keys: [order_id], keep: first}
  impute:  {revenue: median, region: mode, discount: {constant: 0}, customer_id: drop_rows}
```

Precedence is built-in defaults, then the YAML, then the flags (`--dedupe`, `--key`, `--impute`). An unknown key or an invalid value stops the run with exit code 1 and names the key path, for example `cleaning.numbers.min_parse_ratio: expected a number in (0, 1], got 1.5`. A rule that names a column which does not exist is skipped with a warning. Below a `min_parse_ratio` of 1.0, values that do not parse become null, and every one of them is counted as `coerced_to_null`.

`NA` is in the default null list. If it is a real value in your data (North America, Namibia), give `null_tokens:` a list without it.

With `diff --clean`, both sides are cleaned with the same rules: the `cleaning:` section of the metrics file in use (`-m`, else `./metrics.yaml`), or the defaults. What changed appears as a `clean` layer of INFO findings, and `--json` gains a `cleaning` object with both reports. When the rules drop or fill values (`dedupe`, `impute`), datadelta says so on stderr (unless `--quiet`), because the integrity and null-rate findings then describe the cleaned data.

---

## Analysis layers

Findings are grouped into five layers, shown in this order. Each layer stands on its own: a finding in one never changes another.

```text
clean          diff --clean only. What standardizing changed on each side. INFO, never re-weighted.
schema         Columns added, removed, or retyped. A silently broken join starts here.
integrity      Primary key health: duplicate keys, deleted keys, new keys.
               --key names the column; otherwise the first ID-like column is used.
distribution   Per column present on both sides, by semantic type:
                 numeric    mean shift, and a Kolmogorov-Smirnov test for changes of shape
                 category   share shift per value, new and vanished categories
                 datetime   change of range
                 every type change of null rate
custom         Your metrics.yaml KPIs, judged by your own thresholds. Never re-weighted.
```

How severe is a change? `--threshold` (default `0.10`) draws the WARN line: a null rate or a category's share moving by more than 10 percentage points, or a column's mean by more than 10 percent. A null rate or a mean moving by more than twice the threshold is a FAIL. A significant change of shape (Kolmogorov-Smirnov p < 0.05 with a statistic above 0.1) is a WARN. A removed column, any duplicate key, more than 10% of keys deleted, or more than half of a column's categories vanishing is a FAIL; fewer deletions or vanished categories are a WARN. An added or retyped column is a WARN; new keys, new categories and a changed date range are INFO.

---

## Scenarios

`--scenario` changes emphasis, not detection. Each scenario moves the findings of some layers one step up (promote) or down (demote) the scale pass, info, warn, fail. The `custom` and `clean` layers are never re-weighted.

```bash
--scenario etl        # Promotes: schema + integrity
--scenario migration  # Promotes: integrity + schema   Demotes: distribution
--scenario ab-test    # Promotes: distribution         Demotes: integrity
--scenario general    # no re-weighting (the default)
```

---

## Custom metrics: `metrics.yaml`

Write your KPIs down once and every diff evaluates them. datadelta picks up `./metrics.yaml` automatically; `-m PATH` points elsewhere and `--no-metrics` leaves it out.

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

| Type | Computes, on each side | Needs |
|------|------------------------|-------|
| `staleness` | share of rows whose date is more than `threshold_days` days before today | `column`, `threshold_days` |
| `ratio` | `sum(numerator) / sum(denominator)` (0 when the denominator sums to 0) | `numerator`, `denominator` |
| `rate` | share of rows where `column` equals `match_value` | `column`, `match_value` |
| `completeness` | share of non-null values in `column` | `column` |
| `custom` | a pandas expression over `df` | `expression` |

| Threshold | The finding is | when |
|-----------|----------------|------|
| `fail_if_above` | FAIL | the after value is above it |
| `warn_if_above` | WARN | the after value is above it |
| `fail_if_below` | FAIL | the after value is below it |
| `warn_if_below` | WARN | the after value is below it |
| `fail_if_delta_pct` | FAIL | abs(after − before) / abs(before) is above it |
| `warn_if_delta_pct` | WARN | abs(after − before) / abs(before) is above it |

They are checked in that order and the first match wins. A metric whose thresholds are not crossed is a PASS; a metric without thresholds is INFO. A metric that cannot be computed (a missing column, a refused expression) becomes a WARN that names the problem, and the rest of the diff carries on.

`custom` expressions are not run with Python's `eval`. metrics.yaml is often shared, so a small evaluator checks every expression against a whitelist before anything runs: `df` with column access (`df['x']`, `df[['a', 'b']]`, boolean masks), arithmetic and comparisons, the methods and attributes `mean` `sum` `count` `nunique` `median` `std` `var` `min` `max` `quantile` `size` `shape` `abs` `round` `isna` `notna` `isnull` `notnull` `fillna` `between` `isin` `astype` `clip` `dropna`, text through `str` (`contains` `startswith` `endswith` `lower` `upper` `strip` `len`), dates through `dt` (`days` `year` `month` `day` `dayofweek`), `np.log` `np.log1p` `np.sqrt` `np.abs` `np.where` `np.nan`, `pd.to_datetime` `pd.Timestamp` `pd.Timedelta`, and the functions `abs` `len` `min` `max` `round` `float` `int`. Anything else (imports, names starting with `_`, `open`, lambdas, comprehensions) is refused before it runs and reported as a WARN.

Rather not write it yourself? `init` drafts one with Claude:

```bash
datadelta init \
  --from orders.csv \
  --business "We are a logistics company. Key metrics: dead stock rate, cancellation rate, revenue per unit."
```

`init` sends up to 5 sample values per column to Claude; add `--no-samples` to keep every cell value local (see [What is sent to the LLM](#what-is-sent-to-the-llm)).

The `business_context` text also goes into the `--story` prompt, so the narrative speaks your company's vocabulary rather than statistics.

---

## Data sources

| Source | Name it as |
|--------|------------|
| CSV, JSON, Parquet | a path: `orders.csv`, `orders.json`, `orders.parquet` (read by DuckDB) |
| Excel | `orders.xlsx` (the first sheet) |
| SQLite file | `shop.sqlite` or `shop.db` (the first table by name) |
| SQLite, one table | `sqlite:////absolute/path/shop.db::orders` |
| PostgreSQL | `postgresql://user:password@host:5432/shop::orders` (extra `postgres`) |
| MySQL | `mysql+pymysql://user:password@host/shop::orders` (extra `mysql`) |
| SQL Server | `mssql+pyodbc://user:password@host/shop::orders` (extra `mssql`, and an ODBC driver) |

A database source is a SQLAlchemy URL followed by `::` and a table name. URLs already use `:`, `/`, `@` and `?`, so datadelta splits on the last `::`: everything before it goes to SQLAlchemy unchanged, everything after it is the table. Quote the whole string in your shell. When a connection fails, the error shows the URL with the password masked (`user:***@host`).

Formats can be mixed in one diff. When two readers type the same data differently (Excel and CSV often do), add `--clean`.

---

## LLM providers and privacy

`--story` adds a three-to-five sentence narrative under the report. It is collected in full first and then printed, so it never interleaves with the report or the progress display.

| Provider | Flag | API key env var | Extra to install |
|----------|------|-----------------|------------------|
| Claude (default) | `--llm claude` | `ANTHROPIC_API_KEY` | none (included) |
| OpenAI | `--llm openai` | `OPENAI_API_KEY` | `openai` |
| DeepSeek | `--llm deepseek` | `DEEPSEEK_API_KEY` | `openai` (DeepSeek's API is OpenAI-compatible) |
| Gemini | `--llm gemini` | `GEMINI_API_KEY` | `gemini` |

A missing key, a missing SDK or a provider error prints a warning on stderr (with `--quiet`, the story is simply left out); the report is printed anyway and the exit code does not change. `--story` is ignored with `--json`, because a narrative would corrupt the JSON stream.

### What is sent to the LLM

datadelta never uploads your dataset. Only two features call an LLM, and this is exactly what they send:

- **`diff --story`** sends the diff findings as JSON: column names, aggregate statistics (row counts, means, quantiles, null rates, date ranges, custom metric values) and, per finding, at most 10 category labels (the rest are summarized as `+k more`). Primary-key sample values are removed before sending, and category findings on the key column (`--key`, or the detected id column) carry counts instead of labels. With `--clean`, the findings also include one clean finding per cleaning action (side, column, action, count and a fixed note such as `currency symbol $ removed`), and the two cleaning reports are reduced to their totals; before → after example values never leave your machine. The `business_context` text from your metrics.yaml goes into the system prompt.
- **`init`** sends your business description and, for each column, its name, dtype, distinct count, null rate and up to 5 sample values (the first non-null values, so they can come from the same first few rows). Pass `--no-samples` to send only each column's name and dtype: no cell values, and no statistics computed from them.

`clean`, `--json`, `--export` and `diff` without `--story` make no LLM calls.

---

## Live progress

While datadelta works, stderr shows a small live view: a header with the command and a clock, one row per stage (load, clean, profile, schema, distribution, integrity, metrics, story, export) with a vermilion bar for stages that know their size, the item in progress, and a running tally of findings. When the work is done the view folds into a single line such as `✓ 9 stages · 1.4s`, and the report follows on stdout.

- Progress never touches stdout: `--json | jq` and `> report.txt` receive only the report.
- When stderr is not a terminal (a CI log, `2> log.txt`, also with `FORCE_COLOR` set), the live view becomes one plain line per finished stage, for example `[datadelta] distribution done 5/5 2 findings 0.4s`.
- `--quiet`: no progress display, status lines or warnings on stderr (errors still appear).

---

## CI

Exit code is `1` when any FAIL finding exists. The full contract, the same in terminal and `--json` mode:

| Exit code | Meaning |
|-----------|---------|
| `0` | no FAIL finding (warnings are allowed) |
| `1` | at least one FAIL finding, or an error: data that cannot be loaded, an invalid `cleaning:` section, a file that cannot be written |
| `2` | a usage error: an unknown option or an invalid value |

stdout carries only the JSON, so it can be kept as an artifact while progress goes to the log:

```yaml
# .github/workflows/data-quality.yml
- name: Validate the daily ETL output
  run: |
    datadelta diff data/yesterday.parquet data/today.parquet --scenario etl --json > diff_result.json
  # the step fails when the diff contains a FAIL finding (exit code 1)
```

---

## Known limitations

- Excel: only the first sheet is read. Legacy `.xls` files cannot be read (the Excel reader, openpyxl, handles `.xlsx` only); save them as `.xlsx` or CSV.
- A `.sqlite` or `.db` file is compared through its first table in alphabetical order. To choose a table, use `sqlite:////absolute/path/file.db::table`.
- Database URLs must use the `postgresql://`, `mysql://`, `mysql+pymysql://`, `mssql://`, `mssql+pyodbc://` or `sqlite:///` prefix (`postgres://` is read as a file path). The table name after `::` is quoted as written, so a schema-qualified `schema.table` is read as one table name.
- CSV files must be UTF-8. A file in another encoding stops with a one-line `Error loading data` message; convert it to UTF-8 first.
- Semantic types are inferred from the before side. A column that holds a different kind of value on each side (numbers stored as text in one file) is reported as a type change, and its number and category checks are skipped; `--clean` usually aligns the two sides.
- A date such as `03/04/2024` reads both ways. When nothing in the column decides, `dates: {dayfirst: ...}` does (default: month first), and the report counts the ambiguous values.
- `--watch` is experimental and limited. It watches only the directory of the before file, so a change to an after file that lives in another directory does not trigger a re-run. The re-run happens on the watcher's own thread: a re-run that ends in a FAIL finding or a load error leaves the watcher running, but any other unexpected error stops that thread, and later changes are then silently ignored while the command still looks alive. Re-running the command is the dependable way.

---

## CLI reference

```text
datadelta --version                Print "data ▲ datadelta <version>" and exit

datadelta diff BEFORE AFTER [OPTIONS]

  -s, --scenario    etl | migration | ab-test | general      [default: general]
  -k, --key         Primary key column (auto-detected if omitted)
  -t, --threshold   Relative change that counts as a WARN    [default: 0.10]
  -m, --metrics     Path to metrics.yaml                     [default: ./metrics.yaml if present]
      --no-metrics  Do not load metrics.yaml
      --clean       Clean both sides with the same rules before comparing
      --story       Add a narrative from an LLM (terminal mode only)
      --llm         claude | openai | deepseek | gemini      [default: claude]
  -e, --export      Also write a self-contained HTML report
      --json        Print the result as JSON on stdout
  -q, --quiet       No progress display, status lines or warnings on stderr (errors still appear)
  -w, --watch       Re-run when the files change (experimental)

datadelta clean INPUT [OPTIONS]

  -o, --output      Output file: .csv .json .parquet .xlsx   [default: <input dir>/<stem>.clean<suffix>]
      --config      YAML file with a cleaning: section       [default: ./metrics.yaml if present]
      --dedupe      exact | key                              [default: off]
  -k, --key         Key column for --dedupe key and the duplicate-key count
      --impute      median (numeric columns) | mode (every column)   [default: off]
      --dry-run     Report only; write nothing
      --json        Print the cleaning report as JSON on stdout
  -q, --quiet       No progress display, status lines or warnings on stderr (errors still appear)

datadelta init --from FILE --business TEXT [OPTIONS]

  -f, --from        Sample data file (required)
  -b, --business    Business description (required)
  -o, --output      Output path                              [default: metrics.yaml]
      --no-samples  Send only column names and dtypes to the LLM (no sample values, no statistics)
```

---

## License

MIT. See [LICENSE](LICENSE).
