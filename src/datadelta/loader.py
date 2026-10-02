"""
loader.py — Load any supported data source into a pandas DataFrame.

Supported sources:
  File-based (path as string):
    .csv        → DuckDB read_csv_auto()
    .json       → DuckDB read_json_auto()
    .parquet    → DuckDB read_parquet()
    .xlsx/.xls  → pandas read_excel() via openpyxl
    .sqlite/.db → sqlite3 → pandas read_sql()

  Database (connection string as string):
    postgresql://user:pass@host:5432/dbname::table_name
    mysql+pymysql://user:pass@host/dbname::table_name
    mssql+pyodbc://user:pass@host/dbname::table_name
    sqlite:////abs/path/to/db.sqlite::table_name

WHY DUCKDB for flat files?
  DuckDB reads CSV/JSON/Parquet directly from disk with a SQL query — no
  import step, no temp table, no config. One line of code handles
  automatic type inference, header detection, encoding, and compression.

  `duckdb_connection().execute("SELECT * FROM read_csv_auto('file.csv')").df()`
  (a connection with DuckDB's progress bar off: see duckdb_connection()).

  For Excel, DuckDB doesn't have a native reader, so we fall back to
  pandas + openpyxl. The result is the same: a DataFrame.

WHY SQLALCHEMY for databases?
  SQLAlchemy provides a unified connection interface for every major database.
  The user only needs to install the right driver (psycopg2 for Postgres,
  pymysql for MySQL) and pass a standard connection string. SQLAlchemy
  handles the rest.

CONNECTION STRING SYNTAX:
  We use a custom `::table_name` suffix because connection strings already
  use every standard delimiter character. The double-colon is unambiguous:
    postgresql://user:pass@host/db::my_table
                                   ^^^^^^^^^^ split here

LOSSLESS READS (load_file(..., lossless=True): `clean` and `diff --clean`)
  The typed readers guess, and some guesses change data: DuckDB sniffs
  "01/02/2024" as day-first, reads 20-digit ids as one rounded float and
  moves "+02:00" timestamps into this machine's zone; pandas' Excel reader
  turns the text "02134" into 2134 and the text "NA" into a missing value.
  The clean steps make those decisions under their lossless rules and
  report them, so the cleaner's read leaves them to the steps:
    - CSV is read as text (DuckDB all_varchar). A column is typed on
      reading only when every value is the plain spelling of its type,
      so that typing loses nothing: integers ("-12", no leading zero,
      64 bits), decimals that read back as the same float ("1.50",
      "2e-3"), true / false, ISO dates ("2024-01-05") and ISO date-times
      without a UTC offset ("2024-01-05 10:30:00"). This is what the
      typed reader gives such columns, so a tidy file has nothing to
      report; every other column reaches the steps as text.
    - Excel: every cell keeps the type the workbook gives it. Text cells
      stay text (no number or missing-value guessing); a column without
      text cells gets its number, date or boolean dtype.
    - JSON: numbers, booleans and nested values keep their JSON types.
      Columns DuckDB would read as dates or times (JSON has no such
      type: they are strings) or as 128-bit integers are read as text,
      then typed by the same plain-spelling rule as CSV.
  Parquet and databases carry their own types and read as usual.
"""

import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb

from .progress import NullProgress, ProgressSink, error_summary
from .theme    import SEP


# All formats we handle natively (no SQL connection string)
FILE_FORMATS = {".csv", ".json", ".parquet", ".xlsx", ".xls", ".sqlite", ".db"}

# Prefixes that tell us the input is a database connection string
SQL_PREFIXES = ("postgresql://", "mysql://", "mysql+pymysql://",
                "mssql://", "mssql+pyodbc://", "sqlite:///")


def load_file(
    source:    str,
    progress:  ProgressSink | None = None,
    stage_key: str  = "load",
    label:     str  = "load",
    *,
    lossless:  bool = False,
) -> pd.DataFrame:
    """
    Universal entry point. Accepts either a file path (str or Path)
    or a database connection string with ::table_name suffix.
    Always returns a pandas DataFrame.

    lossless=True is the cleaner's read: CSV and Excel values that need a
    decision reach the clean steps as written (see LOSSLESS READS above).

    Progress: one stage `stage_key` whose note is source_label(source).
    It ends "done" with "<rows> rows · <cols> cols", or "failed" with the
    first line of the error, which is then re-raised unchanged.
    """
    source   = str(source)
    progress = progress if progress is not None else NullProgress()

    progress.stage_start(stage_key, label, note=source_label(source))
    try:
        df = _load(source, lossless=lossless)
    except Exception as e:
        progress.stage_end(stage_key, "failed", summary=error_summary(e))
        raise
    progress.stage_end(stage_key, "done", summary=f"{len(df):,} rows {SEP} {len(df.columns)} cols")
    return df


def source_label(source: str) -> str:
    """
    How a source is named in progress output and dashboard titles: a
    file's name, or a connection string with its credentials masked. Any
    "://" source is masked, not just the known SQL prefixes (_load does the
    same), since Path(source).name would print a bare authority whole.
    """
    source = str(source)
    if "://" in source:
        return _mask_password(source)
    return Path(source).name or source


def _load(source: str, lossless: bool = False) -> pd.DataFrame:
    """Dispatch on the source: connection string, or file by suffix."""
    # ── Database connection string ────────────────────────────────────────────
    if _is_sql_connection(source):
        return _load_sql(source)

    # ── File-based source ─────────────────────────────────────────────────────
    path = Path(source)
    if not path.exists():
        shown = _mask_password(source) if "://" in source else path     # a URL with an unknown prefix
        raise FileNotFoundError(
            f"File not found: {shown}\n"
            f"  If this is a database URL, check the prefix is one of: {SQL_PREFIXES}"
        )

    suffix = path.suffix.lower()
    if suffix not in FILE_FORMATS:
        raise ValueError(
            f"Unsupported format: '{suffix}'\n"
            f"  Supported: {sorted(FILE_FORMATS)}"
        )

    if suffix in (".xlsx", ".xls"):
        return _load_excel(path, lossless=lossless)

    if suffix in (".sqlite", ".db"):
        return _load_sqlite(path)

    # DuckDB handles CSV / JSON / Parquet natively
    return _load_via_duckdb(path, suffix, lossless=lossless)


# ─────────────────────────────────────────────────────────────────────────────
# Internal loaders
# ─────────────────────────────────────────────────────────────────────────────

# JSON has no date, time or 128-bit types: DuckDB detects these from text
# (and turns "+02:00" into naive UTC), and .df() turns HUGEINT into a float.
# Scalar type names, matched exactly: "DATE[]" (a list of dates) and
# "STRUCT(d DATE)" keep their JSON shape.
_JSON_TEXT_TYPES = frozenset({
    "DATE", "TIME", "TIME WITH TIME ZONE",
    "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
    "HUGEINT", "UHUGEINT",
})


def _load_via_duckdb(path: Path, suffix: str, lossless: bool = False) -> pd.DataFrame:
    """
    Use DuckDB's native readers for CSV, JSON, Parquet.
    `read_csv_auto` automatically detects: delimiter, header, types, encoding.
    `.df()` converts the DuckDB result to a pandas DataFrame.

    lossless=True (see LOSSLESS READS): CSV is read with all_varchar,
    which keeps the dialect and header detection; JSON columns whose
    detected type is one of _JSON_TEXT_TYPES are read again as text.
    Both then type plain spellings only.

    The path goes into the query as an escaped SQL string literal, never
    as raw text: a quote in a file name ("O'Brien.csv") is part of the
    name, not the end of the literal. A DuckDB error (a file in another
    encoding, a directory, a malformed file) becomes one ValueError line,
    "Failed to read <path>: <reason>", like the other load errors.
    """
    source  = _sql_text(str(path))
    readers = {
        ".csv":     f"SELECT * FROM read_csv_auto({source})",
        ".json":    f"SELECT * FROM read_json_auto({source})",
        ".parquet": f"SELECT * FROM read_parquet({source})",
    }
    con = duckdb_connection()
    try:
        if lossless and suffix == ".csv":
            return _type_plain_columns(con.execute(f"SELECT * FROM read_csv({source}, all_varchar = true)").df())
        if lossless and suffix == ".json":
            return _read_json_lossless(con, source, readers[suffix])
        return con.execute(readers[suffix]).df()
    except duckdb.Error as e:
        raise ValueError(f"Failed to read {path}: {_duckdb_reason(e)}") from e
    finally:
        con.close()


def _read_json_lossless(con: duckdb.DuckDBPyConnection, source: str, typed_query: str) -> pd.DataFrame:
    """read_json with the detected schema, except that text-born columns stay VARCHAR (`source`: the path as a SQL literal)."""
    schema = [(name, kind) for name, kind, *_ in con.execute(f"DESCRIBE {typed_query}").fetchall()]
    text   = [name for name, kind in schema if kind in _JSON_TEXT_TYPES]
    if not text:
        return con.execute(typed_query).df()
    spec = ", ".join(f"{_sql_text(name)}: {_sql_text('VARCHAR' if name in text else kind)}" for name, kind in schema)
    return _type_plain_columns(
        con.execute(f"SELECT * FROM read_json({source}, columns = {{{spec}}})").df(),
        only = text,
    )


def _duckdb_reason(error: Exception) -> str:
    """
    DuckDB's message on one line: its first paragraph (the rest is a dump
    of the reader options and the query), without the "Original Line:"
    line, which quotes the file's data.
    """
    lines = []
    for line in str(error).strip().splitlines():
        if not line.strip():
            break
        if not line.startswith("Original Line:"):
            lines.append(line.strip())
    return "; ".join(lines)


def _sql_text(value: str) -> str:
    """A SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    A fresh in-memory DuckDB connection with DuckDB's own progress bar off.
    DuckDB draws that bar on stdout (file descriptor 1) once a query runs
    longer than two seconds: a multi-GB file, or a slow CI runner. stdout
    carries only the report or the JSON document, so every connection
    datadelta opens goes through here.
    """
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    return con


def _load_excel(path: Path, lossless: bool = False) -> pd.DataFrame:
    """
    Load Excel files using pandas + openpyxl.
    We read the first sheet by default.
    openpyxl is the engine for .xlsx; xlrd handles legacy .xls.

    lossless=True: dtype=object keeps every cell as the workbook typed it
    (the text "02134" stays text), only empty cells become missing (the
    text "NA" is the null_tokens step's call), and infer_objects() then
    gives each column without text cells its number, date or bool dtype.
    """
    try:
        # sheet_name=0 → first sheet; header=0 → first row is header
        if lossless:
            df = pd.read_excel(
                path, sheet_name=0, header=0, engine="openpyxl",
                dtype=object, keep_default_na=False, na_values=[""],
            ).infer_objects()
        else:
            df = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
    except Exception as e:
        raise ValueError(f"Failed to read Excel file '{path}': {e}") from e

    # Strip leading/trailing whitespace from column names (common Excel issue)
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Lossless CSV typing: plain spellings only (see LOSSLESS READS)
# ─────────────────────────────────────────────────────────────────────────────

_PLAIN_BOOL     = re.compile(r"(?i:true|false)")
_PLAIN_INT      = re.compile(r"0|-?[1-9][0-9]*")
_PLAIN_DECIMAL  = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][-+]?[0-9]+)?")
_PLAIN_DATETIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?)?")

# (lowest, highest, numpy dtype, nullable dtype) for integer columns, in order of preference
_INT_STORAGES = (
    (-(2 ** 63), 2 ** 63 - 1, np.int64,  "Int64"),
    (0,          2 ** 64 - 1, np.uint64, "UInt64"),
)


def _type_plain_columns(df: pd.DataFrame, only: list | None = None) -> pd.DataFrame:
    """Type every text column (of `only`, when given) whose values are all plain spellings of one type."""
    for col in (df.columns if only is None else only):
        typed = _plain_typed(df[col])
        if typed is not None:
            df[col] = typed
    return df


def _plain_typed(s: pd.Series) -> pd.Series | None:
    """
    `s` typed without loss, or None when some value needs a clean step's
    decision (or the column is empty). Each distinct value is checked once.
    Nullable dtypes (Int64, boolean) when values are missing, as DuckDB's
    typed reader gives.
    """
    codes, uniques = pd.factorize(s)                    # missing values get code -1
    texts = [u for u in uniques if isinstance(u, str)]
    if not texts or len(texts) != len(uniques):
        return None
    missing = codes < 0

    def every(pattern: re.Pattern) -> bool:
        return all(pattern.fullmatch(text) for text in texts)

    if every(_PLAIN_BOOL):
        values = np.array([text.lower() == "true" for text in texts])[codes]
        array  = pd.arrays.BooleanArray(values, missing) if missing.any() else values
    elif every(_PLAIN_INT):
        numbers = [int(text) for text in texts]
        storage = next(((dtype, nullable) for low, high, dtype, nullable in _INT_STORAGES
                        if all(low <= n <= high for n in numbers)), None)
        if storage is None:
            return None                                 # beyond 64 bits: stays text
        values = np.array(numbers, dtype=storage[0])[codes]
        array  = pd.arrays.IntegerArray(values, missing) if missing.any() else values
    elif every(_PLAIN_DECIMAL):
        floats = [float(text) for text in texts]
        if not all(_same_number(text, value) for text, value in zip(texts, floats)):
            return None                                 # a float would round, underflow or overflow it
        array = np.array(floats, dtype="float64")[codes]
        array[missing] = np.nan
    elif every(_PLAIN_DATETIME):
        stamps = pd.to_datetime(pd.Series(texts, dtype=object), format="ISO8601", errors="coerce")
        if stamps.isna().any():
            return None                                 # not a calendar date, or out of range
        array = stamps.to_numpy(dtype="datetime64[us]")[codes]
        array[missing] = np.datetime64("NaT", "us")
    else:
        return None
    return pd.Series(array, index=s.index, name=s.name)


def _same_number(text: str, value: float) -> bool:
    """True when `value` reads back as exactly the number `text` spells ("1.50" and 1.5 do)."""
    if not np.isfinite(value):
        return False
    shortest = repr(value)
    if shortest == text:
        return True
    try:
        return Decimal(shortest) == Decimal(text)
    except InvalidOperation:
        return False


def _load_sqlite(path: Path) -> pd.DataFrame:
    """
    Load from a local SQLite file.
    Auto-detects the first table if there are multiple.
    """
    import sqlite3
    conn = sqlite3.connect(path)
    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        conn
    )
    if tables.empty:
        raise ValueError(f"No tables found in SQLite file: {path}")
    table_name = tables.iloc[0]["name"]
    df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    conn.close()
    return df


def _load_sql(source: str) -> pd.DataFrame:
    """
    Load from a SQL database using a connection string.

    The source format is:
        <sqlalchemy_url>::<table_name>
    Example:
        postgresql://alice:secret@localhost:5432/warehouse::orders

    We split on the LAST '::' to get the connection string and table name.
    This is safe because connection strings may contain ':' characters (e.g. port).

    SQLAlchemy creates a connection that works with any supported DB engine.
    The actual DB driver (psycopg2, pymysql, etc.) must be installed separately.
    """
    try:
        import sqlalchemy
    except ImportError:
        raise ImportError("sqlalchemy is required for SQL connections: pip install sqlalchemy")

    if "::" not in source:
        raise ValueError(
            f"SQL connection string must include '::table_name' suffix.\n"
            f"  Example: postgresql://user:pass@host/db::my_table\n"
            f"  Got: {_mask_password(source)}"
        )

    # rsplit with maxsplit=1 → split only on the last '::'
    # This handles edge cases like schemas: "db::schema.table"
    conn_str, table_name = source.rsplit("::", 1)

    try:
        engine = sqlalchemy.create_engine(conn_str)
        with engine.connect() as conn:
            # Quoted table name handles reserved words and schemas
            df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
        return df
    except Exception as e:
        raise ConnectionError(
            f"Failed to connect to database or read table '{table_name}'.\n"
            f"  Connection string: {_mask_password(conn_str)}\n"
            f"  Error: {e}"
        ) from e


def _is_sql_connection(source: str) -> bool:
    """Check if the source looks like a database connection string."""
    return any(source.startswith(prefix) for prefix in SQL_PREFIXES)


# The password of a URL: from the ":" after the user name up to the LAST
# "@", so unencoded "@", "/" or "?" in it are masked too (an "@" further on,
# in the path or the query, makes it mask more, never less).
_URL_PASSWORD   = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*://[^:/@?#]*:).*(@)", re.DOTALL)
# A password passed as a query parameter: ?password=..., &pwd=...
_QUERY_PASSWORD = re.compile(r"([?&](?:password|passwd|pwd)=)[^&#]*", re.IGNORECASE)


def _mask_password(source: str) -> str:
    """
    `source` with every password replaced by ***, for anything printed:
      postgresql://user:SECRET@host/db::t         → postgresql://user:***@host/db::t
      postgresql://user:SE@CRET@host/db::t        → postgresql://user:***@host/db::t
      postgresql://user@host/db?password=SECRET   → postgresql://user@host/db?password=***
    The URL (the part before the last "::") is rendered by SQLAlchemy with
    hide_password=True. SQLAlchemy ends the password at the FIRST "@", so
    when the URL holds more than one "@", or SQLAlchemy cannot parse it, a
    regex masks up to the last "@" instead. password= query values are
    masked either way. A source without a password (a file path, a sqlite
    URL) comes back unchanged.
    """
    url, sep, table = source.rpartition("::") if "::" in source else (source, "", "")
    masked = _render_without_password(url)
    if masked is None:
        masked = _URL_PASSWORD.sub(r"\1***\2", url)
    return _QUERY_PASSWORD.sub(r"\1***", masked) + sep + table


def _render_without_password(url: str) -> str | None:
    """SQLAlchemy's rendering with the password hidden; None when there is none or it cannot be trusted."""
    if "://" not in url or url.count("@") != 1:
        return None
    try:
        from sqlalchemy.engine import make_url
        parsed = make_url(url)
    except Exception:                       # not a URL SQLAlchemy can parse
        return None
    if parsed.password is None:
        return None
    return parsed.render_as_string(hide_password=True)