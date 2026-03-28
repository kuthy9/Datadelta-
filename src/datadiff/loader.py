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

  `duckdb.query("SELECT * FROM read_csv_auto('file.csv')").df()`

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
"""

from pathlib import Path
import pandas as pd
import duckdb


# All formats we handle natively (no SQL connection string)
FILE_FORMATS = {".csv", ".json", ".parquet", ".xlsx", ".xls", ".sqlite", ".db"}

# Prefixes that tell us the input is a database connection string
SQL_PREFIXES = ("postgresql://", "mysql://", "mysql+pymysql://",
                "mssql://", "mssql+pyodbc://", "sqlite:///")


def load_file(source: str) -> pd.DataFrame:
    """
    Universal entry point. Accepts either a file path (str or Path)
    or a database connection string with ::table_name suffix.
    Always returns a pandas DataFrame.
    """
    source = str(source)

    # ── Database connection string ────────────────────────────────────────────
    if _is_sql_connection(source):
        return _load_sql(source)

    # ── File-based source ─────────────────────────────────────────────────────
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(
            f"File not found: {path}\n"
            f"  If this is a database URL, check the prefix is one of: {SQL_PREFIXES}"
        )

    suffix = path.suffix.lower()
    if suffix not in FILE_FORMATS:
        raise ValueError(
            f"Unsupported format: '{suffix}'\n"
            f"  Supported: {sorted(FILE_FORMATS)}"
        )

    if suffix in (".xlsx", ".xls"):
        return _load_excel(path)

    if suffix in (".sqlite", ".db"):
        return _load_sqlite(path)

    # DuckDB handles CSV / JSON / Parquet natively
    return _load_via_duckdb(path, suffix)


# ─────────────────────────────────────────────────────────────────────────────
# Internal loaders
# ─────────────────────────────────────────────────────────────────────────────

def _load_via_duckdb(path: Path, suffix: str) -> pd.DataFrame:
    """
    Use DuckDB's native readers for CSV, JSON, Parquet.
    `read_csv_auto` automatically detects: delimiter, header, types, encoding.
    `.df()` converts the DuckDB result to a pandas DataFrame.
    """
    readers = {
        ".csv":     f"SELECT * FROM read_csv_auto('{path}')",
        ".json":    f"SELECT * FROM read_json_auto('{path}')",
        ".parquet": f"SELECT * FROM read_parquet('{path}')",
    }
    query = readers[suffix]
    return duckdb.query(query).df()


def _load_excel(path: Path) -> pd.DataFrame:
    """
    Load Excel files using pandas + openpyxl.
    We read the first sheet by default.
    openpyxl is the engine for .xlsx; xlrd handles legacy .xls.
    """
    try:
        # sheet_name=0 → first sheet; header=0 → first row is header
        df = pd.read_excel(path, sheet_name=0, header=0, engine="openpyxl")
    except Exception as e:
        raise ValueError(f"Failed to read Excel file '{path}': {e}") from e

    # Strip leading/trailing whitespace from column names (common Excel issue)
    df.columns = [str(c).strip() for c in df.columns]
    return df


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
            f"  Got: {source}"
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


def _mask_password(conn_str: str) -> str:
    """
    Mask the password in a connection string for safe error logging.
    postgresql://user:SECRET@host/db → postgresql://user:***@host/db
    """
    import re
    return re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", conn_str)