"""
io.py — Write a cleaned DataFrame back to disk.

The output format follows the file extension: .csv .parquet .xlsx .json.
`datadelta clean orders.csv` writes orders.clean.csv next to the input
unless -o names another file; a database source has no "next to", so it
needs -o.

DATES
  CSV and JSON have no date type, so datetime columns are written as
  ISO 8601 text: "2024-01-05" when every value is a whole day,
  "2024-01-05T10:30:00" otherwise, and with the UTC offset kept
  ("2024-01-05T10:30:00+02:00") for timezone-aware columns.
  Excel cannot store timezone-aware datetimes (openpyxl rejects them),
  so those are converted to UTC and written without a zone.
  Parquet keeps real types; it is written by DuckDB, a core dependency,
  so no extra Parquet library is needed.

EXACT VALUES
  JSON is written by the standard library, not pandas' to_json, which
  rounds floats to 10 decimal places (1e-12 became 0.0). In .xlsx, text
  that starts with "=" stays text: openpyxl would store it as a live
  formula without a value (see _write_xlsx).

FAILED WRITES
  write_frame() builds the file next to the target under a temporary
  name and moves it onto the target (os.replace) only when it is
  complete. A failure part-way (openpyxl rejects a control character
  only while saving, DuckDB cannot open the path) therefore leaves no
  partial file and cannot damage an earlier good output; the temporary
  file is removed. Whatever the writer raised arrives as WriteError, an
  OSError whose one-line message names the path and the cause.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from pathlib import Path

import pandas as pd

from ..jsonutil import dumps
from ..loader   import SQL_PREFIXES, duckdb_connection


SUPPORTED_OUTPUTS: frozenset[str] = frozenset({".csv", ".parquet", ".xlsx", ".json"})

_SUPPORTED_LIST = ", ".join(sorted(SUPPORTED_OUTPUTS))


class WriteError(OSError):
    """The cleaned frame could not be written. One line: "could not write <path>: <cause>"."""


# ─────────────────────────────────────────────────────────────────────────────
# Output path
# ─────────────────────────────────────────────────────────────────────────────

def default_output_path(source: str) -> Path:
    """
    <dir>/<stem>.clean<suffix> next to the input file.
    ValueError for database sources and for inputs whose format cannot be
    written (.xls, .sqlite, ...): those need an explicit -o.
    """
    source = str(source)
    if source.startswith(SQL_PREFIXES):
        raise ValueError(f"a database source needs an output file: use -o PATH ({_SUPPORTED_LIST})")
    path = Path(source)
    if path.suffix.lower() not in SUPPORTED_OUTPUTS:
        raise ValueError(
            f"cannot write '{path.suffix or path.name}' files; choose an output with -o PATH ({_SUPPORTED_LIST})"
        )
    return path.with_name(f"{path.stem}.clean{path.suffix}")


# ─────────────────────────────────────────────────────────────────────────────
# Writers
# ─────────────────────────────────────────────────────────────────────────────

def write_frame(df: pd.DataFrame, path: Path) -> None:
    """
    Write `df` in the format named by the suffix of `path` (ValueError if unsupported).

    Atomic: the target is replaced only by a complete file (see FAILED
    WRITES above). Any failure of the writer is raised as WriteError.
    """
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_OUTPUTS:
        raise ValueError(f"unsupported output format '{path.suffix}' (supported: {_SUPPORTED_LIST})")

    # Same directory, so os.replace never crosses a filesystem; same suffix,
    # so a writer that looks at the extension still sees the right one.
    temp = path.with_name(f".{path.stem}.{uuid.uuid4().hex[:8]}{path.suffix}")
    try:
        _write(df, temp, suffix)
        os.replace(temp, path)
    except Exception as e:
        raise WriteError(f"could not write {path}: {_one_line(e, temp, path)}") from e
    finally:
        with contextlib.suppress(OSError):
            temp.unlink(missing_ok=True)


def _write(df: pd.DataFrame, path: Path, suffix: str) -> None:
    if suffix == ".csv":
        _with_iso_dates(df).to_csv(path, index=False, encoding="utf-8")
    elif suffix == ".json":
        path.write_text(_json_records(df) + "\n", encoding="utf-8")
    elif suffix == ".xlsx":
        _write_xlsx(_without_timezones(df), path)
    else:
        _write_parquet(df, path)


def _one_line(error: Exception, temp: Path, target: Path) -> str:
    """
    The writer's message on one line: the temporary name replaced by the
    target's, and control characters shown escaped (openpyxl quotes the
    offending character itself) so a message never disturbs the terminal.
    """
    text = " ".join(str(error).split()) or type(error).__name__
    text = text.replace(str(temp), str(target))
    return "".join(c if c.isprintable() else c.encode("unicode_escape").decode("ascii") for c in text)


def _datetime_columns(df: pd.DataFrame) -> list:
    return [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col].dtype)]


def _iso_text(s: pd.Series) -> pd.Series:
    """One datetime column as ISO 8601 strings (missing values stay missing)."""
    values = s.dropna()
    aware = s.dt.tz is not None
    if not aware and bool((values == values.dt.normalize()).all()):
        return s.dt.strftime("%Y-%m-%d")
    fmt = "%Y-%m-%dT%H:%M:%S"
    if bool((values.dt.microsecond != 0).any()):
        fmt += ".%f"
    if not aware:
        return s.dt.strftime(fmt)
    # %z gives "+0200"; ISO 8601 extended format is "+02:00".
    return s.dt.strftime(fmt + "%z").str.replace(r"([+-]\d{2})(\d{2})$", r"\1:\2", regex=True)


def _with_iso_dates(df: pd.DataFrame) -> pd.DataFrame:
    columns = _datetime_columns(df)
    if not columns:
        return df
    out = df.copy()
    for col in columns:
        out[col] = _iso_text(df[col])
    return out


def _without_timezones(df: pd.DataFrame) -> pd.DataFrame:
    """Excel has no time zones: aware columns become naive UTC."""
    columns = [col for col in _datetime_columns(df) if df[col].dt.tz is not None]
    if not columns:
        return df
    out = df.copy()
    for col in columns:
        out[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return out


def _json_records(df: pd.DataFrame) -> str:
    """
    The frame as a JSON array of records, through the standard library.
    pandas' to_json rounds floats to 10 decimal places (1e-12 becomes 0.0);
    json.dumps writes the shortest text that reads back as the same float.
    jsonutil turns NaN / NA into null and numpy scalars into plain numbers.
    """
    return dumps(_with_iso_dates(df).to_dict(orient="records"))


def _write_xlsx(df: pd.DataFrame, path: Path) -> None:
    """
    openpyxl stores every str that starts with "=" as a formula, so a text
    cell "=1+2" would come back as a formula without a value, and text from
    the data would become live formulas in the workbook someone opens
    (CWE-1236). A DataFrame never holds formulas: every formula cell is
    set back to text before the file is saved.
    """
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
        for sheet in writer.sheets.values():
            for row in sheet.iter_rows():
                for cell in row:
                    if cell.data_type == "f":
                        cell.data_type = "s"


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    frame = df.set_axis([str(c) for c in df.columns], axis=1)
    con = duckdb_connection()                       # progress bar off: stdout carries the report
    try:
        con.from_df(frame).write_parquet(str(path))
    finally:
        con.close()
