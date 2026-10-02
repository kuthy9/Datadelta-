"""
dates.py — Parse date text into real datetimes, without guessing silently.

A date stored as text ("2024-01-05", "05/01/2024", "2024年1月5日") is a
category to the diff: no range check, no ordering, and a CSV-vs-XLSX
comparison reports a false type change because one loader typed the
column and the other did not.

ACCEPTED SPELLINGS (several may be mixed in one column)
  year first      %Y-%m-%d   %Y/%m/%d   %Y.%m.%d   %Y年%m月%d日
  day/month       %d/%m/%Y   %m/%d/%Y   %d.%m.%Y   %m.%d.%Y
  each optionally followed by " %H:%M" or " %H:%M:%S", and ISO 8601
  timestamps ("2024-01-05T10:30:00", fractional seconds, "Z", "+02:00").

DAY/MONTH ORDER (the d/m/Y family, decided for the whole column)
  - some value has a first field above 12 ("25/12/2024")  → day first
  - some value has a second field above 12 ("12/25/2024") → month first
  - both kinds of value                                   → "skipped",
    note "inconsistent day/month order"
  - neither: plan.dates.dayfirst decides (default month first) and an
    "ambiguous" action counts the values that could be read both ways
    ("03/04/2024"; "04/04/2024" reads the same either way).

LOSSLESS RULES (the whole column decides)
  - Convert only when the share of parseable non-null values reaches
    min_parse_ratio (default 1.0). Below 1.0 the rest become null and
    are reported as "coerced_to_null".
  - A mostly-date column that cannot be converted, e.g. because of an
    impossible calendar date such as "2024-02-30", is left as text with
    a "skipped" action whose examples are the offending values.
  - Mixed UTC offsets (or offsets mixed with naive times) are "skipped":
    picking one zone would silently shift some values.
  - A timestamp with a non-zero digit below the microsecond
    (".123456789") is "skipped" ("dates: sub-microsecond precision"):
    datetime64[us] would cut it, and two distinct values could become
    one. Zeros there (".123456000") parse.
  - Columns that already hold datetimes (or mix in non-text objects)
    are never touched.

RESULT TYPE
  datetime64[us] (tz-aware with the column's single offset when it has
  one). Microseconds are what DuckDB, the CSV/JSON/Parquet loader, gives
  natively typed date columns, so a cleaned XLSX column and the same
  column read from CSV end up with the same dtype.

SPEED
  Each distinct string is classified once by one compiled regex
  (pd.factorize), then every group of identically formatted values is
  parsed by a single pd.to_datetime(format=...) call. Text columns that
  do not look like dates cost one regex pass over their distinct values.
"""

from __future__ import annotations

import re
from collections import Counter
from datetime    import timedelta, timezone

import numpy as np
import pandas as pd

from ..plan   import DateOptions
from ..report import CleanAction
from .base    import SKIP_REPORT_RATIO, StepContext, as_mask, change_examples, is_text_column, str_mask


NAME = "dates"

UNIT = "us"
ISO_FORMAT = "ISO8601"

# Every format the step understands, in the order notes list them.
_DATE_PARTS = ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y年%m月%d日",
               "%d/%m/%Y", "%m/%d/%Y", "%d.%m.%Y", "%m.%d.%Y")
CANDIDATE_FORMATS: tuple[str, ...] = tuple(
    date + time for date in _DATE_PARTS for time in ("", " %H:%M", " %H:%M:%S")
) + (ISO_FORMAT,)

_TIME = r"(?: (?P<hh>\d{1,2}):(?P<mi>\d{2})(?::(?P<ss>\d{2}))?)?"

_YMD_RE = re.compile(r"\d{4}(?P<sep>[-/.])\d{1,2}(?P=sep)\d{1,2}" + _TIME)
_CJK_RE = re.compile(r"\d{4}年\d{1,2}月\d{1,2}日" + _TIME)
_DMY_RE = re.compile(r"(?P<a>\d{1,2})(?P<sep>[/.])(?P<b>\d{1,2})(?P=sep)\d{4}" + _TIME)
_ISO_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:[.,]\d{1,9})?)?"
    r"(?P<tz>Z|[+-]\d{2}(?::?\d{2})?)?"
)

# The fractional seconds of an ISO timestamp; digits after the sixth are
# below datetime64[us].
_FRACTION_RE = re.compile(r":\d{2}[.,](\d+)")

_NAT = np.datetime64("NaT", UNIT)


# ─────────────────────────────────────────────────────────────────────────────
# Step
# ─────────────────────────────────────────────────────────────────────────────

def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    options = ctx.plan.dates or DateOptions()
    out = None
    actions: list[CleanAction] = []

    for col in ctx.parse_columns(df):
        s = df[col]
        if not is_text_column(s):
            continue
        new, column_actions = _convert(col, s, options)
        actions.extend(column_actions)
        if new is not None:
            if out is None:
                out = df.copy()
            out[col] = new

    return (df if out is None else out), actions


# ─────────────────────────────────────────────────────────────────────────────
# One distinct string → its format
# ─────────────────────────────────────────────────────────────────────────────

def _time_suffix(m: re.Match) -> str:
    if m.group("hh") is None:
        return ""
    return " %H:%M:%S" if m.group("ss") is not None else " %H:%M"


def _offset(token: str | None) -> str:
    """Normalize a UTC offset: None → "" (naive), "Z" / "+00" / "+0000" → "+00:00"."""
    if token is None:
        return ""
    if token == "Z":
        return "+00:00"
    digits = token[1:].replace(":", "")
    return f"{token[0]}{digits[:2]}:{digits[2:4] or '00'}"


def _classify(value: str) -> tuple[str, str, int, int, str]:
    """
    (kind, format, first_field, second_field, offset) for one string.
    kind: "fixed" (format known), "dmy" (format depends on the column's
    day/month order; the format holds "{a}"/"{b}" slots), "iso", or "".
    """
    m = _YMD_RE.fullmatch(value)
    if m:
        sep = m.group("sep")
        return "fixed", f"%Y{sep}%m{sep}%d" + _time_suffix(m), 0, 0, ""
    m = _CJK_RE.fullmatch(value)
    if m:
        return "fixed", "%Y年%m月%d日" + _time_suffix(m), 0, 0, ""
    m = _DMY_RE.fullmatch(value)
    if m:
        sep = m.group("sep")
        return "dmy", "{a}" + sep + "{b}" + sep + "%Y" + _time_suffix(m), int(m.group("a")), int(m.group("b")), ""
    m = _ISO_RE.fullmatch(value)
    if m:
        return "iso", ISO_FORMAT, 0, 0, _offset(m.group("tz"))
    return "", "", 0, 0, ""


def _beyond_microseconds(value: str) -> bool:
    """True when the fraction of a second has a non-zero digit after the sixth (".1234567")."""
    m = _FRACTION_RE.search(value)
    return m is not None and m.group(1)[6:].strip("0") != ""


def _tz_from_offset(offset: str) -> timezone:
    sign = -1 if offset[0] == "-" else 1
    delta = timedelta(hours=int(offset[1:3]), minutes=int(offset[4:6]))
    return timezone.utc if not delta else timezone(sign * delta)


# ─────────────────────────────────────────────────────────────────────────────
# One column
# ─────────────────────────────────────────────────────────────────────────────

def _skipped(col: str, values: pd.Series, row_mask: np.ndarray, note: str) -> CleanAction:
    """The column stays text; the values that blocked it are the examples."""
    mask = pd.Series(row_mask, index=values.index)
    return CleanAction(NAME, col, "skipped", int(mask.sum()),
                       examples=change_examples(values, values, mask), note=note)


def _convert(col: str, s: pd.Series, options: DateOptions) -> tuple[pd.Series | None, list[CleanAction]]:
    """Return (new column or None, actions) for one text column."""
    values = s.reset_index(drop=True)                   # positional; the index is restored at the end
    nonnull = as_mask(values.notna()).to_numpy()
    total = int(nonnull.sum())
    if total == 0 or int(str_mask(values).sum()) != total:
        return None, []                                 # empty, or holds non-text objects

    # ── Classify each distinct string once ───────────────────────────────────
    codes, distinct = pd.factorize(values)              # nulls get code -1
    weights = np.bincount(codes[codes >= 0], minlength=len(distinct))
    kind, fmt, a, b, offsets = zip(*map(_classify, distinct))
    kind, fmt, offsets = (np.array(field, dtype=object) for field in (kind, fmt, offsets))
    a, b = np.array(a, dtype=int), np.array(b, dtype=int)
    if weights[kind != ""].sum() / total < min(SKIP_REPORT_RATIO, options.min_parse_ratio):
        return None, []                                 # ordinary text: nothing to convert or report

    def rows(distinct_mask: np.ndarray) -> np.ndarray:
        """Row mask from a mask over the distinct values."""
        return np.append(distinct_mask, False)[codes]

    # ── Day/month order for the d/m/Y family ─────────────────────────────────
    dmy = kind == "dmy"
    day_first_evidence = dmy & (a > 12)
    month_first_evidence = dmy & (b > 12)
    if day_first_evidence.any() and month_first_evidence.any():
        minority = (day_first_evidence if weights[day_first_evidence].sum() <= weights[month_first_evidence].sum()
                    else month_first_evidence)
        return None, [_skipped(col, values, rows(minority), "inconsistent day/month order")]
    if day_first_evidence.any():
        dayfirst, inferred = True, True
    elif month_first_evidence.any():
        dayfirst, inferred = False, True
    else:
        dayfirst, inferred = options.dayfirst, False
    order = ("%d", "%m") if dayfirst else ("%m", "%d")
    fmt = np.array([f.format(a=order[0], b=order[1]) if k == "dmy" else f for f, k in zip(fmt, kind)], dtype=object)

    # ── Time zones: one offset for the whole column, or none ─────────────────
    shaped = kind != ""
    zone_counts = Counter({z: int(weights[shaped & (offsets == z)].sum()) for z in set(offsets[shaped])})
    if len(zone_counts) > 1:
        main = max(zone_counts.items(), key=lambda item: item[1])[0]
        return None, [_skipped(col, values, rows(shaped & (offsets != main)), "mixed timezone offsets")]
    zone = next(iter(zone_counts), "")

    # ── Sub-microsecond digits: datetime64[us] would drop them ──────────────
    finer = (kind == "iso") & np.array([_beyond_microseconds(v) for v in distinct], dtype=bool)
    if finer.any():
        return None, [_skipped(col, values, rows(finer), "dates: sub-microsecond precision")]

    # ── Parse each format group with one vectorized call ─────────────────────
    parsed = np.full(len(distinct), _NAT)
    used: list[str] = []
    for group_fmt in dict.fromkeys(fmt[shaped]):
        idx = np.flatnonzero(shaped & (fmt == group_fmt))
        group = pd.Series(np.asarray(distinct, dtype=object)[idx], dtype=object)
        if group_fmt == ISO_FORMAT:
            stamps = pd.to_datetime(group, format=ISO_FORMAT, errors="coerce", utc=bool(zone))
            if zone:
                stamps = stamps.dt.tz_localize(None)
        else:
            stamps = pd.to_datetime(group, format=group_fmt, errors="coerce")
        parsed[idx] = stamps.to_numpy(dtype=f"datetime64[{UNIT}]")
        if stamps.notna().any():
            used.append(group_fmt)

    ok = ~np.isnat(parsed)
    n_ok = int(weights[ok].sum())
    bad = rows(~ok)

    # ── Whole-column lossless check ──────────────────────────────────────────
    if n_ok / total < options.min_parse_ratio:
        if n_ok / total >= SKIP_REPORT_RATIO:
            n_bad = int(bad.sum())
            return None, [_skipped(col, values, bad, f"{n_bad:,} of {total:,} values are not valid dates")]
        return None, []

    # ── Convert ───────────────────────────────────────────────────────────────
    result = pd.Series(np.append(parsed, _NAT)[codes], index=s.index)
    if zone:
        result = result.dt.tz_localize("UTC").dt.tz_convert(_tz_from_offset(zone))

    converted = pd.Series(rows(ok), index=s.index)
    used.sort(key=CANDIDATE_FORMATS.index)
    note = "formats " + ", ".join("ISO 8601" if f == ISO_FORMAT else f for f in used)
    if inferred:
        note += "; day/month order inferred: " + ("day first" if dayfirst else "month first")
    actions = [CleanAction(NAME, col, "parsed_date", n_ok,
                           examples=change_examples(s, result, converted), note=note)]

    ambiguous = rows(dmy & ok & (a <= 12) & (b <= 12) & (a != b)) if not inferred else np.zeros(len(values), bool)
    if ambiguous.any():
        read_as = "day/month" if dayfirst else "month/day"
        actions.append(CleanAction(
            NAME, col, "ambiguous", int(ambiguous.sum()),
            examples = change_examples(s, result, pd.Series(ambiguous, index=s.index)),
            note     = f"day/month order cannot be told from the data; read as {read_as} "
                       f"(cleaning.dates.dayfirst: {'true' if dayfirst else 'false'})",
        ))

    n_bad = int(bad.sum())
    if n_bad:
        actions.append(CleanAction(
            NAME, col, "coerced_to_null", n_bad,
            examples = change_examples(s, result, pd.Series(bad, index=s.index)),
            note     = f"{n_bad:,} of {total:,} values are not valid dates (min_parse_ratio {options.min_parse_ratio:g})",
        ))
    return result, actions
