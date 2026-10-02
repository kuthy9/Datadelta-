"""
numbers.py — Parse numeric text into real numbers, without losing information.

Exports and spreadsheets write numbers as text: "1,200", "$5", "(3.5)",
"12%", full-width "１２３". As text they are categories, so the diff
cannot compare means or distributions, and a CSV-vs-XLSX comparison
reports a false type change.

WHAT IS ACCEPTED (per value, after full-width folding and strip)
  - optional sign, optional currency symbol ($ ¥ € £, and full-width ￥ ￡ ＄)
    before or after the number (one symbol at most)
  - thousands separators "," or a space between 3-digit groups, used
    consistently (no-break, thin and ideographic spaces count as spaces)
  - decimals and exponents ("1.5", ".5", "1e3")
  - accounting negatives "(3.5)" → -3.5
  - a trailing "%" → value / 100
  The Unicode minus sign (U+2212) counts as "-".

FULL-WIDTH FOLDING, NOT NFKC
  Only the full-width forms (U+FF01-FF5E, ￡ ￥) and the Unicode spaces
  are folded to ASCII. NFKC would also turn superscript, circled and
  mathematical digits into plain ones: "5²" would read as 52 and "10³"
  as 103, a silent change of value. Those stay text.

LOSSLESS RULES (the whole column decides)
  - Convert only when the share of parseable non-null values reaches
    min_parse_ratio (default 1.0: every value). Below 1.0 the
    unparseable values become null and are reported as "coerced_to_null".
  - A column that cannot be converted but is mostly numeric (at least
    SKIP_REPORT_RATIO of its values parse) is left as text with a
    "skipped" action whose examples are the offending values. Never a
    silent partial conversion.
  - Any value with a leading zero ("00123", "-012") marks the column as
    codes: kept as text, "skipped" with note "leading zeros (codes)".
  - "%" only when every parsed value carries it, otherwise "skipped"
    ("mixed percent and plain numbers"); two different currency symbols
    in one column are "skipped" too ("mixed currency symbols").
  - Values with more than 15 significant digits are not parsed: float64
    cannot hold them exactly (long IDs stay text). The same limit applies
    to the int cells of a mixed object column (abs value >= 10**15): the
    column is left alone and "skipped" with the offending values as
    examples. Float cells are already float64, so they carry over exactly.
  - Already-numeric columns are never touched, and neither is an object
    column without a single text cell that holds dates, booleans, dicts
    ...: there is nothing to parse. In a mixed column the real numbers are
    carried over.
  - An object column of number cells and nulls only (Excel numbers whose
    "N/A" the null_tokens step nulled) gets a numeric dtype without a
    float round trip: Int64 (or UInt64) for int cells, Float64 when float
    cells are present (the 15-digit limit above applies to its ints).

The result is nullable: Int64 when every value is written as an integer
(no decimal point, exponent or percent sign), otherwise Float64.

SPEED
  pandas string methods on object columns are Python loops as well, and
  this parse needs about seven of them. Instead, each DISTINCT string is
  parsed once by one compiled regex (pd.factorize), and the results are
  spread back to the rows with numpy indexing. Repetitive messy columns
  cost almost nothing; 200k distinct values take about 0.2 s.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from typing      import NamedTuple

import numpy as np
import pandas as pd

from ..plan   import ParseOptions
from ..report import CleanAction
from .base    import SKIP_REPORT_RATIO, StepContext, as_mask, change_examples, is_text_column, str_mask


NAME = "numbers"

MAX_SIGNIFICANT_DIGITS = 15

# An int cell of an object column at or above this is rounded by float64.
EXACT_INT_LIMIT = 10 ** MAX_SIGNIFICANT_DIGITS

# (lowest, highest, nullable dtype) for an object column of int cells, in order of preference.
_INT_DTYPES = (
    (-(2 ** 63), 2 ** 63 - 1, "Int64"),
    (0,          2 ** 64 - 1, "UInt64"),
)

_NUMBER_RE = re.compile(r"""
    (?P<open>\(?) \s*                         # "(" of an accounting negative
    (?P<sign>[-+]?) \s*
    (?P<cur>[$¥€£]?) \s*                      # leading currency symbol
    (?P<sign2>[-+]?) \s*                      # sign after the symbol: "$-5"
    (?P<body>
        (?: [1-9][0-9]{0,2} (?P<sep>[, ]) [0-9]{3} (?: (?P=sep) [0-9]{3} )*   # 1,200 / 1 200 000
          | [0-9]+ )
        (?: \.[0-9]* )?
      | \.[0-9]+
    )
    (?P<exp> (?: [eE][-+]?[0-9]+ )? ) \s*
    (?P<cur2>[$¥€£]?) \s*                     # trailing currency symbol
    (?P<pct>%?) \s*
    (?P<close>\)?)
""", re.VERBOSE)


class _Parsed(NamedTuple):
    ok:           bool
    value:        float    # signed; "%" not applied yet
    int_like:     bool     # no decimal point, exponent or "%"
    percent:      bool
    symbol:       str      # currency symbol used, or ""
    leading_zero: bool     # "00123", "-012"


_NOT_A_NUMBER = _Parsed(False, math.nan, False, False, "", False)

# Full-width ASCII (U+FF01-FF5E) → ASCII, full-width ￡ ￥ → £ ¥, and every
# Unicode space (no-break, thin, ideographic ...) → " ".
_FOLD: dict[int, str] = {
    **{code: chr(code - 0xFEE0) for code in range(0xFF01, 0xFF5F)},
    0xFFE1: "£", 0xFFE5: "¥",
    **{code: " " for code in (0x00A0, *range(0x2000, 0x200B), 0x202F, 0x205F, 0x3000)},
    0x2212: "-",                            # minus sign
}


# ─────────────────────────────────────────────────────────────────────────────
# Step
# ─────────────────────────────────────────────────────────────────────────────

def apply(df: pd.DataFrame, ctx: StepContext) -> tuple[pd.DataFrame, list[CleanAction]]:
    min_ratio = (ctx.plan.numbers or ParseOptions()).min_parse_ratio
    out = None
    actions: list[CleanAction] = []

    for col in ctx.parse_columns(df):
        s = df[col]
        if not is_text_column(s):
            continue
        new, column_actions = _convert(col, s, min_ratio)
        actions.extend(column_actions)
        if new is not None:
            if out is None:
                out = df.copy()
            out[col] = new

    return (df if out is None else out), actions


# ─────────────────────────────────────────────────────────────────────────────
# One string
# ─────────────────────────────────────────────────────────────────────────────

def _parse_one(raw: str) -> _Parsed:
    text = raw.translate(_FOLD).strip()
    m = _NUMBER_RE.fullmatch(text)
    if m is None:
        return _NOT_A_NUMBER
    g = m.groupdict()
    paren = g["open"] == "("
    signs = g["sign"] + g["sign2"]
    if paren != (g["close"] == ")") or len(signs) > 1 or (paren and signs) or (g["cur"] and g["cur2"]):
        return _NOT_A_NUMBER
    body = g["body"]
    digits = body.replace(",", "").replace(" ", "")
    if len(digits.replace(".", "").lstrip("0")) > MAX_SIGNIFICANT_DIGITS:
        return _NOT_A_NUMBER
    value = float(digits + g["exp"])
    if not math.isfinite(value):
        return _NOT_A_NUMBER
    return _Parsed(
        ok           = True,
        value        = -value if (paren or signs == "-") else value,
        int_like     = "." not in body and not g["exp"] and not g["pct"],
        percent      = bool(g["pct"]),
        symbol       = g["cur"] or g["cur2"],
        leading_zero = len(body) > 1 and body[0] == "0" and body[1].isdigit(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# One column
# ─────────────────────────────────────────────────────────────────────────────

def _is_plain_number(value: object) -> bool:
    """int / float (numpy too), but not bool — e.g. Excel cells in a mixed column."""
    return isinstance(value, (int, float, np.integer, np.floating)) and not isinstance(value, (bool, np.bool_))


def _beyond_float_precision(value: object) -> bool:
    """An int too large for 15 significant digits, so float64 cannot hold it exactly."""
    return isinstance(value, (int, np.integer)) and abs(int(value)) >= EXACT_INT_LIMIT


def _spread(index: pd.Index, subset: pd.Series) -> pd.Series:
    """A full-length mask from a mask over a subset of the positions."""
    full = pd.Series(False, index=index)
    full.loc[subset.index[subset.to_numpy()]] = True
    return full


def _skipped(col: str, values: pd.Series, mask: pd.Series, note: str) -> CleanAction:
    return CleanAction(NAME, col, "skipped", int(mask.sum()),
                       examples=change_examples(values, values, mask), note=note)


def _type_number_cells(
    col:     str,
    s:       pd.Series,
    values:  pd.Series,
    nonnull: pd.Series,
) -> tuple[pd.Series | None, list[CleanAction]]:
    """
    An object column without a single text cell. When every non-null cell
    is a number (Excel number cells whose "N/A" the null_tokens step
    nulled), the column gets a numeric dtype without a float round trip:
    int cells only → Int64, else UInt64; with float cells → Float64, which
    holds an int cell exactly only below EXACT_INT_LIMIT. A column that no
    dtype holds exactly is "skipped". Other cells (dates, booleans,
    Decimals, dicts ...) leave the column alone: there is nothing to parse.
    """
    cells = values[nonnull]
    if not cells.map(_is_plain_number).all():
        return None, []
    ints = as_mask(cells.map(lambda v: isinstance(v, (int, np.integer))))
    if ints.all():
        whole   = [int(v) for v in cells]
        dtype   = next((name for low, high, name in _INT_DTYPES if all(low <= n <= high for n in whole)), None)
        low, high, _name = _INT_DTYPES[0]
        blocked = as_mask(cells.map(lambda v: not low <= int(v) <= high))
    else:
        blocked = ints & as_mask(cells.map(_beyond_float_precision))
        dtype   = None if blocked.any() else "Float64"
    if dtype is None:
        return None, [_skipped(col, values, _spread(values.index, blocked),
                               "numbers: values beyond 15 significant digits")]

    convert = int if dtype != "Float64" else float
    result = pd.Series(
        pd.array([convert(v) if ok else None for v, ok in zip(values, nonnull)], dtype=dtype),
        index = s.index,
        name  = s.name,
    )
    return result, [CleanAction(NAME, col, "parsed_number", int(nonnull.sum()), note="cells already held numbers")]


def _convert(col: str, s: pd.Series, min_ratio: float) -> tuple[pd.Series | None, list[CleanAction]]:
    """Return (new column or None, actions) for one text column."""
    values = s.reset_index(drop=True)                   # positional; the index is restored at the end
    nonnull = as_mask(values.notna())
    total = int(nonnull.sum())
    if total == 0:
        return None, []

    # ── Which cells are text, which are already numbers ──────────────────────
    is_str = str_mask(values)
    if not is_str.any():
        return _type_number_cells(col, s, values, nonnull)
    numeric_objects = nonnull & ~is_str
    if numeric_objects.any():
        numeric_objects &= as_mask(values.map(_is_plain_number))
        if (nonnull & ~is_str & ~numeric_objects).any():
            return None, []                             # holds dates, booleans or other objects
    n_objects = int(numeric_objects.sum())

    # ── Parse each distinct string once, spread back to the rows ─────────────
    text = values[is_str]
    codes, distinct = pd.factorize(text)
    table = pd.DataFrame([_parse_one(t) for t in distinct], columns=list(_Parsed._fields))
    parsed = table.iloc[codes].set_axis(text.index)

    ok = as_mask(parsed["ok"])
    n_ok = int(ok.sum()) + n_objects
    bad = _spread(values.index, ~ok)

    # ── Whole-column lossless checks ─────────────────────────────────────────
    if n_ok / total < min_ratio:
        if n_ok / total >= SKIP_REPORT_RATIO:
            return None, [_skipped(col, values, bad, f"{int(bad.sum()):,} of {total:,} values are not numbers")]
        return None, []                                 # ordinary text: nothing to convert or report

    if n_objects:
        huge = values[numeric_objects].map(_beyond_float_precision).astype(bool)
        if huge.any():
            return None, [_skipped(col, values, _spread(values.index, huge),
                                   "numbers: values beyond 15 significant digits")]

    leading_zero = ok & as_mask(parsed["leading_zero"])
    if leading_zero.any():
        return None, [_skipped(col, values, _spread(values.index, leading_zero), "leading zeros (codes)")]

    symbol = parsed["symbol"]
    symbols = Counter(symbol[ok & as_mask(symbol != "")].tolist())
    if len(symbols) > 1:
        main = max(symbols.items(), key=lambda item: item[1])[0]
        odd = ok & as_mask((symbol != "") & (symbol != main))
        return None, [_skipped(col, values, _spread(values.index, odd), "mixed currency symbols")]

    percent = ok & as_mask(parsed["percent"])
    n_percent = int(percent.sum())
    if 0 < n_percent < n_ok:
        if n_percent <= n_ok - n_percent:
            odd = _spread(values.index, percent)
        else:
            odd = _spread(values.index, ok & ~percent) | numeric_objects
        return None, [_skipped(col, values, odd, "mixed percent and plain numbers")]

    # ── Convert ───────────────────────────────────────────────────────────────
    object_values = values[numeric_objects]
    all_int = bool(as_mask(parsed["int_like"])[ok].all()) and all(
        isinstance(v, (int, np.integer)) for v in object_values
    )

    numbers = parsed["value"].astype("float64").where(ok)
    if n_percent:
        numbers = numbers / 100
    result = pd.Series(np.nan, index=values.index, dtype="float64")
    result.loc[text.index] = numbers.to_numpy()
    if n_objects:
        result.loc[numeric_objects] = pd.to_numeric(object_values).astype("float64").to_numpy()
    result = result.astype("Int64" if all_int else "Float64")
    result.index = s.index

    converted = _spread(values.index, ok) | numeric_objects
    notes = []
    if symbols:
        notes.append(f"currency symbol {next(iter(symbols))} removed")
    if n_percent:
        notes.append("percent values divided by 100")
    actions = [CleanAction(NAME, col, "parsed_number", n_ok,
                           examples=change_examples(s, result, converted), note="; ".join(notes))]

    n_bad = int(bad.sum())
    if n_bad:
        actions.append(CleanAction(
            NAME, col, "coerced_to_null", n_bad,
            examples = change_examples(s, result, bad),
            note     = f"{n_bad:,} of {total:,} values are not numbers (min_parse_ratio {min_ratio:g})",
        ))
    return result, actions
