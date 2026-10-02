"""
jsonutil.py — Strict JSON for `--json` output.

WHY NOT PLAIN json.dumps()?
  Diff results are full of values the standard library mishandles:
    - NaN / ±inf (e.g. the mean of an all-null column). json.dumps()
      writes them as the bare words NaN / Infinity, which is NOT valid
      JSON — jq, JSON.parse() and most CI tooling reject the document.
    - numpy scalars (np.int64, np.bool_) raise TypeError outright.
    - pandas Timestamps, dates, sets and tuples are not serializable.

  to_jsonable() walks the structure once and converts everything to
  plain Python values (NaN → None, numpy → native, dates → ISO 8601).
  dumps() then serializes with allow_nan=False, so a non-finite float
  that slipped through is a loud bug instead of silently invalid output.

  Clean examples carry raw cell values (a DuckDB TIME or INTERVAL, a
  Postgres NUMERIC, bytes), so every other value becomes its text:
  `--json` must not fail on whatever a column holds.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, time, timedelta
from decimal  import Decimal
from typing   import Any

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Conversion
# ─────────────────────────────────────────────────────────────────────────────

def to_jsonable(obj: Any) -> Any:
    """
    Recursively convert `obj` into values json.dumps(allow_nan=False) accepts.

      numpy scalar         → int / float / bool
      NaN, ±inf, NaT, NA   → None
      Timestamp/datetime   → ISO 8601 string (date → "YYYY-MM-DD", time → "HH:MM:SS")
      timedelta-like       → ISO 8601 duration ("P1DT2H0M3S")
      Decimal              → its exact digits, as a string
      set / frozenset      → sorted list
      tuple / ndarray      → list
      dict / list          → converted element by element
      anything else        → str(obj)
    """
    # ── Missing-value singletons ──────────────────────────────────────────
    if obj is None or obj is pd.NaT or obj is pd.NA:
        return None

    # ── Durations, before numpy integers: np.timedelta64 is one ──────────
    #    (pd.Timedelta is a timedelta subclass)
    if isinstance(obj, (timedelta, np.timedelta64)):
        delta = pd.Timedelta(obj)
        return None if pd.isna(delta) else delta.isoformat()

    # ── Plain scalars (bool before int: bool is an int subclass) ──────────
    if isinstance(obj, (bool, str)):
        return obj
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        value = float(obj)
        return value if math.isfinite(value) else None
    if isinstance(obj, int):
        return obj

    # ── Dates and times (pd.Timestamp is a datetime subclass) ────────────
    if isinstance(obj, np.datetime64):
        ts = pd.Timestamp(obj)
        return None if pd.isna(ts) else ts.isoformat()
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()

    # ── Exact decimals (Postgres NUMERIC): a float would round them ──────
    if isinstance(obj, Decimal):
        return str(obj)

    # ── Containers ────────────────────────────────────────────────────────
    if isinstance(obj, dict):
        return {_key(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, (set, frozenset)):
        items = [to_jsonable(v) for v in obj]
        try:
            return sorted(items)
        except TypeError:
            return sorted(items, key=repr)   # mixed types: stable, deterministic order
    if isinstance(obj, np.ndarray):
        return [to_jsonable(v) for v in obj.tolist()]

    # Anything else (bytes, Interval, Period, custom objects): its text.
    return str(obj)


def _key(key: Any) -> Any:
    """JSON object keys must be str / int / float / bool / None."""
    key = to_jsonable(key)
    if key is None or isinstance(key, (str, int, float, bool)):
        return key
    return str(key)


# ─────────────────────────────────────────────────────────────────────────────
# Serialization
# ─────────────────────────────────────────────────────────────────────────────

def dumps(obj: Any, indent: int = 2) -> str:
    """Serialize to strictly valid JSON. Non-ASCII text (e.g. CJK) is kept as-is."""
    return json.dumps(
        to_jsonable(obj),
        indent       = indent,
        ensure_ascii = False,
        allow_nan    = False,
    )
