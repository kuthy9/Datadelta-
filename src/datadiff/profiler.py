"""
profiler.py — Detect the semantic type of each column automatically.

Instead of relying only on pandas dtype (int64, object, etc.), we infer
what the column *means* in a business context. This matters because the
same dtype can serve very different purposes:
  - An integer column could be an order_id (ID) or a revenue (numeric)
  - A string column could be a country (category) or a description (text)

Semantic types and their diff strategies:
  id        → track key integrity (duplicates, deletions, new keys)
  numeric   → distribution drift (KS test, mean shift)
  category  → share shift per value (% APAC went from 30% to 0%)
  datetime  → range change detection
  text      → null rate only (diffing free text is not meaningful)
  unknown   → null rate only
"""

from dataclasses import dataclass, field
from typing import Literal
import pandas as pd


SemanticType = Literal["id", "numeric", "category", "datetime", "text", "unknown"]

# Thresholds for heuristic type detection
CATEGORY_CARDINALITY_THRESHOLD = 0.05   # unique/total < 5%  → probably categorical
ID_CARDINALITY_THRESHOLD       = 0.95   # unique/total > 95% → probably a key


@dataclass
class ColumnProfile:
    """Profile of a single column, computed from the before dataset."""
    name: str
    semantic_type: SemanticType
    dtype_before: str | None        # pandas dtype string, e.g. "int64", "object"
    dtype_after: str | None = None  # None if column is absent from that side
    exists_in_before: bool = True
    exists_in_after:  bool = True


@dataclass
class DataProfile:
    """
    Unified profile of both datasets.
    `columns` is keyed by column name and covers the union of both datasets.
    """
    columns:     dict[str, ColumnProfile] = field(default_factory=dict)
    all_columns: list[str]               = field(default_factory=list)

    def get(self, col: str) -> ColumnProfile | None:
        return self.columns.get(col)


def profile_columns(df_before: pd.DataFrame, df_after: pd.DataFrame) -> DataProfile:
    """
    Build a unified column profile from both DataFrames.
    We preserve column order using dict.fromkeys() which deduplicates
    while maintaining insertion order (Python 3.7+ guarantee).
    """
    # Union of both column sets, preserving order
    all_cols = list(dict.fromkeys(list(df_before.columns) + list(df_after.columns)))
    profile  = DataProfile(all_columns=all_cols)

    for col in all_cols:
        in_before = col in df_before.columns
        in_after  = col in df_after.columns

        # Use whichever side has the column for type inference
        # Prefer the before dataset as the "reference"
        series = df_before[col] if in_before else df_after[col]

        profile.columns[col] = ColumnProfile(
            name          = col,
            semantic_type = _infer_semantic_type(series),
            dtype_before  = str(df_before[col].dtype) if in_before else None,
            dtype_after   = str(df_after[col].dtype)  if in_after  else None,
            exists_in_before = in_before,
            exists_in_after  = in_after,
        )

    return profile


def _infer_semantic_type(series: pd.Series) -> SemanticType:
    """
    Heuristic pipeline for semantic type detection.
    Order matters: datetime check must come before numeric to avoid
    misclassifying Unix timestamps as numeric.
    """
    # 1. Datetime check (regardless of dtype — could be stored as string)
    if _looks_like_datetime(series):
        return "datetime"

    dtype = series.dtype

    # 2. Numeric types (int or float)
    if pd.api.types.is_numeric_dtype(dtype):
        n_total  = max(len(series), 1)
        n_unique = series.nunique()
        unique_ratio = n_unique / n_total

        # High-cardinality integers are likely IDs (order_id, user_id, etc.)
        if unique_ratio > ID_CARDINALITY_THRESHOLD and pd.api.types.is_integer_dtype(dtype):
            return "id"

        return "numeric"

    # 3. String / object types
    if dtype == object or pd.api.types.is_string_dtype(dtype):
        n_total      = max(len(series), 1)
        unique_ratio = series.nunique() / n_total
        col_lower    = str(series.name).lower() if series.name else ""

        # Explicit keyword matching: column names ending/starting with id-like words
        if any(kw in col_lower for kw in ("_id", "id_", "uuid", "guid", "_key", "code_")):
            return "id"

        # Low-cardinality strings → categorical (status, region, country, tier...)
        if unique_ratio < CATEGORY_CARDINALITY_THRESHOLD:
            return "category"

        # High-cardinality strings → text (names, descriptions, emails...)
        if unique_ratio > 0.5:
            return "text"

        # Middle ground: treat as category (safer default for diff purposes)
        return "category"

    return "unknown"


def _looks_like_datetime(series: pd.Series) -> bool:
    """
    Detect datetime columns that might be stored as strings.
    We try to parse a sample and accept if >80% succeed.
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    if series.dtype == object:
        sample = series.dropna().head(50)
        if len(sample) == 0:
            return False
        try:
            parsed = pd.to_datetime(sample, errors="coerce")
            # Accept if most values parsed successfully (not all NaT)
            success_rate = parsed.notna().mean()
            return success_rate > 0.8
        except (ValueError, TypeError):
            return False

    return False