"""Value parsing helpers for loader inputs."""

from __future__ import annotations

from typing import Any, Optional
import pandas as pd


def is_missing(val: Any) -> bool:
    """Return True when a value should be treated as missing.

    Args:
        val: Value to check.

    Returns:
        True for None, NaN, or empty strings; False for valid values including 0.
    """
    return val is None or (isinstance(val, str) and val.strip() == "") or pd.isna(val)


def get_float(row: Any, col: str, default: Optional[float] = None) -> Optional[float]:
    """Return a float from a row-like mapping, preserving explicit zeros.

    Args:
        row: Row-like mapping with `.get`.
        col: Column name to extract.
        default: Default to return when the value is missing.

    Returns:
        Float value or the default when missing.
    """
    val = row.get(col, None)
    if is_missing(val):
        return default
    return float(val)
