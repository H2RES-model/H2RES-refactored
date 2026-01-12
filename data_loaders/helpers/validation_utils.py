"""Shared validation helpers for loader inputs."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional
import pandas as pd


def require_columns(df: pd.DataFrame, required: Iterable[str], path_label: str) -> None:
    """Ensure required columns are present.

    Args:
        df: Input DataFrame to validate.
        required: Required column names.
        path_label: Label used in error messages.

    Raises:
        ValueError: If any required columns are missing.
    """
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"{path_label} missing columns: {sorted(missing)}")


def require_values(
    df: pd.DataFrame,
    required_str: List[str],
    required_num: List[str],
    path_label: str,
    name_col: Optional[str] = "name",
) -> None:
    """Ensure required values are present and numeric where needed.

    Args:
        df: Input DataFrame to validate.
        required_str: Columns that must be non-empty strings.
        required_num: Columns that must be numeric.
        path_label: Label used in error messages.
        name_col: Column name used for reporting missing values.

    Raises:
        ValueError: If any required values are missing or non-numeric.
    """
    if name_col is not None and name_col in df.columns:
        name_series = df[name_col].astype(str)
    else:
        # When a name column is not present, construct a Series from the index
        # so that we can use .loc[...] below (Index doesn't have .loc).
        name_series = pd.Series(df.index.astype(str), index=df.index)

    missing_values: Dict[str, List[str]] = {}
    for col in required_str:
        mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
        if mask.any():
            missing_values[col] = name_series.loc[mask].tolist()
    for col in required_num:
        vals = pd.to_numeric(df[col], errors="coerce")
        mask = vals.isna()
        if mask.any():
            missing_values[col] = name_series.loc[mask].tolist()
    if missing_values:
        raise ValueError(f"{path_label} has missing required values: {missing_values}")
