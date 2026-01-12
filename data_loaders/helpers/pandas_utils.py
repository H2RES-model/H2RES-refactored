"""Pandas utilities for version-compatible reshaping."""

from __future__ import annotations

from typing import List, cast
import pandas as pd


def stack_compat(df: pd.DataFrame, id_vars: List[str], value_cols: List[str]) -> pd.Series:
    """Stack wide data with compatibility across pandas versions. 

    Args:
        df: Input DataFrame.
        id_vars: Identifier columns to keep as index.
        value_cols: Value columns to stack.

    Returns:
        Stacked Series with a MultiIndex of id_vars + column names.

    Notes:
        Uses the future stack implementation when available.
    """
    indexed = df.set_index(id_vars)[value_cols]
    try:
        res = indexed.stack(future_stack=True)
    except TypeError:
        res = indexed.stack(dropna=True)
    # Ensure a Series return type for the type checker; at runtime this should be a Series
    return cast(pd.Series, res)
