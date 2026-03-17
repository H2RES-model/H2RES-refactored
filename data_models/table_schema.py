from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional

import pandas as pd


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    dtype: str
    description: str
    unit: str = "n.a."
    status: str = "optional"


@dataclass(frozen=True)
class TableSpec:
    name: str
    description: str
    index: tuple[str, ...] = field(default_factory=tuple)
    columns: tuple[ColumnSpec, ...] = field(default_factory=tuple)

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(col.name for col in self.columns if col.status == "mandatory")

    @property
    def column_map(self) -> Dict[str, ColumnSpec]:
        return {col.name: col for col in self.columns}


def empty_table(spec: TableSpec) -> pd.DataFrame:
    cols = [col.name for col in spec.columns]
    return pd.DataFrame(columns=cols)


def ensure_dataframe(df: Any, spec: TableSpec) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{spec.name} must be a pandas DataFrame.")
    missing = set(spec.required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"{spec.name} missing columns: {sorted(missing)}")
    return df


def normalize_dataframe(
    df: pd.DataFrame,
    spec: TableSpec,
    *,
    copy: bool = True,
) -> pd.DataFrame:
    table = ensure_dataframe(df.copy() if copy else df, spec)
    for col in spec.columns:
        if col.name not in table.columns:
            continue
        if col.dtype == "string":
            table[col.name] = table[col.name].astype("string")
        elif col.dtype == "int":
            table[col.name] = pd.to_numeric(table[col.name], errors="raise").astype(int)
        elif col.dtype == "float":
            table[col.name] = pd.to_numeric(table[col.name], errors="raise").astype(float)
    return table


def validate_table(
    df: pd.DataFrame,
    spec: TableSpec,
    *,
    keys: Iterable[str] = (),
    non_negative: Iterable[str] = (),
    positive: Iterable[str] = (),
    index_col: Optional[str] = None,
) -> pd.DataFrame:
    if index_col and index_col not in df.columns and df.index.name == index_col:
        df = df.reset_index()
    table = normalize_dataframe(df, spec, copy=True)
    if keys:
        validate_unique_keys(table, list(keys), spec.name)
    if non_negative:
        validate_non_negative(table, list(non_negative), spec.name)
    for column in positive:
        if column not in table.columns:
            continue
        bad = table[pd.to_numeric(table[column], errors="coerce") <= 0]
        if not bad.empty:
            raise ValueError(f"{spec.name} column '{column}' must be > 0.")
    if index_col:
        return table.set_index(index_col, drop=True)
    return table.reset_index(drop=True)


def validate_non_negative(df: pd.DataFrame, columns: Iterable[str], table_name: str) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            continue
        bad = df[pd.to_numeric(df[col], errors="coerce") < 0]
        if not bad.empty:
            raise ValueError(f"{table_name} column '{col}' contains negative values.")
    return df


def validate_unique_keys(df: pd.DataFrame, keys: Iterable[str], table_name: str) -> pd.DataFrame:
    from data_loaders.helpers.model_factory import validation_mode

    mode = validation_mode()
    if mode == "off":
        return df

    key_cols = [col for col in keys if col in df.columns]
    if not key_cols:
        return df

    # Large time-series duplicate scans are one of the biggest ingestion
    # bottlenecks. Keep static/small-table uniqueness in fast mode, but
    # reserve period/year keyed checks for strict validation.
    if mode == "fast" and {"period", "year"}.issubset(set(key_cols)):
        return df

    dupes = df.duplicated(subset=key_cols, keep=False)
    if dupes.any():
        raise ValueError(
            f"{table_name} contains duplicate keys for columns {key_cols}: "
            f"{df.loc[dupes, key_cols].drop_duplicates().to_dict('records')}"
        )
    return df


def dataframe_to_multiindex_dict(df: pd.DataFrame, value_column: str) -> Dict[tuple[Any, ...], Any]:
    if value_column not in df.columns:
        return {}
    required = [idx for idx in df.index.names if idx is not None]
    if not required:
        raise ValueError("DataFrame must use a named index before exporting to dict.")
    series = df[value_column].dropna()
    out: Dict[tuple[Any, ...], Any] = {}
    for idx, value in series.items():
        key = idx if isinstance(idx, tuple) else (idx,)
        out[tuple(key)] = value
    return out
