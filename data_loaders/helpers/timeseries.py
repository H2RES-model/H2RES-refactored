"""Shared helpers for canonical time-series tables."""

from __future__ import annotations

from typing import Iterable, Optional, Sequence, cast

import pandas as pd

from data_models.SystemSets import SystemSets
from data_loaders.helpers.io import TableCache, inspect_table, read_table

TIME_COLS = ["year", "period"]


def empty_frame(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def empty_timeseries(value_name: str) -> pd.DataFrame:
    return empty_frame(["unit", "period", "year", value_name])


def normalize_string_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
    return out


def require_time_columns(df: pd.DataFrame, path_label: str) -> None:
    for col in TIME_COLS:
        if col not in df.columns:
            raise ValueError(f"{path_label} missing column '{col}'")


def coerce_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["year"] = pd.to_numeric(out["year"], errors="raise").astype(int)
    out["period"] = pd.to_numeric(out["period"], errors="raise").astype(int)
    return out


def filter_modeled_horizon(df: pd.DataFrame, sets: SystemSets) -> pd.DataFrame:
    out = coerce_time_columns(df)
    return out[out["year"].isin(sets.years) & out["period"].isin(sets.periods)]


def stack_compat(df: pd.DataFrame, id_vars: Sequence[str], value_cols: Sequence[str]) -> pd.Series:
    indexed = df.set_index(list(id_vars))[list(value_cols)]
    try:
        stacked = indexed.stack(future_stack=True)
    except TypeError:
        stacked = indexed.stack(dropna=True)
    return cast(pd.Series, stacked)


def melt_wide_timeseries(
    df: pd.DataFrame,
    *,
    key_name: str,
    value_name: str,
    value_columns: Sequence[str],
) -> pd.DataFrame:
    if not value_columns:
        return empty_frame([key_name, "period", "year", value_name])
    stacked = stack_compat(df, TIME_COLS, value_columns).reset_index()
    stacked.columns = ["year", "period", key_name, value_name]
    stacked[key_name] = stacked[key_name].astype(str)
    return stacked[[key_name, "period", "year", value_name]].reset_index(drop=True)


def merge_keyed_frames(
    existing: pd.DataFrame,
    new: pd.DataFrame,
    *,
    keys: Sequence[str],
) -> pd.DataFrame:
    parts = [frame for frame in (existing, new) if not frame.empty]
    if not parts:
        columns = existing.columns if len(existing.columns) >= len(new.columns) else new.columns
        return empty_frame(list(columns))
    return (
        pd.concat(parts, ignore_index=True)
        .drop_duplicates(subset=list(keys), keep="first")
        .reset_index(drop=True)
    )


def load_wide_timeseries(
    *,
    path: Optional[str],
    sets: SystemSets,
    units: Sequence[str],
    value_name: str,
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    if path is None:
        return empty_timeseries(value_name)

    inspection = inspect_table(path, cache=table_cache)
    available_cols = set(inspection.columns)
    value_cols = [unit for unit in units if unit in available_cols]
    if not value_cols:
        return empty_timeseries(value_name)
    df = read_table(path, columns=TIME_COLS + value_cols, cache=table_cache)
    require_time_columns(df, path)
    df = filter_modeled_horizon(df, sets)
    if df.empty:
        return empty_timeseries(value_name)

    return melt_wide_timeseries(
        df,
        key_name="unit",
        value_name=value_name,
        value_columns=value_cols,
    )


def load_long_timeseries(
    *,
    path: Optional[str],
    sets: SystemSets,
    units: Iterable[str],
    value_name: str,
    value_columns: Sequence[str],
    unit_columns: Sequence[str] = ("unit", "unit_name"),
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    if path is None:
        return empty_timeseries(value_name)

    inspection = inspect_table(path, cache=table_cache)
    available_cols = set(inspection.columns)
    unit_col = next((col for col in unit_columns if col in available_cols), None)
    if unit_col is None:
        raise ValueError(f"{path} must include one of {list(unit_columns)}")

    value_col = next((col for col in value_columns if col in available_cols), None)
    if value_col is None:
        raise ValueError(f"{path} must include one of {list(value_columns)}")

    df = read_table(path, columns=[unit_col, value_col, *TIME_COLS], cache=table_cache)
    require_time_columns(df, path)
    df = df.dropna(subset=[unit_col, value_col, "year", "period"]).copy()
    if df.empty:
        return empty_timeseries(value_name)

    df = normalize_string_columns(df, [unit_col])
    df[value_col] = pd.to_numeric(df[value_col], errors="raise")
    df = filter_modeled_horizon(df, sets)
    if df.empty:
        return empty_timeseries(value_name)

    unknown_units = set(df[unit_col]) - {str(unit) for unit in units}
    if unknown_units:
        raise ValueError(f"{path} contains unknown units: {sorted(unknown_units)}")

    out = df[[unit_col, "period", "year", value_col]].copy()
    out.rename(columns={unit_col: "unit", value_col: value_name}, inplace=True)
    out["unit"] = out["unit"].astype(str)
    return out[["unit", "period", "year", value_name]].reset_index(drop=True)
