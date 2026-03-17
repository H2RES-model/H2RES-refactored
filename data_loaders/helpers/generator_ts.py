from __future__ import annotations

from typing import Dict, List, Optional
import math

import numpy as np
import pandas as pd

from data_models.Bus import Bus
from data_models.SystemSets import SystemSets
from data_loaders.helpers.io import TableCache, inspect_table, read_table
from data_loaders.helpers.timeseries import (
    empty_timeseries,
    filter_modeled_horizon,
    load_long_timeseries,
    load_wide_timeseries,
    require_time_columns,
)


def load_profile_ts(
    *,
    path: Optional[str],
    sets: SystemSets,
    units: List[str],
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    return load_wide_timeseries(
        path=path,
        sets=sets,
        units=units,
        value_name="p_t",
        table_cache=table_cache,
    )


def load_var_cost_ts(
    *,
    path: Optional[str],
    sets: SystemSets,
    units: List[str],
    fuel: Optional[Dict[str, str]],
    var_cost_no_fuel: Optional[Dict[str, float]],
    efficiency: Optional[Dict[str, float]],
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    if path is None:
        return empty_timeseries("var_cost")
    if fuel is None or var_cost_no_fuel is None or efficiency is None:
        raise ValueError("fuel, var_cost_no_fuel, and efficiency are required for fuel cost loading.")

    fuel_costs = _read_fuel_cost_table(path, sets=sets, units=units, fuel=fuel, table_cache=table_cache)
    unit_frame = _build_unit_fuel_frame(
        units,
        fuel=fuel,
        var_cost_no_fuel=var_cost_no_fuel,
        efficiency=efficiency,
    )
    electric_units = unit_frame[unit_frame["fuel"].str.lower() == "electricity"].copy()
    fuel_units = unit_frame[unit_frame["fuel"].str.lower() != "electricity"].copy()

    frames = [
        _expand_fuel_var_costs(fuel_costs, fuel_units, path=path),
        _expand_electric_var_costs(fuel_costs, electric_units),
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return empty_timeseries("var_cost")
    return pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["unit", "period", "year"],
        keep="first",
    ).reset_index(drop=True)


def _read_fuel_cost_table(
    path: str,
    *,
    sets: SystemSets,
    units: List[str],
    fuel: Dict[str, str],
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    available_columns = list(inspect_table(path, cache=table_cache).columns)
    column_map = {str(column).strip().lower(): str(column) for column in available_columns}
    used_fuels = {str(fuel[unit]).strip().lower() for unit in units}
    columns = ["year", "period"] + [column_map[name] for name in used_fuels if name in column_map]
    table = read_table(path, columns=columns, cache=table_cache)
    require_time_columns(table, path)
    return filter_modeled_horizon(table, sets)


def _build_unit_fuel_frame(
    units: List[str],
    *,
    fuel: Dict[str, str],
    var_cost_no_fuel: Dict[str, float],
    efficiency: Dict[str, float],
) -> pd.DataFrame:
    unit_frame = pd.DataFrame(
        {
            "unit": units,
            "fuel": [str(fuel[unit]).strip() for unit in units],
            "var_cost_no_fuel": [float(var_cost_no_fuel[unit]) for unit in units],
            "efficiency": [float(efficiency[unit]) for unit in units],
        }
    )
    invalid = unit_frame.loc[
        ~unit_frame["efficiency"].map(lambda value: math.isfinite(value) and value > 0),
        "unit",
    ].astype(str).tolist()
    if invalid:
        raise ValueError(f"Non-positive efficiency for units: {sorted(invalid)}")
    return unit_frame


def _expand_fuel_var_costs(
    fuel_costs: pd.DataFrame,
    unit_frame: pd.DataFrame,
    *,
    path: str,
) -> pd.DataFrame:
    fuel_columns = [column for column in fuel_costs.columns if column not in {"year", "period"} and column.lower() != "electricity"]
    if unit_frame.empty:
        return empty_timeseries("var_cost")
    if not fuel_columns:
        raise ValueError(f"{path} has no fuel columns.")

    missing: list[str] = []
    for fuel_name in fuel_columns:
        if fuel_costs[fuel_name].isna().any():
            missing.extend(unit_frame.loc[unit_frame["fuel"] == fuel_name, "unit"].astype(str).tolist())
    if missing:
        raise ValueError(f"Missing fuel price for units: {sorted(dict.fromkeys(missing))}")

    long_costs = fuel_costs[["year", "period", *fuel_columns]].melt(
        id_vars=["year", "period"],
        var_name="fuel",
        value_name="fuel_cost",
    ).dropna(subset=["fuel_cost"])
    merged = long_costs.merge(unit_frame, on="fuel", how="inner")
    if merged.empty:
        return empty_timeseries("var_cost")
    merged["var_cost"] = merged["var_cost_no_fuel"] + merged["fuel_cost"].astype(float) / merged["efficiency"]
    return merged[["unit", "period", "year", "var_cost"]].reset_index(drop=True)


def _expand_electric_var_costs(
    fuel_costs: pd.DataFrame,
    electric_units: pd.DataFrame,
) -> pd.DataFrame:
    if electric_units.empty or fuel_costs.empty:
        return empty_timeseries("var_cost")

    years = fuel_costs["year"].to_numpy(dtype=int)
    periods = fuel_costs["period"].to_numpy(dtype=int)
    n_time = len(years)
    if n_time == 0:
        return empty_timeseries("var_cost")

    return pd.DataFrame(
        {
            "unit": np.repeat(electric_units["unit"].astype(str).to_numpy(dtype=object), n_time).astype(str),
            "period": np.tile(periods, len(electric_units)).astype(int),
            "year": np.tile(years, len(electric_units)).astype(int),
            "var_cost": np.repeat(electric_units["var_cost_no_fuel"].to_numpy(dtype=float), n_time).astype(float),
        }
    )


def load_efficiency_ts(
    *,
    path: Optional[str],
    sets: SystemSets,
    units: List[str],
    buses: Optional[Bus] = None,
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    if path is None:
        return empty_timeseries("efficiency_ts")

    columns = set(inspect_table(path, cache=table_cache).columns)
    if "unit" in columns or "unit_name" in columns:
        df = load_long_timeseries(
            path=path,
            sets=sets,
            units=units,
            value_name="efficiency_ts",
            value_columns=("efficiency", "value"),
            table_cache=table_cache,
        )
        if df.empty or buses is None or "bus" not in columns or not getattr(buses, "name", None):
            return df
        raw_df = read_table(path, cache=table_cache)
        known_buses = set(map(str, buses.name))
        unknown_buses = set(raw_df["bus"].dropna().astype(str)) - known_buses
        if unknown_buses:
            raise ValueError(f"{path} contains unknown buses: {sorted(unknown_buses)}")
        return df

    return load_wide_timeseries(
        path=path,
        sets=sets,
        units=units,
        value_name="efficiency_ts",
        table_cache=table_cache,
    )
