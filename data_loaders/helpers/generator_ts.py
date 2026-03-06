from __future__ import annotations

from typing import Dict, List, Optional
import math

import numpy as np
import pandas as pd

from data_models.Bus import Bus
from data_models.SystemSets import SystemSets
from data_loaders.helpers.io import TableCache, read_columns, read_table, resolve_table_path
from data_loaders.helpers.pandas_utils import stack_compat


def _find_header_row(df_raw: pd.DataFrame) -> Optional[int]:
    for idx in range(len(df_raw)):
        if df_raw.shape[1] < 2:
            return None
        first = str(df_raw.iat[idx, 0]).strip().lower()
        second = str(df_raw.iat[idx, 1]).strip().lower()
        if first == "year" and second == "period":
            return idx
    return None


def _parse_meta_rows(df_meta: pd.DataFrame, columns: list) -> Dict[str, Dict[str, str]]:
    meta: Dict[str, Dict[str, str]] = {}
    col_names = ["" if pd.isna(c) else str(c).strip() for c in columns]
    for row in df_meta.itertuples(index=False, name=None):
        label = None
        label_idx = None
        for i, cell in enumerate(row):
            if pd.notna(cell) and str(cell).strip() != "":
                label = str(cell).strip().lower()
                label_idx = i
                break
        if label is None or label_idx is None or label not in {"system", "region", "zone", "bus"}:
            continue
        mapping: Dict[str, str] = {}
        for j in range(label_idx + 1, len(col_names)):
            unit_name = col_names[j]
            if unit_name in {"year", "period", ""}:
                continue
            cell_val = row[j]
            if pd.notna(cell_val) and str(cell_val).strip() != "":
                mapping[unit_name] = str(cell_val).strip()
        if mapping:
            meta[label] = mapping
    return meta


def load_profile_ts(
    *,
    path: Optional[str],
    sets: SystemSets,
    units: List[str],
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=["unit", "period", "year", "p_t"])

    profile_cols_available = set(read_columns(path, cache=table_cache))
    requested_profile_cols = ["year", "period"] + [u for u in units if u in profile_cols_available]
    df_ts = read_table(path, columns=requested_profile_cols, cache=table_cache)
    for col in ("year", "period"):
        if col not in df_ts.columns:
            raise ValueError(f"{path} missing column '{col}'")

    df_ts = df_ts[df_ts["year"].isin(sets.years) & df_ts["period"].isin(sets.periods)]
    profile_cols = [c for c in df_ts.columns if c in units]
    if not profile_cols:
        return pd.DataFrame(columns=["unit", "period", "year", "p_t"])

    stacked = stack_compat(df_ts, ["year", "period"], profile_cols)
    profile_df = stacked.reset_index()
    profile_df.columns = ["year", "period", "unit", "p_t"]
    profile_df["unit"] = profile_df["unit"].astype(str)
    return profile_df[["unit", "period", "year", "p_t"]].reset_index(drop=True)


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
        return pd.DataFrame(columns=["unit", "period", "year", "var_cost"])
    if fuel is None or var_cost_no_fuel is None or efficiency is None:
        raise ValueError("fuel, var_cost_no_fuel, and efficiency are required for fuel cost loading.")

    fc_cols_list = read_columns(path, cache=table_cache)
    fc_col_by_lower = {str(c).strip().lower(): str(c) for c in fc_cols_list}
    used_fuels = {str(fuel[u]).strip().lower() for u in units}
    requested_fc_cols = ["year", "period"] + [fc_col_by_lower[f] for f in used_fuels if f in fc_col_by_lower]
    df_fc = read_table(path, columns=requested_fc_cols, cache=table_cache)
    for col in ("year", "period"):
        if col not in df_fc.columns:
            raise ValueError(f"{path} missing column '{col}'")

    df_fc = df_fc[df_fc["year"].isin(sets.years) & df_fc["period"].isin(sets.periods)]
    fuel_cols = [c for c in df_fc.columns if c not in {"year", "period"}]
    if not fuel_cols:
        all_electric_units = all(str(fuel[u]).strip().lower() == "electricity" for u in units)
        if not all_electric_units:
            raise ValueError(f"{path} has no fuel columns.")

    unit_fuel = {u: str(fuel[u]) for u in units}
    unit_var_cost_no_fuel = {u: float(var_cost_no_fuel[u]) for u in units}
    unit_efficiency = {u: float(efficiency[u]) for u in units}
    invalid_eff = [u for u in units if not math.isfinite(unit_efficiency[u]) or unit_efficiency[u] <= 0]
    if invalid_eff:
        raise ValueError(f"Non-positive efficiency for units: {sorted(invalid_eff)}")

    df_units = pd.DataFrame(
        {
            "unit": units,
            "fuel": [unit_fuel[u] for u in units],
            "var_cost_no_fuel": [unit_var_cost_no_fuel[u] for u in units],
            "efficiency": [unit_efficiency[u] for u in units],
        }
    )
    elec_units = df_units[df_units["fuel"].str.lower() == "electricity"]["unit"].tolist()
    df_units = df_units[df_units["fuel"].str.lower() != "electricity"]

    missing_units: List[str] = []
    missing_unit_set = set()
    for fuel_name in fuel_cols:
        if fuel_name.lower() == "electricity":
            continue
        if df_fc[fuel_name].isna().any():
            units_for_fuel = df_units[df_units["fuel"] == fuel_name]["unit"].tolist()
            for unit in units_for_fuel:
                if unit not in missing_unit_set:
                    missing_unit_set.add(unit)
                    missing_units.append(unit)
    if missing_units:
        raise ValueError(f"Missing fuel price for units: {missing_units}")

    var_cost_frames: list[pd.DataFrame] = []
    fuel_cols_no_elec = [c for c in fuel_cols if c.lower() != "electricity"]
    if fuel_cols_no_elec and not df_units.empty:
        df_long = df_fc[["year", "period"] + fuel_cols_no_elec].melt(
            id_vars=["year", "period"],
            var_name="fuel",
            value_name="fuel_cost",
        )
        df_long = df_long.dropna(subset=["fuel_cost"])
        df_merge = df_long.merge(df_units, on="fuel", how="inner")
        df_merge["var_cost"] = df_merge["var_cost_no_fuel"] + (df_merge["fuel_cost"].astype(float) / df_merge["efficiency"])
        var_cost_frames.append(df_merge[["unit", "period", "year", "var_cost"]].copy())

    if elec_units:
        years = df_fc["year"].astype(int).to_numpy()
        periods = df_fc["period"].astype(int).to_numpy()
        n_t = len(years)
        n_u = len(elec_units)
        if n_t > 0 and n_u > 0:
            var_cost_frames.append(
                pd.DataFrame(
                    {
                        "unit": np.repeat(np.asarray(elec_units, dtype=object), n_t).astype(str),
                        "period": np.tile(periods, n_u).astype(int),
                        "year": np.tile(years, n_u).astype(int),
                        "var_cost": np.repeat(
                            np.asarray([unit_var_cost_no_fuel[u] for u in elec_units], dtype=float),
                            n_t,
                        ).astype(float),
                    }
                )
            )

    if not var_cost_frames:
        return pd.DataFrame(columns=["unit", "period", "year", "var_cost"])
    return (
        pd.concat(var_cost_frames, ignore_index=True)
        .drop_duplicates(subset=["unit", "period", "year"], keep="first")
        .reset_index(drop=True)
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
        return pd.DataFrame(columns=["unit", "period", "year", "efficiency_ts"])

    resolved = resolve_table_path(path)
    meta: Dict[str, Dict[str, str]] = {}
    if resolved.suffix.lower() == ".csv":
        df_raw = pd.read_csv(resolved, header=None)
        header_row = _find_header_row(df_raw)
        if header_row is None:
            df = pd.read_csv(resolved)
        else:
            header = df_raw.iloc[header_row].tolist()
            df = df_raw.iloc[header_row + 1 :].copy()
            df.columns = header
            meta = _parse_meta_rows(df_raw.iloc[:header_row], header)
    else:
        df = read_table(resolved, cache=table_cache)

    for col in ("year", "period"):
        if col not in df.columns:
            raise ValueError(f"{path} missing column '{col}'")

    unit_col = "unit_name" if "unit_name" in df.columns else "unit" if "unit" in df.columns else None
    if unit_col is not None:
        value_col = "efficiency" if "efficiency" in df.columns else "value" if "value" in df.columns else None
        if value_col is None:
            raise ValueError(f"{path} must include an 'efficiency' or 'value' column for long format.")
        df = df.dropna(subset=[unit_col, value_col, "year", "period"])
        df[unit_col] = df[unit_col].astype(str)
        df["year"] = df["year"].astype(int)
        df["period"] = df["period"].astype(int)
        df[value_col] = pd.to_numeric(df[value_col], errors="raise")
        df = df[df["year"].isin(sets.years) & df["period"].isin(sets.periods)]
        if df.empty:
            return pd.DataFrame(columns=["unit", "period", "year", "efficiency_ts"])
        unknown_units = set(df[unit_col]) - set(units)
        if unknown_units:
            raise ValueError(f"{path} contains unknown units: {sorted(unknown_units)}")
        if "bus" in df.columns and buses is not None and getattr(buses, "name", None):
            unknown_buses = set(df["bus"].dropna().astype(str)) - set(buses.name)
            if unknown_buses:
                raise ValueError(f"{path} contains unknown buses: {sorted(unknown_buses)}")
        out = df[[unit_col, "period", "year", value_col]].copy()
        out.rename(columns={unit_col: "unit", value_col: "efficiency_ts"}, inplace=True)
        out["unit"] = out["unit"].astype(str)
        return out[["unit", "period", "year", "efficiency_ts"]]

    df["year"] = df["year"].astype(int)
    df["period"] = df["period"].astype(int)
    df = df[df["year"].isin(sets.years) & df["period"].isin(sets.periods)]
    if df.empty:
        return pd.DataFrame(columns=["unit", "period", "year", "efficiency_ts"])

    unit_cols = [c for c in df.columns if c not in {"year", "period"} and pd.notna(c) and str(c).strip() != ""]
    if not unit_cols:
        return pd.DataFrame(columns=["unit", "period", "year", "efficiency_ts"])
    unknown_units = set(map(str, unit_cols)) - set(units)
    if unknown_units:
        raise ValueError(f"{path} contains unknown units: {sorted(unknown_units)}")
    if "bus" in meta and buses is not None and getattr(buses, "name", None):
        unknown_buses = set(meta["bus"].values()) - set(buses.name)
        if unknown_buses:
            raise ValueError(f"{path} contains unknown buses in metadata: {sorted(unknown_buses)}")

    stacked = stack_compat(df, ["year", "period"], unit_cols)
    out = stacked.reset_index()
    out.columns = ["year", "period", "unit", "efficiency_ts"]
    out["unit"] = out["unit"].astype(str)
    return out[["unit", "period", "year", "efficiency_ts"]]
