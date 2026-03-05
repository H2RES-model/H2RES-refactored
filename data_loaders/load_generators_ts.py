"""Generator time-series loader for profiles, costs, and efficiencies."""

from __future__ import annotations

from typing import Dict, Optional, Tuple, cast, List
import math
import numpy as np
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Bus import Bus
from data_loaders.helpers.io import TableCache, read_columns, read_table, resolve_table_path
from data_loaders.helpers.pandas_utils import stack_compat

UPY = Tuple[str, int, int]  # (unit, period, year)


def load_generators_ts(
    *,
    sets: SystemSets,
    units: List[str],
    renewable_profiles_path: Optional[str] = None,
    fuel_cost_path: Optional[str] = None,
    efficiency_ts_path: Optional[str] = None,
    fuel: Optional[Dict[str, str]] = None,
    var_cost_no_fuel: Optional[Dict[str, float]] = None,
    efficiency: Optional[Dict[str, float]] = None,
    buses: Optional[Bus] = None,
    table_cache: Optional[TableCache] = None,
) -> Dict[str, Dict[UPY, float]]:
    """Load generator time series for profiles, variable costs, and efficiencies.

    When used: called by `load_generators` to populate time-series inputs.

    Args:
        sets: SystemSets containing modeled years and periods.
        units: List of generator units to load time series for.
        renewable_profiles_path: Optional RES profile time series.
        fuel_cost_path: Optional fuel cost time series.
        efficiency_ts_path: Optional efficiency time series.
        fuel: Optional per-unit fuel mapping (required for fuel cost).
        var_cost_no_fuel: Optional per-unit non-fuel cost (required for fuel cost).
        efficiency: Optional per-unit efficiency (required for fuel cost).
        buses: Optional Bus model used to validate metadata.

    Returns:
        Dict with keys "p_t", "var_cost", "efficiency_ts" containing (unit, period, year) mappings.

    Raises:
        ValueError: If required columns are missing, units are unknown, or fuel costs are invalid.
    """

    def parse_efficiency_ts(path: str) -> Dict[UPY, float]:
        """Parse an efficiency time-series file into a per-unit mapping.

        Args:
            path: Path to the efficiency time-series file.

        Returns:
            Dict mapping (unit, period, year) to efficiency values.

        Raises:
            ValueError: If required columns are missing or units/buses are unknown.

        Notes:
            Supported formats:
                1) Wide format with columns [year, period, <unit_1>, <unit_2>, ...]
                2) Wide format with metadata rows above header (system/region/zone/bus)
                3) Long format with columns [year, period, unit or unit_name, efficiency or value]
        """

        def _find_header_row(df_raw: pd.DataFrame) -> Optional[int]:
            for idx in range(len(df_raw)):
                if df_raw.shape[1] < 2:
                    return None
                first = str(df_raw.iat[idx, 0]).strip().lower()
                second = str(df_raw.iat[idx, 1]).strip().lower()
                if first == "year" and second == "period":
                    return idx
            return None

        def _parse_meta_rows(
            df_meta: pd.DataFrame, columns: list
        ) -> Dict[str, Dict[str, str]]:
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
                if label is None or label_idx is None:
                    continue
                if label not in {"system", "region", "zone", "bus"}:
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
                raise ValueError(
                    f"{path} must include an 'efficiency' or 'value' column for long format."
                )

            df = df.dropna(subset=[unit_col, value_col, "year", "period"])
            df[unit_col] = df[unit_col].astype(str)
            df["year"] = df["year"].astype(int)
            df["period"] = df["period"].astype(int)
            df[value_col] = pd.to_numeric(df[value_col], errors="raise")

            df = df[df["year"].isin(sets.years) & df["period"].isin(sets.periods)]
            if df.empty:
                return {}

            unknown_units = set(df[unit_col]) - set(units)
            if unknown_units:
                raise ValueError(f"{path} contains unknown units: {sorted(unknown_units)}")

            if "bus" in df.columns and buses is not None and getattr(buses, "name", None):
                unknown_buses = set(df["bus"].dropna().astype(str)) - set(buses.name)
                if unknown_buses:
                    raise ValueError(f"{path} contains unknown buses: {sorted(unknown_buses)}")

            return cast(
                Dict[UPY, float],
                df.set_index([unit_col, "period", "year"])[value_col].astype(float).to_dict(),
            )

        df["year"] = df["year"].astype(int)
        df["period"] = df["period"].astype(int)
        df = df[df["year"].isin(sets.years) & df["period"].isin(sets.periods)]
        if df.empty:
            return {}

        id_vars = ["year", "period"]
        unit_cols = [
            c for c in df.columns
            if c not in id_vars and pd.notna(c) and str(c).strip() != ""
        ]
        if not unit_cols:
            return {}

        unknown_units = set(map(str, unit_cols)) - set(units)
        if unknown_units:
            raise ValueError(f"{path} contains unknown units: {sorted(unknown_units)}")

        if "bus" in meta and buses is not None and getattr(buses, "name", None):
            unknown_buses = set(meta["bus"].values()) - set(buses.name)
            if unknown_buses:
                raise ValueError(f"{path} contains unknown buses in metadata: {sorted(unknown_buses)}")

        stacked = stack_compat(df, id_vars, unit_cols)
        out = {}
        for idx, val in stacked.items():
            year, period, unit = idx  # type: ignore
            out[(str(unit), int(period), int(year))] = float(val)
        del stacked
        return out

    p_t: Dict[UPY, float] = {}
    if renewable_profiles_path is not None:
        profile_cols_available = set(read_columns(renewable_profiles_path, cache=table_cache))
        requested_profile_cols = ["year", "period"] + [u for u in units if u in profile_cols_available]
        df_ts = read_table(renewable_profiles_path, columns=requested_profile_cols, cache=table_cache)
        for col in ("year", "period"):
            if col not in df_ts.columns:
                raise ValueError(f"{renewable_profiles_path} missing column '{col}'")

        df_ts = df_ts[df_ts["year"].isin(sets.years) & df_ts["period"].isin(sets.periods)]

        profile_cols = [c for c in df_ts.columns if c in units]
        if profile_cols:
            stacked = stack_compat(df_ts, ["year", "period"], profile_cols)
            p_t = {}
            for idx, val in stacked.items():
                year, period, unit = idx  # type: ignore
                p_t[(str(unit), int(period), int(year))] = float(val)
            del stacked

    var_cost: Dict[UPY, float] = {}
    if fuel_cost_path is not None:
        if fuel is None or var_cost_no_fuel is None or efficiency is None:
            raise ValueError("fuel, var_cost_no_fuel, and efficiency are required for fuel cost loading.")

        fc_cols_list = read_columns(fuel_cost_path, cache=table_cache)
        fc_col_by_lower = {str(c).strip().lower(): str(c) for c in fc_cols_list}
        used_fuels = {str(fuel[u]).strip().lower() for u in units}
        requested_fc_cols = ["year", "period"] + [
            fc_col_by_lower[f] for f in used_fuels if f in fc_col_by_lower
        ]
        df_fc = read_table(fuel_cost_path, columns=requested_fc_cols, cache=table_cache)
        for col in ("year", "period"):
            if col not in df_fc.columns:
                raise ValueError(f"{fuel_cost_path} missing column '{col}'")

        df_fc = df_fc[df_fc["year"].isin(sets.years) & df_fc["period"].isin(sets.periods)]

        id_vars = ["year", "period"]
        fuel_cols = [c for c in df_fc.columns if c not in id_vars]
        if not fuel_cols:
            all_electric_units = all(
                str(fuel[u]).strip().lower() == "electricity"
                for u in units
            )
            if not all_electric_units:
                raise ValueError(f"{fuel_cost_path} has no fuel columns.")

        # Precompute per-unit inputs and fuel groupings.
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
                for u in units_for_fuel:
                    if u not in missing_unit_set:
                        missing_unit_set.add(u)
                        missing_units.append(u)
        if missing_units:
            raise ValueError(f"Missing fuel price for units: {missing_units}")

        var_cost = {}
        fuel_cols_no_elec = [c for c in fuel_cols if c.lower() != "electricity"]
        if fuel_cols_no_elec and not df_units.empty:
            df_long = df_fc[id_vars + fuel_cols_no_elec].melt(
                id_vars=id_vars, var_name="fuel", value_name="fuel_cost"
            )
            df_long = df_long.dropna(subset=["fuel_cost"])
            df_merge = df_long.merge(df_units, on="fuel", how="inner")
            df_merge["var_cost"] = df_merge["var_cost_no_fuel"] + (
                df_merge["fuel_cost"].astype(float) / df_merge["efficiency"]
            )
            var_cost.update(
                {
                    (str(u), int(p), int(y)): float(vc)
                    for u, p, y, vc in zip(
                        df_merge["unit"],
                        df_merge["period"],
                        df_merge["year"],
                        df_merge["var_cost"],
                    )
                }
            )
            del df_long, df_merge

        if elec_units:
            years = df_fc["year"].astype(int).to_numpy()
            periods = df_fc["period"].astype(int).to_numpy()
            n_t = len(years)
            n_u = len(elec_units)
            if n_t > 0 and n_u > 0:
                units_arr = np.repeat(np.asarray(elec_units, dtype=object), n_t)
                years_arr = np.tile(years, n_u)
                periods_arr = np.tile(periods, n_u)
                vc_arr = np.repeat(
                    np.asarray([unit_var_cost_no_fuel[u] for u in elec_units], dtype=float),
                    n_t,
                )
                var_cost.update(
                    {
                        (str(u), int(p), int(y)): float(vc)
                        for u, p, y, vc in zip(units_arr, periods_arr, years_arr, vc_arr)
                    }
                )

    efficiency_ts: Dict[UPY, float] = {}
    if efficiency_ts_path is not None:
        efficiency_ts = parse_efficiency_ts(efficiency_ts_path)

    return {
        "p_t": p_t,
        "var_cost": var_cost,
        "efficiency_ts": efficiency_ts,
    }
