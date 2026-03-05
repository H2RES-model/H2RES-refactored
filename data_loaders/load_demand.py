"""Demand loader for electricity, heating, and cooling time series."""

from __future__ import annotations

from typing import Dict, Optional, Tuple, List
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Demand import Demand
from data_models.Bus import Bus
from data_loaders.helpers.io import TableCache, read_table
from data_loaders.helpers.model_factory import build_model

Key = Tuple[str, str, str, str, int, int]  # (system, region, bus, carrier, period, year)


def load_demand(
    *,
    sets: SystemSets,
    electricity_path: Optional[str],
    heating_path: Optional[str] = None,
    cooling_path: Optional[str] = None,
    buses: Optional[Bus] = None,
    existing_demand: Optional[Demand] = None,
    table_cache: Optional[TableCache] = None,
) -> Demand:
    """Load demand time series and aggregate into a Demand model.

    When used: called by `load_sector` to populate sector demand inputs.

    Args:
        sets: SystemSets containing modeled years and periods.
        electricity_path: Path to electricity demand file.
        heating_path: Optional path to heating demand file.
        cooling_path: Optional path to cooling demand file.
        buses: Optional Bus model for validating demand columns against buses.
        existing_demand: Existing Demand to merge into (existing wins).

    Returns:
        Demand model with time series keyed by (system, region, bus, carrier, period, year).

    Raises:
        ValueError: If required columns are missing, buses are unknown, or demand is negative.

    Notes:
        Demand columns are interpreted as bus identifiers when a Bus model is provided.
    """

    # --------------------------------------------------------------
    # 1. Build bus lookup (system, region, carrier) from Bus model
    # --------------------------------------------------------------
    default_bus = sets.buses[0] if getattr(sets, "buses", None) else "SystemBus"
    default_carrier = sets.carriers[0] if getattr(sets, "carriers", None) else "electricity"
    bus_lookup: Dict[str, Tuple[str, str, str, str]] = {}  # lower_bus -> (system, region, bus_id, carrier)

    if buses is not None:
        for bus_id in buses.name:
            b_lower = str(bus_id).lower()
            bus_lookup[b_lower] = (
                str(buses.system.get(bus_id, "")),
                str(buses.region.get(bus_id, "")),
                str(bus_id),
                str(buses.carrier.get(bus_id, default_carrier)),
            )

    def bus_info(column_name: str, carrier_hint: str) -> Tuple[str, str, str, str]:
        """
        Return (system, region, bus_id, carrier) for a demand column.
        If Bus model is provided, the column name must match a known bus (case-insensitive),
        otherwise a ValueError is raised. If no Bus model is provided, fall back to defaults.
        """
        c_lower = str(carrier_hint).lower() if carrier_hint else default_carrier.lower()
        col_lower = str(column_name).lower()

        if bus_lookup:
            if col_lower not in bus_lookup:
                raise ValueError(
                    f"Demand column '{column_name}' references unknown bus. Add it to buses.csv first."
                )
            sys, reg, bus_id, carrier = bus_lookup[col_lower]
            # enforce carrier consistency
            if carrier.lower() != c_lower:
                raise ValueError(
                    f"Carrier mismatch for demand column '{column_name}': "
                    f"bus carrier is '{carrier}', demand file carrier is '{carrier_hint}'."
                )
            return sys, reg, bus_id, carrier

        # Fallback when no Bus object is provided
        return "", "", column_name, carrier_hint or default_carrier

    # --------------------------------------------------------------
    # 2. Helper to read one CSV for one carrier
    # --------------------------------------------------------------
    def _read_single(path: Optional[str], carrier: str) -> Dict[Key, float]:
        if path is None:
            return {}

        df = read_table(path, cache=table_cache, mutable=True)

        # required columns
        for col in ("year", "period"):
            if col not in df.columns:
                raise ValueError(
                    f"{path} missing required column '{col}'"
                )

        # convert year/period to int
        try:
            df["year"] = df["year"].astype(int)
            df["period"] = df["period"].astype(int)
        except Exception:
            raise ValueError(f"{path} has non-integer year/period values.")

        # horizon filter
        df = df[df["year"].isin(sets.years) & df["period"].isin(sets.periods)]
        if df.empty:
            return {}

        id_vars = ["year", "period"]
        value_cols = [c for c in df.columns if c not in id_vars]
        if not value_cols:
            return {}

        for col in value_cols:
            df[col] = pd.to_numeric(df[col], errors="raise")

        df_long = df.melt(
            id_vars=id_vars,
            value_vars=value_cols,
            var_name="bus_col",
            value_name="demand",
        ).dropna(subset=["demand"])
        if df_long.empty:
            return {}

        demand_arr = df_long["demand"].to_numpy(dtype=float)
        if (demand_arr < 0).any():
            raise ValueError(f"Negative demand in {path} for {carrier}")

        bus_meta = {
            col: bus_info(col, carrier)
            for col in value_cols
        }
        meta_df = pd.DataFrame.from_dict(
            bus_meta, orient="index", columns=["system", "region", "bus_id", "carrier_val"]
        )
        meta_df.index.name = "bus_col"
        df_long = df_long.join(meta_df, on="bus_col", how="left")

        years = df_long["year"].to_numpy(dtype=int)
        periods = df_long["period"].to_numpy(dtype=int)
        systems = df_long["system"].to_numpy(dtype=object)
        regions = df_long["region"].to_numpy(dtype=object)
        buses_arr = df_long["bus_id"].to_numpy(dtype=object)
        carriers = df_long["carrier_val"].to_numpy(dtype=object)

        keys = zip(systems, regions, buses_arr, carriers, periods, years)
        out: Dict[Key, float] = {
            (str(sys), str(reg), str(bus_id), str(carrier_val), int(period), int(year)): float(val)
            for (sys, reg, bus_id, carrier_val, period, year), val in zip(keys, demand_arr)
        }

        return out

    # --------------------------------------------------------------
    # 3. Build final merged dictionary
    # --------------------------------------------------------------
    p_t: Dict[Key, float] = dict(existing_demand.p_t) if existing_demand else {}

    for carrier, path in (
        ("electricity", electricity_path),
        ("heat", heating_path),
        ("cooling", cooling_path),
    ):
        part = _read_single(path, carrier)
        for key, val in part.items():
            p_t[key] = p_t.get(key, 0.0) + val  # summation logic retained

    return build_model(Demand, p_t=p_t)
