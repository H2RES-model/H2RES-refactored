"""Demand loader for electricity, heating, and cooling time series."""

from __future__ import annotations

from typing import Dict, Optional, Tuple
from collections import defaultdict
import os
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Demand import Demand
from data_models.Bus import Bus
from data_loaders.helpers.io import read_table
from data_loaders.helpers.transport_utils import load_ev_inputs, _is_electric_transport_tech

Key = Tuple[str, str, str, str, int, int]  # (system, region, bus, carrier, period, year)


def load_demand(
    *,
    sets: SystemSets,
    electricity_path: Optional[str] = None,
    heating_path: Optional[str] = None,
    cooling_path: Optional[str] = None,
    industry_path: Optional[str] = None,
    transport_demand_path: Optional[str] = None,
    transport_general_params_path: Optional[str] = None,
    transport_zones_path: Optional[str] = None,
    buses: Optional[Bus] = None,
    buses_path: Optional[str] = None,
    existing_demand: Optional[Demand] = None,
) -> Demand:
    """Load demand time series and aggregate into a Demand model.

    When used: called by `load_sector` to populate sector demand inputs.

    Args:
        sets: SystemSets containing modeled years and periods.
        electricity_path: Path to electricity demand file.
        heating_path: Optional path to heating demand file.
        cooling_path: Optional path to cooling demand file.
        transport_demand_path: Optional path to transport demand time series.
        transport_general_params_path: Path to general transport parameters.
        transport_zones_path: Path to transport zones modeling input.
        buses: Optional Bus model for validating demand columns against buses.
        buses_path: Optional buses.csv path (authoritative bus list and carriers).
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

    if buses_path:
        df_buses = read_table(buses_path)
        required_buses_cols = {"bus", "carrier"}
        missing_buses_cols = required_buses_cols - set(df_buses.columns)
        if missing_buses_cols:
            raise ValueError(f"{buses_path} missing required columns: {sorted(missing_buses_cols)}")
        for _, row in df_buses.iterrows():
            bus_id = str(row["bus"]).strip()
            if not bus_id:
                continue
            bus_lookup[bus_id.lower()] = (
                str(row["system"]).strip() if "system" in df_buses.columns and pd.notna(row.get("system")) else "",
                str(row["region"]).strip() if "region" in df_buses.columns and pd.notna(row.get("region")) else "",
                bus_id,
                str(row.get("carrier", default_carrier)).strip() or default_carrier,
            )

    def bus_info(column_name: str, carrier_hint: Optional[str]) -> Tuple[str, str, str, str]:
        """
        Return (system, region, bus_id, carrier) for a demand column.
        If Bus model is provided, the column name must match a known bus (case-insensitive),
        otherwise a ValueError is raised. If no Bus model is provided, fall back to defaults.
        """
        c_lower = str(carrier_hint).lower() if carrier_hint else None
        col_lower = str(column_name).lower()

        if bus_lookup:
            if col_lower not in bus_lookup:
                raise ValueError(
                    f"Demand column '{column_name}' references unknown bus. Add it to buses.csv first."
                )
            sys, reg, bus_id, carrier = bus_lookup[col_lower]
            # enforce carrier consistency
            if c_lower and carrier.lower() != c_lower:
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

        df = read_table(path)

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

        # collect demand columns
        id_vars = ["year", "period"]
        value_cols = [c for c in df.columns if c not in id_vars]
        if not value_cols:
            return {}

        # numeric conversion
        for col in value_cols:
            df[col] = pd.to_numeric(df[col], errors="raise")

        out: Dict[Key, float] = {}

        # Assign each demand column to its own bus (lookup from buses.csv)
        for col in value_cols:
            series = df[["year", "period", col]].dropna(subset=[col])
            if series.empty:
                continue
            if (series[col] < 0).any():
                raise ValueError(f"Negative demand in {path} for {carrier}")
            sys, reg, bus_id, carrier_val = bus_info(col, carrier)
            years = series["year"].astype(int).to_numpy()
            periods = series["period"].astype(int).to_numpy()
            values = series[col].astype(float).to_numpy()

            for y, p, val in zip(years, periods, values):
                out[(sys, reg, bus_id, carrier_val, p, y)] = val

        return out

    # --------------------------------------------------------------
    # 3. Build final merged dictionary
    # --------------------------------------------------------------
    p_t: Dict[Key, float] = dict(existing_demand.p_t) if existing_demand else {}

    for carrier, path in (
        ("electricity", electricity_path),
        ("heat", heating_path),
        ("cooling", cooling_path),
        ("industry_heat", industry_path),
    ):
        part = _read_single(path, carrier)
        for key, val in part.items():
            p_t[key] = p_t.get(key, 0.0) + val  # summation logic retained

    # Transport demand: add EV transport load to electricity demand at bus_in
    if transport_demand_path:
        if transport_general_params_path is None:
            transport_general_params_path = os.path.join(
                "data", "transport", "transport_general_parameters.xlsx"
            )
        if transport_zones_path is None:
            raise ValueError(
                "transport_zones_path is required when transport_demand_path is provided."
            )

        params_df, _ev_availability, ev_demand_profile = load_ev_inputs(
            general_params_path=transport_general_params_path,
            zones_params_path=transport_zones_path,
            ev_demand_path=transport_demand_path,
        )

        # Group TS once per unit for fast lookup inside the per-unit loop.
        profile_by_unit: Dict[str, Dict[Tuple[int, int], float]] = defaultdict(dict)
        for (u, p, y), v in ev_demand_profile.items():
            if int(y) in sets.years and int(p) in sets.periods:
                profile_by_unit[str(u)][(int(p), int(y))] = float(v)

        # Only electric transport techs contribute to electricity demand.
        params_electric = params_df[params_df["tech"].map(_is_electric_transport_tech)].copy()
        for _, row in params_electric.iterrows():
            unit = str(row["name"])
            if "ev_demand_unit" not in row:
                raise ValueError(
                    "Transport params are missing 'ev_demand_unit'. "
                    "Check that transport_demand_path is provided and matches transport sectors."
                )
            unit_ev_demand = float(row["ev_demand_unit"])
            bus_in = str(row.get("bus_in"))

            if bus_lookup and str(bus_in).strip().lower() not in bus_lookup:
                excel_row = int(row.name) + 2 if isinstance(row.name, (int, float)) else None
                line_txt = f", row {excel_row}" if excel_row is not None else ""
                raise ValueError(
                    f"{transport_zones_path}{line_txt}: bus_in '{bus_in}' not found in data/buses.csv"
                )

            # Use the carrier declared at the bus to avoid forcing electricity
            # on non-electric transport buses.
            sys, reg, bus_id, carrier_val = bus_info(bus_in, None)
            for (p, y), profile in profile_by_unit.get(unit, {}).items():
                demand_val = float(profile) * unit_ev_demand
                if demand_val < 0:
                    raise ValueError(
                        f"Negative transport demand for '{unit}' at ({p},{y}): {demand_val}"
                    )
                key = (sys, reg, bus_id, carrier_val, int(p), int(y))
                p_t[key] = p_t.get(key, 0.0) + demand_val

    return Demand(p_t=p_t)
