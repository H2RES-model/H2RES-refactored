"""Demand loader for electricity, heating, and cooling time series."""

from __future__ import annotations

from typing import Dict, Optional, Tuple, List
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Demand import Demand
from data_models.Bus import Bus
from data_loaders.helpers.io import read_table

Key = Tuple[str, str, str, str, int, int]  # (system, region, bus, carrier, period, year)


def load_demand(
    *,
    sets: SystemSets,
    electricity_path: str,
    heating_path: Optional[str] = None,
    cooling_path: Optional[str] = None,
    buses: Optional[Bus] = None,
    existing_demand: Optional[Demand] = None,
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
    ):
        part = _read_single(path, carrier)
        for key, val in part.items():
            p_t[key] = p_t.get(key, 0.0) + val  # summation logic retained

    return Demand(p_t=p_t)
