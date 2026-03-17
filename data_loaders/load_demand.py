"""Demand loader for generic carrier demand time series."""

from __future__ import annotations

from typing import Dict, Optional, Tuple
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Demand import Demand
from data_models.Bus import Bus
from data_models.Transport import Transport
from data_loaders.helpers.io import TableCache, inspect_table, read_table
from data_loaders.helpers.model_factory import build_model
from data_loaders.helpers.timeseries import empty_frame, filter_modeled_horizon, melt_wide_timeseries, require_time_columns
from data_loaders.helpers.transport_integration import transport_to_system_demand

DEMAND_COLUMNS = ["system", "region", "bus", "carrier", "period", "year", "p_t"]


def load_demand(
    *,
    sets: SystemSets,
    carrier_paths: Optional[Dict[str, str]] = None,
    transport: Optional[Transport] = None,
    buses: Optional[Bus] = None,
    buses_path: Optional[str] = None,
    existing_demand: Optional[Demand] = None,
    table_cache: Optional[TableCache] = None,
) -> Demand:
    """Load demand time series and aggregate into a Demand model.

    When used: called by `load_sector` to populate sector demand inputs.

    Args:
        sets: SystemSets containing modeled years and periods.
        carrier_paths: Optional mapping of carrier -> demand file.
        transport: Optional canonical transport component.
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
    bus_lookup: Dict[str, Tuple[str, str, str, str]] = {}  # lower_bus -> (system, region, bus_id, carrier)

    if buses is not None:
        bus_static = buses.static.reset_index()
        for row in bus_static.itertuples(index=False):
            bus_id = str(getattr(row, "bus", "")).strip()
            if not bus_id:
                continue
            bus_lookup[bus_id.lower()] = (
                str(getattr(row, "system", "")).strip(),
                str(getattr(row, "region", "")).strip(),
                bus_id,
                str(getattr(row, "carrier", "")).strip(),
            )

    if buses_path:
        df_buses = read_table(buses_path, cache=table_cache)
        required_buses_cols = {"bus", "carrier"}
        missing_buses_cols = required_buses_cols - set(df_buses.columns)
        if missing_buses_cols:
            raise ValueError(f"{buses_path} missing required columns: {sorted(missing_buses_cols)}")
        for row in df_buses.itertuples(index=False):
            bus_id = str(getattr(row, "bus", "")).strip()
            if not bus_id:
                continue
            bus_lookup[bus_id.lower()] = (
                str(getattr(row, "system")).strip() if hasattr(row, "system") and pd.notna(getattr(row, "system")) else "",
                str(getattr(row, "region")).strip() if hasattr(row, "region") and pd.notna(getattr(row, "region")) else "",
                bus_id,
                str(getattr(row, "carrier", "")).strip(),
            )

    def bus_info(column_name: str, carrier_hint: Optional[str]) -> Tuple[str, str, str, str]:
        """
        Return (system, region, bus_id, carrier) for a demand column.
        If Bus model is provided, the column name must match a known bus
        (case-insensitive), otherwise a ValueError is raised.
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

        if not carrier_hint or not str(carrier_hint).strip():
            raise ValueError(f"Demand column '{column_name}' is missing carrier metadata.")
        return "", "", column_name, str(carrier_hint).strip()

    # --------------------------------------------------------------
    # 2. Helper to read one CSV for one carrier
    # --------------------------------------------------------------
    def _read_single(path: Optional[str], carrier: str) -> pd.DataFrame:
        if path is None:
            return empty_frame(DEMAND_COLUMNS)

        inspection = inspect_table(path, cache=table_cache)
        require_time_columns(pd.DataFrame(columns=inspection.columns), path)
        value_cols = [c for c in inspection.columns if c not in {"year", "period"}]
        if not value_cols:
            return empty_frame(DEMAND_COLUMNS)
        df = read_table(path, columns=["year", "period", *value_cols], cache=table_cache)
        require_time_columns(df, path)
        df = filter_modeled_horizon(df, sets)
        if df.empty:
            return empty_frame(DEMAND_COLUMNS)

        for col in value_cols:
            df[col] = pd.to_numeric(df[col], errors="raise")

        long_df = melt_wide_timeseries(
            df,
            key_name="bus_column",
            value_name="p_t",
            value_columns=value_cols,
        )
        if long_df.empty:
            return empty_frame(DEMAND_COLUMNS)
        if (long_df["p_t"] < 0).any():
            raise ValueError(f"Negative demand in {path} for {carrier}")

        meta_rows = []
        for col in value_cols:
            sys, reg, bus_id, carrier_val = bus_info(col, carrier)
            meta_rows.append(
                {
                    "bus_column": col,
                    "system": sys,
                    "region": reg,
                    "bus": bus_id,
                    "carrier": carrier_val,
                }
            )
        meta_df = pd.DataFrame(meta_rows)
        merged = long_df.merge(meta_df, on="bus_column", how="left", validate="many_to_one")
        return merged[["system", "region", "bus", "carrier", "period", "year", "p_t"]]

    carrier_paths = dict(carrier_paths or {})

    # --------------------------------------------------------------
    # 3. Build final merged dictionary
    # --------------------------------------------------------------
    demand_frames: list[pd.DataFrame] = []
    if existing_demand is not None and not existing_demand.p_t.empty:
        demand_frames.append(existing_demand.p_t.copy())

    for carrier, path in carrier_paths.items():
        part = _read_single(path, carrier)
        if not part.empty:
            demand_frames.append(part)

    # Transport demand from canonical transport component.
    if transport is not None:
        if buses is None:
            raise ValueError("Bus model is required when transport demand is provided.")
        transport_df = transport_to_system_demand(transport, buses)
        if not transport_df.empty:
            if (transport_df["p_t"] < 0).any():
                raise ValueError("Negative transport demand values found.")
            demand_frames.append(transport_df)
    if demand_frames:
        out_df = pd.concat(demand_frames, ignore_index=True)
        out_df = (
            out_df.groupby(["system", "region", "bus", "carrier", "period", "year"], as_index=False)["p_t"]
            .sum()
            .sort_values(["system", "region", "bus", "carrier", "year", "period"], kind="stable")
            .reset_index(drop=True)
        )
    else:
        out_df = empty_frame(DEMAND_COLUMNS)

    return build_model(Demand, p_t=out_df)
