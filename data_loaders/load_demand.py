"""Demand loader for generic carrier demand time series."""

from __future__ import annotations

from typing import Dict, Optional, Tuple
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Demand import Demand
from data_models.Bus import Bus
from data_models.Transport import Transport
from data_loaders.helpers.io import TableCache, read_table
from data_loaders.helpers.model_factory import build_model
from data_loaders.helpers.transport_utils import load_ev_inputs, _is_electric_transport_tech
from data_loaders.helpers.transport_integration import transport_to_system_demand

Key = Tuple[str, str, str, str, int, int]  # (system, region, bus, carrier, period, year)


def load_demand(
    *,
    sets: SystemSets,
    carrier_paths: Optional[Dict[str, str]] = None,
    transport: Optional[Transport] = None,
    transport_demand_path: Optional[str] = None,
    transport_general_params_path: Optional[str] = None,
    transport_zones_path: Optional[str] = None,
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
        bus_static = buses.static.reset_index()
        for _, row in bus_static.iterrows():
            bus_id = str(row["bus"]).strip()
            if not bus_id:
                continue
            bus_lookup[bus_id.lower()] = (
                str(row.get("system", "")).strip(),
                str(row.get("region", "")).strip(),
                bus_id,
                str(row.get("carrier", default_carrier)).strip() or default_carrier,
            )

    if buses_path:
        df_buses = read_table(buses_path, cache=table_cache)
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
    def _read_single(path: Optional[str], carrier: str) -> pd.DataFrame:
        if path is None:
            return pd.DataFrame(columns=["system", "region", "bus", "carrier", "period", "year", "p_t"])

        df = read_table(path, cache=table_cache)

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
            return pd.DataFrame(columns=["system", "region", "bus", "carrier", "period", "year", "p_t"])

        # collect demand columns
        id_vars = ["year", "period"]
        value_cols = [c for c in df.columns if c not in id_vars]
        if not value_cols:
            return pd.DataFrame(columns=["system", "region", "bus", "carrier", "period", "year", "p_t"])

        # numeric conversion
        for col in value_cols:
            df[col] = pd.to_numeric(df[col], errors="raise")

        long_df = df.melt(id_vars=id_vars, value_vars=value_cols, var_name="bus_column", value_name="p_t")
        long_df = long_df.dropna(subset=["p_t"])
        if long_df.empty:
            return pd.DataFrame(columns=["system", "region", "bus", "carrier", "period", "year", "p_t"])
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
    # Backward-compatible raw transport path fallback.
    elif transport_demand_path:
        if transport_general_params_path is None or transport_zones_path is None:
            raise ValueError(
                "transport_general_params_path and transport_zones_path are required when transport_demand_path is provided."
            )

        params_df, _ev_availability, ev_demand_profile = load_ev_inputs(
            general_params_path=transport_general_params_path,
            zones_params_path=transport_zones_path,
            ev_demand_path=transport_demand_path,
        )

        profiles_df = ev_demand_profile[
            ev_demand_profile["year"].astype(int).isin(sets.years)
            & ev_demand_profile["period"].astype(int).isin(sets.periods)
        ].copy()
        profiles_df = profiles_df.rename(columns={"ev_demand": "profile"})
        params_electric = params_df[params_df["tech"].map(_is_electric_transport_tech)].copy()
        if not params_electric.empty:
            if "ev_demand_unit" not in params_electric.columns:
                raise ValueError(
                    "Transport params are missing 'ev_demand_unit'. "
                    "Check that transport_demand_path is provided and matches transport sectors."
                )
            params_electric["bus_in"] = params_electric["bus_in"].astype(str)
            if bus_lookup:
                bad_bus = ~params_electric["bus_in"].str.strip().str.lower().isin(bus_lookup.keys())
                if bad_bus.any():
                    bad_row = params_electric.loc[bad_bus].iloc[0]
                    excel_row = int(bad_row.name) + 2 if isinstance(bad_row.name, (int, float)) else None
                    line_txt = f", row {excel_row}" if excel_row is not None else ""
                    raise ValueError(
                        f"{transport_zones_path}{line_txt}: bus_in '{bad_row['bus_in']}' not found in data/buses.csv"
                    )
            params_electric["bus_lookup"] = params_electric["bus_in"].str.lower().str.strip()
            meta_rows = []
            for bus_in in params_electric["bus_in"].dropna().astype(str).unique().tolist():
                sys, reg, bus_id, carrier_val = bus_info(bus_in, None)
                meta_rows.append(
                    {
                        "bus_lookup": bus_in.lower().strip(),
                        "system": sys,
                        "region": reg,
                        "bus": bus_id,
                        "carrier": carrier_val,
                    }
                )
            meta_df = pd.DataFrame(meta_rows)
            params_electric = (
                params_electric.drop(columns=["system", "region"], errors="ignore")
                .merge(meta_df, on="bus_lookup", how="left", validate="many_to_one")
            )
            if not profiles_df.empty:
                transport_df = params_electric[["name", "system", "region", "bus", "carrier", "ev_demand_unit"]].merge(
                    profiles_df,
                    left_on="name",
                    right_on="unit",
                    how="inner",
                    validate="one_to_many",
                )
                transport_df["p_t"] = transport_df["ev_demand_unit"].astype(float) * transport_df["profile"].astype(float)
                if (transport_df["p_t"] < 0).any():
                    raise ValueError("Negative transport demand values found.")
                demand_frames.append(transport_df[["system", "region", "bus", "carrier", "period", "year", "p_t"]])

    if demand_frames:
        out_df = pd.concat(demand_frames, ignore_index=True)
        out_df = (
            out_df.groupby(["system", "region", "bus", "carrier", "period", "year"], as_index=False)["p_t"]
            .sum()
            .sort_values(["system", "region", "bus", "carrier", "year", "period"], kind="stable")
            .reset_index(drop=True)
        )
    else:
        out_df = pd.DataFrame(columns=["system", "region", "bus", "carrier", "period", "year", "p_t"])

    return build_model(Demand, p_t=out_df)
