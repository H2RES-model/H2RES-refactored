from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Transport import Transport
from data_loaders.helpers.io import TableCache
from data_loaders.helpers.model_factory import build_model
from data_loaders.helpers.transport_utils import (
    _load_transport_inputs,
    _normalize_params,
    _read_transport_demand_totals,
    _read_transport_ts,
    _is_electric_transport_tech,
)


def load_transport(
    *,
    sets: SystemSets,
    general_params_path: str,
    zones_params_path: str,
    availability_path: str,
    demand_path: str,
    table_cache: TableCache | None = None,
) -> Transport:
    """Load and normalize transport into one canonical internal component."""

    del table_cache  # current transport parsers read source files directly

    df_raw = _load_transport_inputs(
        general_params_path=general_params_path,
        zones_params_path=zones_params_path,
    )
    params = _normalize_params(df_raw, zones_params_path).copy()

    params = params.rename(
        columns={
            "name": "unit",
            "transport_sector_bus": "transport_segment",
            "average_bat": "battery_capacity_kwh",
            "average_ch_rate": "charge_rate_kw",
            "ev_grid_eff": "grid_efficiency",
            "ev_sto_min": "storage_min_soc",
            "V2G_cost": "v2g_cost",
            "V2G_year_cost_variability": "v2g_year_cost_variability",
            "life_time": "lifetime",
        }
    )
    params["carrier_in"] = params["fuel_type"].astype(str).str.strip()
    params["supports_grid_connection"] = params["tech"].map(_is_electric_transport_tech) & params["bus_in"].astype(str).str.strip().ne("")

    demand_totals = _read_transport_demand_totals(demand_path)
    segment_totals = params.groupby(["system", "region", "transport_segment"])["fleet_units"].sum()

    def _annual_demand(row: pd.Series) -> float:
        key: Tuple[str, str, str] = (str(row["system"]), str(row["region"]), str(row["transport_segment"]))
        total = float(segment_totals.loc[key])
        share = float(row["fleet_units"]) / total if total > 0 else 0.0
        demand_total = float(demand_totals.get(key, 0.0))
        eff = float(row["efficiency_primary"])
        return demand_total * share / eff / 1000.0 if eff > 0 else 0.0

    params["annual_demand_mwh"] = params.apply(_annual_demand, axis=1)

    static_columns = [
        "unit",
        "system",
        "region",
        "transport_segment",
        "tech",
        "fuel_type",
        "carrier_in",
        "bus_in",
        "efficiency_primary",
        "fleet_units",
        "battery_capacity_kwh",
        "charge_rate_kw",
        "grid_efficiency",
        "storage_min_soc",
        "v2g_cost",
        "v2g_year_cost_variability",
        "lifetime",
        "max_investment",
        "supports_grid_connection",
        "annual_demand_mwh",
    ]
    static = params[static_columns].copy()
    static = static.set_index("unit", drop=True)

    availability_raw = _read_transport_ts(availability_path, "availability")
    demand_profile_raw = _read_transport_ts(demand_path, "demand_profile")
    availability_raw = availability_raw.rename(columns={"transport_sector_bus": "transport_segment"})
    demand_profile_raw = demand_profile_raw.rename(columns={"transport_sector_bus": "transport_segment"})

    join_cols = ["system", "region", "transport_segment"]
    params_join = params[["unit"] + join_cols + ["supports_grid_connection", "annual_demand_mwh", "bus_in", "carrier_in"]].copy()
    for frame in (params_join, availability_raw, demand_profile_raw):
        for col in join_cols:
            if col in frame.columns:
                frame[col] = frame[col].astype(str).str.strip()

    grid_units = params_join[params_join["supports_grid_connection"]].copy()
    availability = pd.DataFrame(columns=["unit", "period", "year", "availability"])
    if not availability_raw.empty and not grid_units.empty:
        availability = grid_units[["unit"] + join_cols].merge(
            availability_raw,
            on=join_cols,
            how="left",
            validate="many_to_many",
        )
        availability = availability.dropna(subset=["availability"])
        availability = availability[
            availability["year"].astype(int).isin(sets.years)
            & availability["period"].astype(int).isin(sets.periods)
        ][["unit", "period", "year", "availability"]].reset_index(drop=True)

    demand_profile = pd.DataFrame(columns=["unit", "period", "year", "demand_profile"])
    if not demand_profile_raw.empty:
        demand_profile = params_join[["unit"] + join_cols].merge(
            demand_profile_raw,
            on=join_cols,
            how="left",
            validate="many_to_many",
        )
        demand_profile = demand_profile.dropna(subset=["demand_profile"])
        demand_profile = demand_profile[
            demand_profile["year"].astype(int).isin(sets.years)
            & demand_profile["period"].astype(int).isin(sets.periods)
        ][["unit", "period", "year", "demand_profile"]].reset_index(drop=True)

    demand = pd.DataFrame(columns=["unit", "period", "year", "demand"])
    if not demand_profile.empty:
        annual_lookup = static.reset_index()[["unit", "annual_demand_mwh"]]
        demand = demand_profile.merge(annual_lookup, on="unit", how="left", validate="many_to_one")
        demand["demand"] = demand["demand_profile"].astype(float) * demand["annual_demand_mwh"].astype(float)
        demand = demand[["unit", "period", "year", "demand"]].reset_index(drop=True)

    return build_model(
        Transport,
        static=static,
        availability=availability,
        demand_profile=demand_profile,
        demand=demand,
    )
