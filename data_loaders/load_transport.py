from __future__ import annotations

import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Transport import Transport
from data_loaders.helpers.io import TableCache
from data_loaders.helpers.model_factory import build_model
from data_loaders.helpers.timeseries import empty_frame
from data_loaders.helpers.transport_utils import (
    _is_electric_transport_tech,
    build_transport_availability,
    build_transport_demand,
    build_transport_params,
)


def load_transport(
    *,
    sets: SystemSets,
    general_params_path: str,
    fleet_and_demand_path: str,
    availability_path: str,
    demand_timeseries_path: str,
    table_cache: TableCache | None = None,
) -> Transport:
    """Load transport directly from the raw source tables."""
    params = build_transport_params(
        general_params_path=general_params_path,
        fleet_and_demand_path=fleet_and_demand_path,
        cache=table_cache,
    )
    availability_raw = build_transport_availability(
        availability_path=availability_path,
        params=params,
        cache=table_cache,
    )
    demand_raw = build_transport_demand(
        demand_timeseries_path=demand_timeseries_path,
        params=params,
        cache=table_cache,
    )
    availability_raw = availability_raw[
        availability_raw["year"].isin(sets.years) & availability_raw["period"].isin(sets.periods)
    ].reset_index(drop=True)
    demand_raw = demand_raw[
        demand_raw["year"].isin(sets.years) & demand_raw["period"].isin(sets.periods)
    ].reset_index(drop=True)

    join_cols = ["system", "region", "transport_segment"]
    params["carrier_in"] = params["fuel_type"].astype(str).str.strip()
    params["supports_grid_connection"] = (
        params["tech"].map(_is_electric_transport_tech)
        & params["bus_in"].astype(str).str.strip().ne("")
    )

    segment_totals = (
        params.groupby(join_cols, as_index=False)["fleet_units"]
        .sum()
        .rename(columns={"fleet_units": "segment_fleet_units"})
    )
    params = params.merge(segment_totals, on=join_cols, how="left", validate="many_to_one")
    params["annual_demand"] = params["annual_demand"].fillna(0.0)

    fleet_units = params["fleet_units"].astype(float)
    segment_fleet_units = params["segment_fleet_units"].astype(float)
    annual_demand = params["annual_demand"].astype(float)
    params["annual_demand_mwh"] = 0.0
    valid = segment_fleet_units > 0
    params.loc[valid, "annual_demand_mwh"] = (
        annual_demand[valid] * fleet_units[valid] / segment_fleet_units[valid] / 1000.0
    )
    params = params.drop(columns=["segment_fleet_units", "annual_demand"])

    static = params[
        [
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
            "lifetime",
            "max_investment",
            "supports_grid_connection",
            "annual_demand_mwh",
        ]
    ].set_index("unit", drop=True)

    grid_units = params.loc[params["supports_grid_connection"], ["unit"] + join_cols].copy()
    availability = empty_frame(["unit", "period", "year", "availability"])
    if not grid_units.empty:
        availability = grid_units.merge(availability_raw, on=join_cols, how="left", validate="many_to_many")
        availability = availability.dropna(subset=["availability"])
        availability = availability[["unit", "period", "year", "availability"]].reset_index(drop=True)

    demand_profile = params[["unit"] + join_cols].merge(demand_raw, on=join_cols, how="left", validate="many_to_many")
    demand_profile = demand_profile.dropna(subset=["demand_profile"])
    demand_profile = demand_profile[["unit", "period", "year", "demand_profile"]].reset_index(drop=True)

    demand = empty_frame(["unit", "period", "year", "demand"])
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
