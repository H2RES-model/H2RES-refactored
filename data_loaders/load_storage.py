"""Storage loader that assembles StorageUnits from static and time-series tables."""

from __future__ import annotations

from typing import Optional

import pandas as pd

from data_models.Bus import Bus
from data_models.StorageUnits import StorageUnits
from data_models.SystemSets import SystemSets
from data_models.Transport import Transport
from data_loaders.helpers.io import TableCache
from data_loaders.helpers.model_factory import build_model, is_strict_validation
from data_loaders.helpers.storage_component import (
    apply_transport_power,
    build_e_nom_ts,
    build_storage_static_frame,
    build_storage_store,
    load_storage_inflow_ts,
    merge_storage_components,
)
from data_loaders.helpers.storage_utils import assert_frame_unit_subset


def load_storage(
    powerplants_path: str,
    storage_path: Optional[str] = None,
    inflow_path: Optional[str] = None,
    transport_general_params_path: Optional[str] = None,
    transport_zones_path: Optional[str] = None,
    transport_availability_path: Optional[str] = None,
    transport_demand_path: Optional[str] = None,
    write_transport_storage_units: bool = True,
    buses_path: Optional[str] = None,
    *,
    transport: Optional[Transport] = None,
    include_chp_tes: bool = True,
    sets: SystemSets,
    buses: Bus,
    existing_storage: Optional[StorageUnits] = None,
    table_cache: Optional[TableCache] = None,
) -> StorageUnits:
    """Build StorageUnits from unit data, templates, and inflow data."""

    store, hydro_units, availability_df, transport_power_df = build_storage_store(
        powerplants_path=powerplants_path,
        storage_path=storage_path,
        transport_general_params_path=transport_general_params_path,
        transport_zones_path=transport_zones_path,
        transport_availability_path=transport_availability_path,
        transport_demand_path=transport_demand_path,
        write_transport_storage_units=write_transport_storage_units,
        buses_path=buses_path,
        transport=transport,
        include_chp_tes=include_chp_tes,
        sets=sets,
        buses=buses,
        table_cache=table_cache,
    )

    if not store.unit_order:
        return existing_storage or build_model(StorageUnits)

    static_df = build_storage_static_frame(store)
    static_df = apply_transport_power(
        static_df,
        transport_power_df,
        canonical_transport=transport is not None,
    )
    inflow_df = load_storage_inflow_ts(
        inflow_path=inflow_path,
        hydro_units=hydro_units,
        sets=sets,
        table_cache=table_cache,
    )
    e_nom_ts_df = build_e_nom_ts(static_df, availability_df)

    static_df, inflow_df, availability_df, e_nom_ts_df, investment_costs, units = merge_storage_components(
        existing_storage=existing_storage,
        static_df=static_df,
        inflow_df=inflow_df,
        availability_df=availability_df,
        e_nom_ts_df=e_nom_ts_df,
    )
    if not static_df.empty and "unit" in static_df.columns:
        static_df = static_df.set_index("unit", drop=True)
    inflow_df = inflow_df.reset_index(drop=True)
    availability_df = availability_df.reset_index(drop=True)
    e_nom_ts_df = e_nom_ts_df.reset_index(drop=True)
    investment_costs = investment_costs.reset_index(drop=True)

    if is_strict_validation():
        assert_frame_unit_subset(
            units,
            (
                ("inflow", inflow_df),
                ("e_nom_inv_cost", investment_costs),
                ("availability", availability_df),
                ("e_nom_ts", e_nom_ts_df),
            ),
        )

    return build_model(
        StorageUnits,
        static=static_df,
        inflow=inflow_df,
        availability=availability_df,
        e_nom_ts=e_nom_ts_df,
        investment_costs=investment_costs,
    )
