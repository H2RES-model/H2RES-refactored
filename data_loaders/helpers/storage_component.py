from __future__ import annotations

from typing import Optional
import gc

import pandas as pd

from data_models.Bus import Bus
from data_models.StorageUnits import StorageUnits
from data_models.SystemSets import SystemSets
from data_models.Transport import Transport
from data_loaders.helpers.defaults import default_carrier, default_electric_bus
from data_loaders.helpers.io import TableCache, read_table
from data_loaders.helpers.storage_loader import (
    load_chp_tes,
    load_hydro_storage,
    load_inflows,
    load_template_storage,
)
from data_loaders.helpers.storage_utils import StorageRecordStore
from data_loaders.helpers.transport_integration import transport_to_storage
from data_loaders.helpers.transport_utils import (
    _is_electric_transport_tech,
    build_transport_storage_units_table,
    load_ev_inputs,
)
from data_loaders.helpers.validation_utils import require_columns


def build_storage_store(
    *,
    powerplants_path: str,
    storage_path: Optional[str],
    transport_general_params_path: Optional[str],
    transport_zones_path: Optional[str],
    transport_availability_path: Optional[str],
    transport_demand_path: Optional[str],
    write_transport_storage_units: bool,
    buses_path: Optional[str],
    transport: Optional[Transport],
    include_chp_tes: bool,
    sets: SystemSets,
    buses: Bus,
    table_cache: Optional[TableCache] = None,
) -> tuple[StorageRecordStore, list[str], pd.DataFrame, pd.DataFrame]:
    """Build storage static records and availability from all supported inputs."""
    default_carrier_value = default_carrier(sets)
    default_bus_value = default_electric_bus(sets, buses)
    store = StorageRecordStore(default_carrier=default_carrier_value, default_bus=default_bus_value)

    df_pp = read_table(powerplants_path, cache=table_cache)
    require_columns(df_pp, {"name", "tech", "p_nom", "capital_cost", "lifetime"}, powerplants_path)
    hydro_units = load_hydro_storage(df_pp, store, default_carrier_value, default_bus_value)
    if include_chp_tes:
        load_chp_tes(df_pp, store, default_carrier_value, default_bus_value)
    del df_pp
    gc.collect()

    load_template_storage(
        storage_path,
        sets,
        store,
        default_carrier_value,
        default_bus_value,
        table_cache=table_cache,
    )

    availability_df = pd.DataFrame(columns=["unit", "period", "year", "availability"])
    transport_power_df = pd.DataFrame(columns=["name", "fleet_units", "average_ch_rate"])

    if transport is not None:
        transport_storage_df, availability_df = transport_to_storage(transport)
        if not transport_storage_df.empty:
            load_template_storage(
                transport_storage_df.rename(columns={"unit": "name"}),
                sets,
                store,
                default_carrier_value,
                default_bus_value,
                table_cache=table_cache,
            )
            transport_power_df = pd.DataFrame(
                {
                    "name": transport_storage_df["unit"].astype(str),
                    "fleet_units": transport_storage_df["e_nom"].astype(float),
                    "average_ch_rate": transport_storage_df["p_charge_nom"].astype(float),
                }
            )
        return store, hydro_units, availability_df, transport_power_df

    if any([transport_zones_path, transport_availability_path, transport_demand_path]):
        missing = [
            name
            for name, value in [
                ("transport_general_params_path", transport_general_params_path),
                ("transport_zones_path", transport_zones_path),
                ("transport_availability_path", transport_availability_path),
                ("transport_demand_path", transport_demand_path),
            ]
            if not value
        ]
        if missing:
            raise ValueError("Missing required transport inputs for EV storage: " + ", ".join(missing))

    if not (transport_general_params_path and transport_zones_path and transport_availability_path and transport_demand_path):
        return store, hydro_units, availability_df, transport_power_df

    known_buses_for_transport = [str(b).strip() for b in getattr(buses, "name", [])]
    if buses_path:
        df_buses_all = read_table(buses_path, cache=table_cache)
        if "bus" not in df_buses_all.columns:
            raise ValueError(f"{buses_path} missing required column 'bus'.")
        known_buses_for_transport = df_buses_all["bus"].dropna().astype(str).str.strip().tolist()

    transport_storage_df = build_transport_storage_units_table(
        general_params_path=transport_general_params_path,
        zones_params_path=transport_zones_path,
        buses=known_buses_for_transport,
    )
    load_template_storage(
        transport_storage_df,
        sets,
        store,
        default_carrier_value,
        default_bus_value,
        table_cache=table_cache,
    )

    if write_transport_storage_units:
        from pathlib import Path

        Path(transport_zones_path).parent.joinpath("transport_storage_units.csv")
        transport_storage_df.to_csv(Path(transport_zones_path).parent / "transport_storage_units.csv", index=False)

    params_df, ev_availability, _ = load_ev_inputs(
        general_params_path=transport_general_params_path,
        zones_params_path=transport_zones_path,
        ev_availability_path=transport_availability_path,
        ev_demand_path=transport_demand_path,
    )
    transport_power_df = params_df[params_df["tech"].map(_is_electric_transport_tech)].copy()

    known = {str(b).strip().lower() for b in known_buses_for_transport}
    if known:
        bus_in_norm = transport_power_df["bus_in"].astype(str).str.strip().str.lower()
        bad_bus = ~bus_in_norm.isin(known)
        if bad_bus.any():
            row = transport_power_df.loc[bad_bus].iloc[0]
            excel_row = int(row.name) + 2 if isinstance(row.name, (int, float)) else None
            line_txt = f", row {excel_row}" if excel_row is not None else ""
            raise ValueError(f"{transport_zones_path}{line_txt}: bus_in '{row['bus_in']}' not found in data/buses.csv")

    storage_units = {str(u) for u in transport_power_df["name"].tolist()}
    if not ev_availability.empty:
        availability_df = ev_availability[
            ev_availability["unit"].astype(str).isin(storage_units)
            & ev_availability["year"].astype(int).isin(sets.years)
            & ev_availability["period"].astype(int).isin(sets.periods)
        ].copy()

    return store, hydro_units, availability_df, transport_power_df[["name", "fleet_units", "average_ch_rate"]].copy()


def build_storage_static_frame(store: StorageRecordStore) -> pd.DataFrame:
    """Convert collected storage records into one indexed static DataFrame."""
    new_unit = [str(u) for u in store.unit_order]
    static_df = pd.DataFrame({"unit": new_unit})
    for col_name, mapping in (
        ("tech", store.tech),
        ("system", store.system),
        ("region", store.region),
        ("carrier_in", store.carrier_in),
        ("carrier_out", store.carrier_out),
        ("bus_in", store.bus_in),
        ("bus_out", store.bus_out),
        ("e_nom", store.e_nom),
        ("e_min", store.e_min),
        ("e_nom_max", store.e_nom_max),
        ("p_charge_nom", store.p_charge_nom),
        ("p_charge_nom_max", store.p_charge_nom_max),
        ("p_discharge_nom", store.p_discharge_nom),
        ("p_discharge_nom_max", store.p_discharge_nom_max),
        ("duration_charge", store.duration_charge),
        ("duration_discharge", store.duration_discharge),
        ("efficiency_charge", store.efficiency_charge),
        ("efficiency_discharge", store.efficiency_discharge),
        ("standby_loss", store.standby_loss),
        ("capital_cost_energy", store.capital_cost_energy),
        ("capital_cost_power_charge", store.capital_cost_power_charge),
        ("capital_cost_power_discharge", store.capital_cost_power_discharge),
        ("lifetime", store.lifetime),
        ("spillage_cost", store.spillage_cost),
    ):
        static_df[col_name] = static_df["unit"].map(mapping)
    return static_df.set_index("unit")


def apply_transport_power(static_df: pd.DataFrame, transport_power_df: pd.DataFrame, *, canonical_transport: bool) -> pd.DataFrame:
    """Apply transport-derived power limits to the static storage table."""
    if transport_power_df.empty:
        return static_df
    power_df = transport_power_df.copy()
    power_df["fleet_units"] = pd.to_numeric(power_df["fleet_units"], errors="coerce")
    power_df["average_ch_rate"] = pd.to_numeric(power_df["average_ch_rate"], errors="coerce")
    power_df = power_df[(power_df["fleet_units"] > 0) & (power_df["average_ch_rate"] > 0)]
    if power_df.empty:
        return static_df
    if canonical_transport:
        p_nom = power_df["average_ch_rate"].to_numpy()
    else:
        p_nom = (power_df["fleet_units"] * power_df["average_ch_rate"] / 1000.0).to_numpy()
    static_df.loc[power_df["name"], "p_charge_nom"] = p_nom
    static_df.loc[power_df["name"], "p_discharge_nom"] = p_nom
    return static_df


def build_e_nom_ts(static_df: pd.DataFrame, availability_df: pd.DataFrame) -> pd.DataFrame:
    """Derive time-varying effective storage energy from availability and e_nom."""
    if availability_df.empty:
        return pd.DataFrame(columns=["unit", "period", "year", "e_nom_ts"])
    e_nom_lookup = static_df[["e_nom"]].dropna(subset=["e_nom"]).reset_index()
    e_nom_ts_df = availability_df.merge(e_nom_lookup, on="unit", how="left", validate="many_to_one")
    e_nom_ts_df["e_nom_ts"] = e_nom_ts_df["availability"].astype(float) * e_nom_ts_df["e_nom"].astype(float)
    return e_nom_ts_df[["unit", "period", "year", "e_nom_ts"]]


def merge_storage_frame(existing: pd.DataFrame, new: pd.DataFrame, *, keys: list[str]) -> pd.DataFrame:
    """Merge two keyed tables while keeping existing rows on duplicate keys."""
    parts = [df for df in (existing, new) if not df.empty]
    if not parts:
        return new
    return pd.concat(parts, ignore_index=True).drop_duplicates(subset=keys, keep="first").reset_index(drop=True)


def merge_storage_components(
    *,
    existing_storage: Optional[StorageUnits],
    static_df: pd.DataFrame,
    inflow_df: pd.DataFrame,
    availability_df: pd.DataFrame,
    e_nom_ts_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    """Merge new storage tables with existing storage, preserving existing values."""
    ex = existing_storage
    if ex is not None and not ex.static.empty:
        static_df = ex.static.combine_first(static_df)
    inflow_df = merge_storage_frame(ex.inflow.copy() if ex else pd.DataFrame(columns=["unit", "period", "year", "inflow"]), inflow_df, keys=["unit", "period", "year"])
    availability_df = merge_storage_frame(ex.availability.copy() if ex else pd.DataFrame(columns=["unit", "period", "year", "availability"]), availability_df, keys=["unit", "period", "year"])
    e_nom_ts_df = merge_storage_frame(ex.e_nom_ts.copy() if ex else pd.DataFrame(columns=["unit", "period", "year", "e_nom_ts"]), e_nom_ts_df, keys=["unit", "period", "year"])
    investment_costs = (ex.investment_costs.copy() if ex else pd.DataFrame(columns=["unit", "year", "e_nom_inv_cost"]))
    if not investment_costs.empty:
        investment_costs = investment_costs.drop_duplicates(subset=["unit", "year"], keep="first").reset_index(drop=True)
    units = sorted(set(static_df.index.astype(str).tolist()))
    return static_df, inflow_df, availability_df, e_nom_ts_df, investment_costs, units


def load_storage_inflow_ts(
    *,
    inflow_path: Optional[str],
    hydro_units: list[str],
    sets: SystemSets,
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    """Load storage inflow time series."""
    return load_inflows(inflow_path, hydro_units, sets, table_cache=table_cache)
