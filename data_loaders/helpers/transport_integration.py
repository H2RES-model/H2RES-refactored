from __future__ import annotations

import pandas as pd

from data_models.Bus import Bus
from data_models.Transport import Transport
from data_loaders.helpers.timeseries import empty_frame

TRANSPORT_STORAGE_COLUMNS = [
    "unit",
    "system",
    "region",
    "tech",
    "carrier_in",
    "carrier_out",
    "bus_in",
    "bus_out",
    "e_nom",
    "e_nom_max",
    "e_min",
    "p_charge_nom",
    "p_charge_nom_max",
    "p_discharge_nom",
    "p_discharge_nom_max",
    "duration_charge",
    "duration_discharge",
    "efficiency_charge",
    "efficiency_discharge",
    "standby_loss",
    "capital_cost_energy",
    "capital_cost_power_charge",
    "capital_cost_power_discharge",
    "lifetime",
    "spillage_cost",
]
EMPTY_STORAGE_AVAILABILITY = empty_frame(["unit", "period", "year", "availability"])


def transport_storage_units(transport: Transport) -> list[str]:
    if transport.static.empty:
        return []
    mask = transport.static["supports_grid_connection"].fillna(False).astype(bool)
    return transport.static.index[mask].astype(str).tolist()


def transport_to_storage(transport: Transport) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert canonical transport tables into storage static rows and availability."""
    if transport.static.empty:
        return pd.DataFrame(columns=TRANSPORT_STORAGE_COLUMNS), EMPTY_STORAGE_AVAILABILITY.copy()

    static = transport.static.copy()
    static = static[static["supports_grid_connection"].fillna(False).astype(bool)].copy()
    if static.empty:
        return pd.DataFrame(columns=TRANSPORT_STORAGE_COLUMNS), EMPTY_STORAGE_AVAILABILITY.copy()

    fleet_units = pd.to_numeric(static["fleet_units"], errors="coerce")
    battery_capacity = pd.to_numeric(static["battery_capacity_kwh"], errors="coerce")
    charge_rate = pd.to_numeric(static["charge_rate_kw"], errors="coerce")
    grid_efficiency = pd.to_numeric(static.get("grid_efficiency"), errors="coerce").fillna(1.0)
    storage_min_soc = pd.to_numeric(static.get("storage_min_soc"), errors="coerce").fillna(0.0)
    v2g_cost = pd.to_numeric(static.get("v2g_cost"), errors="coerce").fillna(0.0)
    lifetime = pd.to_numeric(static.get("lifetime"), errors="coerce").fillna(0).astype(int)
    max_fleet = pd.to_numeric(static.get("max_investment"), errors="coerce").fillna(fleet_units)
    max_fleet = max_fleet.where(max_fleet >= fleet_units, fleet_units)
    static_out = pd.DataFrame({"unit": static.index.astype(str)})
    static_out["system"] = static["system"].astype(str).values
    static_out["region"] = static["region"].astype(str).values
    static_out["tech"] = static["tech"].astype(str).values
    static_out["carrier_in"] = static["carrier_in"].astype(str).values
    static_out["carrier_out"] = static["carrier_in"].astype(str).values
    static_out["bus_in"] = static["bus_in"].astype(str).values
    static_out["bus_out"] = static["bus_in"].astype(str).values
    static_out["e_nom"] = (fleet_units.to_numpy() * battery_capacity.to_numpy()) / 1000.0
    static_out["e_nom_max"] = (max_fleet.to_numpy() * battery_capacity.to_numpy()) / 1000.0
    static_out["e_min"] = storage_min_soc.to_numpy() * static_out["e_nom"].to_numpy()
    static_out["p_charge_nom"] = (fleet_units.to_numpy() * charge_rate.to_numpy()) / 1000.0
    static_out["p_charge_nom_max"] = (max_fleet.to_numpy() * charge_rate.to_numpy()) / 1000.0
    static_out["p_discharge_nom"] = static_out["p_charge_nom"]
    static_out["p_discharge_nom_max"] = static_out["p_charge_nom_max"]
    static_out["duration_charge"] = battery_capacity.to_numpy() / charge_rate.to_numpy()
    static_out["duration_discharge"] = static_out["duration_charge"]
    static_out["efficiency_charge"] = grid_efficiency.to_numpy()
    static_out["efficiency_discharge"] = grid_efficiency.to_numpy()
    static_out["standby_loss"] = 0.0
    static_out["capital_cost_energy"] = 0.0
    static_out["capital_cost_power_charge"] = 0.0
    static_out["capital_cost_power_discharge"] = v2g_cost.to_numpy()
    static_out["lifetime"] = lifetime.to_numpy()
    static_out["spillage_cost"] = 0.0

    availability = transport.availability.copy()
    availability = availability[availability["unit"].astype(str).isin(static_out["unit"].astype(str))].reset_index(drop=True)
    return static_out, availability


def transport_to_system_demand(transport: Transport, buses: Bus) -> pd.DataFrame:
    """Map canonical transport demand into the system demand table."""
    if transport.static.empty or transport.demand.empty:
        return empty_frame(["system", "region", "bus", "carrier", "period", "year", "p_t"])

    static = transport.static.reset_index().rename(columns={"index": "unit"})
    static["bus_in"] = static["bus_in"].astype(str).str.strip()
    static = static[static["bus_in"] != ""].copy()
    if static.empty:
        return empty_frame(["system", "region", "bus", "carrier", "period", "year", "p_t"])

    bus_meta = buses.static.reset_index().rename(columns={"bus": "bus_in"})
    merged = static.merge(bus_meta[["bus_in", "carrier"]], on="bus_in", how="left", validate="many_to_one")
    merged = merged[merged["carrier"].notna()].copy()
    demand = merged[["unit", "system", "region", "bus_in", "carrier", "carrier_in"]].merge(
        transport.demand,
        on="unit",
        how="inner",
        validate="one_to_many",
    )
    demand = demand[demand["carrier_in"].astype(str).str.lower() == demand["carrier"].astype(str).str.lower()].copy()
    demand = demand.rename(columns={"bus_in": "bus", "demand": "p_t"})
    return demand[["system", "region", "bus", "carrier", "period", "year", "p_t"]].reset_index(drop=True)
