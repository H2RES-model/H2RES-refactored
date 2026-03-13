"""Sector-level loader that assembles SystemParameters from input tables."""

from __future__ import annotations

import os
from typing import Any, Optional

from data_models.SystemSets import SystemSets
from data_models.Generators import Generators
from data_models.StorageUnits import StorageUnits
from data_models.Demand import Demand
from data_models.Transport import Transport


from data_models.SystemParameters import (
    Demand,
    MarketParams,
    PolicyParams,
    SystemParameters,
)
from data_loaders.helpers.io import TableCache, combine_tables
from data_loaders.helpers.model_factory import build_model
from data_loaders.helpers.transport_integration import transport_storage_units
from data_loaders.load_sets import load_sets
from data_loaders.load_generators import load_generators
from data_loaders.load_storage import load_storage
from data_loaders.load_bus import load_bus
from data_loaders.load_demand import load_demand
from data_loaders.load_transport import load_transport

SECTOR_DEFAULTS = {
    "electricity": {
        "base_dir": os.path.join("data", "electricity"),
        "defaults": {
            "powerplants_path": "powerplants.csv",
            "storage_path": "storage_units.csv",
            "renewable_profiles_path": "res_profile.csv",
            "inflow_path": "scaled_inflows.csv",
            "electricity_demand_path": "electricity_demand.csv",
        },
        "allowed_carriers": ["electricity"],
        "include_chp_tes": False,
    },
    "heating": {
        "base_dir": os.path.join("data", "heating"),
        "defaults": {
            "powerplants_path": "converters.csv",
            "storage_path": "storage_units.csv",
            "heating_demand_path": "heat_demand.csv",
        },
        "allowed_carriers": ["heat"],
        "include_chp_tes": True,
    },
    "cooling": {
        "base_dir": os.path.join("data", "cooling"),
        "defaults": {
            "powerplants_path": "converters.csv",
            "storage_path": "storage_units.csv",
            "cooling_demand_path": "cooling_demand.csv",
        },
        "allowed_carriers": ["cooling"],
        "include_chp_tes": True,
    },
    "industry": {
        "base_dir": os.path.join("data", "industry"),
        "defaults": {
            "powerplants_path": "converters.csv",
            "storage_path": "storage_units.csv",
            "industry_demand_path": "industry_demand.csv",
        },
        "allowed_carriers": ["industry_heat"],
        "include_chp_tes": True,
    },
}


def _apply_sector_defaults(sector_key: str | None, values: dict[str, Optional[str]]) -> dict[str, Optional[str]]:
    if not sector_key:
        return values
    if sector_key not in SECTOR_DEFAULTS:
        raise ValueError(f"Unknown sector '{sector_key}'. Expected one of: {sorted(SECTOR_DEFAULTS)}")
    defaults = SECTOR_DEFAULTS[sector_key]
    base_dir = defaults["base_dir"]
    if values.get("buses_path") is None:
        values["buses_path"] = os.path.join("data", "buses.csv")
    if values.get("fuel_cost_path") is None:
        values["fuel_cost_path"] = os.path.join("data", "fuel_cost.csv")
    for key, filename in defaults["defaults"].items():
        values[key] = values.get(key) or os.path.join(base_dir, filename)
    return values


def _should_reload_sets(existing_system: Optional[SystemParameters], *, powerplants_path: Optional[str], renewable_profiles_path: Optional[str], fuel_cost_path: Optional[str], storage_path: Optional[str]) -> bool:
    return not (existing_system and not any([powerplants_path, renewable_profiles_path, fuel_cost_path, storage_path]))


def _should_reload_buses(existing_system: Optional[SystemParameters], *, powerplants_path: Optional[str], storage_path: Optional[str], electricity_demand_path: Optional[str], heating_demand_path: Optional[str], cooling_demand_path: Optional[str], industry_demand_path: Optional[str]) -> bool:
    return not (
        existing_system and not any([powerplants_path, storage_path, electricity_demand_path, heating_demand_path, cooling_demand_path, industry_demand_path])
    )


def _should_reload_generators(existing_system: Optional[SystemParameters], *, powerplants_path: Optional[str]) -> bool:
    return not (existing_system and not powerplants_path)


def _should_reload_storage(existing_system: Optional[SystemParameters], *, powerplants_path: Optional[str], storage_path: Optional[str], inflow_path: Optional[str], transport_enabled: bool) -> bool:
    return not (existing_system and not any([powerplants_path, storage_path, inflow_path, transport_enabled]))


def _should_reload_demand(existing_system: Optional[SystemParameters], *, carrier_paths: dict[str, str], transport_enabled: bool) -> bool:
    return not (existing_system and not any([*carrier_paths.values(), transport_enabled]))


def load_sector(
    *,
    powerplants_path: Optional[str] = None,
    storage_path: Optional[str] = None,
    renewable_profiles_path: Optional[str] = None,
    fuel_cost_path: Optional[str] = None,
    buses_path: Optional[str] = None,
    efficiency_ts_path: Optional[str] = None,
    electricity_demand_path: Optional[str] = None,
    heating_demand_path: Optional[str] = None,
    cooling_demand_path: Optional[str] = None,
    industry_demand_path: Optional[str] = None,
    transport_general_parameters_path: Optional[Any] = None,
    transport_fleet_and_demand_path: Optional[Any] = None,
    transport_availability_path: Optional[Any] = None,
    transport_demand_timeseries_path: Optional[Any] = None,
    write_transport_storage_units: bool = True,
    inflow_path: Optional[str] = None,
    fuels_powerplants_path: Optional[str] = None,
    fuels_storage_path: Optional[str] = None,
    fuels_demand_path: Optional[str] = None,
    fuels_demand_carrier: str = "hydrogen",
    sector: Optional[str] = None,
    existing_system: Optional[SystemParameters] = None,
    table_cache: Optional[TableCache] = None,
) -> SystemParameters:
    """Assemble SystemParameters for a single sector.

    When used: called by `load_system` to load one sector and merge into the
    overall SystemParameters object.

    Args:
        powerplants_path: Path to powerplants (or converters) input file.
        storage_path: Path to storage template file.
        renewable_profiles_path: Path to RES profile time series.
        fuel_cost_path: Path to fuel cost time series.
        buses_path: Path to buses metadata file.
        efficiency_ts_path: Path to efficiency time series for generators.
        electricity_demand_path: Path to electricity demand time series.
        heating_demand_path: Path to heating demand time series.
        cooling_demand_path: Path to cooling demand time series.
        transport_general_parameters_path: Path to raw transport metadata table.
        transport_fleet_and_demand_path: Path to raw transport fleet and annual demand table.
        transport_availability_path: Path to transport availability time series.
        transport_demand_timeseries_path: Path to transport demand profile table.
        write_transport_storage_units: Write transport_storage_units.csv to disk.
        inflow_path: Path to hydro inflow time series.
        sector: Sector name used for defaults ("electricity", "heating", "cooling", "industry").
        existing_system: Existing SystemParameters to merge into (existing wins).

    Returns:
        SystemParameters populated with sets, buses, generators, storage, demand,
        and default market/policy parameters.

    Raises:
        ValueError: If required inputs are missing or sector is unknown.
        NotImplementedError: If a supported sector is requested but not implemented.

    Notes:
        If `sector` is provided, missing paths are auto-resolved from the
        standard data folder layout.
    """

    sector_key = sector.strip().lower() if sector else None
    normalized_inputs = _apply_sector_defaults(
        sector_key,
        {
            "powerplants_path": powerplants_path,
            "storage_path": storage_path,
            "renewable_profiles_path": renewable_profiles_path,
            "fuel_cost_path": fuel_cost_path,
            "buses_path": buses_path,
            "efficiency_ts_path": efficiency_ts_path,
            "electricity_demand_path": electricity_demand_path,
            "heating_demand_path": heating_demand_path,
            "cooling_demand_path": cooling_demand_path,
            "industry_demand_path": industry_demand_path,
            "inflow_path": inflow_path,
        },
    )
    powerplants_path = normalized_inputs["powerplants_path"]
    storage_path = normalized_inputs["storage_path"]
    renewable_profiles_path = normalized_inputs["renewable_profiles_path"]
    fuel_cost_path = normalized_inputs["fuel_cost_path"]
    buses_path = normalized_inputs["buses_path"]
    efficiency_ts_path = normalized_inputs["efficiency_ts_path"]
    electricity_demand_path = normalized_inputs["electricity_demand_path"]
    heating_demand_path = normalized_inputs["heating_demand_path"]
    cooling_demand_path = normalized_inputs["cooling_demand_path"]
    industry_demand_path = normalized_inputs["industry_demand_path"]
    inflow_path = normalized_inputs["inflow_path"]
    fuels_demand_carrier = str(fuels_demand_carrier).strip() or "hydrogen"

    combined_powerplants = combine_tables(
        [powerplants_path, fuels_powerplants_path],
        cache=table_cache,
        unique_key="name",
        source_name="powerplants inputs",
    )
    combined_storage = combine_tables(
        [storage_path, fuels_storage_path],
        cache=table_cache,
        unique_key="name",
        source_name="storage inputs",
    )

    transport_sources = [
        transport_general_parameters_path,
        transport_fleet_and_demand_path,
        transport_availability_path,
        transport_demand_timeseries_path,
    ]
    if any(source is not None for source in transport_sources):
        if not all(source is not None for source in transport_sources):
            raise ValueError(
                "transport_general_parameters_path, transport_fleet_and_demand_path, "
                "transport_availability_path, and transport_demand_timeseries_path "
                "must be provided together."
            )

    transport_enabled = sector_key == "electricity"
    transport_general_for_sector = transport_general_parameters_path if transport_enabled else None
    transport_fleet_for_sector = transport_fleet_and_demand_path if transport_enabled else None
    transport_availability_for_sector = transport_availability_path if transport_enabled else None
    transport_demand_for_sector = transport_demand_timeseries_path if transport_enabled else None
    transport = None
    include_fuels_demand = bool(fuels_demand_path)
    if include_fuels_demand and existing_system is not None and not existing_system.demands.p_t.empty:
        carriers = existing_system.demands.p_t.get("carrier")
        if carriers is not None and carriers.astype(str).eq(fuels_demand_carrier).any():
            include_fuels_demand = False

    if existing_system is None:
        required = {
            "powerplants_path": powerplants_path,
            "storage_path": storage_path,
            "fuel_cost_path": fuel_cost_path,
        }
        if sector_key == "electricity":
            required["inflow_path"] = inflow_path
        missing = [name for name, val in required.items() if not val]
        if missing:
            raise ValueError(f"Missing required inputs for initial load: {missing}")
        if not (
            electricity_demand_path
            or heating_demand_path
            or cooling_demand_path
            or industry_demand_path
            or transport_demand_for_sector is not None
        ):
            raise ValueError("At least one demand CSV path is required for initial load.")

    # --------------------------------------------------------------
    # 1. Core sets (years, periods, units, subsets)
    # --------------------------------------------------------------
    if not _should_reload_sets(
        existing_system,
        powerplants_path=powerplants_path,
        renewable_profiles_path=renewable_profiles_path,
        fuel_cost_path=fuel_cost_path,
        storage_path=storage_path,
    ):
        sets = existing_system.sets
    else:
        if not powerplants_path:
            raise ValueError(
                "powerplants_path is required to load/update sets."
            )
        sets = load_sets(
            powerplants_path=combined_powerplants,
            renewable_profiles_path=renewable_profiles_path,
            fuel_cost_path=fuel_cost_path,
            buses_path=buses_path,
            storage_path=combined_storage,
            existing_sets=existing_system.sets if existing_system else None,
            table_cache=table_cache,
        )
        if transport_enabled and all(
            source is not None
            for source in (
                transport_general_for_sector,
                transport_fleet_for_sector,
                transport_availability_for_sector,
                transport_demand_for_sector,
            )
        ):
            transport = load_transport(
                sets=sets,
                general_params_path=transport_general_for_sector,
                fleet_and_demand_path=transport_fleet_for_sector,
                availability_path=transport_availability_for_sector,
                demand_timeseries_path=transport_demand_for_sector,
                table_cache=table_cache,
            )
            transport_storage = transport_storage_units(transport)
            if transport_storage:
                sets = sets.model_copy(
                    update={
                        "storage_units": sorted(set(sets.storage_units).union(transport_storage)),
                    }
                )

    # --------------------------------------------------------------
    # 2. Buses (uses demand headers + csv templates)
    # --------------------------------------------------------------
    if not _should_reload_buses(
        existing_system,
        powerplants_path=powerplants_path,
        storage_path=storage_path,
        electricity_demand_path=electricity_demand_path,
        heating_demand_path=heating_demand_path,
        cooling_demand_path=cooling_demand_path,
        industry_demand_path=industry_demand_path,
    ):
        buses = existing_system.buses
    else:
        if not powerplants_path or not storage_path:
            raise ValueError(
                "powerplants_path and storage_path are required to load/update buses."
            )
        allowed_carriers_map = {name: cfg["allowed_carriers"] for name, cfg in SECTOR_DEFAULTS.items()}
        demand_paths: dict[str, str] = {}
        for carrier, path in (
            ("electricity", electricity_demand_path),
            ("heat", heating_demand_path),
            ("cooling", cooling_demand_path),
            ("industry_heat", industry_demand_path),
        ):
            if path:
                demand_paths[carrier] = path
        buses = load_bus(
            powerplants_path=powerplants_path or combined_powerplants,
            storage_path=storage_path or combined_storage,
            buses_path=buses_path,
            demand_paths=demand_paths,
            extra_demand_paths={fuels_demand_carrier: fuels_demand_path} if include_fuels_demand else None,
            transport_static=transport.static if transport is not None else None,
            extra_powerplants_path=fuels_powerplants_path,
            extra_storage_path=fuels_storage_path,
            allowed_carriers=allowed_carriers_map.get(sector_key),
            sets=sets,
            existing_buses=existing_system.buses if existing_system else None,
            table_cache=table_cache,
        )

    # --------------------------------------------------------------
    # 3. Generators
    # --------------------------------------------------------------
    if not _should_reload_generators(existing_system, powerplants_path=powerplants_path):
        generators = existing_system.generators
    else:
        if not powerplants_path:
            raise ValueError("powerplants_path is required to load/update generators.")
        generators = load_generators(
            powerplants_path=combined_powerplants,
            renewable_profiles_path=renewable_profiles_path,
            fuel_cost_path=fuel_cost_path,
            efficiency_ts_path=efficiency_ts_path,
            sets=sets,
            buses=buses,
            existing_generators=existing_system.generators if existing_system else None,
            table_cache=table_cache,
        )

    # --------------------------------------------------------------
    # 4. Storage
    # --------------------------------------------------------------
    if not _should_reload_storage(
        existing_system,
        powerplants_path=powerplants_path,
        storage_path=storage_path,
        inflow_path=inflow_path,
        transport_enabled=transport_availability_for_sector is not None or transport_demand_for_sector is not None,
    ):
        storage = existing_system.storage_units
    else:
        if not powerplants_path or not storage_path:
            raise ValueError("powerplants_path and storage_path are required to load/update storage.")
        storage = load_storage(
            powerplants_path=combined_powerplants,
            storage_path=combined_storage if not combined_storage.empty else (storage_path or powerplants_path),
            inflow_path=inflow_path,
            transport=transport,
            write_transport_storage_units=write_transport_storage_units,
            include_chp_tes=SECTOR_DEFAULTS.get(sector_key or "", {}).get("include_chp_tes", True),
            sets=sets,
            buses=buses,
            existing_storage=existing_system.storage_units if existing_system else None,
            table_cache=table_cache,
        )

    # --------------------------------------------------------------
    # 5. Demand (electricity/heat/cooling/industry/transport)
    # --------------------------------------------------------------
    demand_electricity_path = electricity_demand_path
    demand_heating_path = heating_demand_path
    demand_cooling_path = cooling_demand_path
    demand_industry_path = industry_demand_path
    if sector_key == "electricity":
        demand_heating_path = None
        demand_cooling_path = None
        demand_industry_path = None
    elif sector_key == "heating":
        demand_electricity_path = None
        demand_cooling_path = None
        demand_industry_path = None
    elif sector_key == "cooling":
        demand_electricity_path = None
        demand_heating_path = None
        demand_industry_path = None
    elif sector_key == "industry":
        demand_electricity_path = None
        demand_heating_path = None
        demand_cooling_path = None

    carrier_paths: dict[str, str] = {}
    for carrier, path in (
        ("electricity", demand_electricity_path),
        ("heat", demand_heating_path),
        ("cooling", demand_cooling_path),
        ("industry_heat", demand_industry_path),
        (fuels_demand_carrier, fuels_demand_path if include_fuels_demand else None),
    ):
        if path:
            carrier_paths[carrier] = path

    if not _should_reload_demand(
        existing_system,
        carrier_paths=carrier_paths,
        transport_enabled=transport_demand_for_sector is not None,
    ):
        demand = existing_system.demands
    else:
        demand = load_demand(
            sets=sets,
            carrier_paths=carrier_paths,
            transport=transport,
            buses=buses,
            buses_path=buses_path,
            existing_demand=existing_system.demands if existing_system else None,
            table_cache=table_cache,
        )

    # --------------------------------------------------------------
    # 6. Market parameters (empty placeholder)
    # --------------------------------------------------------------
    market = existing_system.market if existing_system else MarketParams(
        import_ntc={},
        exports={},
        exports_dat=False,
        import_price=None,
        import_price_increase=None,
    )

    # --------------------------------------------------------------
    # 7. Policy parameters (empty placeholder)
    # --------------------------------------------------------------
    policy = existing_system.policy if existing_system else PolicyParams(
        NPV={},
        rps={},
        CO2_price={},
        CO2_limit={},
        # rps_inv, res_inv, ceep_limit, ceep_parameter
        # use their defaults from the Pydantic model
    )

    # --------------------------------------------------------------
    # 8. Assemble and return central SystemParameters dataclass
    # --------------------------------------------------------------
    return build_model(
        SystemParameters,
        sets=sets,
        buses=buses,
        generators=generators,
        storage_units=storage,
        demands=demand,
        transport_units=transport or (existing_system.transport_units if existing_system is not None else Transport()),
        market=market,
        policy=policy,
    )
