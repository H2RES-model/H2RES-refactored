"""Sector-level loader that assembles SystemParameters from input tables."""

from __future__ import annotations

import os
from typing import Optional

from data_models.SystemSets import SystemSets
from data_models.Generators import Generators
from data_models.StorageUnits import StorageUnits
from data_models.Demand import Demand


from data_models.SystemParameters import (
    Demand,
    MarketParams,
    PolicyParams,
    SystemParameters,
)
from data_loaders.helpers.io import TableCache
from data_loaders.helpers.model_factory import build_model
from data_loaders.load_sets import load_sets
from data_loaders.load_generators import load_generators
from data_loaders.load_storage import load_storage
from data_loaders.load_bus import load_bus
from data_loaders.load_demand import load_demand


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
    inflow_path: Optional[str] = None,
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
        inflow_path: Path to hydro inflow time series.
        sector: Sector name used for defaults ("electricity", "heating", "cooling").
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

    sector_map = {
        "electricity": "electricity",
        "heating": "heating",
        "cooling": "cooling",
        "industry": "industry",
    }
    sector_key = sector.strip().lower() if sector else None
    if sector_key and sector_key not in sector_map:
        raise ValueError(f"Unknown sector '{sector}'. Expected one of: {sorted(sector_map)}")

    if sector_key:
        base_dir = os.path.join("data", sector_key)
        if buses_path is None:
            buses_path = os.path.join("data", "buses.csv")
        if fuel_cost_path is None:
            fuel_cost_path = os.path.join("data", "fuel_cost.csv")

        if sector_key == "electricity":
            powerplants_path = powerplants_path or os.path.join(base_dir, "powerplants.csv")
            storage_path = storage_path or os.path.join(base_dir, "storage_units.csv")
            renewable_profiles_path = renewable_profiles_path or os.path.join(base_dir, "res_profile.csv")
            inflow_path = inflow_path or os.path.join(base_dir, "scaled_inflows.csv")
            electricity_demand_path = electricity_demand_path or os.path.join(base_dir, "electricity_demand.csv")
        elif sector_key == "heating":
            powerplants_path = powerplants_path or os.path.join(base_dir, "converters.csv")
            storage_path = storage_path or os.path.join(base_dir, "storage_units.csv")
            heating_demand_path = heating_demand_path or os.path.join(base_dir, "heat_demand.csv")
        elif sector_key == "cooling":
            cooling_demand_path = cooling_demand_path or os.path.join(base_dir, "cooling_demand.csv")
        elif sector_key == "industry":
            powerplants_path = powerplants_path or os.path.join(base_dir, "converters.csv")
            storage_path = storage_path or os.path.join(base_dir, "storage_units.csv")
            industry_demand_path = industry_demand_path or os.path.join(base_dir, "industry_demand.csv")

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
        if not (electricity_demand_path or heating_demand_path or cooling_demand_path or industry_demand_path):
            raise ValueError("At least one demand CSV path is required for initial load.")

    # --------------------------------------------------------------
    # 1. Core sets (years, periods, units, subsets)
    # --------------------------------------------------------------
    if existing_system and not any(
        [powerplants_path, renewable_profiles_path, fuel_cost_path, storage_path]
    ):
        sets = existing_system.sets
    else:
        if not powerplants_path:
            raise ValueError(
                "powerplants_path is required to load/update sets."
            )
        sets = load_sets(
            powerplants_path=powerplants_path,
            renewable_profiles_path=renewable_profiles_path,
            fuel_cost_path=fuel_cost_path,
            buses_path=buses_path,
            storage_path=storage_path,
            existing_sets=existing_system.sets if existing_system else None,
            table_cache=table_cache,
        )

    # --------------------------------------------------------------
    # 2. Buses (uses demand headers + csv templates)
    # --------------------------------------------------------------
    if existing_system and not any(
        [powerplants_path, storage_path, electricity_demand_path, heating_demand_path, cooling_demand_path, industry_demand_path]
    ):
        buses = existing_system.bus
    else:
        if not powerplants_path or not storage_path:
            raise ValueError(
                "powerplants_path and storage_path are required to load/update buses."
            )
        buses = load_bus(
            powerplants_path=powerplants_path,
            storage_path=storage_path,
            buses_path=buses_path,
            electricity_demand_path=electricity_demand_path,
            heating_demand_path=heating_demand_path,
            cooling_demand_path=cooling_demand_path,
            industry_demand_path=industry_demand_path,
            sector=sector,
            sets=sets,
            existing_buses=existing_system.bus if existing_system else None,
        )

    # --------------------------------------------------------------
    # 3. Generators
    # --------------------------------------------------------------
    if existing_system and not powerplants_path:
        generators = existing_system.generators
    else:
        if not powerplants_path:
            raise ValueError("powerplants_path is required to load/update generators.")
        generators = load_generators(
            powerplants_path=powerplants_path,
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
    if existing_system and not any([powerplants_path, storage_path, inflow_path]):
        storage = existing_system.storage
    else:
        if not powerplants_path or not storage_path:
            raise ValueError("powerplants_path and storage_path are required to load/update storage.")
        storage = load_storage(
            powerplants_path=powerplants_path,
            storage_path=storage_path or powerplants_path,
            inflow_path=inflow_path,
            sector=sector,
            sets=sets,
            buses=buses,
            existing_storage=existing_system.storage if existing_system else None,
            table_cache=table_cache,
        )

    # --------------------------------------------------------------
    # 5. Demand (electricity/heat/cooling)
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

    if existing_system and not any(
        [demand_electricity_path, demand_heating_path, demand_cooling_path, demand_industry_path]
    ):
        demand = existing_system.demand
    else:
        demand = load_demand(
            sets=sets,
            electricity_path=demand_electricity_path or None, # type: ignore
            heating_path=demand_heating_path or None,
            cooling_path=demand_cooling_path or None,
            industry_path=demand_industry_path or None,
            buses=buses,
            existing_demand=existing_system.demand if existing_system else None,
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
        bus = buses,
        generators=generators,
        storage=storage,
        demand=demand,
        market=market,
        policy=policy,
    )
