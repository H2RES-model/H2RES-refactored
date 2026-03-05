"""System-level loader entry point for the data pipeline."""

from pathlib import Path
from typing import Dict, Iterable, Optional, Union

from data_loaders.load_sector import load_sector
from data_loaders.helpers.io import TableCache

PathLike = Union[str, Path]

def load_system(
    *,
    sectors: Union[str, Iterable[str]],
    electricity_paths: Dict[str, PathLike],
    heating_paths: Optional[Dict[str, PathLike]] = None,
    cooling_paths: Optional[Dict[str, PathLike]] = None,
    industry_paths: Optional[Dict[str, PathLike]] = None,
    buses_path: Optional[PathLike] = None,
    fuel_cost_path: Optional[PathLike] = None,
    table_cache: Optional[TableCache] = None,
) -> object:
    """Load system parameters for one or more sectors.

    When used: top-level entry point called by the pipeline/orchestrator to
    build a full SystemParameters object via per-sector loading.

    Args:
        sectors: Sector name or list of sector names to load (order preserved).
        electricity_paths: Mapping of required electricity input paths.
        heating_paths: Optional mapping of heating input paths.
        cooling_paths: Optional mapping of cooling input paths.
        buses_path: Optional path to buses metadata used across sectors.
        fuel_cost_path: Optional path to fuel cost time-series used across sectors.

    Returns:
        A SystemParameters instance built by `load_sector`.

    Raises:
        ValueError: If an unknown sector is requested or required paths are missing.
        NotImplementedError: If a sector is recognized but not supported.

    Notes:
        Required keys in `electricity_paths`:
            - powerplants_path
            - storage_path
            - renewable_profiles_path
            - inflow_path
            - electricity_demand_path
        Optional keys in `heating_paths` / `cooling_paths`:
            - powerplants_path
            - storage_path
            - efficiency_ts_path
            - heating_demand_path / cooling_demand_path
    """

    if isinstance(sectors, str):
        sectors = [sectors]

    allowed = {"electricity", "heating", "cooling", "industry", "transport"}
    sectors = [s.strip().lower() for s in sectors]
    unknown = [s for s in sectors if s not in allowed]
    if unknown:
        raise ValueError(f"Unknown sectors: {unknown}. Allowed: {sorted(allowed)}")

    # Preserve order and remove duplicates
    ordered = []
    seen = set()
    for s in sectors:
        if s not in seen:
            ordered.append(s)
            seen.add(s)

    required_elec = {
        "powerplants_path",
        "storage_path",
        "renewable_profiles_path",
        "inflow_path",
        "electricity_demand_path",
    }
    missing = required_elec - set(electricity_paths.keys())
    if missing:
        raise ValueError(f"Missing required electricity paths: {sorted(missing)}")

    def _coerce_paths(d: Optional[Dict[str, PathLike]]) -> Dict[str, str]:
        if not d:
            return {}
        return {k: str(Path(v)) for k, v in d.items() if v is not None}

    electricity_kwargs = _coerce_paths(electricity_paths)
    heating_kwargs = _coerce_paths(heating_paths)
    cooling_kwargs = _coerce_paths(cooling_paths)
    industry_kwargs = _coerce_paths(industry_paths)

    cache = table_cache if table_cache is not None else {}

    system = None
    for sector in ordered:
        if sector == "electricity":
            kwargs = dict(electricity_kwargs)
            kwargs["sector"] = "electricity"
        elif sector == "heating":
            if not heating_kwargs:
                raise ValueError("heating_paths must be provided when loading heating sector.")
            kwargs = dict(heating_kwargs)
            kwargs["sector"] = "heating"
        elif sector == "cooling":
            if not cooling_kwargs:
                raise ValueError("cooling_paths must be provided when loading cooling sector.")
            kwargs = dict(cooling_kwargs)
            kwargs["sector"] = "cooling"
        elif sector == "industry":
            if not industry_kwargs:
                raise ValueError("industry_paths must be provided when loading industry sector.")
            kwargs = dict(industry_kwargs)
            kwargs["sector"] = "industry"
        elif sector == "transport":
            raise NotImplementedError("transport not supported by load_sector yet.")
        else:
            raise ValueError(f"Unhandled sector: {sector}")

        if buses_path is not None:
            kwargs["buses_path"] = str(Path(buses_path))
        if fuel_cost_path is not None:
            kwargs["fuel_cost_path"] = str(Path(fuel_cost_path))

        if system is not None:
            kwargs["existing_system"] = system # type: ignore error
        kwargs["table_cache"] = cache

        system = load_sector(**kwargs) # type: ignore error

    return system
