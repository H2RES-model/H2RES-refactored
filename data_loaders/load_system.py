"""System-level loader entry point for the data pipeline."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Union

from data_loaders.helpers.io import LoaderCache, TableCache
from data_loaders.helpers.loader_config import (
    SectorConfig,
    SectorName,
    SystemLoadConfig,
    build_legacy_system_config,
    build_system_load_config,
)
from data_loaders.load_sector import load_sector
from data_loaders.prebuilt_system import load_or_build_prebuilt_system

PathLike = Union[str, Path]

SECTOR_ORDER = ("electricity", "heating", "cooling", "industry")


@contextmanager
def _validation_mode(mode: str | None):
    if not mode:
        yield
        return
    previous = os.environ.get("H2RES_VALIDATION_MODE")
    os.environ["H2RES_VALIDATION_MODE"] = mode
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("H2RES_VALIDATION_MODE", None)
        else:
            os.environ["H2RES_VALIDATION_MODE"] = previous


def _sector_kwargs(config: SystemLoadConfig, sector: str) -> dict[str, Any]:
    sector_config: SectorConfig = getattr(config, sector)
    kwargs = {key: str(value) for key, value in sector_config.paths.items()}
    kwargs["sector"] = sector
    if config.shared.buses_path is not None:
        kwargs["buses_path"] = str(config.shared.buses_path)
    if config.shared.fuel_cost_path is not None:
        kwargs["fuel_cost_path"] = str(config.shared.fuel_cost_path)
    if config.fuels.enabled:
        if config.fuels.converters_path is not None:
            kwargs["fuels_powerplants_path"] = str(config.fuels.converters_path)
        if config.fuels.storage_path is not None:
            kwargs["fuels_storage_path"] = str(config.fuels.storage_path)
        if config.fuels.demand_path is not None:
            kwargs["fuels_demand_path"] = str(config.fuels.demand_path)
            kwargs["fuels_demand_carrier"] = config.fuels.demand_carrier
    return kwargs


def _transport_kwargs(config: SystemLoadConfig) -> dict[str, Any]:
    if not config.transport.enabled:
        return {}
    return {key: str(value) for key, value in config.transport.paths.items()}


def _source_paths(config: SystemLoadConfig) -> list[Path]:
    paths: list[Path] = []
    if config.shared.buses_path is not None:
        paths.append(config.shared.buses_path)
    if config.shared.fuel_cost_path is not None:
        paths.append(config.shared.fuel_cost_path)
    if config.fuels.enabled:
        for path in (config.fuels.converters_path, config.fuels.storage_path, config.fuels.demand_path):
            if path is not None:
                paths.append(path)
    for sector in config.sectors:
        if sector == "transport":
            continue
        paths.extend(getattr(config, sector).paths.values())
    if config.transport.enabled:
        paths.extend(config.transport.paths.values())
    unique: dict[str, Path] = {}
    for path in paths:
        unique[str(path)] = path
    return list(unique.values())


def _source_scopes(config: SystemLoadConfig) -> dict[Path, str]:
    scopes: dict[Path, str] = {}
    if config.shared.buses_path is not None:
        scopes[config.shared.buses_path] = "shared:buses"
    if config.shared.fuel_cost_path is not None:
        scopes[config.shared.fuel_cost_path] = "shared:fuel_cost"
    if config.fuels.enabled:
        if config.fuels.converters_path is not None:
            scopes[config.fuels.converters_path] = "shared:fuels_converters"
        if config.fuels.storage_path is not None:
            scopes[config.fuels.storage_path] = "shared:fuels_storage"
        if config.fuels.demand_path is not None:
            scopes[config.fuels.demand_path] = "shared:fuels_demand"
    for sector in ("electricity", "heating", "cooling", "industry"):
        sector_config = getattr(config, sector)
        if not sector_config.enabled:
            continue
        for path in sector_config.paths.values():
            scopes[path] = f"sector:{sector}"
    if config.transport.enabled:
        for path in config.transport.paths.values():
            scopes[path] = "transport"
    return scopes


def _build_from_config(config: SystemLoadConfig, *, table_cache: Optional[TableCache] = None) -> object:
    cache: TableCache = table_cache if table_cache is not None else LoaderCache()
    transport_kwargs = _transport_kwargs(config)
    system = None
    for sector in SECTOR_ORDER:
        sector_config = getattr(config, sector)
        if not sector_config.enabled:
            continue
        kwargs = _sector_kwargs(config, sector)
        if sector == "electricity" and transport_kwargs:
            kwargs.update(transport_kwargs)
            kwargs["write_transport_storage_units"] = config.transport.write_transport_storage_units
        if system is not None:
            kwargs["existing_system"] = system
        kwargs["table_cache"] = cache
        system = load_sector(**kwargs)
    return system


def _build_incremental_from_config(
    config: SystemLoadConfig,
    *,
    existing_system: object,
    sectors_to_rebuild: Sequence[str],
    table_cache: Optional[TableCache] = None,
) -> object:
    cache: TableCache = table_cache if table_cache is not None else LoaderCache()
    transport_kwargs = _transport_kwargs(config)
    rebuild_set = set(str(sector) for sector in sectors_to_rebuild)
    system = existing_system
    for sector in SECTOR_ORDER:
        sector_config = getattr(config, sector)
        if not sector_config.enabled or sector not in rebuild_set:
            continue
        kwargs = _sector_kwargs(config, sector)
        if sector == "electricity" and transport_kwargs:
            kwargs.update(transport_kwargs)
            kwargs["write_transport_storage_units"] = config.transport.write_transport_storage_units
        kwargs["existing_system"] = system
        kwargs["table_cache"] = cache
        system = load_sector(**kwargs)
    return system


def load_system_from_config(
    config: SystemLoadConfig | Mapping[str, Any],
    *,
    table_cache: Optional[TableCache] = None,
) -> object:
    normalized = config if isinstance(config, SystemLoadConfig) else build_system_load_config(config)

    def build_raw() -> object:
        with _validation_mode(normalized.shared.validation_mode):
            return _build_from_config(normalized, table_cache=table_cache)

    def build_incremental(existing_system: object, sectors: Sequence[str]) -> object:
        with _validation_mode(normalized.shared.validation_mode):
            return _build_incremental_from_config(
                normalized,
                existing_system=existing_system,
                sectors_to_rebuild=sectors,
                table_cache=table_cache,
            )

    if normalized.shared.use_prebuilt:
        return load_or_build_prebuilt_system(
            path=normalized.shared.prebuilt_dir,
            build_fn=build_raw,
            source_paths=_source_paths(normalized),
            source_scopes=_source_scopes(normalized),
            validation_mode=normalized.shared.validation_mode,
            validate=normalized.shared.validation_mode == "strict",
            overwrite_if_stale=normalized.shared.rebuild_prebuilt_if_stale,
            incremental_build_fn=build_incremental,
        )
    return build_raw()


def load_system(
    *,
    sectors: Union[str, Iterable[str]],
    electricity_paths: Dict[str, PathLike],
    heating_paths: Optional[Dict[str, PathLike]] = None,
    cooling_paths: Optional[Dict[str, PathLike]] = None,
    industry_paths: Optional[Dict[str, PathLike]] = None,
    fuels_paths: Optional[Dict[str, PathLike]] = None,
    transport_paths: Optional[Dict[str, PathLike]] = None,
    write_transport_storage_units: bool = True,
    buses_path: Optional[PathLike] = None,
    fuel_cost_path: Optional[PathLike] = None,
    table_cache: Optional[TableCache] = None,
) -> object:
    """Compatibility wrapper over config-driven system loading."""
    config = build_legacy_system_config(
        sectors=sectors,
        electricity_paths=electricity_paths,
        heating_paths=heating_paths,
        cooling_paths=cooling_paths,
        industry_paths=industry_paths,
        fuels_paths=fuels_paths,
        transport_paths=transport_paths,
        write_transport_storage_units=write_transport_storage_units,
        buses_path=buses_path,
        fuel_cost_path=fuel_cost_path,
    )
    return load_system_from_config(config, table_cache=table_cache)
