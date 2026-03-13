from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

SectorName = Literal["electricity", "heating", "cooling", "industry", "transport"]
ValidationMode = Literal["fast", "strict", "off"]

ALLOWED_SECTORS: tuple[SectorName, ...] = ("electricity", "heating", "cooling", "industry", "transport")


@dataclass(frozen=True)
class SharedConfig:
    data_dir: Path = Path("data")
    buses_path: Path | None = None
    fuel_cost_path: Path | None = None
    prebuilt_dir: Path = Path("data/prebuilt_system")
    validation_mode: ValidationMode = "fast"
    use_prebuilt: bool = True
    rebuild_prebuilt_if_stale: bool = True


@dataclass(frozen=True)
class SectorConfig:
    enabled: bool
    paths: dict[str, Path] = field(default_factory=dict)
    demand_carriers: tuple[str, ...] = ()


@dataclass(frozen=True)
class TransportConfig:
    enabled: bool = False
    paths: dict[str, Path] = field(default_factory=dict)
    write_transport_storage_units: bool = True


@dataclass(frozen=True)
class FuelsConfig:
    enabled: bool = False
    converters_path: Path | None = None
    storage_path: Path | None = None
    demand_path: Path | None = None
    demand_carrier: str = "hydrogen"


@dataclass(frozen=True)
class SystemLoadConfig:
    sectors: tuple[SectorName, ...]
    shared: SharedConfig
    electricity: SectorConfig
    heating: SectorConfig
    cooling: SectorConfig
    industry: SectorConfig
    fuels: FuelsConfig
    transport: TransportConfig

    def sector_config(self, sector: SectorName) -> SectorConfig:
        return getattr(self, sector)


SECTOR_DEFAULTS: dict[SectorName, dict[str, str]] = {
    "electricity": {
        "powerplants_path": "electricity/powerplants.csv",
        "storage_path": "electricity/storage_units.csv",
        "renewable_profiles_path": "electricity/res_profile.csv",
        "inflow_path": "electricity/scaled_inflows.csv",
        "electricity_demand_path": "electricity/electricity_demand.csv",
    },
    "heating": {
        "powerplants_path": "heating/converters.csv",
        "storage_path": "heating/storage_units.csv",
        "efficiency_ts_path": "heating/COP.csv",
        "heating_demand_path": "heating/heat_demand.csv",
    },
    "cooling": {
        "powerplants_path": "cooling/converters.csv",
        "storage_path": "cooling/storage_units.csv",
        "efficiency_ts_path": "cooling/COP.csv",
        "cooling_demand_path": "cooling/cooling_demand.csv",
    },
    "industry": {
        "powerplants_path": "industry/converters.csv",
        "storage_path": "industry/storage_units.csv",
        "industry_demand_path": "industry/industry_demand.csv",
    },
    "transport": {
    "transport_general_parameters_path": "transport/transport_general_parameters.xlsx",
    "transport_fleet_and_demand_path": "transport/transport_fleet_and_demand.xlsx",
    "transport_availability_path": "transport/transport_storage_availability.csv",
    "transport_demand_timeseries_path": "transport/transport_demand_timeseries.csv",
    }
}

SECTOR_DEMAND_CARRIERS: dict[SectorName, tuple[str, ...]] = {
    "electricity": ("electricity",),
    "heating": ("heat",),
    "cooling": ("cooling",),
    "industry": ("industry_heat",),
    "transport": (),
}

TRANSPORT_DEFAULTS = {
    "transport_general_parameters_path": "transport/transport_general_parameters.xlsx",
    "transport_fleet_and_demand_path": "transport/transport_fleet_and_demand.xlsx",
    "transport_availability_path": "transport/transport_storage_availability.csv",
    "transport_demand_timeseries_path": "transport/transport_demand_timeseries.csv",
}


def _normalize_sector_list(sectors: str | Iterable[str]) -> tuple[SectorName, ...]:
    values = [sectors] if isinstance(sectors, str) else list(sectors)
    normalized: list[SectorName] = []
    seen: set[str] = set()
    for value in values:
        key = str(value).strip().lower()
        if key not in ALLOWED_SECTORS:
            raise ValueError(f"Unknown sectors: {key}. Allowed: {sorted(ALLOWED_SECTORS)}")
        if key not in seen:
            normalized.append(key)  # type: ignore[arg-type]
            seen.add(key)
    return tuple(normalized)


def _resolve_path(value: Any, *, data_dir: Path) -> Path | None:
    if value is None or value == "":
        return None
    path = Path(value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == data_dir.name:
        return path
    return data_dir / path


def _normalize_sector_config(
    sector: SectorName,
    mapping: Mapping[str, Any] | None,
    *,
    data_dir: Path,
    enabled: bool,
) -> SectorConfig:
    raw = dict(mapping or {})
    defaults = SECTOR_DEFAULTS[sector]
    paths: dict[str, Path] = {}
    for key, rel_path in defaults.items():
        resolved = _resolve_path(raw.get(key, rel_path if enabled else None), data_dir=data_dir)
        if resolved is not None:
            paths[key] = resolved
    for key, value in raw.items():
        if key in paths:
            continue
        resolved = _resolve_path(value, data_dir=data_dir)
        if resolved is not None:
            paths[key] = resolved
    return SectorConfig(enabled=enabled, paths=paths, demand_carriers=SECTOR_DEMAND_CARRIERS[sector])


def _normalize_transport_config(
    mapping: Mapping[str, Any] | None,
    *,
    data_dir: Path,
    enabled: bool,
) -> TransportConfig:
    raw = dict(mapping or {})
    write_transport_storage_units = bool(raw.pop("write_transport_storage_units", True))
    if "mode" in raw:
        raw.pop("mode")
    if "canonical" in raw:
        raw.update(dict(raw.pop("canonical") or {}))
    if "raw" in raw:
        raw.update(dict(raw.pop("raw") or {}))
    paths = {
        key: resolved
        for key, value in {**TRANSPORT_DEFAULTS, **raw}.items()
        if (resolved := _resolve_path(value if enabled else None, data_dir=data_dir)) is not None
    }
    if enabled:
        required = set(TRANSPORT_DEFAULTS)
        missing = required - set(paths)
        if missing:
            raise ValueError(f"Missing transport paths: {sorted(missing)}")
    return TransportConfig(
        enabled=enabled,
        paths=paths,
        write_transport_storage_units=write_transport_storage_units,
    )


def _normalize_fuels_config(
    mapping: Mapping[str, Any] | None,
    *,
    data_dir: Path,
) -> FuelsConfig:
    raw = dict(mapping or {})
    enabled = bool(raw.get("enabled", False))
    return FuelsConfig(
        enabled=enabled,
        converters_path=_resolve_path(raw.get("converters_path", "fuels/converters.csv" if enabled else None), data_dir=data_dir),
        storage_path=_resolve_path(raw.get("storage_path", "fuels/storage_units.csv" if enabled else None), data_dir=data_dir),
        demand_path=_resolve_path(raw.get("demand_path", "fuels/demand.csv" if enabled else None), data_dir=data_dir),
        demand_carrier=str(raw.get("demand_carrier", "hydrogen")).strip() or "hydrogen",
    )


def build_system_load_config(config: Mapping[str, Any]) -> SystemLoadConfig:
    shared_raw = dict(config.get("shared", {}))
    data_dir = Path(shared_raw.get("data_dir", "data"))
    shared = SharedConfig(
        data_dir=data_dir,
        buses_path=_resolve_path(shared_raw.get("buses_path", "buses.csv"), data_dir=data_dir),
        fuel_cost_path=_resolve_path(shared_raw.get("fuel_cost_path", "fuel_cost.csv"), data_dir=data_dir),
        prebuilt_dir=_resolve_path(shared_raw.get("prebuilt_dir", "prebuilt_system"), data_dir=data_dir) or Path("data/prebuilt_system"),
        validation_mode=str(shared_raw.get("validation_mode", "fast")).strip().lower() or "fast",  # type: ignore[arg-type]
        use_prebuilt=bool(shared_raw.get("use_prebuilt", True)),
        rebuild_prebuilt_if_stale=bool(shared_raw.get("rebuild_prebuilt_if_stale", True)),
    )
    sectors = _normalize_sector_list(config.get("sectors", ("electricity",)))
    transport_enabled = "transport" in sectors
    if transport_enabled and "electricity" not in sectors:
        raise ValueError("transport requires electricity sector to be loaded.")

    return SystemLoadConfig(
        sectors=sectors,
        shared=shared,
        electricity=_normalize_sector_config("electricity", config.get("electricity"), data_dir=data_dir, enabled="electricity" in sectors),
        heating=_normalize_sector_config("heating", config.get("heating"), data_dir=data_dir, enabled="heating" in sectors),
        cooling=_normalize_sector_config("cooling", config.get("cooling"), data_dir=data_dir, enabled="cooling" in sectors),
        industry=_normalize_sector_config("industry", config.get("industry"), data_dir=data_dir, enabled="industry" in sectors),
        fuels=_normalize_fuels_config(config.get("fuels"), data_dir=data_dir),
        transport=_normalize_transport_config(config.get("transport"), data_dir=data_dir, enabled=transport_enabled),
    )


def build_legacy_system_config(
    *,
    sectors: str | Iterable[str],
    electricity_paths: Mapping[str, Any],
    heating_paths: Mapping[str, Any] | None = None,
    cooling_paths: Mapping[str, Any] | None = None,
    industry_paths: Mapping[str, Any] | None = None,
    fuels_paths: Mapping[str, Any] | None = None,
    transport_paths: Mapping[str, Any] | None = None,
    write_transport_storage_units: bool = True,
    buses_path: Any = None,
    fuel_cost_path: Any = None,
) -> SystemLoadConfig:
    return build_system_load_config(
        {
            "sectors": sectors,
            "shared": {
                "buses_path": buses_path,
                "fuel_cost_path": fuel_cost_path,
            },
            "electricity": dict(electricity_paths),
            "heating": dict(heating_paths or {}),
            "cooling": dict(cooling_paths or {}),
            "industry": dict(industry_paths or {}),
            "fuels": dict(fuels_paths or {}),
            "transport": {
                **(dict(transport_paths or {})),
                "write_transport_storage_units": write_transport_storage_units,
            },
        }
    )
