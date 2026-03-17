"""Persist, validate, and reuse assembled prebuilt SystemParameters datasets."""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Sequence, Union

import pandas as pd

from data_models.Bus import Bus
from data_models.Demand import Demand
from data_models.Generators import Generators
from data_models.StorageUnits import StorageUnits
from data_models.Transport import Transport
from data_models.SystemParameters import MarketParams, PolicyParams, SystemParameters
from data_models.SystemSets import SystemSets

PathLike = Union[str, Path]
PREBUILT_SYSTEM_FORMAT_VERSION = 4
PREBUILT_SYSTEM_FORMATS = {"h2res-prebuilt-system", "h2res-system-snapshot"}
CURRENT_INCREMENTAL_FORMAT_VERSION = PREBUILT_SYSTEM_FORMAT_VERSION


@dataclass(frozen=True, slots=True)
class PrebuiltRefreshAnalysis:
    changed_paths: tuple[str, ...]
    scope: str
    sectors: tuple[str, ...]
    reason: str
    supports_incremental: bool


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_table(path: Path, frame: pd.DataFrame, *, index_name: str | None = None) -> None:
    table = frame.reset_index() if index_name is not None and frame.index.name == index_name else frame.copy()
    table.to_parquet(path, index=False)


def _read_table(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _restore_index(frame: pd.DataFrame, index_name: str) -> pd.DataFrame:
    if index_name in frame.columns:
        return frame.set_index(index_name, drop=True)
    frame = frame.copy()
    frame.index.name = index_name
    return frame


def _source_metadata(path: Path) -> Dict[str, Any]:
    stat = path.stat()
    return {
        "path": path.as_posix(),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _normalize_source_paths(source_paths: Iterable[PathLike] | None) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for source in source_paths or ():
        path = Path(source)
        if path.exists() and path.is_file():
            metadata = _source_metadata(path)
            result[metadata["path"]] = metadata
    return result


def _normalize_source_scopes(source_scopes: Mapping[PathLike, str] | None) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for raw_path, scope in (source_scopes or {}).items():
        result[Path(raw_path).as_posix()] = str(scope)
    return result


def _load_prebuilt_system_unvalidated(path: Path) -> SystemParameters:
    sets = SystemSets.model_construct(**_read_json(path / "sets.json"))
    market = MarketParams.model_construct(**_read_json(path / "market.json"))
    policy = PolicyParams.model_construct(**_read_json(path / "policy.json"))

    bus = Bus.model_construct(
        static=_restore_index(_read_table(path / "bus_static.parquet"), "bus"),
        attachments=_read_table(path / "bus_attachments.parquet"),
    )
    generators = Generators.model_construct(
        static=_restore_index(_read_table(path / "generators_static.parquet"), "unit"),
        p_t=_read_table(path / "generators_p_t.parquet"),
        var_cost=_read_table(path / "generators_var_cost.parquet"),
        efficiency_ts=_read_table(path / "generators_efficiency_ts.parquet"),
    )
    storage = StorageUnits.model_construct(
        static=_restore_index(_read_table(path / "storage_static.parquet"), "unit"),
        inflow=_read_table(path / "storage_inflow.parquet"),
        availability=_read_table(path / "storage_availability.parquet"),
        e_nom_ts=_read_table(path / "storage_e_nom_ts.parquet"),
        investment_costs=_read_table(path / "storage_investment_costs.parquet"),
    )
    demand = Demand.model_construct(p_t=_read_table(path / "demand_p_t.parquet"))
    transport = Transport.model_construct(
        static=_restore_index(_read_table(path / "transport_static.parquet"), "unit"),
        availability=_read_table(path / "transport_availability.parquet"),
        demand_profile=_read_table(path / "transport_demand_profile.parquet"),
        demand=_read_table(path / "transport_demand.parquet"),
    )

    return SystemParameters.model_construct(
        sets=sets,
        buses=bus,
        generators=generators,
        storage_units=storage,
        demands=demand,
        transport_units=transport,
        market=market,
        policy=policy,
    )


def save_prebuilt_system(
    system: SystemParameters,
    path: PathLike,
    *,
    overwrite: bool = False,
    source_paths: Iterable[PathLike] | None = None,
    source_scopes: Mapping[PathLike, str] | None = None,
    validation_mode: str | None = None,
) -> Path:
    """Save an assembled prebuilt system as Parquet tables plus JSON metadata."""
    out_dir = Path(path)
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists() and not overwrite:
        raise FileExistsError(f"Prebuilt system already exists at '{out_dir}'. Pass overwrite=True to replace it.")

    parent = out_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    temp_dir = parent / f"{out_dir.name}.tmp-{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True, exist_ok=False)

    _write_json(temp_dir / "sets.json", system.sets.model_dump(mode="json"))
    _write_json(temp_dir / "market.json", system.market.model_dump(mode="json"))
    _write_json(temp_dir / "policy.json", system.policy.model_dump(mode="json"))

    _write_table(temp_dir / "bus_static.parquet", system.buses.static, index_name="bus")
    _write_table(temp_dir / "bus_attachments.parquet", system.buses.attachments)

    _write_table(temp_dir / "generators_static.parquet", system.generators.static, index_name="unit")
    _write_table(temp_dir / "generators_p_t.parquet", system.generators.p_t)
    _write_table(temp_dir / "generators_var_cost.parquet", system.generators.var_cost)
    _write_table(temp_dir / "generators_efficiency_ts.parquet", system.generators.efficiency_ts)

    _write_table(temp_dir / "storage_static.parquet", system.storage_units.static, index_name="unit")
    _write_table(temp_dir / "storage_inflow.parquet", system.storage_units.inflow)
    _write_table(temp_dir / "storage_availability.parquet", system.storage_units.availability)
    _write_table(temp_dir / "storage_e_nom_ts.parquet", system.storage_units.e_nom_ts)
    _write_table(temp_dir / "storage_investment_costs.parquet", system.storage_units.investment_costs)

    _write_table(temp_dir / "demand_p_t.parquet", system.demands.p_t)
    _write_table(temp_dir / "transport_static.parquet", system.transport_units.static, index_name="unit")
    _write_table(temp_dir / "transport_availability.parquet", system.transport_units.availability)
    _write_table(temp_dir / "transport_demand_profile.parquet", system.transport_units.demand_profile)
    _write_table(temp_dir / "transport_demand.parquet", system.transport_units.demand)
    _write_json(
        temp_dir / "manifest.json",
        {
            "format": "h2res-prebuilt-system",
            "format_version": PREBUILT_SYSTEM_FORMAT_VERSION,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "validation_mode": validation_mode,
            "source_files": _normalize_source_paths(source_paths),
            "source_scopes": _normalize_source_scopes(source_scopes),
        },
    )

    backup_dir: Path | None = None
    try:
        if out_dir.exists():
            backup_dir = out_dir.with_name(f"{out_dir.name}.backup-{uuid.uuid4().hex}")
            out_dir.rename(backup_dir)
        temp_dir.rename(out_dir)
    except Exception:
        if backup_dir is not None and backup_dir.exists() and not out_dir.exists():
            backup_dir.rename(out_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    else:
        if backup_dir is not None:
            shutil.rmtree(backup_dir, ignore_errors=True)
    return out_dir


def _load_prebuilt_system_validated(in_dir: Path) -> SystemParameters:
    sets = SystemSets(**_read_json(in_dir / "sets.json"))
    market = MarketParams(**_read_json(in_dir / "market.json"))
    policy = PolicyParams(**_read_json(in_dir / "policy.json"))

    bus = Bus(
        static=_read_table(in_dir / "bus_static.parquet"),
        attachments=_read_table(in_dir / "bus_attachments.parquet"),
    )
    generators = Generators(
        static=_read_table(in_dir / "generators_static.parquet"),
        p_t=_read_table(in_dir / "generators_p_t.parquet"),
        var_cost=_read_table(in_dir / "generators_var_cost.parquet"),
        efficiency_ts=_read_table(in_dir / "generators_efficiency_ts.parquet"),
    )
    storage = StorageUnits(
        static=_read_table(in_dir / "storage_static.parquet"),
        inflow=_read_table(in_dir / "storage_inflow.parquet"),
        availability=_read_table(in_dir / "storage_availability.parquet"),
        e_nom_ts=_read_table(in_dir / "storage_e_nom_ts.parquet"),
        investment_costs=_read_table(in_dir / "storage_investment_costs.parquet"),
    )
    demand = Demand(p_t=_read_table(in_dir / "demand_p_t.parquet"))
    transport = Transport(
        static=_read_table(in_dir / "transport_static.parquet"),
        availability=_read_table(in_dir / "transport_availability.parquet"),
        demand_profile=_read_table(in_dir / "transport_demand_profile.parquet"),
        demand=_read_table(in_dir / "transport_demand.parquet"),
    )

    return SystemParameters(
        sets=sets,
        buses=bus,
        generators=generators,
        storage_units=storage,
        demands=demand,
        transport_units=transport,
        market=market,
        policy=policy,
    )


def _read_manifest(path: Path) -> Dict[str, Any]:
    manifest = _read_json(path / "manifest.json")
    if manifest.get("format") not in PREBUILT_SYSTEM_FORMATS:
        raise ValueError(f"Unsupported prebuilt system format in '{path}'.")
    version = int(manifest.get("format_version", -1))
    if version not in {1, 2, 3, PREBUILT_SYSTEM_FORMAT_VERSION}:
        raise ValueError(f"Unsupported prebuilt system format version in '{path}': {manifest.get('format_version')}")
    return manifest


def analyze_prebuilt_refresh(
    path: PathLike,
    *,
    source_paths: Iterable[PathLike] | None = None,
    source_scopes: Mapping[PathLike, str] | None = None,
) -> PrebuiltRefreshAnalysis:
    in_dir = Path(path)
    manifest_path = in_dir / "manifest.json"
    current_sources = _normalize_source_paths(source_paths)
    current_scopes = _normalize_source_scopes(source_scopes)
    if not manifest_path.exists():
        return PrebuiltRefreshAnalysis(
            changed_paths=tuple(sorted(current_sources)),
            scope="full",
            sectors=(),
            reason="prebuilt snapshot missing",
            supports_incremental=False,
        )

    try:
        manifest = _read_manifest(in_dir)
    except Exception as exc:
        return PrebuiltRefreshAnalysis(
            changed_paths=tuple(sorted(current_sources)),
            scope="full",
            sectors=(),
            reason=f"invalid prebuilt manifest: {exc}",
            supports_incremental=False,
        )

    cached_sources = manifest.get("source_files", {})
    changed: set[str] = set()
    if set(cached_sources) != set(current_sources):
        changed.update(set(cached_sources).symmetric_difference(set(current_sources)))
    for key, current in current_sources.items():
        cached = cached_sources.get(key, {})
        if int(cached.get("size", -1)) != int(current["size"]) or int(cached.get("mtime_ns", -1)) != int(current["mtime_ns"]):
            changed.add(key)

    if not changed:
        return PrebuiltRefreshAnalysis(
            changed_paths=(),
            scope="none",
            sectors=(),
            reason="prebuilt snapshot is current",
            supports_incremental=True,
        )

    version = int(manifest.get("format_version", -1))
    if version != CURRENT_INCREMENTAL_FORMAT_VERSION:
        return PrebuiltRefreshAnalysis(
            changed_paths=tuple(sorted(changed)),
            scope="full",
            sectors=(),
            reason=f"stale legacy prebuilt format version {version}",
            supports_incremental=False,
        )

    affected_sectors: set[str] = set()
    transport_changed = False
    for changed_path in changed:
        scope = current_scopes.get(changed_path) or str(manifest.get("source_scopes", {}).get(changed_path, ""))
        if not scope:
            return PrebuiltRefreshAnalysis(
                changed_paths=tuple(sorted(changed)),
                scope="full",
                sectors=(),
                reason=f"changed file has no rebuild scope: {changed_path}",
                supports_incremental=False,
            )
        if scope.startswith("shared:"):
            return PrebuiltRefreshAnalysis(
                changed_paths=tuple(sorted(changed)),
                scope="full",
                sectors=(),
                reason=f"shared input changed: {changed_path}",
                supports_incremental=False,
            )
        if scope == "transport":
            transport_changed = True
            affected_sectors.add("electricity")
            continue
        if scope.startswith("sector:"):
            affected_sectors.add(scope.split(":", 1)[1])
            continue
        return PrebuiltRefreshAnalysis(
            changed_paths=tuple(sorted(changed)),
            scope="full",
            sectors=(),
            reason=f"unknown rebuild scope '{scope}' for {changed_path}",
            supports_incremental=False,
        )

    sectors = tuple(sorted(affected_sectors, key=lambda name: ("electricity", "heating", "cooling", "industry").index(name)))
    if not sectors:
        return PrebuiltRefreshAnalysis(
            changed_paths=tuple(sorted(changed)),
            scope="full",
            sectors=(),
            reason="unable to derive affected sectors",
            supports_incremental=False,
        )
    if transport_changed and sectors == ("electricity",):
        return PrebuiltRefreshAnalysis(
            changed_paths=tuple(sorted(changed)),
            scope="electricity+transport",
            sectors=sectors,
            reason="transport inputs changed, rebuild electricity with transport integration",
            supports_incremental=True,
        )
    if len(sectors) == 1:
        return PrebuiltRefreshAnalysis(
            changed_paths=tuple(sorted(changed)),
            scope=f"sector:{sectors[0]}",
            sectors=sectors,
            reason=f"only {sectors[0]} inputs changed",
            supports_incremental=True,
        )
    return PrebuiltRefreshAnalysis(
        changed_paths=tuple(sorted(changed)),
        scope="multi-sector",
        sectors=sectors,
        reason=f"multiple sector inputs changed: {', '.join(sectors)}",
        supports_incremental=True,
    )


def load_prebuilt_system(path: PathLike, *, validate: bool = False) -> SystemParameters:
    """Load a prebuilt SystemParameters dataset from Parquet tables plus JSON metadata."""
    in_dir = Path(path)
    _read_manifest(in_dir)
    if validate:
        return _load_prebuilt_system_validated(in_dir)
    return _load_prebuilt_system_unvalidated(in_dir)


def is_prebuilt_system_stale(path: PathLike, *, source_paths: Iterable[PathLike] | None = None) -> bool:
    """Return True when the prebuilt system is missing or out of date."""
    return analyze_prebuilt_refresh(path, source_paths=source_paths).scope != "none"


def load_or_build_prebuilt_system(
    *,
    path: PathLike,
    build_fn: Callable[[], SystemParameters],
    source_paths: Iterable[PathLike] | None = None,
    source_scopes: Mapping[PathLike, str] | None = None,
    validation_mode: str | None = None,
    validate: bool = False,
    overwrite_if_stale: bool = True,
    incremental_build_fn: Callable[[SystemParameters, Sequence[str]], SystemParameters] | None = None,
) -> SystemParameters:
    """Load a current prebuilt system or rebuild and persist it."""
    prebuilt_path = Path(path)
    analysis = analyze_prebuilt_refresh(
        prebuilt_path,
        source_paths=source_paths,
        source_scopes=source_scopes,
    )
    if analysis.scope == "none":
        return load_prebuilt_system(prebuilt_path, validate=validate)

    if analysis.supports_incremental and incremental_build_fn is not None and prebuilt_path.exists():
        existing = load_prebuilt_system(prebuilt_path, validate=validate)
        system = incremental_build_fn(existing, analysis.sectors)
    else:
        system = build_fn()
    if overwrite_if_stale:
        save_prebuilt_system(
            system,
            prebuilt_path,
            overwrite=True,
            source_paths=source_paths,
            source_scopes=source_scopes,
            validation_mode=validation_mode,
        )
    return system
