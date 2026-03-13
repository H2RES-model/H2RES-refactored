from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Hashable, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd

PathLike = Union[str, Path]
CacheKey = Tuple[Hashable, ...]
LegacyTableCache = Dict[CacheKey, object]
TableCache = Union["LoaderCache", MutableMapping[CacheKey, object]]


@dataclass(slots=True)
class TableInspection:
    resolved_path: Path
    suffix: str
    columns: Tuple[str, ...]


@dataclass(slots=True)
class LoaderCache:
    resolved_paths: Dict[str, Path] = field(default_factory=dict)
    inspections: Dict[str, TableInspection] = field(default_factory=dict)
    tables: Dict[CacheKey, object] = field(default_factory=dict)


def _norm_columns(columns: Optional[Sequence[str]]) -> Optional[Tuple[str, ...]]:
    if columns is None:
        return None
    return tuple(str(c) for c in columns)


def _table_key(
    resolved: Path,
    *,
    nrows: Optional[int],
    columns: Optional[Tuple[str, ...]],
) -> CacheKey:
    return ("table", str(resolved), nrows, columns)


def _source_key(resolved: Path) -> CacheKey:
    return ("source", str(resolved))


def _legacy_columns_key(resolved: Path) -> CacheKey:
    return ("columns", str(resolved))


def _legacy_resolved_key(raw_path: PathLike) -> CacheKey:
    return ("resolved-path", str(Path(raw_path)))


def _as_loader_cache(cache: Optional[TableCache]) -> Optional[LoaderCache]:
    if cache is None:
        return None
    if isinstance(cache, LoaderCache):
        return cache
    if isinstance(cache, MutableMapping):
        resolved_paths: Dict[str, Path] = {}
        inspections: Dict[str, TableInspection] = {}
        tables: Dict[CacheKey, object] = {}
        for key, value in cache.items():
            if not isinstance(key, tuple) or not key:
                continue
            kind = key[0]
            if kind == "resolved-path" and len(key) == 2 and isinstance(value, Path):
                resolved_paths[str(key[1])] = value
            elif kind == "columns" and len(key) == 2 and isinstance(value, list):
                path = Path(str(key[1]))
                inspections[str(path)] = TableInspection(
                    resolved_path=path,
                    suffix=path.suffix.lower(),
                    columns=tuple(str(col) for col in value),
                )
            elif kind == "table":
                tables[key] = value
            elif kind == "source":
                tables[key] = value
        loader_cache = LoaderCache(
            resolved_paths=resolved_paths,
            inspections=inspections,
            tables=tables,
        )
        cache.clear()
        cache.update(loader_cache.tables)
        for raw_path, resolved in loader_cache.resolved_paths.items():
            cache[_legacy_resolved_key(raw_path)] = resolved
        for inspection in loader_cache.inspections.values():
            cache[_legacy_columns_key(inspection.resolved_path)] = list(inspection.columns)
        return loader_cache
    raise TypeError("Unsupported table cache type.")


def _sync_legacy_cache(cache: Optional[TableCache], loader_cache: Optional[LoaderCache]) -> None:
    if cache is None or loader_cache is None or isinstance(cache, LoaderCache):
        return
    cache.clear()
    cache.update(loader_cache.tables)
    for raw_path, resolved in loader_cache.resolved_paths.items():
        cache[_legacy_resolved_key(raw_path)] = resolved
    for inspection in loader_cache.inspections.values():
        cache[_legacy_columns_key(inspection.resolved_path)] = list(inspection.columns)


def _inspect_csv_columns(path: Path) -> Tuple[str, ...]:
    return tuple(str(col) for col in pd.read_csv(path, nrows=0).columns)


def _inspect_excel_columns(path: Path) -> Tuple[str, ...]:
    return tuple(str(col) for col in pd.read_excel(path, nrows=0).columns)


def _inspect_parquet_columns(path: Path) -> Tuple[str, ...]:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        return tuple(str(col) for col in pd.read_parquet(path).columns)
    return tuple(str(col) for col in pq.ParquetFile(path).schema.names)


def _inspect_feather_columns(path: Path) -> Tuple[str, ...]:
    try:
        import pyarrow.feather as pf  # type: ignore
    except Exception:
        return tuple(str(col) for col in pd.read_feather(path).columns)
    return tuple(str(col) for col in pf.read_table(path, columns=[]).schema.names)


def _read_available_columns(path: Path) -> Tuple[str, ...]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _inspect_csv_columns(path)
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return _inspect_excel_columns(path)
    if suffix == ".parquet":
        return _inspect_parquet_columns(path)
    if suffix == ".feather":
        return _inspect_feather_columns(path)
    raise ValueError(f"Unsupported file type: {path}")


def resolve_table_path(path: PathLike, *, cache: Optional[TableCache] = None) -> Path:
    raw_path = str(Path(path))
    loader_cache = _as_loader_cache(cache)
    if loader_cache is not None and raw_path in loader_cache.resolved_paths:
        return loader_cache.resolved_paths[raw_path]

    p = Path(path)
    candidates = [p]
    suffix = p.suffix.lower()
    if suffix in {".csv", ".parquet", ".feather"}:
        candidates.append(p.with_suffix(".parquet" if suffix == ".csv" else ".csv"))
    elif suffix in {".xlsx", ".xls", ".xlsm"}:
        candidates.extend(p.with_suffix(ext) for ext in (".xlsx", ".xlsm", ".xls") if ext != suffix)
    elif suffix == "":
        candidates.extend(p.with_suffix(ext) for ext in (".csv", ".parquet", ".feather", ".xlsx", ".xlsm", ".xls"))

    for candidate in candidates:
        if candidate.exists():
            if loader_cache is not None:
                loader_cache.resolved_paths[raw_path] = candidate
                _sync_legacy_cache(cache, loader_cache)
            return candidate

    raise FileNotFoundError(f"Could not find data file for '{path}' (csv/parquet/feather/excel).")


def inspect_table(path: PathLike, *, cache: Optional[TableCache] = None) -> TableInspection:
    loader_cache = _as_loader_cache(cache)
    resolved = resolve_table_path(path, cache=loader_cache)
    cache_key = str(resolved)
    if loader_cache is not None and cache_key in loader_cache.inspections:
        return loader_cache.inspections[cache_key]

    if loader_cache is not None:
        source = loader_cache.tables.get(_source_key(resolved))
        if isinstance(source, pd.DataFrame):
            inspection = TableInspection(
                resolved_path=resolved,
                suffix=resolved.suffix.lower(),
                columns=tuple(str(col) for col in source.columns),
            )
            loader_cache.inspections[cache_key] = inspection
            _sync_legacy_cache(cache, loader_cache)
            return inspection

    inspection = TableInspection(
        resolved_path=resolved,
        suffix=resolved.suffix.lower(),
        columns=_read_available_columns(resolved),
    )
    if loader_cache is not None:
        loader_cache.inspections[cache_key] = inspection
        _sync_legacy_cache(cache, loader_cache)
    return inspection


def read_table(
    path: PathLike,
    *,
    nrows: Optional[int] = None,
    columns: Optional[Sequence[str]] = None,
    cache: Optional[TableCache] = None,
    mutable: bool = False,
) -> pd.DataFrame:
    loader_cache = _as_loader_cache(cache)
    inspection = inspect_table(path, cache=loader_cache)
    use_columns = _norm_columns(columns)
    key = _table_key(inspection.resolved_path, nrows=nrows, columns=use_columns)
    if loader_cache is not None and key in loader_cache.tables:
        cached = loader_cache.tables[key]
        if not isinstance(cached, pd.DataFrame):
            raise TypeError("Invalid table cache entry type.")
        return cached.copy(deep=False) if mutable else cached

    source_key = _source_key(inspection.resolved_path)
    if loader_cache is not None and nrows is None and use_columns is not None and source_key in loader_cache.tables:
        source = loader_cache.tables[source_key]
        if not isinstance(source, pd.DataFrame):
            raise TypeError("Invalid source cache entry type.")
        missing = [column for column in use_columns if column not in source.columns]
        if not missing:
            table = source.loc[:, list(use_columns)]
            loader_cache.tables[key] = table
            _sync_legacy_cache(cache, loader_cache)
            return table.copy(deep=False) if mutable else table
    if loader_cache is not None and nrows is None and use_columns is not None:
        for cached_key, cached_value in loader_cache.tables.items():
            if not isinstance(cached_key, tuple) or len(cached_key) != 4:
                continue
            kind, resolved_str, cached_nrows, cached_columns = cached_key
            if kind != "table" or resolved_str != str(inspection.resolved_path) or cached_nrows is not None:
                continue
            if not isinstance(cached_value, pd.DataFrame):
                continue
            if all(column in cached_value.columns for column in use_columns):
                table = cached_value.loc[:, list(use_columns)]
                loader_cache.tables[key] = table
                _sync_legacy_cache(cache, loader_cache)
                return table.copy(deep=False) if mutable else table

    resolved = inspection.resolved_path
    if inspection.suffix == ".csv":
        table = pd.read_csv(resolved, nrows=nrows, usecols=list(use_columns) if use_columns else None)
    elif inspection.suffix in {".xlsx", ".xls", ".xlsm"}:
        table = pd.read_excel(resolved, nrows=nrows, usecols=list(use_columns) if use_columns else None)
    elif inspection.suffix == ".parquet":
        table = pd.read_parquet(resolved, columns=list(use_columns) if use_columns else None)
    elif inspection.suffix == ".feather":
        table = pd.read_feather(resolved, columns=list(use_columns) if use_columns else None)
    else:
        raise ValueError(f"Unsupported file type: {resolved}")

    if loader_cache is not None:
        if nrows is None and use_columns is None:
            loader_cache.tables[source_key] = table
        loader_cache.tables[key] = table
        _sync_legacy_cache(cache, loader_cache)
    return table.copy(deep=False) if mutable else table


def read_columns(path: PathLike, *, cache: Optional[TableCache] = None) -> List[str]:
    inspection = inspect_table(path, cache=cache)
    return list(inspection.columns)


def combine_tables(
    sources: Iterable[Union[PathLike, pd.DataFrame, None]],
    *,
    cache: Optional[TableCache] = None,
    unique_key: Optional[str] = None,
    source_name: str = "table sources",
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for source in sources:
        if source is None:
            continue
        if isinstance(source, pd.DataFrame):
            if not source.empty:
                frames.append(source.copy())
            continue
        table = read_table(source, cache=cache, mutable=True)
        if not table.empty:
            frames.append(table)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    if unique_key and unique_key in combined.columns:
        duplicates = combined[combined[unique_key].astype(str).duplicated(keep=False)][unique_key].astype(str).tolist()
        if duplicates:
            raise ValueError(f"Duplicate {unique_key} values in {source_name}: {sorted(set(duplicates))}")
    return combined
