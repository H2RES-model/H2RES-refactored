# data_loaders/io.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Hashable, List, Optional, Sequence, Tuple, Union

import pandas as pd

PathLike = Union[str, Path]
CacheKey = Tuple[Hashable, ...]
TableCache = Dict[CacheKey, object]


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


def _columns_key(resolved: Path) -> CacheKey:
    return ("columns", str(resolved))


def resolve_table_path(path: PathLike) -> Path:
    p = Path(path)
    if p.exists():
        return p

    suffix = p.suffix.lower()
    if suffix in {".csv", ".parquet", ".feather"}:
        alt = p.with_suffix(".parquet" if suffix == ".csv" else ".csv")
        if alt.exists():
            return alt
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        for ext in (".xlsx", ".xlsm", ".xls"):
            if ext == suffix:
                continue
            alt = p.with_suffix(ext)
            if alt.exists():
                return alt
    if suffix == "":
        for ext in (".csv", ".parquet", ".feather", ".xlsx", ".xlsm", ".xls"):
            alt = p.with_suffix(ext)
            if alt.exists():
                return alt

    raise FileNotFoundError(f"Could not find data file for '{path}' (csv/parquet/feather/excel).")


def read_table(
    path: PathLike,
    *,
    nrows: Optional[int] = None,
    columns: Optional[Sequence[str]] = None,
    cache: Optional[TableCache] = None,
    mutable: bool = False,
) -> pd.DataFrame:
    resolved = resolve_table_path(path)
    use_columns = _norm_columns(columns)
    key = _table_key(resolved, nrows=nrows, columns=use_columns)
    if cache is not None and key in cache:
        cached = cache[key]
        if not isinstance(cached, pd.DataFrame):
            raise TypeError("Invalid table cache entry type.")
        return cached.copy(deep=False) if mutable else cached

    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        table = pd.read_csv(resolved, nrows=nrows, usecols=use_columns)
    elif suffix in {".xlsx", ".xls", ".xlsm"}:
        table = pd.read_excel(resolved, nrows=nrows, usecols=use_columns)
    elif suffix == ".parquet":
        table = pd.read_parquet(resolved, columns=list(use_columns) if use_columns else None)
    elif suffix == ".feather":
        table = pd.read_feather(resolved, columns=list(use_columns) if use_columns else None)
    else:
        raise ValueError(f"Unsupported file type: {resolved}")

    if cache is not None:
        cache[key] = table
    return table.copy(deep=False) if mutable else table


def read_columns(path: PathLike, *, cache: Optional[TableCache] = None) -> List[str]:
    resolved = resolve_table_path(path)
    key = _columns_key(resolved)
    if cache is not None and key in cache:
        cached = cache[key]
        if not isinstance(cached, list):
            raise TypeError("Invalid columns cache entry type.")
        return list(cached)

    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        cols = list(pd.read_csv(resolved, nrows=0).columns)
    elif suffix in {".xlsx", ".xls", ".xlsm"}:
        cols = list(pd.read_excel(resolved, nrows=0).columns)
    elif suffix == ".parquet":
        try:
            import pyarrow.parquet as pq  # type: ignore
        except Exception:
            cols = list(pd.read_parquet(resolved).columns)
        else:
            cols = list(pq.ParquetFile(resolved).schema.names)
    elif suffix == ".feather":
        try:
            import pyarrow.feather as pf  # type: ignore
        except Exception:
            cols = list(pd.read_feather(resolved).columns)
        else:
            cols = list(pf.read_table(resolved).schema.names)
    else:
        raise ValueError(f"Unsupported file type: {resolved}")

    if cache is not None:
        cache[key] = cols
    return list(cols)
