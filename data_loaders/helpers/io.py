# data_loaders/io.py

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

PathLike = Union[str, Path]


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


def read_table(path: PathLike, *, nrows: Optional[int] = None) -> pd.DataFrame:
    resolved = resolve_table_path(path)
    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(resolved, nrows=nrows)
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(resolved, nrows=nrows)
    if suffix == ".parquet":
        return pd.read_parquet(resolved)
    if suffix == ".feather":
        return pd.read_feather(resolved)
    raise ValueError(f"Unsupported file type: {resolved}")


def read_columns(path: PathLike) -> List[str]:
    resolved = resolve_table_path(path)
    suffix = resolved.suffix.lower()
    if suffix == ".csv":
        return list(pd.read_csv(resolved, nrows=0).columns)
    if suffix in {".xlsx", ".xls", ".xlsm"}:
        return list(pd.read_excel(resolved, nrows=0).columns)
    if suffix == ".parquet":
        try:
            import pyarrow.parquet as pq  # type: ignore
        except Exception:
            return list(pd.read_parquet(resolved).columns)
        return list(pq.ParquetFile(resolved).schema.names)
    if suffix == ".feather":
        try:
            import pyarrow.feather as pf  # type: ignore
        except Exception:
            return list(pd.read_feather(resolved).columns)
        return list(pf.read_table(resolved).schema.names)
    raise ValueError(f"Unsupported file type: {resolved}")
