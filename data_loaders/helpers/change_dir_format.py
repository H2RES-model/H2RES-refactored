from pathlib import Path
from typing import Iterable
import pandas as pd

def convert_dir_file_format(
    src_dir: Path,
    *,
    input_format: str,
    output_format: str,
    recursive: bool = True,
) -> None:
    input_format = input_format.lower().lstrip(".")
    output_format = output_format.lower().lstrip(".")
    supported = {"csv", "parquet", "feather"}

    if input_format not in supported:
        raise ValueError(f"input_format must be one of {sorted(supported)}")
    if output_format not in supported - {"csv"}:
        raise ValueError("output_format must be 'parquet' or 'feather'")

    paths = src_dir.rglob(f"*.{input_format}") if recursive else src_dir.glob(f"*.{input_format}")
    for path in paths:
        if input_format == "csv":
            df = pd.read_csv(path)
        elif input_format == "parquet":
            df = pd.read_parquet(path)
        else:
            df = pd.read_feather(path)

        out_path = path.with_suffix(f".{output_format}")
        if output_format == "parquet":
            df.to_parquet(out_path, index=False)
        else:
            df.to_feather(out_path)

        print(f"wrote {out_path}")
