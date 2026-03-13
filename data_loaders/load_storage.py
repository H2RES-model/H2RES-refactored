"""Storage loader that assembles StorageUnits from static and time-series tables."""

from __future__ import annotations

from typing import Optional, Union
from pathlib import Path
import pandas as pd

from data_models.Bus import Bus
from data_models.StorageUnits import StorageUnits
from data_models.SystemSets import SystemSets
from data_models.Transport import Transport
from data_loaders.helpers.io import TableCache
from data_loaders.helpers.model_factory import build_model, is_strict_validation
from data_loaders.helpers.storage_loader import build_storage_inputs
from data_loaders.helpers.timeseries import empty_frame, merge_keyed_frames


def load_storage(
    powerplants_path: Union[str, pd.DataFrame],
    storage_path: Optional[Union[str, pd.DataFrame]] = None,
    inflow_path: Optional[str] = None,
    write_transport_storage_units: bool = True,
    *,
    transport: Optional[Transport] = None,
    include_chp_tes: bool = True,
    sets: SystemSets,
    buses: Bus,
    existing_storage: Optional[StorageUnits] = None,
    table_cache: Optional[TableCache] = None,
) -> StorageUnits:
    """Build StorageUnits from unit data, templates, and inflow data."""

    static_df, inflow_df, availability_df, e_nom_ts_df, transport_storage_df = build_storage_inputs(
        powerplants_path=powerplants_path,
        storage_path=storage_path,
        inflow_path=inflow_path,
        transport=transport,
        include_chp_tes=include_chp_tes,
        sets=sets,
        table_cache=table_cache,
    )

    if write_transport_storage_units and not transport_storage_df.empty:
        out_path = Path("data/transport/transport_storage_units.csv")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        transport_storage_df.to_csv(out_path, index=False)

    if static_df.empty and existing_storage is None:
        return build_model(StorageUnits)
    if static_df.empty:
        return existing_storage or build_model(StorageUnits)

    existing = existing_storage
    if existing is not None and not existing.static.empty:
        static_df = existing.static.combine_first(static_df)

    inflow_df = merge_keyed_frames(
        existing.inflow.copy() if existing is not None else empty_frame(["unit", "period", "year", "inflow"]),
        inflow_df,
        keys=["unit", "period", "year"],
    )
    availability_df = merge_keyed_frames(
        existing.availability.copy() if existing is not None else empty_frame(["unit", "period", "year", "availability"]),
        availability_df,
        keys=["unit", "period", "year"],
    )
    e_nom_ts_df = merge_keyed_frames(
        existing.e_nom_ts.copy() if existing is not None else empty_frame(["unit", "period", "year", "e_nom_ts"]),
        e_nom_ts_df,
        keys=["unit", "period", "year"],
    )
    investment_costs = (
        existing.investment_costs.copy()
        if existing is not None
        else empty_frame(["unit", "year", "e_nom_inv_cost"])
    )
    if not investment_costs.empty:
        investment_costs = investment_costs.drop_duplicates(subset=["unit", "year"], keep="first").reset_index(drop=True)
    units = sorted(static_df.index.astype(str).tolist())

    if is_strict_validation():
        unit_set = set(units)
        for name, frame in (
            ("inflow", inflow_df),
            ("e_nom_inv_cost", investment_costs),
            ("availability", availability_df),
            ("e_nom_ts", e_nom_ts_df),
        ):
            if frame.empty or "unit" not in frame.columns:
                continue
            extra = set(frame["unit"].dropna().astype(str).unique().tolist()) - unit_set
            assert not extra, f"{name} has unknown units: {sorted(extra)}"

    return build_model(
        StorageUnits,
        static=static_df,
        inflow=inflow_df,
        availability=availability_df,
        e_nom_ts=e_nom_ts_df,
        investment_costs=investment_costs,
    )
