"""Generator loader that merges static and time-series inputs."""

from __future__ import annotations

from typing import Optional, Union
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Generators import Generators
from data_models.Bus import Bus
from data_loaders.helpers.io import TableCache
from data_loaders.helpers.model_factory import build_model
from data_loaders.helpers.timeseries import empty_frame, merge_keyed_frames
from data_loaders.load_generators_static import load_generators_static
from data_loaders.load_generators_ts import load_generators_ts

def load_generators(
    powerplants_path: Union[str, pd.DataFrame],
    sets: SystemSets,
    buses: Bus,
    renewable_profiles_path: Optional[str] = None,
    fuel_cost_path: Optional[str] = None,
    efficiency_ts_path: Optional[str] = None,
    existing_generators: Optional[Generators] = None,
    table_cache: Optional[TableCache] = None,
) -> Generators:
    """Load generator parameters and time series into a Generators model.

    When used: called by `load_sector` to populate generator inputs for a sector.

    Args:
        powerplants_path: Path to powerplants (or converters) input file.
        sets: SystemSets with unit lists and time horizon.
        buses: Bus model for validating bus mappings.
        renewable_profiles_path: Optional RES profile time series.
        fuel_cost_path: Optional fuel cost time series.
        efficiency_ts_path: Optional efficiency time series.
        existing_generators: Existing Generators to merge into (existing wins).

    Returns:
        Generators model containing static parameters and time-series data.

    Raises:
        ValueError: If required inputs are missing or inconsistent.
    """

    static = load_generators_static(
        powerplants_path=powerplants_path,
        sets=sets,
        buses=buses,
        table_cache=table_cache,
    )
    static_indexed = static.set_index("unit") if not static.empty else static
    fuel_map = static_indexed["fuel"].dropna().astype(str).to_dict() if not static.empty else {}
    var_cost_no_fuel_map = static_indexed["var_cost_no_fuel"].dropna().astype(float).to_dict() if not static.empty else {}
    efficiency_map = static_indexed["efficiency"].dropna().astype(float).to_dict() if not static.empty else {}

    units = static["unit"].astype(str).tolist() if not static.empty else []
    if not units:
        return existing_generators or build_model(
            Generators,
            static=empty_frame([col.name for col in Generators.TABLE_SPECS["static"].columns]),
            p_t=empty_frame([col.name for col in Generators.TABLE_SPECS["p_t"].columns]),
            var_cost=empty_frame([col.name for col in Generators.TABLE_SPECS["var_cost"].columns]),
            efficiency_ts=empty_frame([col.name for col in Generators.TABLE_SPECS["efficiency_ts"].columns]),
        )

    dynamic = load_generators_ts(
        sets=sets,
        units=units,
        renewable_profiles_path=renewable_profiles_path,
        fuel_cost_path=fuel_cost_path,
        efficiency_ts_path=efficiency_ts_path,
        fuel=fuel_map,
        var_cost_no_fuel=var_cost_no_fuel_map,
        efficiency=efficiency_map,
        buses=buses,
        table_cache=table_cache,
    )

    ex = existing_generators
    if ex is not None and not ex.static.empty:
        static = ex.static.combine_first(static_indexed)
    else:
        static = static_indexed

    if ex is not None:
        for name in ("p_t", "var_cost", "efficiency_ts"):
            existing_frame = getattr(ex, name)
            frame = dynamic.get(name, empty_frame(["unit", "period", "year", name]))
            dynamic[name] = merge_keyed_frames(
                existing_frame.reset_index(drop=True),
                frame.reset_index(drop=True),
                keys=["unit", "period", "year"],
            )

    return build_model(
        Generators,
        static=static,
        p_t=dynamic.get("p_t", empty_frame(["unit", "period", "year", "p_t"])),
        var_cost=dynamic.get("var_cost", empty_frame(["unit", "period", "year", "var_cost"])),
        efficiency_ts=dynamic.get("efficiency_ts", empty_frame(["unit", "period", "year", "efficiency_ts"])),
    )
