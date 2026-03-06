"""Generator time-series loader for profiles, costs, and efficiencies."""

from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

from data_models.Bus import Bus
from data_models.SystemSets import SystemSets
from data_loaders.helpers.generator_ts import (
    load_efficiency_ts,
    load_profile_ts,
    load_var_cost_ts,
)
from data_loaders.helpers.io import TableCache


def load_generators_ts(
    *,
    sets: SystemSets,
    units: List[str],
    renewable_profiles_path: Optional[str] = None,
    fuel_cost_path: Optional[str] = None,
    efficiency_ts_path: Optional[str] = None,
    fuel: Optional[Dict[str, str]] = None,
    var_cost_no_fuel: Optional[Dict[str, float]] = None,
    efficiency: Optional[Dict[str, float]] = None,
    buses: Optional[Bus] = None,
    table_cache: Optional[TableCache] = None,
) -> Dict[str, pd.DataFrame]:
    """Load generator time series for profiles, variable costs, and efficiencies."""

    p_t = load_profile_ts(
        path=renewable_profiles_path,
        sets=sets,
        units=units,
        table_cache=table_cache,
    )
    var_cost = load_var_cost_ts(
        path=fuel_cost_path,
        sets=sets,
        units=units,
        fuel=fuel,
        var_cost_no_fuel=var_cost_no_fuel,
        efficiency=efficiency,
        table_cache=table_cache,
    )
    efficiency_ts = load_efficiency_ts(
        path=efficiency_ts_path,
        sets=sets,
        units=units,
        buses=buses,
        table_cache=table_cache,
    )

    dynamic: Dict[str, pd.DataFrame] = {}
    if not p_t.empty:
        dynamic["p_t"] = p_t
    if not var_cost.empty:
        dynamic["var_cost"] = var_cost
    if not efficiency_ts.empty:
        dynamic["efficiency_ts"] = efficiency_ts
    return dynamic
