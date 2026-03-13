"""Storage table readers and assembly helpers."""

from __future__ import annotations

from typing import Optional, Union

import numpy as np
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Transport import Transport
from data_loaders.helpers.io import TableCache, inspect_table, read_table, resolve_table_path
from data_loaders.helpers.timeseries import empty_frame, load_wide_timeseries
from data_loaders.helpers.transport_integration import transport_to_storage
from data_loaders.helpers.validation_utils import require_columns, require_values

STORAGE_TABLE_COLUMNS = [
    "unit",
    "system",
    "region",
    "tech",
    "carrier_in",
    "carrier_out",
    "bus_in",
    "bus_out",
    "e_nom",
    "e_nom_max",
    "e_min",
    "p_charge_nom",
    "p_charge_nom_max",
    "p_discharge_nom",
    "p_discharge_nom_max",
    "duration_charge",
    "duration_discharge",
    "efficiency_charge",
    "efficiency_discharge",
    "standby_loss",
    "capital_cost_energy",
    "capital_cost_power_charge",
    "capital_cost_power_discharge",
    "lifetime",
    "spillage_cost",
]
STORAGE_INPUT_COLUMNS = ["name", *STORAGE_TABLE_COLUMNS[1:]]
REQUIRED_STORAGE_COLUMNS = {
    "name",
    "tech",
    "carrier_in",
    "carrier_out",
    "bus_in",
    "bus_out",
    "e_nom",
    "e_nom_max",
    "e_min",
    "duration_charge",
    "duration_discharge",
    "efficiency_charge",
    "efficiency_discharge",
    "standby_loss",
    "capital_cost_energy",
    "capital_cost_power_charge",
    "capital_cost_power_discharge",
    "lifetime",
    "spillage_cost",
}
EMPTY_INFLOW = empty_frame(["unit", "period", "year", "inflow"])
EMPTY_AVAILABILITY = empty_frame(["unit", "period", "year", "availability"])


def _string_series(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype=object)
    return df[column].fillna(default).astype(str)


def _float_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(default).astype(float)


def _int_series(df: pd.DataFrame, column: str, default: int = 0) -> pd.Series:
    return _float_series(df, column, float(default)).astype(int)


def _empty_static() -> pd.DataFrame:
    return empty_frame(STORAGE_TABLE_COLUMNS)


def _derive_power(
    explicit: pd.Series,
    base_energy: pd.Series,
    duration: pd.Series,
    fallback: pd.Series | None = None,
) -> pd.Series:
    derived = np.divide(
        base_energy.to_numpy(dtype=float),
        duration.to_numpy(dtype=float),
        out=np.full(len(base_energy), np.nan, dtype=float),
        where=duration.to_numpy(dtype=float) > 0,
    )
    values = pd.to_numeric(explicit, errors="coerce")
    values = values.where(values.notna(), pd.Series(derived, index=explicit.index))
    if fallback is not None:
        values = values.where(values.notna(), pd.to_numeric(fallback, errors="coerce"))
    return values.fillna(0.0).astype(float)


def hydro_storage_table(
    powerplants: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str]]:
    hydro = powerplants[powerplants["tech"].isin(["HDAM", "HPHS"])].copy()
    if hydro.empty:
        return _empty_static(), []

    p_nom = _float_series(hydro, "p_nom")
    p_nom_max = _float_series(hydro, "p_nom_max", default=0.0).where(
        _float_series(hydro, "p_nom_max", default=np.nan).notna(),
        p_nom,
    )
    p_charge = _float_series(hydro, "p_charge_nom", default=np.nan).where(
        _float_series(hydro, "p_charge_nom", default=np.nan).notna(),
        p_nom,
    )
    p_charge_max = _float_series(hydro, "p_charge_nom_max", default=np.nan).where(
        _float_series(hydro, "p_charge_nom_max", default=np.nan).notna(),
        p_charge,
    )
    e_nom = _float_series(hydro, "e_nom")
    e_nom_max = _float_series(hydro, "e_nom_max", default=np.nan).where(
        _float_series(hydro, "e_nom_max", default=np.nan).notna(),
        e_nom,
    )
    efficiency_discharge = _float_series(hydro, "efficiency_discharge", default=np.nan).where(
        _float_series(hydro, "efficiency_discharge", default=np.nan).notna(),
        _float_series(hydro, "efficiency", 1.0),
    )

    require_values(
        hydro,
        required_str=["name", "tech", "carrier_out", "bus_out"],
        required_num=[
            "p_nom",
            "e_nom",
            "capital_cost",
            "lifetime",
        ],
        path_label="powerplants hydro storage rows",
        name_col="name",
    )

    out = pd.DataFrame(
        {
            "unit": _string_series(hydro, "name"),
            "system": _string_series(hydro, "system"),
            "region": _string_series(hydro, "region"),
            "tech": _string_series(hydro, "tech", "Hydro"),
            "carrier_in": _string_series(hydro, "carrier_in"),
            "carrier_out": _string_series(hydro, "carrier_out"),
            "bus_in": _string_series(hydro, "bus_in"),
            "bus_out": _string_series(hydro, "bus_out"),
            "e_nom": e_nom,
            "e_nom_max": e_nom_max,
            "e_min": 0.0,
            "p_charge_nom": p_charge,
            "p_charge_nom_max": p_charge_max,
            "p_discharge_nom": p_nom,
            "p_discharge_nom_max": p_nom_max,
            "duration_charge": np.nan,
            "duration_discharge": np.nan,
            "efficiency_charge": _float_series(hydro, "efficiency_charge", 1.0),
            "efficiency_discharge": efficiency_discharge,
            "standby_loss": _float_series(hydro, "standby_loss"),
            "capital_cost_energy": 0.0,
            "capital_cost_power_charge": _float_series(hydro, "capital_cost"),
            "capital_cost_power_discharge": _float_series(hydro, "capital_cost"),
            "lifetime": _int_series(hydro, "lifetime"),
            "spillage_cost": _float_series(hydro, "spillage_cost"),
        }
    )
    return out[STORAGE_TABLE_COLUMNS], out["unit"].astype(str).tolist()


def chp_tes_table(
    powerplants: pd.DataFrame,
) -> pd.DataFrame:
    if "chp_type" not in powerplants.columns:
        return _empty_static()

    chp = powerplants.copy()
    chp_type = _string_series(chp, "chp_type").str.upper()
    bus_out_2 = _string_series(chp, "bus_out_2")
    mask = chp["chp_type"].notna() & (chp_type != "N") & bus_out_2.ne("")
    chp = chp[mask].copy()
    if chp.empty:
        return _empty_static()

    require_values(
        chp,
        required_str=["name", "carrier_out_2", "bus_out_2"],
        required_num=["e_nom", "p_charge_nom", "lifetime"],
        path_label="powerplants CHP TES rows",
        name_col="name",
    )

    e_nom = _float_series(chp, "e_nom")
    p_charge = _float_series(chp, "p_charge_nom")
    p_discharge = _float_series(chp, "chp_max_heat", default=np.nan).where(
        _float_series(chp, "chp_max_heat", default=np.nan).notna(),
        _float_series(chp, "p_nom"),
    )
    keep = (e_nom != 0) | (p_charge != 0) | (p_discharge != 0)
    chp = chp[keep].copy()
    if chp.empty:
        return _empty_static()

    carrier_out_2 = _string_series(chp, "carrier_out_2")
    carrier_out = carrier_out_2.where(carrier_out_2.ne(""), _string_series(chp, "carrier_out", "heat"))
    e_nom = _float_series(chp, "e_nom")
    p_charge = _float_series(chp, "p_charge_nom")
    p_discharge = _float_series(chp, "chp_max_heat", default=np.nan).where(
        _float_series(chp, "chp_max_heat", default=np.nan).notna(),
        _float_series(chp, "p_nom"),
    )

    out = pd.DataFrame(
        {
            "unit": _string_series(chp, "name"),
            "system": _string_series(chp, "system"),
            "region": _string_series(chp, "region"),
            "tech": "CHP_TES",
            "carrier_in": _string_series(chp, "carrier_in"),
            "carrier_out": carrier_out,
            "bus_in": _string_series(chp, "bus_in"),
            "bus_out": _string_series(chp, "bus_out_2"),
            "e_nom": e_nom,
            "e_nom_max": e_nom,
            "e_min": 0.0,
            "p_charge_nom": p_charge,
            "p_charge_nom_max": p_charge,
            "p_discharge_nom": p_discharge,
            "p_discharge_nom_max": p_discharge,
            "duration_charge": np.nan,
            "duration_discharge": np.nan,
            "efficiency_charge": _float_series(chp, "efficiency_charge", 1.0),
            "efficiency_discharge": _float_series(chp, "efficiency_discharge", 1.0),
            "standby_loss": _float_series(chp, "standby_loss"),
            "capital_cost_energy": 0.0,
            "capital_cost_power_charge": 0.0,
            "capital_cost_power_discharge": 0.0,
            "lifetime": _int_series(chp, "lifetime"),
            "spillage_cost": _float_series(chp, "spillage_cost"),
        }
    )
    return out[STORAGE_TABLE_COLUMNS]


def read_storage_source(
    source: Optional[str | pd.DataFrame],
    *,
    table_cache: Optional[TableCache] = None,
) -> tuple[pd.DataFrame, str]:
    if isinstance(source, pd.DataFrame):
        return source.copy(), "<storage-dataframe>"
    if not source:
        return pd.DataFrame(), "<storage>"

    try:
        resolve_table_path(source)
    except FileNotFoundError:
        return pd.DataFrame(), source

    available = set(inspect_table(source, cache=table_cache).columns)
    usecols = [column for column in STORAGE_INPUT_COLUMNS if column in available]
    return read_table(source, columns=usecols or None, cache=table_cache), source


def normalize_storage_table(
    source: Optional[str | pd.DataFrame],
    *,
    sets: SystemSets,
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    table, path_label = read_storage_source(source, table_cache=table_cache)
    if table.empty:
        return _empty_static()

    require_columns(table, REQUIRED_STORAGE_COLUMNS, path_label)
    if getattr(sets, "storage_units", None):
        allowed_units = {str(unit) for unit in sets.storage_units}
        table = table[table["name"].astype(str).isin(allowed_units)].copy()
    if table.empty:
        return _empty_static()

    require_values(
        table,
        required_str=["name", "tech", "carrier_in", "carrier_out", "bus_in", "bus_out"],
        required_num=[
            "e_nom",
            "e_nom_max",
            "e_min",
            "duration_charge",
            "duration_discharge",
            "efficiency_charge",
            "efficiency_discharge",
            "standby_loss",
            "capital_cost_energy",
            "capital_cost_power_charge",
            "capital_cost_power_discharge",
            "lifetime",
            "spillage_cost",
        ],
        path_label=path_label,
        name_col="name",
    )

    numeric_columns = [
        column
        for column in STORAGE_INPUT_COLUMNS
        if column not in {"name", "tech", "carrier_in", "carrier_out", "bus_in", "bus_out", "system", "region"}
    ]
    for column in numeric_columns:
        if column in table.columns:
            table[column] = pd.to_numeric(table[column], errors="coerce")

    for column in ("duration_charge", "duration_discharge"):
        bad = table[pd.to_numeric(table[column], errors="coerce") <= 0]
        if not bad.empty:
            units = bad["name"].astype(str).tolist()
            raise ValueError(f"{path_label} invalid {column} for units: {units}")

    out = pd.DataFrame(
        {
            "unit": table["name"].astype(str),
            "system": _string_series(table, "system"),
            "region": _string_series(table, "region"),
            "tech": _string_series(table, "tech"),
            "carrier_in": _string_series(table, "carrier_in"),
            "carrier_out": _string_series(table, "carrier_out"),
            "bus_in": _string_series(table, "bus_in"),
            "bus_out": _string_series(table, "bus_out"),
            "e_nom": _float_series(table, "e_nom"),
            "e_nom_max": _float_series(table, "e_nom_max"),
            "e_min": _float_series(table, "e_min"),
            "duration_charge": _float_series(table, "duration_charge", default=np.nan),
            "duration_discharge": _float_series(table, "duration_discharge", default=np.nan),
            "efficiency_charge": _float_series(table, "efficiency_charge", 1.0),
            "efficiency_discharge": _float_series(table, "efficiency_discharge", 1.0),
            "standby_loss": _float_series(table, "standby_loss"),
            "capital_cost_energy": _float_series(table, "capital_cost_energy"),
            "capital_cost_power_charge": _float_series(table, "capital_cost_power_charge"),
            "capital_cost_power_discharge": _float_series(table, "capital_cost_power_discharge"),
            "lifetime": _int_series(table, "lifetime"),
            "spillage_cost": _float_series(table, "spillage_cost"),
        }
    )
    out["p_charge_nom"] = _derive_power(
        table.get("p_charge_nom", pd.Series(index=table.index, dtype=float)),
        out["e_nom"],
        out["duration_charge"],
    )
    out["p_charge_nom_max"] = _derive_power(
        table.get("p_charge_nom_max", pd.Series(index=table.index, dtype=float)),
        out["e_nom_max"],
        out["duration_charge"],
        fallback=out["p_charge_nom"],
    )
    out["p_discharge_nom"] = _derive_power(
        table.get("p_discharge_nom", pd.Series(index=table.index, dtype=float)),
        out["e_nom"],
        out["duration_discharge"],
    )
    out["p_discharge_nom_max"] = _derive_power(
        table.get("p_discharge_nom_max", pd.Series(index=table.index, dtype=float)),
        out["e_nom_max"],
        out["duration_discharge"],
        fallback=out["p_discharge_nom"],
    )
    return out[STORAGE_TABLE_COLUMNS].reset_index(drop=True)


def build_storage_inputs(
    *,
    powerplants_path: Union[str, pd.DataFrame],
    storage_path: Optional[Union[str, pd.DataFrame]],
    inflow_path: Optional[str],
    transport: Optional[Transport],
    include_chp_tes: bool,
    sets: SystemSets,
    table_cache: Optional[TableCache] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    powerplants = powerplants_path.copy() if isinstance(powerplants_path, pd.DataFrame) else read_table(powerplants_path, cache=table_cache)
    powerplants_label = "<powerplants-dataframe>" if isinstance(powerplants_path, pd.DataFrame) else str(powerplants_path)
    require_columns(powerplants, {"name", "tech", "p_nom", "capital_cost", "lifetime"}, powerplants_label)

    hydro_static, hydro_units = hydro_storage_table(powerplants)
    static_parts = [hydro_static]
    if include_chp_tes:
        static_parts.append(chp_tes_table(powerplants))

    template_static = normalize_storage_table(
        storage_path,
        sets=sets,
        table_cache=table_cache,
    )
    static_parts.append(template_static)

    availability = EMPTY_AVAILABILITY.copy()
    transport_static = _empty_static()
    if transport is not None:
        transport_static, availability = transport_to_storage(transport)
        if not transport_static.empty:
            static_parts.append(
                normalize_storage_table(
                    transport_static.rename(columns={"unit": "name"}),
                    sets=sets,
                )
            )

    static = pd.concat([part for part in static_parts if not part.empty], ignore_index=True) if any(
        not part.empty for part in static_parts
    ) else _empty_static()
    if not static.empty:
        static = static.drop_duplicates(subset=["unit"], keep="last").set_index("unit", drop=True)

    inflow = load_wide_timeseries(
        path=inflow_path,
        sets=sets,
        units=hydro_units,
        value_name="inflow",
        table_cache=table_cache,
    ) if hydro_units else EMPTY_INFLOW.copy()

    e_nom_ts = empty_frame(["unit", "period", "year", "e_nom_ts"])
    if not availability.empty and not static.empty:
        e_nom_lookup = static[["e_nom"]].dropna(subset=["e_nom"]).reset_index()
        e_nom_ts = availability.merge(e_nom_lookup, on="unit", how="left", validate="many_to_one")
        e_nom_ts["e_nom_ts"] = e_nom_ts["availability"].astype(float) * e_nom_ts["e_nom"].astype(float)
        e_nom_ts = e_nom_ts[["unit", "period", "year", "e_nom_ts"]].reset_index(drop=True)

    return static, inflow, availability.reset_index(drop=True), e_nom_ts, transport_static.reset_index(drop=True)
