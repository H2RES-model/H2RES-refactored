"""Storage-specific loader helpers for building StorageUnits inputs."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd

from data_models.SystemSets import SystemSets
from data_loaders.helpers.io import TableCache, read_columns, read_table, resolve_table_path
from data_loaders.helpers.pandas_utils import stack_compat
from data_loaders.helpers.storage_utils import StorageRecordStore
from data_loaders.helpers.validation_utils import require_columns, require_values

UPY = Tuple[str, int, int]


def load_hydro_storage(
    df_pp: pd.DataFrame,
    store: StorageRecordStore,
    default_carrier: str,
    default_bus: str,
) -> List[str]:
    """Load hydro storage units (HDAM, HPHS) from the powerplants table.

    Args:
        df_pp: Powerplants DataFrame.
        store: StorageRecordStore that receives per-unit fields.
        default_carrier: Default carrier when missing in input.
        default_bus: Default bus when missing in input.

    Returns:
        List of hydro unit names extracted from the powerplants table.
    """
    df_hydro = df_pp[df_pp["tech"].isin(["HDAM", "HPHS"])].copy()
    hydro_units_local = df_hydro["name"].astype(str).tolist()
    for row in df_hydro.itertuples(index=False):
        # Map hydro generators into storage fields (charge/discharge, energy).
        name = str(getattr(row, "name", ""))
        bus_in_val = str(getattr(row, "bus_in", default_bus))
        bus_out_val = str(getattr(row, "bus_out", default_bus))
        carrier_in_val = str(getattr(row, "carrier_in", default_carrier))
        carrier_out_val = str(getattr(row, "carrier_out", "electricity"))
        p_discharge = float(getattr(row, "p_nom", 0.0) or 0.0)
        p_discharge_max = float(getattr(row, "p_nom_max", p_discharge) or p_discharge)
        p_charge_nom_val = float(getattr(row, "p_charge_nom", p_discharge) or p_discharge)
        p_charge_nom_max_val = float(getattr(row, "p_charge_nom_max", p_charge_nom_val) or p_charge_nom_val)
        e_nom_val = float(getattr(row, "e_nom", 0.0) or 0.0)
        e_nom_max_val = float(getattr(row, "e_nom_max", e_nom_val) or e_nom_val)

        store.add_record(
            unit=name,
            tech=str(getattr(row, "tech", "Hydro")),
            system=str(getattr(row, "system", "")),
            region=str(getattr(row, "region", "")),
            bus_in=bus_in_val,
            bus_out=bus_out_val,
            carrier_in=carrier_in_val,
            carrier_out=carrier_out_val,
            e_nom=e_nom_val,
            e_nom_max=e_nom_max_val,
            e_min=0.0,
            p_charge_nom=p_charge_nom_val,
            p_charge_nom_max=p_charge_nom_max_val if p_charge_nom_max_val is not None else p_charge_nom_val,
            p_discharge_nom=p_discharge,
            p_discharge_nom_max=p_discharge_max if p_discharge_max is not None else p_discharge,
            efficiency_charge=float(getattr(row, "efficiency_charge", 1.0) or 1.0),
            efficiency_discharge=float(getattr(row, "efficiency_discharge", getattr(row, "efficiency", 1.0)) or 1.0),
            standby_loss=float(getattr(row, "standby_loss", 0.0) or 0.0),
            capital_cost_energy=0.0,
            capital_cost_power_charge=float(getattr(row, "capital_cost", 0.0) or 0.0),
            capital_cost_power_discharge=float(getattr(row, "capital_cost", 0.0) or 0.0),
            lifetime=int(getattr(row, "lifetime", 0) or 0),
            spillage_cost=float(getattr(row, "spillage_cost", 0.0) or 0.0),
        )
    return hydro_units_local


def load_chp_tes(
    df_pp: pd.DataFrame,
    store: StorageRecordStore,
    default_carrier: str,
    default_bus: str,
) -> None:
    """Load CHP thermal storage (TES) units for non-electricity sectors.

    Args:
        df_pp: Powerplants DataFrame.
        store: StorageRecordStore that receives per-unit fields.
        default_carrier: Default carrier when missing in input.
        default_bus: Default bus when missing in input.
    """
    if "chp_type" not in df_pp.columns:
        return
    chp_mask = df_pp["chp_type"].astype(str).str.upper() != "N"
    df_chp = df_pp[chp_mask & df_pp["chp_type"].notna()].copy()
    for row in df_chp.itertuples(index=False):
        name = str(getattr(row, "name", ""))
        bus_out_2_val = getattr(row, "bus_out_2", None)
        if pd.isna(bus_out_2_val) or str(bus_out_2_val).strip() == "":
            continue
        carrier_in_val = str(getattr(row, "carrier_in", default_carrier))
        carrier_out_2_val = getattr(row, "carrier_out_2", None)
        if pd.notna(carrier_out_2_val) and str(carrier_out_2_val).strip() != "":
            carrier_out_val = str(carrier_out_2_val)
        else:
            carrier_out_val = str(getattr(row, "carrier_out", "heat"))
        bus_in_val = str(getattr(row, "bus_in", default_bus))
        bus_out_val = str(bus_out_2_val)
        e_nom_val = float(getattr(row, "e_nom", 0.0) or 0.0)
        p_charge_nom_val = float(getattr(row, "p_charge_nom", 0.0) or 0.0)
        p_discharge = float(getattr(row, "chp_max_heat", getattr(row, "p_nom", 0.0)) or 0.0)
        if e_nom_val == 0 and p_charge_nom_val == 0 and p_discharge == 0:
            continue
        store.add_record(
            unit=name,
            tech="CHP_TES",
            system=str(getattr(row, "system", "")),
            region=str(getattr(row, "region", "")),
            carrier_in=carrier_in_val,
            carrier_out=carrier_out_val,
            bus_in=bus_in_val,
            bus_out=bus_out_val,
            e_nom=e_nom_val,
            e_nom_max=e_nom_val,
            e_min=0.0,
            p_charge_nom=p_charge_nom_val,
            p_charge_nom_max=p_charge_nom_val,
            p_discharge_nom=p_discharge,
            p_discharge_nom_max=p_discharge,
            efficiency_charge=float(getattr(row, "efficiency_charge", 1.0) or 1.0),
            efficiency_discharge=float(getattr(row, "efficiency_discharge", 1.0) or 1.0),
            standby_loss=float(getattr(row, "standby_loss", 0.0) or 0.0),
            capital_cost_energy=0.0,
            capital_cost_power_charge=0.0,
            capital_cost_power_discharge=0.0,
            lifetime=int(getattr(row, "lifetime", 0) or 0),
            spillage_cost=float(getattr(row, "spillage_cost", 0.0) or 0.0),
        )


def load_template_storage(
    path: Optional[Union[str, pd.DataFrame]],
    sets: SystemSets,
    store: StorageRecordStore,
    default_carrier: str,
    default_bus: str,
    table_cache: Optional[TableCache] = None,
) -> None:
    """Load storage units from a template table.

    Args:
        path: Path to the storage template table.
        sets: SystemSets with valid storage unit names.
        store: StorageRecordStore that receives per-unit fields.
        default_carrier: Default carrier when missing in input.
        default_bus: Default bus when missing in input.

    Raises:
        ValueError: If required columns or values are missing, or duration is invalid.
    """
    if isinstance(path, pd.DataFrame):
        df_st = path.copy()
        path_label = "<storage-dataframe>"
    elif path:
        path_label = path
        try:
            _ = resolve_table_path(path)
            st_cols_available = set(read_columns(path, cache=table_cache))
            st_cols = [
                c
                for c in (
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
                    "system",
                    "region",
                    "p_charge_nom",
                    "p_charge_nom_max",
                    "p_discharge_nom",
                    "p_discharge_nom_max",
                )
                if c in st_cols_available
            ]
            df_st = read_table(path, columns=st_cols or None, cache=table_cache)
        except FileNotFoundError:
            df_st = pd.DataFrame()
    else:
        df_st = pd.DataFrame()
        path_label = "<storage>"

    required_st = {
        "name", "tech", "carrier_in", "carrier_out", "bus_in", "bus_out",
        "e_nom", "e_nom_max", "e_min", "duration_charge", "duration_discharge",
        "efficiency_charge", "efficiency_discharge", "standby_loss",
        "capital_cost_energy", "capital_cost_power_charge", "capital_cost_power_discharge",
        "lifetime", "spillage_cost",
    }
    if not df_st.empty:
        require_columns(df_st, required_st, path_label)
        if "name" in df_st.columns and getattr(sets, "storage_units", None):
            allowed_units = {str(u) for u in sets.storage_units}
            df_st = df_st[df_st["name"].astype(str).isin(allowed_units)]
        for col, default in (
            ("carrier_in", default_carrier),
            ("carrier_out", default_carrier),
            ("bus_in", default_bus),
            ("bus_out", default_bus),
        ):
            if col in df_st.columns:
                blank = df_st[col].isna() | (df_st[col].astype(str).str.strip() == "")
                if blank.any():
                    df_st.loc[blank, col] = default
        required_str = ["name", "tech", "carrier_in", "carrier_out", "bus_in", "bus_out"]
        required_num = [
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
        ]
        require_values(df_st, required_str, required_num, path_label, name_col="name")

    if not df_st.empty:
        dur_ch_arr = pd.to_numeric(df_st["duration_charge"], errors="coerce").to_numpy(dtype=float)
        dur_dis_arr = pd.to_numeric(df_st["duration_discharge"], errors="coerce").to_numpy(dtype=float)
        e_nom_arr = pd.to_numeric(df_st["e_nom"], errors="coerce").to_numpy(dtype=float)
        e_nom_max_arr = pd.to_numeric(df_st["e_nom_max"], errors="coerce").to_numpy(dtype=float)

        p_charge_nom_arr = np.divide(
            e_nom_arr, dur_ch_arr, out=np.full_like(e_nom_arr, np.nan), where=dur_ch_arr > 0
        )
        p_discharge_nom_arr = np.divide(
            e_nom_arr, dur_dis_arr, out=np.full_like(e_nom_arr, np.nan), where=dur_dis_arr > 0
        )
        p_charge_nom_max_arr = np.divide(
            e_nom_max_arr, dur_ch_arr, out=np.full_like(e_nom_max_arr, np.nan), where=dur_ch_arr > 0
        )
        p_discharge_nom_max_arr = np.divide(
            e_nom_max_arr, dur_dis_arr, out=np.full_like(e_nom_max_arr, np.nan), where=dur_dis_arr > 0
        )

        for idx, row in enumerate(df_st.itertuples(index=False)):
            # Fill optional power columns from duration when not explicitly provided.
            name = str(getattr(row, "name", ""))
            e_nom_val = float(getattr(row, "e_nom", 0.0) or 0.0)
            e_nom_max_val = float(getattr(row, "e_nom_max", e_nom_val) or e_nom_val)

            dur_ch = float(getattr(row, "duration_charge", np.nan))
            if np.isnan(dur_ch):
                dur_ch = None
            if dur_ch is not None and dur_ch <= 0:
                raise ValueError(
                    f"{path_label} invalid duration_charge for unit '{name}': {dur_ch}"
                )
            dur_dis = float(getattr(row, "duration_discharge", np.nan))
            if np.isnan(dur_dis):
                dur_dis = None
            if dur_dis is not None and dur_dis <= 0:
                raise ValueError(
                    f"{path_label} invalid duration_discharge for unit '{name}': {dur_dis}"
                )

            p_charge_nom_val = getattr(row, "p_charge_nom", None)
            p_charge_nom_val = float(p_charge_nom_val) if p_charge_nom_val is not None and not pd.isna(p_charge_nom_val) else None
            if p_charge_nom_val is None:
                derived = p_charge_nom_arr[idx]
                p_charge_nom_val = float(derived) if not np.isnan(derived) else None
            if p_charge_nom_val is None:
                p_charge_nom_val = 0.0

            p_discharge_nom_val = getattr(row, "p_discharge_nom", None)
            p_discharge_nom_val = float(p_discharge_nom_val) if p_discharge_nom_val is not None and not pd.isna(p_discharge_nom_val) else None
            if p_discharge_nom_val is None:
                derived = p_discharge_nom_arr[idx]
                p_discharge_nom_val = float(derived) if not np.isnan(derived) else None
            if p_discharge_nom_val is None:
                p_discharge_nom_val = 0.0

            p_charge_nom_max_val = getattr(row, "p_charge_nom_max", None)
            p_charge_nom_max_val = float(p_charge_nom_max_val) if p_charge_nom_max_val is not None and not pd.isna(p_charge_nom_max_val) else None
            if p_charge_nom_max_val is None:
                derived = p_charge_nom_max_arr[idx]
                p_charge_nom_max_val = float(derived) if not np.isnan(derived) else None
            if p_charge_nom_max_val is None:
                p_charge_nom_max_val = p_charge_nom_val

            p_discharge_nom_max_val = getattr(row, "p_discharge_nom_max", None)
            p_discharge_nom_max_val = float(p_discharge_nom_max_val) if p_discharge_nom_max_val is not None and not pd.isna(p_discharge_nom_max_val) else None
            if p_discharge_nom_max_val is None:
                derived = p_discharge_nom_max_arr[idx]
                p_discharge_nom_max_val = float(derived) if not np.isnan(derived) else None
            if p_discharge_nom_max_val is None:
                p_discharge_nom_max_val = p_discharge_nom_val

            store.add_record(
                unit=name,
                tech=str(getattr(row, "tech")),
                system=str(getattr(row, "system", "")),
                region=str(getattr(row, "region", "")),
                carrier_in=str(getattr(row, "carrier_in", default_carrier)),
                carrier_out=str(getattr(row, "carrier_out", default_carrier)),
                bus_in=str(getattr(row, "bus_in", default_bus)),
                bus_out=str(getattr(row, "bus_out", default_bus)),
                e_nom=e_nom_val,
                e_nom_max=e_nom_max_val,
                e_min=float(getattr(row, "e_min", 0.0) or 0.0),
                p_charge_nom=p_charge_nom_val,
                p_charge_nom_max=p_charge_nom_max_val,
                p_discharge_nom=p_discharge_nom_val,
                p_discharge_nom_max=p_discharge_nom_max_val,
                duration_charge=dur_ch if dur_ch is not None else None,
                duration_discharge=dur_dis if dur_dis is not None else None,
                efficiency_charge=float(getattr(row, "efficiency_charge", 1.0) or 1.0),
                efficiency_discharge=float(getattr(row, "efficiency_discharge", 1.0) or 1.0),
                standby_loss=float(getattr(row, "standby_loss", 0.0) or 0.0),
                capital_cost_energy=float(getattr(row, "capital_cost_energy", 0.0) or 0.0),
                capital_cost_power_charge=float(getattr(row, "capital_cost_power_charge", 0.0) or 0.0),
                capital_cost_power_discharge=float(getattr(row, "capital_cost_power_discharge", 0.0) or 0.0),
                lifetime=int(getattr(row, "lifetime", 0) or 0),
                spillage_cost=float(getattr(row, "spillage_cost", 0.0) or 0.0),
            )


def load_inflows(
    path: Optional[str],
    units: List[str],
    sets: SystemSets,
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
    """Load hydro inflows and convert to a long table.

    Args:
        path: Path to the inflow table.
        units: List of hydro units to include.
        sets: SystemSets with modeled years and periods.

    Returns:
        Long DataFrame with columns unit, period, year, inflow.

    Raises:
        ValueError: If required columns are missing.
    """
    if not path or not units:
        return pd.DataFrame(columns=["unit", "period", "year", "inflow"])
    inflow_cols_available = set(read_columns(path, cache=table_cache))
    inflow_cols = ["year", "period"] + [u for u in units if u in inflow_cols_available]
    df_in = read_table(path, columns=inflow_cols, cache=table_cache)
    for col in ("year", "period"):
        if col not in df_in.columns:
            raise ValueError(f"{path} missing column '{col}'")
    df_in = df_in[df_in["year"].isin(sets.years) & df_in["period"].isin(sets.periods)]
    id_vars = ["year", "period"]
    value_cols = [c for c in df_in.columns if c not in id_vars and c in units]
    if value_cols:
        stacked = stack_compat(df_in, id_vars, value_cols)
        out = stacked.reset_index()
        out.columns = ["year", "period", "unit", "inflow"]
        out["unit"] = out["unit"].astype(str)
        return out[["unit", "period", "year", "inflow"]].reset_index(drop=True)
    return pd.DataFrame(columns=["unit", "period", "year", "inflow"])
