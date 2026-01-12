"""Storage-specific loader helpers for building StorageUnits inputs."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import gc
import pandas as pd

from data_models.SystemSets import SystemSets
from data_loaders.helpers.io import read_table, resolve_table_path
from data_loaders.helpers.pandas_utils import stack_compat
from data_loaders.helpers.storage_utils import StorageRecordStore
from data_loaders.helpers.validation_utils import require_columns, require_values
from data_loaders.helpers.value_utils import get_float

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
    for _, row in df_hydro.iterrows():
        # Map hydro generators into storage fields (charge/discharge, energy).
        name = str(row["name"])
        bus_in_val = str(row.get("bus_in", default_bus))
        bus_out_val = str(row.get("bus_out", default_bus))
        carrier_in_val = str(row.get("carrier_in", default_carrier))
        carrier_out_val = str(row.get("carrier_out", "electricity"))
        p_discharge = get_float(row, "p_nom", default=0.0)
        p_discharge_max = get_float(row, "p_nom_max", default=p_discharge)
        p_charge_nom_val = get_float(row, "p_charge_nom", default=p_discharge)
        p_charge_nom_max_val = get_float(row, "p_charge_nom_max", default=p_charge_nom_val)
        e_nom_val = get_float(row, "e_nom", default=0.0)
        e_nom_max_val = get_float(row, "e_nom_max", default=e_nom_val)

        store.add_record(
            unit=name,
            tech=str(row.get("tech", "Hydro")),
            system=str(row.get("system", "")) if "system" in row else "",
            region=str(row.get("region", "")) if "region" in row else "",
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
            efficiency_charge=float(row.get("efficiency_charge", 1.0)),
            efficiency_discharge=float(row.get("efficiency_discharge", row.get("efficiency", 1.0))),
            standby_loss=float(row.get("standby_loss", 0.0)),
            capital_cost_energy=0.0,
            capital_cost_power_charge=float(row.get("capital_cost", 0.0)),
            capital_cost_power_discharge=float(row.get("capital_cost", 0.0)),
            lifetime=int(row.get("lifetime", 0)),
            spillage_cost=float(row.get("spillage_cost", 0.0)),
        )
    del df_hydro
    return hydro_units_local


def load_chp_tes(
    df_pp: pd.DataFrame,
    store: StorageRecordStore,
    default_carrier: str,
    default_bus: str,
    sector_key: Optional[str],
) -> None:
    """Load CHP thermal storage (TES) units for non-electricity sectors.

    Args:
        df_pp: Powerplants DataFrame.
        store: StorageRecordStore that receives per-unit fields.
        default_carrier: Default carrier when missing in input.
        default_bus: Default bus when missing in input.
        sector_key: Sector name for conditional loading.
    """
    if sector_key == "electricity" or "chp_type" not in df_pp.columns:
        return
    chp_mask = df_pp["chp_type"].astype(str).str.upper() != "N"
    df_chp = df_pp[chp_mask & df_pp["chp_type"].notna()].copy()
    for _, row in df_chp.iterrows():
        name = str(row["name"])
        bus_out_2_val = row.get("bus_out_2", None)
        if pd.isna(bus_out_2_val) or str(bus_out_2_val).strip() == "":
            continue
        carrier_in_val = str(row.get("carrier_in", default_carrier))
        carrier_out_2_val = row.get("carrier_out_2", None)
        if pd.notna(carrier_out_2_val) and str(carrier_out_2_val).strip() != "":
            carrier_out_val = str(carrier_out_2_val)
        else:
            carrier_out_val = str(row.get("carrier_out", "heat"))
        bus_in_val = str(row.get("bus_in", default_bus))
        bus_out_val = str(bus_out_2_val)
        e_nom_val = float(row.get("e_nom", 0.0))
        p_charge_nom_val = float(row.get("p_charge_nom", 0.0))
        p_discharge = float(row.get("chp_max_heat", row.get("p_nom", 0.0)))
        if e_nom_val == 0 and p_charge_nom_val == 0 and p_discharge == 0:
            continue
        store.add_record(
            unit=name,
            tech="CHP_TES",
            system=str(row.get("system", "")) if "system" in row else "",
            region=str(row.get("region", "")) if "region" in row else "",
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
            efficiency_charge=float(row.get("efficiency_charge", 1.0)),
            efficiency_discharge=float(row.get("efficiency_discharge", 1.0)),
            standby_loss=float(row.get("standby_loss", 0.0)),
            capital_cost_energy=0.0,
            capital_cost_power_charge=0.0,
            capital_cost_power_discharge=0.0,
            lifetime=int(row.get("lifetime", 0)),
            spillage_cost=float(row.get("spillage_cost", 0.0)),
        )
    del df_chp


def duration_to_power(e_val: Optional[float], dur_h: Optional[float]) -> Optional[float]:
    """Convert energy and duration into nominal power when duration is valid.

    Args:
        e_val: Energy capacity.
        dur_h: Duration in hours.

    Returns:
        Nominal power if duration is positive, otherwise None.
    """
    if e_val is None:
        return None
    if dur_h is None or dur_h <= 0:
        return None
    return e_val / dur_h


def load_template_storage(
    path: Optional[str],
    sets: SystemSets,
    store: StorageRecordStore,
    default_carrier: str,
    default_bus: str,
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
    if path:
        try:
            _ = resolve_table_path(path)
            df_st = read_table(path)
        except FileNotFoundError:
            df_st = pd.DataFrame()
    else:
        df_st = pd.DataFrame()

    required_st = {
        "name", "tech", "carrier_in", "carrier_out", "bus_in", "bus_out",
        "e_nom", "e_nom_max", "e_min", "duration_charge", "duration_discharge",
        "efficiency_charge", "efficiency_discharge", "standby_loss",
        "capital_cost_energy", "capital_cost_power_charge", "capital_cost_power_discharge",
        "lifetime", "spillage_cost",
    }
    if not df_st.empty:
        require_columns(df_st, required_st, path or "<storage>")
        if "name" in df_st.columns and getattr(sets, "storage_units", None):
            allowed_units = {str(u) for u in sets.storage_units}
            df_st = df_st[df_st["name"].astype(str).isin(allowed_units)].copy()
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
        require_values(df_st, required_str, required_num, path or "<storage>", name_col="name")

    if not df_st.empty:
        for _, row in df_st.iterrows():
            # Fill optional power columns from duration when not explicitly provided.
            name = str(row["name"])
            e_nom_val = get_float(row, "e_nom", default=0.0)
            e_nom_max_val = get_float(row, "e_nom_max", default=e_nom_val)

            dur_ch = get_float(row, "duration_charge", default=None)
            if dur_ch is not None and dur_ch <= 0:
                raise ValueError(
                    f"{path} invalid duration_charge for unit '{name}': {dur_ch}"
                )
            dur_dis = get_float(row, "duration_discharge", default=None)
            if dur_dis is not None and dur_dis <= 0:
                raise ValueError(
                    f"{path} invalid duration_discharge for unit '{name}': {dur_dis}"
                )

            p_charge_nom_val = get_float(row, "p_charge_nom", default=None)
            if p_charge_nom_val is None:
                p_charge_nom_val = duration_to_power(e_nom_val, dur_ch)
            if p_charge_nom_val is None:
                p_charge_nom_val = 0.0

            p_discharge_nom_val = get_float(row, "p_discharge_nom", default=None)
            if p_discharge_nom_val is None:
                p_discharge_nom_val = duration_to_power(e_nom_val, dur_dis)
            if p_discharge_nom_val is None:
                p_discharge_nom_val = 0.0

            p_charge_nom_max_val = get_float(row, "p_charge_nom_max", default=None)
            if p_charge_nom_max_val is None:
                p_charge_nom_max_val = duration_to_power(e_nom_max_val, dur_ch)
            if p_charge_nom_max_val is None:
                p_charge_nom_max_val = p_charge_nom_val

            p_discharge_nom_max_val = get_float(row, "p_discharge_nom_max", default=None)
            if p_discharge_nom_max_val is None:
                p_discharge_nom_max_val = duration_to_power(e_nom_max_val, dur_dis)
            if p_discharge_nom_max_val is None:
                p_discharge_nom_max_val = p_discharge_nom_val

            store.add_record(
                unit=name,
                tech=str(row["tech"]),
                system=str(row.get("system", "")) if "system" in row else "",
                region=str(row.get("region", "")) if "region" in row else "",
                carrier_in=str(row.get("carrier_in", default_carrier)),
                carrier_out=str(row.get("carrier_out", default_carrier)),
                bus_in=str(row.get("bus_in", default_bus)),
                bus_out=str(row.get("bus_out", default_bus)),
                e_nom=e_nom_val,
                e_nom_max=e_nom_max_val,
                e_min=float(row.get("e_min", 0.0)),
                p_charge_nom=p_charge_nom_val,
                p_charge_nom_max=p_charge_nom_max_val,
                p_discharge_nom=p_discharge_nom_val,
                p_discharge_nom_max=p_discharge_nom_max_val,
                duration_charge=dur_ch if dur_ch is not None else None,
                duration_discharge=dur_dis if dur_dis is not None else None,
                efficiency_charge=float(row.get("efficiency_charge", 1.0)),
                efficiency_discharge=float(row.get("efficiency_discharge", 1.0)),
                standby_loss=float(row.get("standby_loss", 0.0)),
                capital_cost_energy=float(row.get("capital_cost_energy", 0.0)),
                capital_cost_power_charge=float(row.get("capital_cost_power_charge", 0.0)),
                capital_cost_power_discharge=float(row.get("capital_cost_power_discharge", 0.0)),
                lifetime=int(row.get("lifetime", 0)),
                spillage_cost=float(row.get("spillage_cost", 0.0)),
            )
    del df_st
    gc.collect()


def load_inflows(
    path: Optional[str],
    units: List[str],
    sets: SystemSets,
) -> Dict[UPY, float]:
    """Load hydro inflows and convert to a long mapping.

    Args:
        path: Path to the inflow table.
        units: List of hydro units to include.
        sets: SystemSets with modeled years and periods.

    Returns:
        Mapping keyed by (unit, period, year).

    Raises:
        ValueError: If required columns are missing.
    """
    inflow_local: Dict[UPY, float] = {}
    if not path or not units:
        return inflow_local
    df_in = read_table(path)
    for col in ("year", "period"):
        if col not in df_in.columns:
            raise ValueError(f"{path} missing column '{col}'")
    df_in = df_in[df_in["year"].isin(sets.years) & df_in["period"].isin(sets.periods)].copy()
    id_vars = ["year", "period"]
    value_cols = [c for c in df_in.columns if c not in id_vars and c in units]
    if value_cols:
        stacked = stack_compat(df_in, id_vars, value_cols)
        inflow_local = {}
        for idx, val in stacked.items():
            year, period, unit = idx  # type: ignore
            inflow_local[(str(unit), int(period), int(year))] = float(val)
        del stacked
    del df_in
    gc.collect()
    return inflow_local
