"""Static generator parameter loader."""

from __future__ import annotations

from typing import Dict, Optional, Literal, cast
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Bus import Bus
from data_loaders.helpers.io import read_table


def load_generators_static(
    powerplants_path: str,
    sets: SystemSets,
    buses: Bus,
) -> Dict[str, object]:
    """Load static generator parameters from the powerplants table.

    When used: called by `load_generators` to populate static generator inputs.

    Args:
        powerplants_path: Path to powerplants (or converters) input file.
        sets: SystemSets containing known generator units.
        buses: Bus model used for default bus selection.

    Returns:
        Mapping with `units` and per-unit parameter dicts (tech, fuel, costs, etc.).

    Raises:
        ValueError: If required columns or values are missing, or units are unknown.
    """

    def norm_unit_type(val: object) -> Literal["supply", "conversion"]:
        v = str(val).strip().lower()
        return cast(
            Literal["supply", "conversion"],
            "conversion" if v.startswith("conv") else "supply",
        )

    def series_with_default(
        table: pd.DataFrame, col: str, default_value: object, dtype: type
    ) -> pd.Series:
        if col in table.columns:
            return table[col].fillna(dtype(default_value)).astype(dtype)
        return pd.Series(default_value, index=table.index, dtype=dtype)

    def optional_str_full(table: pd.DataFrame, col: str) -> pd.Series:
        """
        Always return a full-length Series indexed by units.
        Values are stripped strings or None.
        """
        out = pd.Series([None] * len(table), index=table.index, dtype=object)
        if col not in table.columns:
            return out
        s = table[col].astype(object)

        s_str = s.astype(str)
        mask = s.notna() & (s_str.str.strip() != "")
        out.loc[mask] = s_str.loc[mask].str.strip()
        return out

    df = read_table(powerplants_path)

    required_cols = {
        "name",
        "tech",
        "fuel",
        "unit_type",
        "p_nom",
        "p_nom_max",
        "cap_factor",
        "capital_cost",
        "ramping_cost",
        "ramp_up_rate",
        "ramp_down_rate",
        "co2_intensity",
        "decom_start_existing",
        "decom_start_new",
        "final_cap",
        "lifetime",
        "var_cost_no_fuel",
        "efficiency",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"{powerplants_path} missing columns: {sorted(missing)}")

    gen_units = set(sets.units)
    if not gen_units:
        return {
            "units": [],
            "tech": {},
            "fuel": {},
            "unit_type": {},
            "carrier_in": {},
            "carrier_out": {},
            "bus_in": {},
            "bus_out": {},
            "bus_out_2": {},
            "carrier_out_2": {},
            "p_nom": {},
            "p_nom_max": {},
            "cap_factor": {},
            "capital_cost": {},
            "ramping_cost": {},
            "ramp_up_rate": {},
            "ramp_down_rate": {},
            "co2_intensity": {},
            "decom_start_existing": {},
            "decom_start_new": {},
            "final_cap": {},
            "lifetime": {},
            "var_cost_no_fuel": {},
            "efficiency": {},
            "chp_power_to_heat": {},
            "chp_power_loss_factor": {},
            "chp_max_heat": {},
            "chp_type": {},
        }

    df_gen = df[df["name"].isin(gen_units)].copy()
    if df_gen.empty:
        raise ValueError("No rows in powerplants match SystemSets.units.")

    df_gen = df_gen.set_index("name")
    units = df_gen.index.astype(str).tolist()
    required_str = ["tech", "fuel", "unit_type"]
    required_num = [
        "p_nom",
        "p_nom_max",
        "capital_cost",
        "decom_start_existing",
        "decom_start_new",
        "final_cap",
        "lifetime",
        "var_cost_no_fuel",
        "efficiency",
    ]
    missing_values: Dict[str, list] = {}
    for col in required_str:
        mask = df_gen[col].isna() | (df_gen[col].astype(str).str.strip() == "")
        if mask.any():
            missing_values[col] = df_gen.index[mask].astype(str).tolist()
    for col in required_num:
        vals = pd.to_numeric(df_gen[col], errors="coerce")
        mask = vals.isna()
        if mask.any():
            missing_values[col] = df_gen.index[mask].astype(str).tolist()
    if missing_values:
        raise ValueError(f"{powerplants_path} has missing required values: {missing_values}")

    default_carrier = sets.carriers[0] if getattr(sets, "carriers", None) else "electricity"
    default_bus = None
    if getattr(buses, "carrier", None):
        for bus_id, carrier in buses.carrier.items():
            if str(carrier).lower() == "electricity":
                default_bus = bus_id
                break
    if default_bus is None:
        default_bus = sets.buses[0] if getattr(sets, "buses", None) else "SystemBus"

    tech: Dict[str, str] = df_gen["tech"].astype(str).to_dict()
    system: Dict[str, str] = (
        df_gen["system"].astype(str).to_dict()
        if "system" in df_gen.columns
        else {}
    )
    region: Dict[str, str] = (
        df_gen["region"].astype(str).to_dict()
        if "region" in df_gen.columns
        else {}
    )
    fuel: Dict[str, str] = df_gen["fuel"].astype(str).to_dict()
    unit_type: Dict[str, Literal["supply", "conversion"]] = {
        u: norm_unit_type(df_gen.at[u, "unit_type"]) for u in units
    }

    carrier_out: Dict[str, str] = (
        series_with_default(df_gen, "carrier_out", default_carrier, str).to_dict()
    )
    carrier_out_2: Dict[str, Optional[str]] = (
        optional_str_full(df_gen, "carrier_out_2").to_dict()
    )
    carrier_in: Dict[str, Optional[str]] = (
        optional_str_full(df_gen, "carrier_in").to_dict()
    )

    bus_out: Dict[str, str] = (
        series_with_default(df_gen, "bus_out", default_bus, str).to_dict()
    )
    bus_out_2: Dict[str, Optional[str]] = (
        optional_str_full(df_gen, "bus_out_2").to_dict()
    )
    bus_in: Dict[str, Optional[str]] = (
        optional_str_full(df_gen, "bus_in").to_dict()
    )

    p_nom: Dict[str, float] = df_gen["p_nom"].astype(float).to_dict()
    p_nom_max: Dict[str, float] = df_gen["p_nom_max"].astype(float).to_dict()
    cap_factor: Dict[str, float] = df_gen["cap_factor"].astype(float).to_dict()
    capital_cost: Dict[str, float] = df_gen["capital_cost"].astype(float).to_dict()
    ramping_cost: Dict[str, float] = df_gen["ramping_cost"].astype(float).to_dict()
    ramp_up_rate: Dict[str, float] = df_gen["ramp_up_rate"].astype(float).to_dict()
    ramp_down_rate: Dict[str, float] = df_gen["ramp_down_rate"].astype(float).to_dict()
    co2_intensity: Dict[str, float] = df_gen["co2_intensity"].astype(float).to_dict()
    decom_start_existing: Dict[str, int] = df_gen["decom_start_existing"].astype(int).to_dict()
    decom_start_new: Dict[str, int] = df_gen["decom_start_new"].astype(int).to_dict()
    final_cap: Dict[str, float] = df_gen["final_cap"].astype(float).to_dict()
    lifetime: Dict[str, int] = df_gen["lifetime"].astype(int).to_dict()
    var_cost_no_fuel: Dict[str, float] = df_gen["var_cost_no_fuel"].astype(float).to_dict()
    efficiency: Dict[str, float] = df_gen["efficiency"].astype(float).to_dict()

    chp_power_to_heat: Dict[str, float] = (
        df_gen["chp_power_to_heat"].dropna().astype(float).to_dict()
        if "chp_power_to_heat" in df_gen.columns
        else {}
    )
    chp_power_loss_factor: Dict[str, float] = (
        df_gen["chp_power_loss_factor"].dropna().astype(float).to_dict()
        if "chp_power_loss_factor" in df_gen.columns
        else {}
    )
    chp_max_heat: Dict[str, float] = (
        df_gen["chp_max_heat"].dropna().astype(float).to_dict()
        if "chp_max_heat" in df_gen.columns
        else {}
    )
    chp_type: Dict[str, str] = (
        df_gen["chp_type"].dropna().astype(str).to_dict()
        if "chp_type" in df_gen.columns
        else {}
    )

    return {
        "units": units,
        "system": system,
        "region": region,
        "tech": tech,
        "fuel": fuel,
        "unit_type": unit_type,
        "carrier_in": carrier_in,
        "carrier_out": carrier_out,
        "bus_in": bus_in,
        "bus_out": bus_out,
        "bus_out_2": bus_out_2,
        "carrier_out_2": carrier_out_2,
        "p_nom": p_nom,
        "p_nom_max": p_nom_max,
        "cap_factor": cap_factor,
        "capital_cost": capital_cost,
        "ramping_cost": ramping_cost,
        "ramp_up_rate": ramp_up_rate,
        "ramp_down_rate": ramp_down_rate,
        "co2_intensity": co2_intensity,
        "decom_start_existing": decom_start_existing,
        "decom_start_new": decom_start_new,
        "final_cap": final_cap,
        "lifetime": lifetime,
        "var_cost_no_fuel": var_cost_no_fuel,
        "efficiency": efficiency,
        "chp_power_to_heat": chp_power_to_heat,
        "chp_power_loss_factor": chp_power_loss_factor,
        "chp_max_heat": chp_max_heat,
        "chp_type": chp_type,
    }
