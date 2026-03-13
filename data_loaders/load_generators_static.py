"""Static generator parameter loader."""

from __future__ import annotations

from typing import Optional, Union
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Bus import Bus
from data_loaders.helpers.io import TableCache, inspect_table, read_table


def load_generators_static(
    powerplants_path: Union[str, pd.DataFrame],
    sets: SystemSets,
    buses: Bus,
    table_cache: Optional[TableCache] = None,
) -> pd.DataFrame:
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

    def required_str_full(table: pd.DataFrame, col: str, *, units_with_values: Optional[pd.Index] = None) -> pd.Series:
        if col not in table.columns:
            raise ValueError(f"{powerplants_path} missing required column: '{col}'")
        out = table[col].astype(object)
        stripped = out.astype(str).str.strip()
        mask_missing = out.isna() | stripped.eq("")
        if units_with_values is not None:
            relevant = table.index.isin(units_with_values)
            mask_missing = mask_missing & relevant
        if mask_missing.any():
            missing_units = table.index[mask_missing].astype(str).tolist()
            raise ValueError(f"{powerplants_path} has missing required values in '{col}': {missing_units}")
        return stripped

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

    if isinstance(powerplants_path, pd.DataFrame):
        df = powerplants_path.copy()
    else:
        cols_available = set(inspect_table(powerplants_path, cache=table_cache).columns)
        selected_cols = [
            c
            for c in (
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
                "system",
                "region",
                "carrier_out",
                "carrier_out_2",
                "carrier_in",
                "bus_out",
                "bus_out_2",
                "bus_in",
                "chp_power_to_heat",
                "chp_power_loss_factor",
                "chp_max_heat",
                "chp_type",
            )
            if c in cols_available
        ]
        df = read_table(powerplants_path, columns=selected_cols or None, cache=table_cache)

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
        return pd.DataFrame(columns=[
            "unit",
            "system",
            "region",
            "tech",
            "fuel",
            "unit_type",
            "carrier_in",
            "carrier_out",
            "bus_in",
            "bus_out",
            "bus_out_2",
            "carrier_out_2",
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
            "chp_power_to_heat",
            "chp_power_loss_factor",
            "chp_max_heat",
            "chp_type",
        ])

    df_gen = df[df["name"].isin(gen_units)].copy()
    if df_gen.empty:
        raise ValueError("No rows in powerplants match SystemSets.units.")

    df_gen = df_gen.set_index("name")
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
    missing_values: dict[str, list] = {}
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

    static_df = pd.DataFrame(index=df_gen.index)
    static_df.index.name = "unit"
    static_df["system"] = df_gen["system"].astype(str) if "system" in df_gen.columns else ""
    static_df["region"] = df_gen["region"].astype(str) if "region" in df_gen.columns else ""
    static_df["tech"] = df_gen["tech"].astype(str)
    static_df["fuel"] = df_gen["fuel"].astype(str)
    unit_type_series = df_gen["unit_type"].astype(str).str.strip().str.lower()
    static_df["unit_type"] = pd.Series("supply", index=df_gen.index, dtype="string")
    static_df.loc[unit_type_series.str.startswith("conv"), "unit_type"] = "conversion"
    static_df["carrier_in"] = optional_str_full(df_gen, "carrier_in").astype("string")
    static_df["carrier_out"] = required_str_full(df_gen, "carrier_out").astype("string")
    static_df["bus_in"] = optional_str_full(df_gen, "bus_in").astype("string")
    static_df["bus_out"] = required_str_full(df_gen, "bus_out").astype("string")
    static_df["bus_out_2"] = optional_str_full(df_gen, "bus_out_2").astype("string")
    secondary_units = static_df.index[static_df["bus_out_2"].fillna("").astype(str).str.strip() != ""]
    static_df["carrier_out_2"] = required_str_full(
        df_gen,
        "carrier_out_2",
        units_with_values=secondary_units,
    ).astype("string") if len(secondary_units) else optional_str_full(df_gen, "carrier_out_2").astype("string")
    for col in [
        "p_nom",
        "p_nom_max",
        "cap_factor",
        "capital_cost",
        "ramping_cost",
        "ramp_up_rate",
        "ramp_down_rate",
        "co2_intensity",
        "final_cap",
        "var_cost_no_fuel",
        "efficiency",
    ]:
        static_df[col] = pd.to_numeric(df_gen[col], errors="raise").astype(float)
    for col in ["decom_start_existing", "decom_start_new", "lifetime"]:
        static_df[col] = pd.to_numeric(df_gen[col], errors="raise").astype(int)
    for col in ["chp_power_to_heat", "chp_power_loss_factor", "chp_max_heat"]:
        static_df[col] = pd.to_numeric(df_gen[col], errors="coerce").astype(float) if col in df_gen.columns else pd.Series(index=df_gen.index, dtype=float)
    static_df["chp_type"] = df_gen["chp_type"].astype("string") if "chp_type" in df_gen.columns else pd.Series(index=df_gen.index, dtype="string")
    return static_df.reset_index()
