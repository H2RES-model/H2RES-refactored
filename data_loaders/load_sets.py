"""SystemSets loader that defines core indices and technology subsets."""

from __future__ import annotations

from typing import List, Optional
import pandas as pd

from data_models.SystemSets import SystemSets
from data_loaders.helpers.io import TableCache, read_table
from data_loaders.helpers.iter_utils import union_lists
from data_loaders.helpers.model_factory import build_model

def load_sets(
    powerplants_path: str,
    renewable_profiles_path: Optional[str] = None,
    fuel_cost_path: Optional[str] = None,
    buses_path: Optional[str] = None,
    storage_path: Optional[str] = None,
    existing_sets: Optional[SystemSets] = None,
    table_cache: Optional[TableCache] = None,
) -> SystemSets:
    """Load SystemSets from core input tables.

    When used: called by `load_sector` to define time indices and unit subsets.

    Args:
        powerplants_path: Path to the powerplants (or converters) file.
        renewable_profiles_path: Optional RES profile file to define time horizon.
        fuel_cost_path: Optional fuel cost file used as fallback for time horizon.
        buses_path: Optional buses metadata file for buses and carriers.
        storage_path: Optional storage template file for storage unit names.
        existing_sets: Existing SystemSets to merge into (existing wins).

    Returns:
        SystemSets with years, periods, units, and technology subsets populated.

    Raises:
        ValueError: If required columns are missing or no time horizon can be derived.

    Notes:
        `units` includes generator and converter units, while `storage_units` includes
        hydro storage units plus template storages.
    """

    # --------------------------------------------------------------
    # 1. Read input CSVs
    # --------------------------------------------------------------
    powerplant_cols = [
        "name",
        "tech",
        "fuel",
        "carrier_out",
        "bus_out",
        "p_nom",
        "p_nom_max",
        "cap_factor",
        "capital_cost",
        "lifetime",
        "ramping_cost",
        "ramp_up_rate",
        "ramp_down_rate",
        "co2_intensity",
        "decom_start_existing",
        "decom_start_new",
        "final_cap",
        "var_cost_no_fuel",
        "efficiency",
    ]
    df_powerplant = read_table(powerplants_path, columns=powerplant_cols, cache=table_cache)
    df_profiles = (
        read_table(renewable_profiles_path, columns=["year", "period"], cache=table_cache)
        if renewable_profiles_path
        else None
    )
    df_fc = (
        read_table(fuel_cost_path, columns=["year", "period"], cache=table_cache)
        if fuel_cost_path
        else None
    )
    df_buses = read_table(buses_path, columns=["bus", "carrier"], cache=table_cache) if buses_path else None
    df_storage = read_table(storage_path, columns=["name"], cache=table_cache) if storage_path else None

    # --------------------------------------------------------------
    # 2. Basic validation of required columns
    # --------------------------------------------------------------
    required_powerplant_cols = {
        "name",
        "tech",
        "fuel",
        "carrier_out", 
        "bus_out",
        "p_nom",
        "p_nom_max",
        "cap_factor",
        "capital_cost",
        "lifetime",
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
    missing_unit_cols = required_powerplant_cols - set(df_powerplant.columns)
    if missing_unit_cols:
        raise ValueError(
            f"Missing required columns in powerplant file ({powerplants_path}): "
            f"{sorted(missing_unit_cols)}"
        )

    for name, df in [("availability factor", df_profiles), ("fuel cost", df_fc)]:
        if df is None:
            continue
        missing_ts = {"year", "period"} - set(df.columns)
        if missing_ts:
            raise ValueError(
                f"{name.capitalize()} file is missing required columns "
                f"{sorted(missing_ts)}"
            )

    if df_buses is not None:
        missing_b = {"bus", "carrier"} - set(df_buses.columns)
        if missing_b:
            raise ValueError(
                f"Missing required columns in buses file ({buses_path}): "
                f"{sorted(missing_b)}"
            )
    if df_storage is not None:
        if "name" not in df_storage.columns:
            raise ValueError(
                f"Missing required column 'name' in storage file ({storage_path})"
            )

    # --------------------------------------------------------------
    # 3. Time sets from availability factor or fuel cost file
    # --------------------------------------------------------------
    time_df = df_profiles if df_profiles is not None else df_fc
    if time_df is None:
        if existing_sets:
            years = sorted(set(existing_sets.years))
            periods = sorted(set(existing_sets.periods))
        else:
            raise ValueError(
                "At least one time-series file (renewable_profiles_path or fuel_cost_path) "
                "is required to define years and periods."
            )
    else:
        years_new: List[int] = sorted(int(y) for y in time_df["year"].unique())
        periods_new: List[int] = sorted(int(p) for p in time_df["period"].unique())
        years = sorted(set(years_new).union(set(existing_sets.years) if existing_sets else set()))
        periods = sorted(set(periods_new).union(set(existing_sets.periods) if existing_sets else set()))

    # --------------------------------------------------------------
    # 4. Buses and carriers from buses.csv (fallback to defaults)
    # --------------------------------------------------------------
    if df_buses is not None:
        buses_new: List[str] = sorted(df_buses["bus"].dropna().astype(str).unique().tolist())
        carriers_new: List[str] = sorted(df_buses["carrier"].dropna().astype(str).unique().tolist())
        if not carriers_new:
            carriers_new = ["Electricity"]
    else:
        buses_new = ["SystemBus"]
        carriers_new = ["Electricity"]

    carriers = sorted(set(union_lists(carriers_new, getattr(existing_sets, "carriers", []))))
    buses = sorted(set(union_lists(buses_new, getattr(existing_sets, "buses", []))))

    # --------------------------------------------------------------
    # 5. Units and technology/fuel maps
    # --------------------------------------------------------------
    # Use the renamed columns: name, tech, fuel
    unit_series = df_powerplant["name"].astype(str)
    tech_series = df_powerplant["tech"].astype(str)
    fuel_series = df_powerplant["fuel"].astype(str)
    fuel_lower = fuel_series.str.lower()
    tech_upper = tech_series.str.upper()
    tech_lower = tech_series.str.lower()

    all_unit_names: List[str] = unit_series.tolist()
    tech_map = dict(zip(unit_series, tech_series))
    fuel_map = dict(zip(unit_series, fuel_series))

    # --------------------------------------------------------------
    # 6. Identify hydro techs and split generator vs storage units
    # --------------------------------------------------------------
    hdam_units = unit_series[tech_upper == "HDAM"].tolist()
    hphs_units = unit_series[tech_upper == "HPHS"].tolist()
    hror_units = unit_series[tech_upper == "HROR"].tolist()

    # Hydro storage units = dam + pumped hydro (storage-only)
    hydro_storage_units = sorted(set(hdam_units + hphs_units))

    # Generator units: all units 
    units_new: List[str] = sorted(u for u in all_unit_names)
    units = sorted(set(union_lists(units_new, getattr(existing_sets, "units", []))))

    # Storage units: hydro storage plus template-based storages from storage_units.csv
    storage_template_units: List[str] = []
    if df_storage is not None and "name" in df_storage.columns:
        storage_template_units = df_storage["name"].dropna().astype(str).tolist()
    storage_units_new: List[str] = sorted(set(hydro_storage_units) | set(storage_template_units))
    storage_units = sorted(set(union_lists(storage_units_new, getattr(existing_sets, "storage_units", []))))
    battery_units = sorted(set(getattr(existing_sets, "battery_units", [])))
    tes_units = sorted(set(getattr(existing_sets, "tes_units", [])))
    hydrogen_storage_units = sorted(set(getattr(existing_sets, "hydrogen_storage_units", [])))

    # --------------------------------------------------------------
    # 7. Build generator subsets (⊆ units)
    # --------------------------------------------------------------
    wind_units_new = unit_series[fuel_lower == "wind"].tolist()
    solar_units_new = unit_series[fuel_lower == "solar"].tolist()
    biomass_units_new = unit_series[fuel_lower == "biomass"].tolist()

    fossil_fuels = {"coal", "gas", "oil", "nuclear"}
    fossil_units_new = unit_series[fuel_lower.isin(fossil_fuels)].tolist()

    # CHP units: tech contains 'CHP' string (case-insensitive)
    chp_units_new = unit_series[tech_lower.str.contains("chp", regex=False)].tolist()

    # Non-conventional renewables (wind + solar for now)
    ncre_units_new = sorted(set(wind_units_new + solar_units_new))

    # For now, we do not classify disp/nondisp explicitly
    disp_units_new: List[str] = []
    nondisp_units_new: List[str] = []

    # Merge new subsets with existing (no duplicates)
    fossil_units = sorted(set(union_lists(fossil_units_new, getattr(existing_sets, "fossil_units", []))))
    biomass_units = sorted(set(union_lists(biomass_units_new, getattr(existing_sets, "biomass_units", []))))
    hror_units = sorted(set(union_lists(hror_units, getattr(existing_sets, "hror_units", []))))
    wind_units = sorted(set(union_lists(wind_units_new, getattr(existing_sets, "wind_units", []))))
    solar_units = sorted(set(union_lists(solar_units_new, getattr(existing_sets, "solar_units", []))))
    chp_units = sorted(set(union_lists(chp_units_new, getattr(existing_sets, "chp_units", []))))
    ncre_units = sorted(set(union_lists(ncre_units_new, getattr(existing_sets, "ncre_units", []))))
    disp_units = sorted(set(union_lists(disp_units_new, getattr(existing_sets, "disp_units", []))))
    nondisp_units = sorted(set(union_lists(nondisp_units_new, getattr(existing_sets, "nondisp_units", []))))

    hydro_storage_units = sorted(set(union_lists(hydro_storage_units, getattr(existing_sets, "hydro_storage_units", []))))
    hdam_units = sorted(set(union_lists(hdam_units, getattr(existing_sets, "hdam_units", []))))
    hphs_units = sorted(set(union_lists(hphs_units, getattr(existing_sets, "hphs_units", []))))

    # --------------------------------------------------------------
    # 8. Construct SystemSets
    # --------------------------------------------------------------
    sets = build_model(
        SystemSets,
        years=years,
        periods=periods,
        carriers=carriers,
        buses=buses,

        units=units,
        storage_units=storage_units,

        fossil_units=fossil_units,
        biomass_units=biomass_units,
        hror_units=hror_units,
        wind_units=wind_units,
        solar_units=solar_units,
        chp_units=chp_units,
        ncre_units=ncre_units,
        disp_units=disp_units,
        nondisp_units=nondisp_units,

        hydro_storage_units=hydro_storage_units,
        hdam_units=hdam_units,
        hphs_units=hphs_units,

        battery_units=battery_units,
        tes_units=tes_units,
        hydrogen_storage_units=hydrogen_storage_units,
    )

    return sets
