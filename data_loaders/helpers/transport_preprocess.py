"""One-time normalization of raw transport inputs into canonical Parquet tables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_loaders.helpers.io import read_table
from data_loaders.helpers.timeseries import empty_frame
from data_loaders.helpers.validation_utils import require_columns, require_values

TRANSPORT_KEY_COLS = ["system", "region", "transport_segment"]


def _make_unit_names(df: pd.DataFrame) -> pd.Series:
    parts = (
        df[["transport_sector_bus", "tech", "fuel_type"]]
        .fillna("")
        .astype(str)
        .apply(lambda col: col.str.strip().str.replace(" ", "_", regex=False))
    )
    return parts["transport_sector_bus"] + "_" + parts["tech"] + "_" + parts["fuel_type"]


def _read_segment_timeseries(
    path: str,
    *,
    value_name: str,
    segment_meta: pd.DataFrame,
) -> pd.DataFrame:
    df = read_table(path)
    require_columns(df, {"year", "period"}, path)

    valid_segments = set(segment_meta["transport_segment"].astype(str).str.strip())
    segment_cols = [col for col in df.columns if col not in {"year", "period"} and str(col).strip() in valid_segments]
    if not segment_cols:
        return empty_frame([*TRANSPORT_KEY_COLS, "year", "period", value_name])

    long_df = df.melt(
        id_vars=["year", "period"],
        value_vars=segment_cols,
        var_name="transport_segment",
        value_name=value_name,
    ).dropna(subset=[value_name])
    if long_df.empty:
        return empty_frame([*TRANSPORT_KEY_COLS, "year", "period", value_name])

    long_df["transport_segment"] = long_df["transport_segment"].astype(str).str.strip()
    long_df["year"] = pd.to_numeric(long_df["year"], errors="raise").astype(int)
    long_df["period"] = pd.to_numeric(long_df["period"], errors="raise").astype(int)
    long_df[value_name] = pd.to_numeric(long_df[value_name], errors="raise")

    out = long_df.merge(
        segment_meta,
        on="transport_segment",
        how="left",
        validate="many_to_one",
    )[[*TRANSPORT_KEY_COLS, "year", "period", value_name]]
    missing_segments = (
        out.loc[out["system"].isna(), "transport_segment"].dropna().astype(str).unique().tolist()
    )
    if missing_segments:
        raise ValueError(f"{path} contains unknown transport segments: {sorted(missing_segments)}")
    return out


def build_transport_params_table(
    general_params_path: str,
    fleet_and_demand_path: str,
) -> pd.DataFrame:
    df_gen = read_table(general_params_path)
    df_fleet = read_table(fleet_and_demand_path)

    require_columns(df_gen, {"transport_sector_bus", "tech", "fuel_type"}, general_params_path)
    require_columns(
        df_fleet,
        {
            "system",
            "region",
            "transport_sector_bus",
            "tech",
            "fuel_type",
            "bus_in",
            "fleet_units",
            "efficiency_primary",
            "transport_demand_total",
        },
        fleet_and_demand_path,
    )

    df = df_fleet.merge(
        df_gen,
        on=["transport_sector_bus", "tech", "fuel_type"],
        how="left",
        validate="many_to_one",
    )
    if "name" not in df.columns:
        df["name"] = _make_unit_names(df)

    defaults = {"ev_sto_min": 0.0, "max_investment": None, "life_time": 0}
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    require_values(
        df,
        required_str=["system", "region", "transport_sector_bus", "name", "tech"],
        required_num=[
            "fleet_units",
            "average_bat",
            "average_ch_rate",
            "V2G_cost",
            "efficiency_primary",
            "ev_grid_eff",
            "transport_demand_total",
        ],
        path_label=fleet_and_demand_path,
        name_col="name",
    )

    numeric_cols = [
        "fleet_units",
        "average_bat",
        "average_ch_rate",
        "V2G_cost",
        "efficiency_primary",
        "ev_grid_eff",
        "ev_sto_min",
        "max_investment",
        "life_time",
        "transport_demand_total",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return pd.DataFrame(
        {
            "unit": df["name"].astype(str).str.strip(),
            "system": df["system"].astype(str).str.strip(),
            "region": df["region"].astype(str).str.strip(),
            "transport_segment": df["transport_sector_bus"].astype(str).str.strip(),
            "tech": df["tech"].astype(str).str.strip(),
            "fuel_type": df["fuel_type"].astype(str).str.strip(),
            "bus_in": df["bus_in"].fillna("").astype(str).str.strip(),
            "efficiency_primary": df["efficiency_primary"].astype(float),
            "fleet_units": df["fleet_units"].astype(float),
            "battery_capacity_kwh": df["average_bat"].astype(float),
            "charge_rate_kw": df["average_ch_rate"].astype(float),
            "grid_efficiency": df["ev_grid_eff"].astype(float),
            "storage_min_soc": df["ev_sto_min"].fillna(0.0).astype(float),
            "v2g_cost": df["V2G_cost"].astype(float),
            "lifetime": df["life_time"].fillna(0).astype(int),
            "max_investment": df["max_investment"],
            "annual_demand": df["transport_demand_total"].astype(float),
        }
    )


def build_transport_tables(
    *,
    general_params_path: str,
    fleet_and_demand_path: str,
    availability_path: str,
    demand_timeseries_path: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build canonical transport params, availability, and demand tables."""
    params = build_transport_params_table(general_params_path, fleet_and_demand_path)
    segment_meta = params[TRANSPORT_KEY_COLS].drop_duplicates().reset_index(drop=True)

    availability = _read_segment_timeseries(
        availability_path,
        value_name="availability",
        segment_meta=segment_meta,
    )
    demand_profile = _read_segment_timeseries(
        demand_timeseries_path,
        value_name="demand_profile",
        segment_meta=segment_meta,
    )
    demand_totals = (
        params.groupby(TRANSPORT_KEY_COLS, as_index=False)["annual_demand"]
        .sum()
        .reset_index(drop=True)
    )
    demand = demand_profile.merge(
        demand_totals,
        on=TRANSPORT_KEY_COLS,
        how="left",
        validate="many_to_one",
    )
    return params.drop(columns=["annual_demand"]), availability, demand


def save_transport_tables(
    *,
    general_params_path: str,
    fleet_and_demand_path: str,
    availability_path: str,
    demand_timeseries_path: str,
    output_dir: str | Path,
) -> tuple[Path, Path, Path]:
    """Write canonical transport Parquet tables used by the runtime loader."""
    params, availability, demand = build_transport_tables(
        general_params_path=general_params_path,
        fleet_and_demand_path=fleet_and_demand_path,
        availability_path=availability_path,
        demand_timeseries_path=demand_timeseries_path,
    )
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    params_path = out_dir / "transport_params.parquet"
    availability_out = out_dir / "transport_availability.parquet"
    demand_out = out_dir / "transport_demand.parquet"
    params.to_parquet(params_path, index=False)
    availability.to_parquet(availability_out, index=False)
    demand.to_parquet(demand_out, index=False)
    return params_path, availability_out, demand_out
