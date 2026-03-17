"""Transport readers that build the internal tables directly from raw inputs."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from data_loaders.helpers.io import TableCache, read_table
from data_loaders.helpers.timeseries import coerce_time_columns, empty_frame, normalize_string_columns
from data_loaders.helpers.validation_utils import require_columns, require_values

TRANSPORT_KEY_COLS = ["system", "region", "transport_segment"]


def _is_electric_transport_tech(tech: object) -> bool:
    """Return True for transport technologies modeled on the electric demand side."""
    tokens = [tok for tok in re.split(r"[^A-Z0-9]+", str(tech or "").strip().upper()) if tok]
    return any(tok in {"EV", "FCEV"} for tok in tokens)


def _make_unit_names(df: pd.DataFrame) -> pd.Series:
    parts = (
        df[["transport_sector_bus", "tech", "fuel_type"]]
        .fillna("")
        .astype(str)
        .apply(lambda col: col.str.strip().str.replace(" ", "_", regex=False))
    )
    return parts["transport_sector_bus"] + "_" + parts["tech"] + "_" + parts["fuel_type"]


def _read_segment_timeseries(
    path: str | Path,
    *,
    value_name: str,
    segment_meta: pd.DataFrame,
    cache: TableCache | None = None,
) -> pd.DataFrame:
    df = read_table(path, cache=cache)
    require_columns(df, {"year", "period"}, str(path))

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

    out = long_df.merge(segment_meta, on="transport_segment", how="left", validate="many_to_one")
    missing_segments = out.loc[out["system"].isna(), "transport_segment"].dropna().astype(str).unique().tolist()
    if missing_segments:
        raise ValueError(f"{path} contains unknown transport segments: {sorted(missing_segments)}")
    return out[[*TRANSPORT_KEY_COLS, "year", "period", value_name]]


def build_transport_params(
    *,
    general_params_path: str | Path,
    fleet_and_demand_path: str | Path,
    cache: TableCache | None = None,
) -> pd.DataFrame:
    """Read raw transport tables and build the transport static table."""
    df_general = read_table(general_params_path, cache=cache)
    df_fleet = read_table(fleet_and_demand_path, cache=cache)

    require_columns(df_general, {"transport_sector_bus", "tech", "fuel_type"}, str(general_params_path))
    require_columns(
        df_fleet,
        {
            "system",
            "region",
            "transport_sector_bus",
            "tech",
            "fuel_type",
            "bus_in",
            "efficiency_primary",
            "fleet_units",
            "transport_demand_total",
        },
        str(fleet_and_demand_path),
    )

    df = df_fleet.merge(
        df_general,
        on=["transport_sector_bus", "tech", "fuel_type"],
        how="left",
        validate="many_to_one",
    )
    if "name" not in df.columns:
        df["name"] = _make_unit_names(df)

    for column, default in {"ev_sto_min": 0.0, "max_investment": None, "life_time": 0}.items():
        if column not in df.columns:
            df[column] = default

    require_values(
        df,
        required_str=["system", "region", "transport_sector_bus", "name", "tech", "fuel_type"],
        required_num=[
            "fleet_units",
            "average_bat",
            "average_ch_rate",
            "V2G_cost",
            "efficiency_primary",
            "ev_grid_eff",
        ],
        path_label=str(fleet_and_demand_path),
        name_col="name",
    )

    for column in [
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
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["transport_demand_total"] = df["transport_demand_total"].fillna(0.0)

    out = pd.DataFrame(
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
    return normalize_string_columns(
        out,
        ["unit", "system", "region", "transport_segment", "tech", "fuel_type", "bus_in"],
    )


def build_transport_availability(
    *,
    availability_path: str | Path,
    params: pd.DataFrame,
    cache: TableCache | None = None,
) -> pd.DataFrame:
    """Read raw transport availability and map it to transport segments."""
    segment_meta = params[TRANSPORT_KEY_COLS].drop_duplicates(ignore_index=True)
    out = _read_segment_timeseries(
        availability_path,
        value_name="availability",
        segment_meta=segment_meta,
        cache=cache,
    )
    out = normalize_string_columns(out, ["system", "region", "transport_segment"])
    out = coerce_time_columns(out)
    out["availability"] = pd.to_numeric(out["availability"], errors="raise")
    return out


def build_transport_demand(
    *,
    demand_timeseries_path: str | Path,
    params: pd.DataFrame,
    cache: TableCache | None = None,
) -> pd.DataFrame:
    """Read raw transport demand profile and attach annual segment totals."""
    segment_meta = params[TRANSPORT_KEY_COLS].drop_duplicates(ignore_index=True)
    profile = _read_segment_timeseries(
        demand_timeseries_path,
        value_name="demand_profile",
        segment_meta=segment_meta,
        cache=cache,
    )
    profile = normalize_string_columns(profile, ["system", "region", "transport_segment"])
    profile = coerce_time_columns(profile)
    profile["demand_profile"] = pd.to_numeric(profile["demand_profile"], errors="raise")
    totals = params.groupby(TRANSPORT_KEY_COLS, as_index=False, sort=False)["annual_demand"].sum()
    out = profile.merge(totals, on=TRANSPORT_KEY_COLS, how="left", validate="many_to_one")
    out["annual_demand"] = pd.to_numeric(out["annual_demand"], errors="raise")
    return out
