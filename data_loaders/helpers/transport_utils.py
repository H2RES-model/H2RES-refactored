"""Transport helpers for parsing params and transport time series."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

import re
import pandas as pd

from data_loaders.helpers.io import read_table
from data_loaders.helpers.validation_utils import require_columns, require_values

def _make_unit_name(row: pd.Series) -> str:
    parts = [row.get("transport_sector_bus"), row.get("tech"), row.get("fuel_type")]
    safe = [str(p).strip().replace(" ", "_") for p in parts]
    return "_".join(safe)

Y = int
P = int
UPY = Tuple[str, P, Y]
_NO_BUS_NEEDED_TOKENS = {"no bus needed", "no_bus_needed"}

#Define which technologies count as electric for the electric demand side
def _is_electric_transport_tech(tech: object) -> bool:
    """Identify transport technologies that use electric energy demand."""
    t = str(tech or "").strip().upper()
    # Match whole tokens to avoid substring false-positives (e.g., HEV vs EV).
    tokens = [tok for tok in re.split(r"[^A-Z0-9]+", t) if tok]
    # Only EV and FCEV are treated as electric demand carriers.
    return any(tok in {"EV", "FCEV"} for tok in tokens)

def _is_no_bus_needed_value(value: object) -> bool:
    """Identify placeholders that mean no electrical bus connection is needed."""
    return str(value or "").strip().lower() in _NO_BUS_NEEDED_TOKENS


def _filter_storage_electric(df: pd.DataFrame) -> pd.DataFrame:
    """Return storage-capable transport rows (EV/FCEV)."""
    if df.empty:
        return df.copy()
    return df[df["tech"].map(_is_electric_transport_tech)].copy()


def _parse_variability_factor(value: object) -> float:
    """Parse yearly V2G variability into a multiplicative factor.

    Accepted examples:
      - "-5%"   -> 0.95
      - "3%"    -> 1.03
      - -0.05   -> 0.95
      - 1.02    -> 1.02
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 1.0
    s = str(value).strip()
    if s.endswith("%"):
        pct = float(s[:-1].replace(",", ".")) / 100.0
        return 1.0 + pct
    v = float(s.replace(",", "."))
    if -1.0 < v < 1.0:
        return 1.0 + v
    return v


def _find_header_row(df_raw: pd.DataFrame) -> Optional[int]:
    """Locate the row where first two columns are year/period."""
    for idx in range(len(df_raw)):
        if df_raw.shape[1] < 2:
            return None
        c0 = str(df_raw.iat[idx, 0]).strip().lower()
        c1 = str(df_raw.iat[idx, 1]).strip().lower()
        if c0 == "year" and c1 == "period":
            return idx
    return None


def _extract_meta_maps(df_raw: pd.DataFrame, header_row: int) -> Dict[str, Dict[int, str]]:
    """Read metadata rows (system/region/transport_sector_bus) by column index."""
    out: Dict[str, Dict[int, str]] = {}
    if header_row <= 0:
        return out
    meta_rows = df_raw.iloc[:header_row]
    for _, row in meta_rows.iterrows():
        label = None
        label_idx = None
        for idx, cell in enumerate(row):
            if pd.notna(cell) and str(cell).strip() != "":
                label = str(cell).strip().lower()
                label_idx = idx
                break
        if label is None or label_idx is None:
            continue
        if label not in {"system", "region", "transport_sector", "transport_sector_bus"}:
            continue
        norm_label = "transport_sector_bus" if label in {"transport_sector", "transport_sector_bus"} else label
        values: Dict[int, str] = {}
        for j in range(label_idx + 1, len(row)):
            cell = row.iloc[j]
            if pd.notna(cell) and str(cell).strip() != "":
                values[j] = str(cell).strip()
        if values:
            out[norm_label] = values
    return out


def _parse_float(value: object, path: str, label: str) -> float:
    """Parse numeric values with comma/percent handling."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        raise ValueError(f"{path} missing numeric value for {label}.")
    s = str(value).strip()
    if s.endswith("%"):
        s = s[:-1]
    s = s.replace(",", ".")
    return float(s)


def _read_transport_demand_totals(path: str) -> Dict[Tuple[str, str, str], float]:
    """Extract transport_demand totals row (by system/region/sector)."""
    df_raw = pd.read_csv(path, header=None)
    header_row = _find_header_row(df_raw)
    if header_row is None:
        raise ValueError(f"{path} missing 'year'/'period' header row.")
    meta = _extract_meta_maps(df_raw, header_row)

    demand_row = None
    demand_label_idx = None
    for idx in range(header_row):
        row = df_raw.iloc[idx]
        label = None
        label_idx = None
        for j, cell in enumerate(row):
            if pd.notna(cell) and str(cell).strip() != "":
                label = str(cell).strip().lower()
                label_idx = j
                break
        if label == "transport_demand":
            demand_row = row
            demand_label_idx = label_idx
            break
    if demand_row is None or demand_label_idx is None:
        raise ValueError(f"{path} missing 'transport_demand' row.")

    totals: Dict[Tuple[str, str, str], float] = {}
    for j in range(demand_label_idx + 1, len(demand_row)):
        sector = meta.get("transport_sector_bus", {}).get(j)
        if not sector:
            continue
        system = meta.get("system", {}).get(j, "")
        region = meta.get("region", {}).get(j, "")
        val = demand_row.iloc[j]
        if pd.isna(val):
            continue
        totals[(str(system), str(region), str(sector))] = _parse_float(
            val, path, f"transport_demand[{sector}]"
        )
    if not totals:
        raise ValueError(f"{path} has no transport_demand values.")
    return totals


def _load_transport_inputs(
    *, general_params_path: str, zones_params_path: str
) -> pd.DataFrame:
    """Merge general transport parameters with user zone modeling inputs."""
    df_gen = read_table(general_params_path)
    df_zone = read_table(zones_params_path)

    if df_gen.empty:
        raise ValueError(f"{general_params_path} is empty.")
    if df_zone.empty:
        raise ValueError(f"{zones_params_path} is empty.")

    # Expect exact column names; no renames are performed here.

    # Merge on sector+tech+fuel_type so zones inherit general parameters.
    merge_keys = ["transport_sector_bus", "tech", "fuel_type"]
    require_columns(df_gen, set(merge_keys), general_params_path)
    require_columns(df_zone, set(merge_keys), zones_params_path)

    # General params must be unique per merge key.
    dupes = df_gen.duplicated(subset=merge_keys, keep=False)
    if dupes.any():
        bad = df_gen.loc[dupes, merge_keys].drop_duplicates().to_dict("records")
        raise ValueError(
            f"{general_params_path} has duplicate keys: {bad}"
        )

    # Join zones (user inputs) with general parameters.
    df = df_zone.merge(
        df_gen,
        on=merge_keys,
        how="left",
        validate="many_to_one",
        suffixes=("", "_gen"),
    )

    # Support common typo in source files.
    if "efficiency_primary" not in df.columns and "efficiency_primay" in df.columns:
        df["efficiency_primary"] = df["efficiency_primay"]

    # Build deterministic unit name if missing.
    if "name" not in df.columns:
        df["name"] = df.apply(_make_unit_name, axis=1)

    # Enforce that general parameters are present after the merge.
    missing_mask = df[["average_bat", "average_ch_rate", "V2G_cost", "V2G_year_cost_variability", "ev_grid_eff"]].isna().any(axis=1)
    if missing_mask.any():
        missing = df.loc[missing_mask, merge_keys]
        raise ValueError(
            f"{zones_params_path} has rows missing general parameters in "
            f"{general_params_path}: {missing.to_dict('records')}"
        )

    return df

def _read_transport_ts(path: str, value_name: str) -> pd.DataFrame:
    """Read transport TS (with optional metadata rows) into long format."""
    df_raw = pd.read_csv(path, header=None)
    header_row = _find_header_row(df_raw)
    if header_row is None:
        # Plain format fallback: must already contain year/period and one value column.
        df = pd.read_csv(path)
        for col in ("year", "period"):
            if col not in df.columns:
                raise ValueError(f"{path} missing required column '{col}'")
        value_cols = [c for c in df.columns if c not in {"year", "period"}]
        if not value_cols:
            raise ValueError(f"{path} has no data columns.")
        if value_name in value_cols:
            c = value_name
        elif len(value_cols) == 1:
            c = value_cols[0]
        else:
            raise ValueError(f"{path} has multiple data columns; expected '{value_name}'.")
        out = df[["year", "period", c]].copy()
        out.rename(columns={c: value_name}, inplace=True)
        out["transport_sector_bus"] = c
        out["system"] = ""
        out["region"] = ""
        return out[["system", "region", "transport_sector_bus", "year", "period", value_name]]

    meta = _extract_meta_maps(df_raw, header_row)
    data = df_raw.iloc[header_row + 1 :].copy()

    rows: List[Dict[str, object]] = []
    ncols = data.shape[1]
    for j in range(2, ncols):
        sector = meta.get("transport_sector_bus", {}).get(j)
        if not sector:
            continue
        system = meta.get("system", {}).get(j, "")
        region = meta.get("region", {}).get(j, "")
        for _, r in data.iterrows():
            y = r.iloc[0]
            p = r.iloc[1]
            v = r.iloc[j]
            if pd.isna(y) or pd.isna(p) or pd.isna(v):
                continue
            rows.append(
                {
                    "system": str(system),
                    "region": str(region),
                    "transport_sector_bus": str(sector),
                    "year": int(float(y)),
                    "period": int(float(p)),
                    value_name: float(v),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["system", "region", "transport_sector_bus", "year", "period", value_name])
    out = pd.DataFrame(rows)
    return out


def _normalize_params(df_params: pd.DataFrame, params_path: str) -> pd.DataFrame:
    """Normalize transport params into a strict transport-input schema."""
    df = df_params.copy()

    required_cols = {
        "system",
        "region",
        "transport_sector_bus",
        "name",
        "tech",
        "fuel_type",
        "efficiency_primary",
        "bus_in",
        "fleet_units",
        "average_bat",
        "average_ch_rate",
        "V2G_cost",
        "V2G_year_cost_variability",
        "ev_grid_eff",
    }
    require_columns(df, required_cols, params_path)

    # Optional columns with defaults.
    defaults = {
        "ev_sto_min": 0.0,
        "max_investment": None,
        "life_time": 0,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    # Required value checks for core columns.
    require_values(
        df,
        required_str=[
            "system",
            "region",
            "transport_sector_bus",
            "name",
            "tech",
            "V2G_year_cost_variability",
        ],
        required_num=[
            "fleet_units",
            "average_bat",
            "average_ch_rate",
            "V2G_cost",
            "efficiency_primary",
            "ev_grid_eff",
        ],
        path_label=params_path,
        name_col="name",
    )

    # Numeric casts and ranges.
    for col in [
        "fleet_units",
        "average_bat",
        "average_ch_rate",
        "V2G_cost",
        "efficiency_primary",
        "ev_grid_eff",
        "ev_sto_min",
        "max_investment",
        "life_time",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    bad_nonneg_cols = [
        "fleet_units",
        "average_bat",
        "average_ch_rate",
        "V2G_cost",
    ]
    for col in bad_nonneg_cols:
        bad = df[df[col] < 0]
        if not bad.empty:
            raise ValueError(f"{params_path} column '{col}' must be >= 0.")

    bad_eff = df[(df["ev_grid_eff"] < 0) | (df["ev_grid_eff"] > 1)]
    if not bad_eff.empty:
        raise ValueError(f"{params_path} column 'ev_grid_eff' must be in [0,1].")

    bad_min = df[(df["ev_sto_min"] < 0) | (df["ev_sto_min"] > 1)]
    if not bad_min.empty:
        raise ValueError(f"{params_path} column 'ev_sto_min' must be in [0,1].")

    # Normalize textual fields.
    for col in [
        "system",
        "region",
        "transport_sector_bus",
        "name",
        "tech",
    ]:
        df[col] = df[col].astype(str).str.strip()
    df["bus_in"] = df["bus_in"].where(~df["bus_in"].isna(), "").astype(str).str.strip()

    electric_mask = df["tech"].map(_is_electric_transport_tech)
    no_bus_needed_mask = df["bus_in"].map(_is_no_bus_needed_value)

    # For non-electric technologies, normalize explicit placeholders to empty.
    df.loc[(~electric_mask) & no_bus_needed_mask, "bus_in"] = ""

    # Require bus_in for EV/FCEV rows; placeholder text is treated as missing.
    missing_bus_in_mask = electric_mask & (
        (df["bus_in"].astype(str).str.strip() == "")
        | df["bus_in"].map(_is_no_bus_needed_value)
    )
    if missing_bus_in_mask.any():
        missing_names = df.loc[missing_bus_in_mask, "name"].astype(str).tolist()
        raise ValueError(
            f"{params_path} has missing required values for 'bus_in' "
            f"in electric transport rows: {missing_names}"
        )

    # Validate efficiency_primary.
    bad_effp = df[(df["efficiency_primary"] <= 0)]
    if not bad_effp.empty:
        raise ValueError(f"{params_path} column 'efficiency_primary' must be > 0.")

    # Storage-capable techs must have positive battery and charge rate.
    mask_storage = df["tech"].map(_is_electric_transport_tech)
    bad_storage = df[mask_storage & ((df["average_bat"] <= 0) | (df["average_ch_rate"] <= 0))]
    if not bad_storage.empty:
        names = bad_storage["name"].astype(str).tolist()
        raise ValueError(
            f"{params_path} storage techs must have average_bat and average_ch_rate > 0. "
            f"Bad rows: {names}"
        )

    # Parse yearly variability factor.
    df["V2G_year_cost_variability_factor"] = df["V2G_year_cost_variability"].map(_parse_variability_factor)
    bad_var = df[df["V2G_year_cost_variability_factor"] <= 0]
    if not bad_var.empty:
        raise ValueError(
            f"{params_path} column 'V2G_year_cost_variability' must result in a positive factor."
        )
    return df


def load_ev_inputs(
    *,
    general_params_path: str,
    zones_params_path: str,
    ev_availability_path: Optional[str] = None,
    ev_demand_path: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[UPY, float], Dict[UPY, float]]:
    """Load transport params and map TS by transport_sector to each unit.

    Returns:
        params_df: Normalized transport rows (all units).
        ev_availability: dict[(unit, period, year)] -> availability factor (storage units only).
        ev_demand_profile: dict[(unit, period, year)] -> demand profile factor.
    """
    df_params_raw = _load_transport_inputs(
        general_params_path=general_params_path,
        zones_params_path=zones_params_path,
    )
    df_all = _normalize_params(df_params_raw, zones_params_path)
    params_df = df_all.copy()

    # Compute sector shares using ALL rows.
    group_cols = ["system", "region", "transport_sector_bus"]
    totals = df_all.groupby(group_cols)["fleet_units"].sum()
    bad_totals = totals[totals <= 0]
    if not bad_totals.empty:
        raise ValueError(
            f"{zones_params_path} has non-positive total fleet_units for "
            f"groups: {bad_totals.index.tolist()}"
        )

    # Demand totals come from transport_demand row in the demand profile file.
    if not ev_demand_path:
        raise ValueError("ev_demand_path is required to load transport demand.")
    demand_totals = _read_transport_demand_totals(ev_demand_path)

    # Add share-based demand to all rows.
    def _group_key(row: pd.Series) -> Tuple[str, str, str]:
        return (str(row["system"]), str(row["region"]), str(row["transport_sector_bus"]))

    shares: List[float] = []
    demand_unit: List[float] = []
    demand_total_list: List[float] = []
    sector_total_list: List[float] = []
    for _, row in params_df.iterrows():
        key = _group_key(row)
        total = float(totals.loc[key])
        share = float(row["fleet_units"]) / total
        if key not in demand_totals:
            raise ValueError(
                f"{ev_demand_path} missing transport_demand for "
                f"system='{key[0]}', region='{key[1]}', transport_sector_bus='{key[2]}'"
            )
        total_dem = float(demand_totals[key])
        shares.append(share)
        demand_total_list.append(total_dem)
        # Convert sector demand to per-unit MWh demand (assume km/pkm/tkm).
        eff = float(row.get("efficiency_primary", 0.0))
        demand_mwh = total_dem * share / eff / 1000.0
        demand_unit.append(demand_mwh)
        sector_total_list.append(total)

    params_df = params_df.copy()
    params_df["ev_demand_share"] = shares
    params_df["ev_demand_total"] = demand_total_list
    params_df["ev_demand_unit"] = demand_unit
    params_df["sector_total_veh"] = sector_total_list

    ev_availability: Dict[UPY, float] = {}
    ev_demand_profile: Dict[UPY, float] = {}

    av_long = _read_transport_ts(ev_availability_path, "ev_availability") if ev_availability_path else pd.DataFrame()
    dem_long = _read_transport_ts(ev_demand_path, "ev_demand") if ev_demand_path else pd.DataFrame()

    params_df_storage = _filter_storage_electric(params_df)

    # Map each unit to its sector profile, filtered by unit system/region.
    for _, row in params_df_storage.iterrows():
        unit = str(row["name"])
        sector = str(row["transport_sector_bus"])
        system = str(row["system"])
        region = str(row["region"])

        if not av_long.empty:
            subset_av = av_long[
                (av_long["transport_sector_bus"].astype(str).str.lower() == sector.lower())
                & (av_long["system"].astype(str).str.lower() == system.lower())
                & (av_long["region"].astype(str).str.lower() == region.lower())
            ]
            if subset_av.empty:
                raise ValueError(
                    f"{ev_availability_path} has no profile for transport_sector_bus='{sector}', "
                    f"system='{system}', region='{region}'."
                )
            for _, tsr in subset_av.iterrows():
                key = (unit, int(tsr["period"]), int(tsr["year"]))
                ev_availability[key] = float(tsr["ev_availability"])

        # availability only for storage units

    # Demand profile for ALL transport rows (including non-storage).
    for _, row in params_df.iterrows():
        unit = str(row["name"])
        sector = str(row["transport_sector_bus"])
        system = str(row["system"])
        region = str(row["region"])
        if not dem_long.empty:
            subset_dem = dem_long[
                (dem_long["transport_sector_bus"].astype(str).str.lower() == sector.lower())
                & (dem_long["system"].astype(str).str.lower() == system.lower())
                & (dem_long["region"].astype(str).str.lower() == region.lower())
            ]
            if subset_dem.empty:
                raise ValueError(
                    f"{ev_demand_path} has no profile for transport_sector_bus='{sector}', "
                    f"system='{system}', region='{region}'."
                )
            for _, tsr in subset_dem.iterrows():
                key = (unit, int(tsr["period"]), int(tsr["year"]))
                ev_demand_profile[key] = float(tsr["ev_demand"])

    if ev_availability:
        bad = [k for k, v in ev_availability.items() if v < 0 or v > 1]
        if bad:
            raise ValueError(
                f"{ev_availability_path} ev_availability must be in [0,1] for keys: {bad[:5]}"
            )
    if ev_demand_profile:
        bad = [k for k, v in ev_demand_profile.items() if v < 0]
        if bad:
            raise ValueError(
                f"{ev_demand_path} ev_demand must be >= 0 for keys: {bad[:5]}"
            )
    if ev_availability_path and not ev_availability:
        raise ValueError(f"{ev_availability_path} has no matching availability profiles.")
    if ev_demand_path and not ev_demand_profile:
        raise ValueError(f"{ev_demand_path} has no matching demand profiles.")

    # Validate that availability and demand cover the same horizon.
    if ev_availability and ev_demand_profile:
        years_av = {y for (_, _, y) in ev_availability.keys()}
        years_dem = {y for (_, _, y) in ev_demand_profile.keys()}
        if years_av != years_dem:
            raise ValueError(
                "EV availability and demand years do not match. "
                f"availability years={sorted(years_av)}, demand years={sorted(years_dem)}"
            )
        periods_av = {p for (_, p, _) in ev_availability.keys()}
        periods_dem = {p for (_, p, _) in ev_demand_profile.keys()}
        if periods_av != periods_dem:
            raise ValueError(
                "EV availability and demand periods do not match. "
                f"availability periods={sorted(periods_av)}, demand periods={sorted(periods_dem)}"
            )
        grid_av = {(p, y) for (_, p, y) in ev_availability.keys()}
        grid_dem = {(p, y) for (_, p, y) in ev_demand_profile.keys()}
        if grid_av != grid_dem:
            raise ValueError(
                "EV availability and demand (period, year) grids do not match."
            )

    return params_df, ev_availability, ev_demand_profile


def build_transport_storage_units_csv(
    *,
    general_params_path: str,
    zones_params_path: str,
    output_path: str,
    buses: Iterable[str] | None = None,
) -> None:
    """Create transport_storage_units.csv from transport inputs.

    The generated table keeps only transport-specific storage fields.
    Connectivity fields like carrier_in/carrier_out/bus_out are filled by
    the storage loader defaults.
    """
    df_raw = _load_transport_inputs(
        general_params_path=general_params_path,
        zones_params_path=zones_params_path,
    )
    params_df_all = _normalize_params(df_raw, zones_params_path)
    params_df = _filter_storage_electric(params_df_all)
    if params_df.empty:
        # No storage-capable transport units; write empty template and return.
        cols = [
            "system",
            "region",
            "name",
            "tech",
            "bus_in",
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
        pd.DataFrame(columns=cols).to_csv(output_path, index=False)
        return

    known_buses = {str(b).strip().lower() for b in buses} if buses is not None else set()

    rows: List[Dict[str, object]] = []
    for _, row in params_df.iterrows():
        unit = str(row["name"])
        bus_in = str(row.get("bus_in", "")).strip()
        excel_row = int(row.name) + 2 if isinstance(row.name, (int, float)) else None

        # Validate buses if a list was provided.
        if known_buses:
            if bus_in.lower() not in known_buses:
                line_txt = f", row {excel_row}" if excel_row is not None else ""
                raise ValueError(
                    f"{zones_params_path}{line_txt}: bus_in '{bus_in}' not found in data/buses.csv"
                )

        fleet_units = float(row["fleet_units"])
        average_bat = float(row["average_bat"])
        average_ch_rate = float(row["average_ch_rate"])
        max_inv_raw = row.get("max_investment", None)
        max_veh = float(max_inv_raw) if pd.notna(max_inv_raw) else fleet_units

        if average_ch_rate <= 0:
            raise ValueError(f"{zones_params_path} average_ch_rate must be > 0 for '{unit}'.")
        if max_veh < fleet_units:
            max_veh = fleet_units

        # Convert vehicle-level parameters into system-scale storage values.
        e_nom = fleet_units * average_bat / 1000.0
        e_nom_max = max_veh * average_bat / 1000.0
        e_min = float(row.get("ev_sto_min", 0.0)) * e_nom
        duration = average_bat / average_ch_rate
        eff = float(row["ev_grid_eff"])
        v2g_cost = float(row["V2G_cost"])
        lifetime = int(float(row.get("life_time", 0) or 0))

        rows.append(
            {
                "system": str(row["system"]),
                "region": str(row["region"]),
                "name": unit,
                "tech": str(row["tech"]),
                "bus_in": bus_in,
                "e_nom": e_nom,
                "e_nom_max": e_nom_max,
                "e_min": e_min,
                "duration_charge": duration,
                "duration_discharge": duration,
                "efficiency_charge": eff,
                "efficiency_discharge": eff,
                "standby_loss": 0.0,
                "capital_cost_energy": 0.0,
                "capital_cost_power_charge": 0.0,
                "capital_cost_power_discharge": v2g_cost,
                "lifetime": lifetime,
                "spillage_cost": 0.0,
            }
        )

    out = pd.DataFrame(rows)
    out.to_csv(output_path, index=False)


