"""Storage loader that builds StorageUnits from templates and powerplants."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import gc
import os

from data_models.SystemSets import SystemSets
from data_models.StorageUnits import StorageUnits
from data_models.Bus import Bus
from data_loaders.helpers.defaults import default_carrier, default_electric_bus
from data_loaders.helpers.io import read_table
from data_loaders.helpers.storage_utils import (
    StorageRecordStore,
    assert_unit_key_subset,
    collect_units_from_storage,
    merge_no_overwrite,
)
from data_loaders.helpers.storage_loader import (
    load_chp_tes,
    load_hydro_storage,
    load_inflows,
    load_template_storage,
)
from data_loaders.helpers.validation_utils import require_columns
from data_loaders.helpers.transport_utils import (
    load_ev_inputs,
    build_transport_storage_units_csv,
    _is_electric_transport_tech,
)

UPY = Tuple[str, int, int]


def load_storage(
    powerplants_path: str,
    storage_path: Optional[str] = None,
    inflow_path: Optional[str] = None,
    transport_general_params_path: Optional[str] = None,
    transport_zones_path: Optional[str] = None,
    transport_availability_path: Optional[str] = None,
    transport_demand_path: Optional[str] = None,
    write_transport_storage_units: bool = True,
    buses_path: Optional[str] = None,
    *,
    sector: Optional[str] = None,
    sets: SystemSets,
    buses: Bus,
    existing_storage: Optional[StorageUnits] = None,
) -> StorageUnits:
    """Build StorageUnits from powerplants, templates, and inflow data.

    When used: called by `load_sector` to populate storage inputs for a sector.

    Args:
        powerplants_path: Path to powerplants file containing hydro and CHP units.
        storage_path: Path to template storage units file (optional).
        inflow_path: Path to inflow data file for hydro units (optional).
          transport_general_params_path: Path to general transport parameters.
          transport_zones_path: Path to transport zones modeling input.
          transport_availability_path: Path to transport availability time series.
          transport_demand_path: Path to transport demand time series.
          write_transport_storage_units: Write transport_storage_units.csv to disk.
          buses_path: Optional buses.csv path for full bus validation/mapping.
          sector: Energy sector ("electricity", "heating", "cooling") for defaults.
          sets: SystemSets with years, periods, carriers, buses, and storage units.
          buses: Bus model for default bus selection and carrier mapping.
          existing_storage: Existing StorageUnits to merge into (existing wins).

    Returns:
        StorageUnits with per-unit attributes and inflow time series.

    Raises:
        ValueError: If required columns or values are missing or sector is unknown.

    Notes:
        When `sector` is provided, standard data paths are auto-resolved.
    """

    sector_map = {
        "electricity": "electricity",
        "heating": "heating",
        "cooling": "cooling",
    }
    sector_key = sector.strip().lower() if sector else None
    if sector_key and sector_key not in sector_map:
        raise ValueError(f"Unknown sector '{sector}'. Expected one of: {sorted(sector_map)}")
    if sector_key:
        # Auto-resolve standard data paths when a sector is specified.
        base_dir = os.path.join("data", sector_key)
        storage_path = storage_path or os.path.join(base_dir, "storage_units.csv")
        if sector_key == "electricity":
            inflow_path = inflow_path or os.path.join(base_dir, "scaled_inflows.csv")

    default_carrier_value = default_carrier(sets)
    default_bus_value = default_electric_bus(sets, buses)

    hydro_units: List[str] = []
    store = StorageRecordStore(default_carrier=default_carrier_value, default_bus=default_bus_value)

    # ------------------------------------------------------------------
    # Hydro storage (HDAM, HPHS)
    # ------------------------------------------------------------------
    df_pp = read_table(powerplants_path)
    pp_required = {"name", "tech", "p_nom", "capital_cost", "lifetime"}
    require_columns(df_pp, pp_required, powerplants_path)

    hydro_units = load_hydro_storage(
        df_pp, store, default_carrier_value, default_bus_value
    )
    load_chp_tes(
        df_pp, store, default_carrier_value, default_bus_value, sector_key
    )
    del df_pp
    gc.collect()

    # ------------------------------------------------------------------
    # Standard storage units from CSV template (with durations)
    # ------------------------------------------------------------------
    load_template_storage(
        storage_path, sets, store, default_carrier_value, default_bus_value
    )

    # ------------------------------------------------------------------
    # EV storage (capacity and availability from transport inputs)
    # ------------------------------------------------------------------
    ev_availability_by_unit: Dict[Tuple[str, int, int], float] = {}
    # Require all transport inputs if any are provided (no fallbacks).
    if transport_general_params_path is None and any(
        [transport_zones_path, transport_availability_path, transport_demand_path]
    ):
        transport_general_params_path = os.path.join(
            "data", "transport", "transport_general_parameters.xlsx"
        )

    if any([transport_zones_path, transport_availability_path, transport_demand_path]):
        missing = [
            name
            for name, val in [
                ("transport_zones_path", transport_zones_path),
                ("transport_availability_path", transport_availability_path),
                ("transport_demand_path", transport_demand_path),
            ]
            if not val
        ]
        if missing:
            raise ValueError(
                "Missing required transport inputs for EV storage: "
                + ", ".join(missing)
            )

    # Only load EV storage if we have all required transport inputs.
    if transport_general_params_path and transport_zones_path and transport_availability_path and transport_demand_path:
        # Build transport storage template from transport inputs.
        # Use buses.csv (when provided) for validation, because
        # the in-memory Bus object may be sector-filtered.
        known_buses_for_transport = [str(b).strip() for b in getattr(buses, "name", [])]
        if buses_path:
            df_buses_all = read_table(buses_path)
            if "bus" not in df_buses_all.columns:
                raise ValueError(f"{buses_path} missing required column 'bus'.")
            known_buses_for_transport = (
                df_buses_all["bus"].dropna().astype(str).str.strip().tolist()
            )

        output_dir = os.path.dirname(transport_zones_path)
        if write_transport_storage_units:
            transport_storage_path = os.path.join(output_dir, "transport_storage_units.csv")
        else:
            import tempfile
            fd, transport_storage_path = tempfile.mkstemp(
                suffix=".csv", prefix="transport_storage_units_"
            )
            os.close(fd)
        build_transport_storage_units_csv(
            general_params_path=transport_general_params_path,
            zones_params_path=transport_zones_path,
            output_path=transport_storage_path,
            buses=known_buses_for_transport,
        )
        load_template_storage(
            transport_storage_path, sets, store, default_carrier_value, default_bus_value
        )

        params_df, ev_availability, _ev_demand_profile = load_ev_inputs(
            general_params_path=transport_general_params_path,
            zones_params_path=transport_zones_path,
            ev_availability_path=transport_availability_path,
            ev_demand_path=transport_demand_path,
        )

        if not write_transport_storage_units:
            try:
                os.remove(transport_storage_path)
            except OSError:
                pass

        # Keep only storage-capable transport units (exclude hybrids).
        params_df_storage = params_df[
            params_df["tech"].map(_is_electric_transport_tech)
        ].copy()

        # Validate buses for each storage-capable EV unit.
        known = {str(b).strip().lower() for b in known_buses_for_transport}
        if known:
            for _, row in params_df_storage.iterrows():
                if str(row["bus_in"]).strip().lower() not in known:
                    excel_row = int(row.name) + 2 if isinstance(row.name, (int, float)) else None
                    line_txt = f", row {excel_row}" if excel_row is not None else ""
                    raise ValueError(
                        f"{transport_zones_path}{line_txt}: bus_in '{row['bus_in']}' not found in data/buses.csv"
                    )

        # Map EV availability into storage availability by (unit, period, year)
        # and keep only modeled horizon points.
        storage_units = {str(u) for u in params_df_storage["name"].tolist()}
        ev_availability_by_unit = {
            (u, p, y): float(v)
            for (u, p, y), v in ev_availability.items()
            if u in storage_units and int(y) in sets.years and int(p) in sets.periods
        }

    if not store.unit_order:
        return existing_storage or StorageUnits()

    # ------------------------------------------------------------------
    # Hydro inflows (wide -> long: year, period, <hydro_unit...>)
    # ------------------------------------------------------------------
    inflow = load_inflows(inflow_path, hydro_units, sets)

    # ------------------------------------------------------------------
    # Build dicts
    # ------------------------------------------------------------------
    # Convert columns to typed dicts keyed by unit.
    unit = [str(u) for u in store.unit_order]

    # ------------------------------------------------------------------
    # Merge with existing_storage (existing wins)
    # ------------------------------------------------------------------
    ex = existing_storage
    # Union of units ensures incremental calls do not drop prior items or referenced keys.
    ex_units = collect_units_from_storage(ex) if ex else set()
    unit = sorted(set(unit).union(ex_units))

    tech = merge_no_overwrite(ex.tech if ex else {}, store.tech)
    system = merge_no_overwrite(ex.system if ex else {}, store.system)
    region = merge_no_overwrite(ex.region if ex else {}, store.region)
    carrier_in = merge_no_overwrite(ex.carrier_in if ex else {}, store.carrier_in)
    carrier_out = merge_no_overwrite(ex.carrier_out if ex else {}, store.carrier_out)
    bus_in = merge_no_overwrite(ex.bus_in if ex else {}, store.bus_in)
    bus_out = merge_no_overwrite(ex.bus_out if ex else {}, store.bus_out)

    e_nom = merge_no_overwrite(ex.e_nom if ex else {}, store.e_nom)
    e_min = merge_no_overwrite(ex.e_min if ex else {}, store.e_min)
    e_nom_max = merge_no_overwrite(ex.e_nom_max if ex else {}, store.e_nom_max)

    p_charge_nom = merge_no_overwrite(ex.p_charge_nom if ex else {}, store.p_charge_nom)
    p_charge_nom_max = merge_no_overwrite(ex.p_charge_nom_max if ex else {}, store.p_charge_nom_max)
    p_discharge_nom = merge_no_overwrite(ex.p_discharge_nom if ex else {}, store.p_discharge_nom)
    p_discharge_nom_max = merge_no_overwrite(ex.p_discharge_nom_max if ex else {}, store.p_discharge_nom_max)
    duration_charge = merge_no_overwrite(ex.duration_charge if ex else {}, store.duration_charge)
    duration_discharge = merge_no_overwrite(ex.duration_discharge if ex else {}, store.duration_discharge)

    efficiency_charge = merge_no_overwrite(ex.efficiency_charge if ex else {}, store.efficiency_charge)
    efficiency_discharge = merge_no_overwrite(ex.efficiency_discharge if ex else {}, store.efficiency_discharge)
    standby_loss = merge_no_overwrite(ex.standby_loss if ex else {}, store.standby_loss)

    capital_cost_energy = merge_no_overwrite(ex.capital_cost_energy if ex else {}, store.capital_cost_energy)
    capital_cost_power_charge = merge_no_overwrite(ex.capital_cost_power_charge if ex else {}, store.capital_cost_power_charge)
    capital_cost_power_discharge = merge_no_overwrite(ex.capital_cost_power_discharge if ex else {}, store.capital_cost_power_discharge)
    lifetime = merge_no_overwrite(ex.lifetime if ex else {}, store.lifetime)
    spillage_cost = merge_no_overwrite(ex.spillage_cost if ex else {}, store.spillage_cost)
    inflow = merge_no_overwrite(ex.inflow if ex else {}, inflow)
    availability = merge_no_overwrite(ex.availability if ex else {}, ev_availability_by_unit)
    e_nom_ts_local: Dict[Tuple[str, int, int], float] = {}
    if ev_availability_by_unit:
        for (u, p, y), avail in ev_availability_by_unit.items():
            if u in e_nom:
                e_nom_ts_local[(u, p, y)] = float(e_nom[u]) * float(avail)
    e_nom_ts = merge_no_overwrite(getattr(ex, "e_nom_ts", {}) if ex else {}, e_nom_ts_local)
    e_nom_inv_cost = merge_no_overwrite(ex.e_nom_inv_cost if ex else {}, {})

    # ------------------------------------------------------------------
    # EV charge/discharge power derived from fleet size and charger rate.
    # ------------------------------------------------------------------
    if transport_general_params_path and transport_zones_path and transport_availability_path and transport_demand_path:
        for _, row in params_df_storage.iterrows():
            unit_name = str(row["name"])
            fleet_units = float(row.get("fleet_units", 0.0))
            avg_rate = float(row.get("average_ch_rate", 0.0))
            if fleet_units <= 0 or avg_rate <= 0:
                continue
            # Charge/discharge power is based on the EV/FCEV fleet size and charger rate.
            p_nom = (fleet_units * avg_rate / 1000.0)
            if unit_name in p_charge_nom:
                p_charge_nom[unit_name] = p_nom
            if unit_name in p_discharge_nom:
                p_discharge_nom[unit_name] = p_nom

    # Internal consistency checks for Pydantic validators.
    assert_unit_key_subset(
        unit,
        (
            ("tech", tech),
            ("system", system),
            ("region", region),
            ("carrier_in", carrier_in),
            ("carrier_out", carrier_out),
            ("bus_in", bus_in),
            ("bus_out", bus_out),
            ("e_nom", e_nom),
            ("e_min", e_min),
            ("e_nom_max", e_nom_max),
            ("p_charge_nom", p_charge_nom),
            ("p_charge_nom_max", p_charge_nom_max),
            ("p_discharge_nom", p_discharge_nom),
            ("p_discharge_nom_max", p_discharge_nom_max),
            ("duration_charge", duration_charge),
            ("duration_discharge", duration_discharge),
            ("efficiency_charge", efficiency_charge),
            ("efficiency_discharge", efficiency_discharge),
            ("standby_loss", standby_loss),
            ("capital_cost_energy", capital_cost_energy),
            ("capital_cost_power_charge", capital_cost_power_charge),
            ("capital_cost_power_discharge", capital_cost_power_discharge),
            ("lifetime", lifetime),
            ("spillage_cost", spillage_cost),
        ),
        (
            ("inflow", inflow),
            ("e_nom_inv_cost", e_nom_inv_cost),
            ("availability", availability),
            ("e_nom_ts", e_nom_ts),
        ),
    )

    # e_nom_inv_cost left empty/unchanged
    return StorageUnits(
        unit=unit,
        tech=tech,
        system=system,
        region=region,
        carrier_in=carrier_in,
        carrier_out=carrier_out,
        bus_in=bus_in,
        bus_out=bus_out,
        e_nom=e_nom,
        e_min=e_min,
        e_nom_max=e_nom_max,
        p_charge_nom=p_charge_nom,
        p_charge_nom_max=p_charge_nom_max,
        p_discharge_nom=p_discharge_nom,
        p_discharge_nom_max=p_discharge_nom_max,
        duration_charge=duration_charge,
        duration_discharge=duration_discharge,
        efficiency_charge=efficiency_charge,
        efficiency_discharge=efficiency_discharge,
        standby_loss=standby_loss,
        capital_cost_energy=capital_cost_energy,
        capital_cost_power_charge=capital_cost_power_charge,
        capital_cost_power_discharge=capital_cost_power_discharge,
        lifetime=lifetime,
        inflow=inflow,
        availability=availability,
        e_nom_ts=e_nom_ts,
        spillage_cost=spillage_cost,
        e_nom_inv_cost=e_nom_inv_cost,
    )
