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

UPY = Tuple[str, int, int]


def load_storage(
    powerplants_path: str,
    storage_path: Optional[str] = None,
    inflow_path: Optional[str] = None,
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
    e_nom_inv_cost = merge_no_overwrite(ex.e_nom_inv_cost if ex else {}, {})

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
        spillage_cost=spillage_cost,
        e_nom_inv_cost=e_nom_inv_cost,
    )
