"""Bus loader that derives buses from templates and demand headers."""

from __future__ import annotations

from typing import Dict, List, Optional
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Bus import Bus
from data_loaders.helpers.defaults import default_carrier, default_electric_bus
from data_loaders.helpers.io import read_columns, read_table
from data_loaders.helpers.transport_utils import _is_electric_transport_tech

def load_bus(
    powerplants_path: str,
    storage_path: str,
    buses_path: Optional[str] = None,
    electricity_demand_path: Optional[str] = None,
    heating_demand_path: Optional[str] = None,
    cooling_demand_path: Optional[str] = None,
    transport_zones_path: Optional[str] = None,
    *,
    sector: Optional[str] = None,
    sets: SystemSets,
    existing_buses: Optional[Bus] = None,
) -> Bus:
    """Build a Bus model from templates, powerplants, and demand headers.

    When used: called by `load_sector` to populate the bus topology for a sector.

    Args:
        powerplants_path: Path to powerplants (or converters) input file.
        storage_path: Path to storage template file.
        buses_path: Optional buses metadata file.
        electricity_demand_path: Optional electricity demand file (for bus discovery).
        heating_demand_path: Optional heating demand file (for bus discovery).
        cooling_demand_path: Optional cooling demand file (for bus discovery).
        transport_zones_path: Optional transport zones input for bus discovery.
        sector: Sector name for filtering carriers.
        sets: SystemSets containing known units and carriers.
        existing_buses: Existing Bus model to merge into (existing wins).

    Returns:
        Bus model with carrier, system, region, and unit mappings per bus.

    Raises:
        ValueError: If required columns are missing or carriers conflict for a bus.

    Notes:
        Demand files are used only to discover bus names when `buses_path` does not
        enumerate all buses explicitly.
    """

    sector_carrier_map = {
        "electricity": "electricity",
        "heating": "heat",
        "cooling": "cooling",
    }
    sector_key = sector.strip().lower() if sector else None
    if sector_key and sector_key not in sector_carrier_map:
        raise ValueError(f"Unknown sector '{sector}'. Expected one of: {sorted(sector_carrier_map)}")
    sector_carrier = sector_carrier_map.get(sector_key) if sector_key else None

    df_pp = read_table(powerplants_path)
    df_storage = read_table(storage_path)

    # Filter to known units
    units_set = set(sets.units)
    storage_units_set = set(getattr(sets, "storage_units", []))

    df_pp = df_pp[df_pp["name"].isin(units_set)].copy()
    df_storage = df_storage[df_storage["name"].isin(storage_units_set)].copy() if "name" in df_storage.columns else df_storage

    # Start from existing bus object (if any) so repeated calls only append.
    bus_names = set(existing_buses.name) if existing_buses else set()
    carrier_map: Dict[str, str] = dict(existing_buses.carrier) if existing_buses else {}
    system_map: Dict[str, str] = dict(getattr(existing_buses, "system", {})) if existing_buses else {}
    region_map: Dict[str, str] = dict(getattr(existing_buses, "region", {})) if existing_buses else {}
    bus_units: Dict[str, List[str]] = {k: list(v) for k, v in (existing_buses.generators_at_bus if existing_buses else {}).items()}
    bus_storage: Dict[str, List[str]] = {k: list(v) for k, v in (existing_buses.storage_at_bus if existing_buses else {}).items()}

    default_carrier_value = default_carrier(sets)
    default_bus_value = default_electric_bus(sets, existing_buses)

    def _carrier_allowed(carrier: Optional[str]) -> bool:
        """Determine whether a carrier is allowed for the current sector.

        - When no sector filter is set (`sector_carrier` is None), all carriers are allowed.
        - If a sector filter is active, return True only when `carrier` (after
          normalizing case/whitespace) matches the expected sector carrier.

        Args:
            carrier: Carrier string to check (may be None or empty).

        Returns:
            True if the carrier is permitted for the current sector, False otherwise.
        """
        if not sector_carrier:
            return True
        if not carrier:
            return False
        return str(carrier).strip().lower() == sector_carrier

    def _add_bus(bus: str, carrier: Optional[str] = None, system: Optional[str] = None, region: Optional[str] = None):
        """Register a bus and validate its metadata.

        Ensures the bus name is recorded, applies a default carrier when one is
        not provided, and verifies that any previously recorded carrier/system/region
        for the bus does not conflict with newly provided values.

        Args:
            bus: Bus name to register.
            carrier: Optional carrier for the bus; defaults to `default_carrier_value` if omitted.
            system: Optional system identifier to associate with the bus.
            region: Optional region identifier to associate with the bus.

        Raises:
            ValueError: If the provided carrier/system/region conflicts with existing mappings.
        """
        if carrier is None or carrier == "":
            carrier = default_carrier_value
        if not _carrier_allowed(carrier):
            return
        bus_names.add(bus)
        if bus in carrier_map and carrier_map[bus] != carrier:
            raise ValueError(f"Inconsistent carrier for bus '{bus}'")
        carrier_map.setdefault(bus, carrier)
        if system is not None:
            if bus in system_map and system_map[bus] != system:
                raise ValueError(f"Inconsistent system for bus '{bus}'")
            system_map.setdefault(bus, system)
        if region is not None:
            if bus in region_map and region_map[bus] != region:
                raise ValueError(f"Inconsistent region for bus '{bus}'")
            region_map.setdefault(bus, region)

    def _assign_unit(bus: str, carrier: str, unit: str, target: Dict[str, List[str]]):
        """Ensure a bus exists and assign a unit to a mapping.

        This helper ensures the given `bus` is present (by calling `_add_bus`),
        checks whether the carrier is allowed for the current sector, and if so,
        appends `unit` to `target[bus]` when it is not already present.
        """
        _add_bus(bus, carrier)
        if not _carrier_allowed(carrier):
            return
        items = target.setdefault(bus, [])
        if unit not in items:
            items.append(unit)

    known_buses_file: Optional[set[str]] = None

    # Buses explicitly listed in buses.csv (if provided)
    if buses_path:
        df_buses = read_table(buses_path)
        required = {"bus", "carrier"}
        missing = required - set(df_buses.columns)
        if missing:
            raise ValueError(f"{buses_path} is missing required columns: {sorted(missing)}")
        known_buses_file = set(df_buses["bus"].astype(str).str.strip().str.lower())
        for _, row in df_buses.iterrows():
            bus = str(row["bus"])
            if not bus:
                continue
            _add_bus(
                bus=bus,
                carrier=str(row.get("carrier", default_carrier_value) or default_carrier_value),
                system=str(row["system"]) if "system" in row and pd.notna(row["system"]) else None,
                region=str(row["region"]) if "region" in row and pd.notna(row["region"]) else None,
            )

    # Generators/converters
    for _, row in df_pp.iterrows():
        name = str(row["name"])
        tech = str(row.get("tech", "")).upper()

        bus_out = str(row.get("bus_out", default_bus_value))
        carrier_out = str(row.get("carrier_out", default_carrier_value))
        _assign_unit(bus_out, carrier_out, name, bus_units)

        # Secondary output bus/carrier (e.g., CHP heat)
        if sector_carrier and carrier_out.strip().lower() != sector_carrier:
            continue
        bus_out_2_val = row.get("bus_out_2", None)
        if pd.notna(bus_out_2_val) and str(bus_out_2_val).strip() != "":
            bus2 = str(bus_out_2_val)
            carrier2 = str(row.get("carrier_out_2", carrier_out))
            _assign_unit(bus2, carrier2, name, bus_units)

        # Hydro storage sits on the generator's bus_out
        if tech in {"HDAM", "HPHS", "HPSP"}:
            _assign_unit(bus_out, carrier_out, name, bus_storage)

        # CHP TES located at bus_out_2 if present
        chp_type = str(row.get("chp_type", "")).upper()
        if chp_type and chp_type != "N" and pd.notna(bus_out_2_val) and str(bus_out_2_val).strip() != "":
            tes_bus = str(bus_out_2_val)
            tes_carrier = str(row.get("carrier_out_2", carrier_out))
            _assign_unit(tes_bus, tes_carrier, name, bus_storage)

    # Storage
    for _, row in df_storage.iterrows():
        bus_out = str(row.get("bus_out", row.get("bus", default_bus_value)))
        carrier = str(row.get("carrier_out", row.get("carrier", default_carrier_value)))
        name = str(row.get("name", row.get("unit_name", "")))
        if not name:
            continue
        _assign_unit(bus_out, carrier, name, bus_storage)

    # Demand buses (electricity / heat / cooling) derived from demand files
    def _add_demand_buses(csv_path: Optional[str], carrier: str):
        if not csv_path:
            return
        columns = read_columns(csv_path)
        if not {"year", "period"}.issubset(columns):
            raise ValueError(f"{csv_path} missing 'year'/'period' columns for demand bus discovery.")
        demand_cols = [c for c in columns if c not in {"year", "period"}]
        for col in demand_cols:
            bus = str(col)
            _add_bus(bus, carrier)

    _add_demand_buses(electricity_demand_path, "electricity")
    _add_demand_buses(heating_demand_path, "heat")
    _add_demand_buses(cooling_demand_path, "cooling")

    # Transport buses (from zones input)
    if transport_zones_path:
        df_tp = read_table(transport_zones_path)
        if not df_tp.empty:
            required_tp = {"transport_sector_bus", "tech", "fuel_type"}
            missing_tp = required_tp - set(df_tp.columns)
            if missing_tp:
                raise ValueError(
                    f"Missing required columns in transport zones file ({transport_zones_path}): "
                    f"{sorted(missing_tp)}"
                )
            if "bus_in" not in df_tp.columns:
                raise ValueError(
                    f"Missing required column 'bus_in' in transport zones file ({transport_zones_path})."
                )
            if "tech" in df_tp.columns:
                df_tp = df_tp[df_tp["tech"].map(_is_electric_transport_tech)].copy()
            for _, row in df_tp.iterrows():
                name = (
                    str(row.get("name", "")).strip()
                    or f"{row.get('transport_sector_bus')}_{row.get('tech')}_{row.get('fuel_type')}"
                )
                bus_in = str(row.get("bus_in", "")).strip()
                if not bus_in:
                    raise ValueError(
                        f"{transport_zones_path} missing bus_in for transport unit '{name}'."
                    )
                if known_buses_file is not None and bus_in.strip().lower() not in known_buses_file:
                    excel_row = int(row.name) + 2 if isinstance(row.name, (int, float)) else None
                    line_txt = f", row {excel_row}" if excel_row is not None else ""
                    raise ValueError(
                        f"{transport_zones_path}{line_txt}: bus_in '{bus_in}' not found in data/buses.csv"
                    )
                carrier_val = carrier_map.get(bus_in, default_carrier_value)
                if name and bus_in:
                    _assign_unit(bus_in, carrier_val, name, bus_storage)

    # Finalize
    bus_names_sorted = sorted(bus_names)
    for b in bus_names_sorted:
        carrier_map.setdefault(b, default_carrier_value)

    return Bus(
        name=bus_names_sorted,
        system=system_map,
        region=region_map,
        carrier=carrier_map,
        generators_at_bus=bus_units,
        storage_at_bus=bus_storage,
    )
