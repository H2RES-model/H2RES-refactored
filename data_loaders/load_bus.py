"""Bus loader that derives buses from templates and demand headers."""

from __future__ import annotations

from typing import Dict, List, Optional, Union
import pandas as pd

from data_models.SystemSets import SystemSets
from data_models.Bus import Bus
from data_loaders.helpers.io import TableCache, inspect_table, read_table
from data_loaders.helpers.model_factory import build_model
from data_loaders.helpers.validation_utils import require_columns, require_values

def load_bus(
    powerplants_path: Union[str, pd.DataFrame],
    storage_path: Union[str, pd.DataFrame],
    buses_path: Optional[str] = None,
    demand_paths: Optional[Dict[str, str]] = None,
    extra_demand_paths: Optional[Dict[str, str]] = None,
    transport_static: Optional[pd.DataFrame] = None,
    extra_powerplants_path: Optional[Union[str, pd.DataFrame]] = None,
    extra_storage_path: Optional[Union[str, pd.DataFrame]] = None,
    *,
    allowed_carriers: Optional[List[str]] = None,
    sets: SystemSets,
    existing_buses: Optional[Bus] = None,
    table_cache: Optional[TableCache] = None,
) -> Bus:
    """Build a Bus model from templates, powerplants, and demand headers.

    When used: called by `load_sector` to populate the bus topology for a sector.

    Args:
        powerplants_path: Path to powerplants (or converters) input file.
        storage_path: Path to storage template file.
        buses_path: Optional buses metadata file.
        demand_paths: Optional mapping of carrier -> demand file for bus discovery.
        transport_static: Optional normalized transport static table.
        allowed_carriers: Optional allowed carriers for this load call.
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

    normalized_allowed = {str(c).strip().lower() for c in (allowed_carriers or []) if str(c).strip()}
    demand_paths = dict(demand_paths or {})
    extra_demand_paths = dict(extra_demand_paths or {})

    df_pp = powerplants_path.copy() if isinstance(powerplants_path, pd.DataFrame) else read_table(powerplants_path, cache=table_cache)
    df_storage = storage_path.copy() if isinstance(storage_path, pd.DataFrame) else read_table(storage_path, cache=table_cache)
    df_pp_extra = (
        extra_powerplants_path.copy()
        if isinstance(extra_powerplants_path, pd.DataFrame)
        else read_table(extra_powerplants_path, cache=table_cache)
        if extra_powerplants_path
        else pd.DataFrame()
    )
    df_storage_extra = (
        extra_storage_path.copy()
        if isinstance(extra_storage_path, pd.DataFrame)
        else read_table(extra_storage_path, cache=table_cache)
        if extra_storage_path
        else pd.DataFrame()
    )

    # Filter to known units
    units_set = set(sets.units)
    storage_units_set = set(getattr(sets, "storage_units", []))

    df_pp = df_pp[df_pp["name"].isin(units_set)].copy()
    df_storage = df_storage[df_storage["name"].isin(storage_units_set)].copy() if "name" in df_storage.columns else df_storage
    if not df_pp_extra.empty:
        df_pp_extra = df_pp_extra[df_pp_extra["name"].isin(units_set)].copy()
    if not df_storage_extra.empty and "name" in df_storage_extra.columns:
        df_storage_extra = df_storage_extra[df_storage_extra["name"].isin(storage_units_set)].copy()

    # Start from existing bus object (if any) so repeated calls only append.
    bus_names = set(existing_buses.name) if existing_buses else set()
    carrier_map: Dict[str, str] = dict(existing_buses.carrier) if existing_buses else {}
    system_map: Dict[str, str] = dict(existing_buses.system) if existing_buses else {}
    region_map: Dict[str, str] = dict(existing_buses.region) if existing_buses else {}
    bus_units: Dict[str, List[str]] = {}
    bus_storage: Dict[str, List[str]] = {}
    if existing_buses is not None and not existing_buses.attachments.empty:
        generator_rows = existing_buses.attachments[existing_buses.attachments["component"] == "generator"]
        storage_rows = existing_buses.attachments[existing_buses.attachments["component"] == "storage"]
        bus_units = {
            str(bus): [str(unit) for unit in units]
            for bus, units in generator_rows.groupby("bus")["unit"].agg(list).items()
        }
        bus_storage = {
            str(bus): [str(unit) for unit in units]
            for bus, units in storage_rows.groupby("bus")["unit"].agg(list).items()
        }

    def _carrier_allowed(carrier: Optional[str]) -> bool:
        """Determine whether a carrier is allowed for the current sector.

        - When no carrier filter is set, all carriers are allowed.
        - If a filter is active, return True only when `carrier` matches it.

        Args:
            carrier: Carrier string to check (may be None or empty).

        Returns:
            True if the carrier is permitted for the current sector, False otherwise.
        """
        if not normalized_allowed:
            return True
        if not carrier:
            return False
        return str(carrier).strip().lower() in normalized_allowed

    def _add_bus(
        bus: str,
        carrier: Optional[str] = None,
        system: Optional[str] = None,
        region: Optional[str] = None,
        *,
        bypass_filter: bool = False,
    ):
        """Register a bus and validate its metadata.

        Ensures the bus name is recorded and verifies that any previously
        recorded carrier/system/region for the bus does not conflict with newly
        provided values.

        Args:
            bus: Bus name to register.
            carrier: Carrier for the bus.
            system: Optional system identifier to associate with the bus.
            region: Optional region identifier to associate with the bus.

        Raises:
            ValueError: If the bus metadata is missing or conflicts with existing mappings.
        """
        bus = str(bus).strip()
        carrier = "" if carrier is None else str(carrier).strip()
        if not bus:
            raise ValueError("Encountered empty bus identifier while building buses.")
        if not carrier:
            raise ValueError(f"Missing required carrier for bus '{bus}'")
        if not bypass_filter and not _carrier_allowed(carrier):
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

    def _process_powerplants(table: pd.DataFrame, *, apply_filter: bool):
        for row in table.itertuples(index=False):
            name = str(getattr(row, "name", ""))
            tech = str(getattr(row, "tech", "")).upper()

            bus_out = str(getattr(row, "bus_out", "")).strip()
            carrier_out = str(getattr(row, "carrier_out", "")).strip()
            if not bus_out:
                raise ValueError(f"Powerplant '{name}' is missing required 'bus_out'")
            if not carrier_out:
                raise ValueError(f"Powerplant '{name}' is missing required 'carrier_out'")
            if apply_filter:
                _assign_unit(bus_out, carrier_out, name, bus_units)
            else:
                _add_bus(bus_out, carrier_out, bypass_filter=True)
                items = bus_units.setdefault(bus_out, [])
                if name not in items:
                    items.append(name)

            bus_out_2_val = getattr(row, "bus_out_2", None)
            if pd.notna(bus_out_2_val) and str(bus_out_2_val).strip() != "":
                bus2 = str(bus_out_2_val).strip()
                carrier2 = str(getattr(row, "carrier_out_2", "")).strip()
                if not carrier2:
                    raise ValueError(f"Powerplant '{name}' has 'bus_out_2' but is missing required 'carrier_out_2'")
                if apply_filter:
                    _assign_unit(bus2, carrier2, name, bus_units)
                else:
                    _add_bus(bus2, carrier2, bypass_filter=True)
                    items = bus_units.setdefault(bus2, [])
                    if name not in items:
                        items.append(name)

            if tech in {"HDAM", "HPHS", "HPSP"}:
                if apply_filter:
                    _assign_unit(bus_out, carrier_out, name, bus_storage)
                else:
                    _add_bus(bus_out, carrier_out, bypass_filter=True)
                    items = bus_storage.setdefault(bus_out, [])
                    if name not in items:
                        items.append(name)

            chp_type = str(getattr(row, "chp_type", "")).upper()
            if chp_type and chp_type != "N" and pd.notna(bus_out_2_val) and str(bus_out_2_val).strip() != "":
                tes_bus = str(bus_out_2_val)
                tes_carrier = str(getattr(row, "carrier_out_2", "")).strip()
                if not tes_carrier:
                    raise ValueError(f"Powerplant '{name}' has CHP TES output bus '{tes_bus}' but no 'carrier_out_2'")
                if apply_filter:
                    _assign_unit(tes_bus, tes_carrier, name, bus_storage)
                else:
                    _add_bus(tes_bus, tes_carrier, bypass_filter=True)
                    items = bus_storage.setdefault(tes_bus, [])
                    if name not in items:
                        items.append(name)

    def _process_storage(table: pd.DataFrame, *, apply_filter: bool):
        for row in table.itertuples(index=False):
            bus_out = str(getattr(row, "bus_out", getattr(row, "bus", ""))).strip()
            carrier = str(getattr(row, "carrier_out", getattr(row, "carrier", ""))).strip()
            name = str(getattr(row, "name", getattr(row, "unit_name", ""))).strip()
            if not name:
                continue
            if not bus_out:
                raise ValueError(f"Storage unit '{name}' is missing required bus output metadata")
            if not carrier:
                raise ValueError(f"Storage unit '{name}' is missing required carrier metadata")
            if apply_filter:
                _assign_unit(bus_out, carrier, name, bus_storage)
            else:
                _add_bus(bus_out, carrier, bypass_filter=True)
                items = bus_storage.setdefault(bus_out, [])
                if name not in items:
                    items.append(name)

    known_buses_file: Optional[set[str]] = None

    # Buses explicitly listed in buses.csv (if provided)
    if buses_path:
        df_buses = read_table(buses_path, cache=table_cache)
        require_columns(df_buses, {"bus", "carrier"}, buses_path)
        require_values(df_buses, required_str=["bus", "carrier"], required_num=[], path_label=buses_path, name_col="bus")
        known_buses_file = set(df_buses["bus"].astype(str).str.strip().str.lower())
        for row in df_buses.itertuples(index=False):
            bus = str(getattr(row, "bus", "")).strip()
            if not bus:
                continue
            _add_bus(
                bus=bus,
                carrier=str(getattr(row, "carrier", "")).strip(),
                system=str(getattr(row, "system")) if hasattr(row, "system") and pd.notna(getattr(row, "system")) else None,
                region=str(getattr(row, "region")) if hasattr(row, "region") and pd.notna(getattr(row, "region")) else None,
            )

    # Generators/converters
    _process_powerplants(df_pp, apply_filter=True)
    if not df_pp_extra.empty:
        _process_powerplants(df_pp_extra, apply_filter=False)

    # Storage
    _process_storage(df_storage, apply_filter=True)
    if not df_storage_extra.empty:
        _process_storage(df_storage_extra, apply_filter=False)

    # Demand buses (electricity / heat / cooling) derived from demand files
    def _add_demand_buses(csv_path: Optional[str], carrier: str, *, apply_filter: bool):
        if not csv_path:
            return
        columns = list(inspect_table(csv_path, cache=table_cache).columns)
        if not {"year", "period"}.issubset(columns):
            raise ValueError(f"{csv_path} missing 'year'/'period' columns for demand bus discovery.")
        demand_cols = [c for c in columns if c not in {"year", "period"}]
        for col in demand_cols:
            bus = str(col)
            if apply_filter:
                _add_bus(bus, carrier)
            else:
                _add_bus(bus, carrier, bypass_filter=True)

    for carrier, path in demand_paths.items():
        _add_demand_buses(path, carrier, apply_filter=True)
    for carrier, path in extra_demand_paths.items():
        _add_demand_buses(path, carrier, apply_filter=False)

    # Transport buses (from zones input)
    if transport_static is not None and not transport_static.empty:
        df_tp = transport_static.reset_index() if transport_static.index.name == "unit" else transport_static.copy()
        df_tp = df_tp[df_tp["supports_grid_connection"].fillna(False).astype(bool)].copy()
        for row in df_tp.itertuples(index=False):
            name = str(getattr(row, "unit", "")).strip()
            bus_in = str(getattr(row, "bus_in", "")).strip()
            if not bus_in:
                continue
            if known_buses_file is not None and bus_in.strip().lower() not in known_buses_file:
                raise ValueError(f"Transport unit '{name}' references unknown bus '{bus_in}' in buses.csv")
            carrier_val = str(getattr(row, "carrier_in", "")).strip()
            if not carrier_val:
                raise ValueError(f"Transport unit '{name}' is missing required 'carrier_in' for bus '{bus_in}'")
            _assign_unit(bus_in, carrier_val, name, bus_storage)
    # Finalize
    bus_names_sorted = sorted(bus_names)
    for b in bus_names_sorted:
        if b not in carrier_map or not str(carrier_map[b]).strip():
            raise ValueError(f"Bus '{b}' is missing required carrier metadata")

    static = pd.DataFrame({"bus": bus_names_sorted})
    static["system"] = static["bus"].map(system_map).fillna("")
    static["region"] = static["bus"].map(region_map).fillna("")
    static["carrier"] = static["bus"].map(carrier_map)
    static = static.set_index("bus", drop=True)

    attachment_rows: list[dict[str, str]] = []
    for bus, units in bus_units.items():
        attachment_rows.extend(
            {"bus": str(bus), "component": "generator", "unit": str(unit), "role": "output"}
            for unit in units
        )
    for bus, units in bus_storage.items():
        attachment_rows.extend(
            {"bus": str(bus), "component": "storage", "unit": str(unit), "role": "output"}
            for unit in units
        )
    attachments = pd.DataFrame(attachment_rows, columns=["bus", "component", "unit", "role"])

    return build_model(Bus, static=static, attachments=attachments.reset_index(drop=True))
