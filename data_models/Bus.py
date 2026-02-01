from __future__ import annotations

from typing import Dict, List, Optional
import pandas as pd
from pydantic import BaseModel, Field, field_validator, ConfigDict

BusId = str
SystemId = str
RegionId = str
Carrier = str
Unit = str


class Bus(BaseModel):
    """
    Network buses with connected units and their carrier.
    """

    name: List[BusId] = Field(
        default_factory=list,
        description="All buses in the system.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    system: Dict[BusId, SystemId] = Field(
        default_factory=dict,
        description="System/country tag for each bus.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    region: Dict[BusId, RegionId] = Field(
        default_factory=dict,
        description="Region/zone for each bus.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    carrier: Dict[BusId, Carrier] = Field(
        default_factory=dict,
        description="Carrier assigned to each bus.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    generators_at_bus: Dict[BusId, List[Unit]] = Field(
        default_factory=dict,
        description="Generator/converter units on each bus.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    storage_at_bus: Dict[BusId, List[Unit]] = Field(
        default_factory=dict,
        description="Storage units on each bus.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("system", "region", "carrier", "generators_at_bus", "storage_at_bus", mode="after")
    def _keys_subset_of_buses(cls, v, info):
        buses_set = set(info.data.get("name", []))
        extra = set(v) - buses_set
        if extra:
            raise ValueError(f"{info.field_name} contains unknown buses: {sorted(extra)}")
        return v

    @field_validator("generators_at_bus", "storage_at_bus", mode="after")
    def _list_values(cls, v):
        for bus_id, items in v.items():
            if not isinstance(items, list):
                raise TypeError(f"{bus_id} values must be lists.")
        return v

    # ------------------------------------------------------------------
    # Builders
    # ------------------------------------------------------------------
    @classmethod
    def from_csv(
        cls,
        buses_csv_path: str,
        *,
        generators_at_bus: Optional[Dict[BusId, List[Unit]]] = None,
        storage_at_bus: Optional[Dict[BusId, List[Unit]]] = None,
        default_carrier: str = "electricity",
    ) -> "Bus":
        """
        Build a `Bus` object from a buses.csv template with columns:
          - system, region, bus, carrier

        Optional `generators_at_bus` and `storage_at_bus` mappings can be passed
        to keep visibility of units per bus.
        """
        df = pd.read_csv(buses_csv_path)
        required = {"bus", "carrier"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"{buses_csv_path} is missing required columns: {sorted(missing)}")

        carrier_map: Dict[BusId, Carrier] = {}
        system_map: Dict[BusId, str] = {}
        region_map: Dict[BusId, str] = {}

        for _, row in df.iterrows():
            bus = str(row["bus"])
            if not bus:
                continue
            carrier_val = str(row.get("carrier", default_carrier) or default_carrier)
            system_val = row.get("system", None)
            region_val = row.get("region", None)

            # Keep first occurrence, but validate consistency if duplicates appear
            if bus in carrier_map and carrier_map[bus] != carrier_val:
                raise ValueError(f"Inconsistent carrier for bus '{bus}' in {buses_csv_path}")
            carrier_map.setdefault(bus, carrier_val)

            if system_val is not None:
                system_val = str(system_val)
                if bus in system_map and system_map[bus] != system_val:
                    raise ValueError(f"Inconsistent system for bus '{bus}' in {buses_csv_path}")
                system_map.setdefault(bus, system_val)

            if region_val is not None:
                region_val = str(region_val)
                if bus in region_map and region_map[bus] != region_val:
                    raise ValueError(f"Inconsistent region for bus '{bus}' in {buses_csv_path}")
                region_map.setdefault(bus, region_val)

        bus_names = sorted(carrier_map)

        gen_map = generators_at_bus or {}
        sto_map = storage_at_bus or {}

        return cls(
            name=bus_names,
            system=system_map,
            region=region_map,
            carrier=carrier_map,
            generators_at_bus=gen_map,
            storage_at_bus=sto_map,
        )
