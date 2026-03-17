from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Literal, Mapping
from pydantic import BaseModel, Field, field_validator, ConfigDict

# Index aliases
U = str
P = int
Y = int
UPY = Tuple[U, P, Y]

BusId = str
Carrier = str
Fuel = str
Sector = str

class SystemSets(BaseModel):
    """
    Core index sets for the H2RES system.

    - Single-node for now, but multi-bus/multi-carrier ready.
    - Distinguishes between:
        * units:          generator / converter units (CCGT, WindPP, PV, CHP, HP, etc.)
        * storage_units:  storage assets (batteries, TES, H2 tanks, HDAM, HPHS, ...)

    - Generator subsets (must be subsets of `units`):
        fossil_units, biomass_units, hror_units, wind_units, solar_units,
        chp_units, ncre_units, disp_units, nondisp_units

    - Storage subsets (must be subsets of `storage_units`):
        hydro_storage_units (all hydro storages),
        hdam_units, hphs_units,
        battery_units, tes_units, hydrogen_storage_units
    """

    # --------------------------
    # Time sets
    # --------------------------
    years: List[Y] = Field(
        description="Model years in the planning horizon.",
        json_schema_extra={"unit": "year", "status": "mandatory"},
    )
    periods: List[P] = Field(
        description="Time periods within a year (e.g., hours).",
        json_schema_extra={"unit": "index", "status": "mandatory"},
    )

    # --------------------------
    # Carriers and buses
    # --------------------------
    carriers: List[Carrier] = Field(
        default_factory=list,
        description="Energy carriers (e.g. Electricity, Heat, H2).",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    buses: List[BusId] = Field(
        default_factory=list,
        description="Network buses; single-node version uses one bus.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    # --------------------------
    # Master index sets
    # --------------------------
    units: List[U] = Field(
        default_factory=list,
        description="All generator / converter units (CCGT, WindPP, PV, CHP, HP, etc.).",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    storage_units: List[U] = Field(
        default_factory=list,
        description="All storage units (battery, TES, H2, HDAM, HPHS, ...).",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    # --------------------------
    # Generator subsets (⊆ units)
    # --------------------------
    fossil_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    biomass_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    hror_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    wind_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    solar_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    chp_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})

    ncre_units: List[U] = Field(
        default_factory=list,
        description="Optional group for non-conventional renewables (e.g. wind, solar, biomass, RoR hydro).",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    disp_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    nondisp_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})

    # --------------------------
    # Storage subsets (⊆ storage_units)
    # --------------------------
    hydro_storage_units: List[U] = Field(
        default_factory=list,
        description="Hydro storage assets (HDAM, HPHS, etc.).",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    hdam_units: List[U] = Field(
        default_factory=list,
        description="Hydro dam storage units (tech == 'HDAM').",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    hphs_units: List[U] = Field(
        default_factory=list,
        description="Pumped hydro storage units (tech == 'HPHS').",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    battery_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    tes_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})
    hydrogen_storage_units: List[U] = Field(default_factory=list, json_schema_extra={"unit": "n.a.", "status": "optional"})

    # --------------------------
    # Demand sectors
    # --------------------------
    demand_sectors: List[Sector] = Field(
        default_factory=lambda: ["Electricity"],
        description="Logical demand sectors (e.g. Electricity, Industry, Transport).",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    model_config = ConfigDict(frozen=True, extra="forbid")

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @field_validator(
        "fossil_units",
        "biomass_units",
        "hror_units",
        "wind_units",
        "solar_units",
        "chp_units",
        "ncre_units",
        "disp_units",
        "nondisp_units",
        mode="after",
    )
    def _gen_subsets_in_units(cls, v, info):
        """Ensure all generator subsets only contain units that exist in `units`."""
        all_units = set(info.data.get("units", []))
        missing = [u for u in v if u not in all_units]
        if missing:
            raise ValueError(
                f"Unknown units in generator subset {info.field_name}: {missing}"
            )
        return v

    @field_validator(
        "hydro_storage_units",
        "hdam_units",
        "hphs_units",
        "battery_units",
        "tes_units",
        "hydrogen_storage_units",
        mode="after",
    )
    def _storage_subsets_in_storage_units(cls, v, info):
        """Ensure all storage subsets only contain units that exist in `storage_units`."""
        all_sto = set(info.data.get("storage_units", []))
        missing = [u for u in v if u not in all_sto]
        if missing:
            raise ValueError(
                f"Unknown units in storage subset {info.field_name}: {missing}"
            )
        return v

