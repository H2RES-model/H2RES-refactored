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
    years: List[Y]
    periods: List[P]

    # --------------------------
    # Carriers and buses
    # --------------------------
    carriers: List[Carrier] = Field(
        default_factory=lambda: ["Electricity"],
        description="Energy carriers (e.g. Electricity, Heat, H2).",
    )
    buses: List[BusId] = Field(
        default_factory=lambda: ["SystemBus"],
        description="Network buses; single-node version uses one bus.",
    )

    # --------------------------
    # Master index sets
    # --------------------------
    units: List[U] = Field(
        default_factory=list,
        description="All generator / converter units (CCGT, WindPP, PV, CHP, HP, etc.).",
    )
    storage_units: List[U] = Field(
        default_factory=list,
        description="All storage units (battery, TES, H2, HDAM, HPHS, ...).",
    )

    # --------------------------
    # Generator subsets (⊆ units)
    # --------------------------
    fossil_units: List[U] = Field(default_factory=list)
    biomass_units: List[U] = Field(default_factory=list)
    hror_units: List[U] = Field(default_factory=list)
    wind_units: List[U] = Field(default_factory=list)
    solar_units: List[U] = Field(default_factory=list)
    chp_units: List[U] = Field(default_factory=list)

    ncre_units: List[U] = Field(
        default_factory=list,
        description="Optional group for non-conventional renewables (e.g. wind, solar, biomass, RoR hydro).",
    )

    disp_units: List[U] = Field(default_factory=list)
    nondisp_units: List[U] = Field(default_factory=list)

    # --------------------------
    # Storage subsets (⊆ storage_units)
    # --------------------------
    hydro_storage_units: List[U] = Field(
        default_factory=list,
        description="Hydro storage assets (HDAM, HPHS, etc.).",
    )
    hdam_units: List[U] = Field(
        default_factory=list,
        description="Hydro dam storage units (tech == 'HDAM').",
    )
    hphs_units: List[U] = Field(
        default_factory=list,
        description="Pumped hydro storage units (tech == 'HPHS').",
    )

    battery_units: List[U] = Field(default_factory=list)
    tes_units: List[U] = Field(default_factory=list)
    hydrogen_storage_units: List[U] = Field(default_factory=list)

    # --------------------------
    # Demand sectors
    # --------------------------
    demand_sectors: List[Sector] = Field(
        default_factory=lambda: ["Electricity"],
        description="Logical demand sectors (e.g. Electricity, Industry, Transport).",
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

