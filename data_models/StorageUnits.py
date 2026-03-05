from __future__ import annotations
from typing import Dict, List, Tuple
from pydantic import BaseModel, Field, field_validator, ConfigDict

U = str
P = int
Y = int
UPY = Tuple[U, P, Y]

BusId = str
Carrier = str

# ===============================================================
# 2. StorageUnits  (energy buffers only, no direct dispatch)
# ===============================================================

class StorageUnits(BaseModel):
    """
    Energy storage assets only (reservoirs, batteries, TES, H2 tanks).

    Examples:
      - Hydro reservoirs (HDAM, PHS)
      - Battery storage systems
      - Thermal energy storage tanks
      - Hydrogen tanks

    Design:
      - Holds energy capacity, SOC, inflows, and energy-side losses.
      - No direct generator dispatch, ramping, or fuel costs here.
      - Power-side economics typically live in Generators (turbines, converters).
    """

    # -----------------------------------------------------------
    # Index & classification
    # -----------------------------------------------------------
    unit: List[U] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=list,
        description="Name of the storage assets (hydro reservoirs, PHS, batteries, TES, H2 tanks, ...).",
    )

    system: Dict[U, str] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=dict,
        description="System/scenario tag (column 'system' in storage_units.csv), e.g. country code.",
    )

    region: Dict[U, str] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=dict,
        description="Region/zone for the storage asset (column 'region' in storage_units.csv).",
    )

    tech: Dict[U, str] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=dict,
        description="Storage technology label (e.g. 'HDAM', 'HPHS', 'BESS', 'TES', 'H2_tank').",
    )

    carrier_in: Dict[U, Carrier] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=dict,
        description="Input energy carrier (e.g. 'Electricity', 'Heat', 'H2').",
    )

    carrier_out: Dict[U, Carrier] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=dict,
        description="Output energy carrier (e.g. 'Electricity', 'Heat', 'H2').",
    )

    bus_in: Dict[U, BusId] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=dict,
        description="Bus where storage charge is connected (e.g. 'SystemBus').",
    )
    
    bus_out: Dict[U, BusId] = Field(
        json_schema_extra={"unit": "n.a.", "status": "optional"},
        default_factory=dict,
        description="Bus where storage discharge is connected (e.g. 'SystemBus').",
    )

    # -----------------------------------------------------------
    # Energy & power capacities
    # -----------------------------------------------------------
    e_nom: Dict[U, float] = Field(
        json_schema_extra={"unit": "MWh", "status": "optional"},
        default_factory=dict,
        description="Existing/committed energy capacity [MWh].",
    )

    e_min: Dict[U, float] = Field(
        json_schema_extra={"unit": "MWh", "status": "optional"},
        default_factory=dict,
        description="Minimum allowed energy level during operation[MWh].",
    )

    e_nom_max: Dict[U, float] = Field(
        json_schema_extra={"unit": "MWh", "status": "optional"},
        default_factory=dict,
        description="Maximum allowed energy capacity to be installed [MWh] (e.g. = e_nom for non-expandable hydro).",
    )

    p_charge_nom: Dict[U, float] = Field(
        json_schema_extra={"unit": "MW", "status": "optional"},
        default_factory=dict,
        description="Maximum charging power [MW] into storage (physical limit).",
    )

    p_charge_nom_max: Dict[U, float] = Field(
        json_schema_extra={"unit": "MW", "status": "optional"},
        default_factory=dict,
        description="Maximum charging capacity[MW] that can be installed.",
    )

    p_discharge_nom: Dict[U, float] = Field(
        json_schema_extra={"unit": "MW", "status": "optional"},
        default_factory=dict,
        description="Maximum discharging power [MW] from storage (physical limit).",
    )

    p_discharge_nom_max: Dict[U, float] = Field(
        json_schema_extra={"unit": "MW", "status": "optional"},
        default_factory=dict,
        description="Maximum discharging capacity [MW] that can be installed.",
    )

    # Duration-based sizing (aligned with storage_units.csv template)
    duration_charge: Dict[U, float] = Field(
        json_schema_extra={"unit": "hours", "status": "optional"},
        default_factory=dict,
        description="Charge duration [h]; combined with e_nom to derive p_charge_nom when templates use duration.",
    )

    duration_discharge: Dict[U, float] = Field(
        json_schema_extra={"unit": "hours", "status": "optional"},
        default_factory=dict,
        description="Discharge duration [h]; combined with e_nom to derive p_discharge_nom when templates use duration.",
    )

    # -----------------------------------------------------------
    # Efficiencies & losses (energy side)
    # -----------------------------------------------------------
    efficiency_charge: Dict[U, float] = Field(
        json_schema_extra={"unit": "p.u.", "status": "optional"},
        default_factory=dict,
        description="Charging efficiency (fraction of input power stored).",
    )

    efficiency_discharge: Dict[U, float] = Field(
        json_schema_extra={"unit": "p.u.", "status": "optional"},
        default_factory=dict,
        description="Discharging efficiency (fraction of stored energy delivered).",
    )

    standby_loss: Dict[U, float] = Field(
        json_schema_extra={"unit": "p.u.", "status": "optional"},
        default_factory=dict,
        description="Fractional standing loss of stored energy per model period.",
    )

    # -----------------------------------------------------------
    # Economics & lifetime (energy side)
    # -----------------------------------------------------------
    capital_cost_energy: Dict[U, float] = Field(
        json_schema_extra={"unit": "EUR/MWh", "status": "optional"},
        default_factory=dict,
        description="Investment cost per unit of energy capacity [€/MWh].",
    )

    capital_cost_power_charge: Dict[U, float] = Field(
        json_schema_extra={"unit": "EUR/MW", "status": "optional"},
        default_factory=dict,
        description="Investment cost per unit of charge power [€/MW].",
    )

    capital_cost_power_discharge: Dict[U, float] = Field(
        json_schema_extra={"unit": "EUR/MW", "status": "optional"},
        default_factory=dict,
        description="Investment cost per unit of discharge power [€/MW].",
    )

    lifetime: Dict[U, int] = Field(
        json_schema_extra={"unit": "years", "status": "optional"},
        default_factory=dict,
        description="Technical/economic lifetime of storage asset [years].",
    )

    # -----------------------------------------------------------
    # Time series / inflows
    # -----------------------------------------------------------
    inflow: Dict[UPY, float] = Field(
        json_schema_extra={"unit": "MWh/period", "status": "optional"},
        default_factory=dict,
        description="Exogenous inflow to storage [MWh/period] by (unit, period, year), e.g. hydro inflows.",
    )

    availability: Dict[UPY, float] = Field(
        json_schema_extra={"unit": "p.u.", "status": "optional"},
        default_factory=dict,
        description="Optional storage availability factor [0-1] by (unit, period, year).",
    )

    e_nom_ts: Dict[UPY, float] = Field(
        json_schema_extra={"unit": "MWh", "status": "optional"},
        default_factory=dict,
        description="Optional time-varying effective energy capacity [MWh] by (unit, period, year).",
    )

    spillage_cost: Dict[U, float] = Field(
        json_schema_extra={"unit": "EUR/MWh", "status": "optional"},
        default_factory=dict,
        description="Penalty cost for spilling energy [€/MWh], if modelled.",
    )

    # -----------------------------------------------------------
    # Optional energy-capacity capex by (unit, year)
    # -----------------------------------------------------------
    e_nom_inv_cost: Dict[Tuple[U, Y], float] = Field(
        json_schema_extra={"unit": "EUR/MWh", "status": "optional"},
        default_factory=dict,
        description="Specific investment cost for energy capacity [€/MWh] per (unit, year).",
    )

    model_config = ConfigDict(frozen=True, extra="forbid")

    # -----------------------------------------------------------
    # Validators
    # -----------------------------------------------------------

    @field_validator(
        "system", "region",
        "tech", "carrier_in", "carrier_out", "bus_in", "bus_out",
        "e_nom", "e_nom_max", "e_min",
        "p_charge_nom", "p_charge_nom_max",
        "p_discharge_nom", "p_discharge_nom_max",
        "duration_charge", "duration_discharge",
        "efficiency_charge", "efficiency_discharge", "standby_loss",
        "capital_cost_energy",
        "capital_cost_power_charge",
        "capital_cost_power_discharge",
        "lifetime", "spillage_cost",
        mode="after",
    )
    def _keys_subset_of_units(cls, v, info):
        """
        Ensure all per-unit storage dicts only reference known units.
        Dicts may be sparse (keys ⊆ units).
        """
        units = set(info.data.get("unit", []))
        extra = set(v) - units
        if extra:
            raise ValueError(f"{info.field_name} contains unknown units: {sorted(extra)}")
        return v

    @field_validator("e_nom_inv_cost", mode="after")
    def _energy_capex_keys_subset_of_units(cls, v, info):
        units = set(info.data.get("unit", []))
        extra_units = {u for (u, y) in v if u not in units}
        if extra_units:
            raise ValueError(
                f"e_nom_inv_cost contains unknown units: {sorted(extra_units)}"
            )
        for (u, y) in v:
            if not isinstance(u, str):
                raise TypeError(f"e_nom_inv_cost key has non-str unit: {u!r}")
        return v

    @field_validator("inflow", "availability", "e_nom_ts", mode="after")
    def _ts_keys_are_upy(cls, v):
        """
        Ensure time-series keys are (unit:str, period:int, year:int).
        """
        for (u, p, y) in v:
            if not isinstance(u, str):
                raise TypeError(f"time-series key has non-str unit: {u!r}")
        return v

    @field_validator("inflow", "availability", "e_nom_ts", mode="after")
    def _ts_units_subset_of_units(cls, v, info):
        units = set(info.data.get("unit", []))
        extra_units = {u for (u, p, y) in v if u not in units}
        if extra_units:
            raise ValueError(
                f"{info.field_name} contains unknown units: {sorted(extra_units)}"
            )
        return v

    @field_validator("duration_charge", "duration_discharge", mode="after")
    def _duration_non_negative(cls, v, info):
        for unit, duration in v.items():
            if duration <= 0:
                raise ValueError(f"{info.field_name} for {unit} must be > 0 hours, got {duration}")
        return v
