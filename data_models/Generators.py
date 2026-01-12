from __future__ import annotations
from typing import Dict, List, Tuple, Optional, Literal
import math
from pydantic import BaseModel, Field, field_validator, ConfigDict

# -------------------------------------------------------------------
# Index aliases
# -------------------------------------------------------------------
U = str   # unit id
P = int   # period index
Y = int   # year index
UPY = Tuple[U, P, Y]

BusId = str
Carrier = str
Fuel = str


# ===============================================================
# 1. Generators 
# ===============================================================

class Generators(BaseModel):
    """
    Power-converting units only (no energy capacity or SOC).

    Examples:
      - CCGT, OCGT, coal plants
      - Wind, PV, run-of-river hydro
      - CHP units
      - Heat pumps, electric boilers, ETES pumps (converters)
      - Hydro turbines, PHS pumps (power-related parameters only)

    Design:
      - No reservoir size, SOC, inflows, or storage losses here.
      - All techno-economic attributes refer to electrical capacity and output.
      - Efficiency can be static per unit, or time-varying (e.g. COP of HP).
    """

    # -----------------------------------------------------------
    # Index & classification
    # -----------------------------------------------------------
    unit: List[U] = Field(
        description="All generator / converter units in the system."
    )

    system: Dict[U, str] = Field(
        default_factory=dict,
        description="System/scenario tag (column 'system' in powerplants.csv), e.g. country code.",
    )

    region: Dict[U, str] = Field(
        default_factory=dict,
        description="Region/zone for the unit (column 'region' in powerplants.csv).",
    )

    tech: Dict[U, str] = Field(
        description="Technology label (e.g. 'CCGT', 'WindPP', 'PV', 'CHP', 'HDAM_turbine', 'HPHS_pump')."
    )

    fuel: Dict[U, Fuel] = Field(
        description="Fuel type (e.g. 'Gas', 'Coal', 'Wind', 'Solar', 'Water', 'Electricity')."
    )

    unit_type: Dict[U, Literal["supply", "conversion"]] = Field(
        default_factory=dict,
        description=(
            "Modelling role of the unit: "
            "'supply' = fuel→output with implicit input carrier; "
            "'conversion' = explicit carrier_in→carrier_out (e.g. HP, ETES pump, PHS pump)."
        ),
    )

    # -----------------------------------------------------------
    # Network & carriers
    # -----------------------------------------------------------
    carrier_in: Dict[U, Optional[Carrier]] = Field(
        default_factory=dict,
        description="Input carrier for conversion units (e.g. 'Electricity' for HP, ETES pumps)."
    )
    
    carrier_out: Dict[U, Carrier] = Field(
        default_factory=dict,
        description="Output carrier (e.g. 'Electricity', 'Heat', 'H2')."
    )

    bus_in: Dict[U, Optional[BusId]] = Field(
        default_factory=dict,
        description="Bus where input power is drawn (for conversion units)."
    )

    bus_out: Dict[U, BusId] = Field(
        default_factory=dict,
        description="Bus where output power is injected."
    )
    
    # -----------------------------------------------------------
    # Static techno-economic parameters (power side)
    # -----------------------------------------------------------
    p_nom: Dict[U, float] = Field(
        description="Existing/committed nominal output power capacity [MW]."
    )

    p_nom_max: Dict[U, float] = Field(
        default_factory=dict,
        description="Maximum allowed power capacity [MW] (upper bound on investment)."
    )

    cap_factor: Dict[U, float] = Field(
        description="Capacity factor of unit."
    )

    capital_cost: Dict[U, float] = Field(
        description="Investment cost per unit of power capacity [€/MW]."
    )

    lifetime: Dict[U, int] = Field(
        description="Technical/economic lifetime of the power asset [years]."
    )

    decom_start_existing: Dict[U, int] = Field(
        description="Year when existing capacity starts decommissioning."
    )
    decom_start_new: Dict[U, int] = Field(
        description="Year when newly built capacity starts decommissioning."
    )

    final_cap: Dict[U, float] = Field(
        description="Residual power capacity at end of horizon [MW]."
    )

    # -----------------------------------------------------------
    # Efficiencies, emissions, variable costs
    # -----------------------------------------------------------
    efficiency: Dict[U, float] = Field(
        description=(
            "Static efficiency per unit (output/input). "
            "For fuel-based generators, fuel→power; for HP, power→heat, etc."
        )
    )

    # Time-varying efficiency, e.g. COP of a heat pump by (unit, period, year).
    # If present, the model may override `efficiency[u]` with `efficiency_ts[u,p,y]`
    # for those indices where a time-varying value is defined.
    efficiency_ts: Dict[UPY, float] = Field(
        default_factory=dict,
        description="Time-varying efficiency (e.g. COP) by (unit, period, year)."
    )

    co2_intensity: Dict[U, float] = Field(
        description="CO2 intensity attributed to output [tCO2/MWh_output]."
    )

    var_cost_no_fuel: Dict[U, float] = Field(
        description="Non-fuel variable O&M cost [€/MWh_output]."
    )

    # -----------------------------------------------------------
    # Ramping & operational flexibility
    # -----------------------------------------------------------
    ramp_up_rate: Dict[U, float] = Field(
        description="Maximum ramp-up rate [MW/period or pu]."
    )
    ramp_down_rate: Dict[U, float] = Field(
        description="Maximum ramp-down rate [MW/period or pu]."
    )
    ramping_cost: Dict[U, float] = Field(
        default_factory=dict,
        description="Ramping cost coefficient (optional)."
    )

    # -----------------------------------------------------------
    # CHP / multi-output metadata (optional)
    # -----------------------------------------------------------
    chp_power_to_heat: Dict[U, float] = Field(
        default_factory=dict,
        description="Back-pressure power-to-heat ratio for CHP units."
    )
    chp_power_loss_factor: Dict[U, float] = Field(
        default_factory=dict,
        description="Slope of condensing-to-heat trade-off for extraction CHP."
    )
    chp_max_heat: Dict[U, float] = Field(
        default_factory=dict,
        description="Maximum thermal output [MW_heat] for CHP units."
    )
    chp_type: Dict[U, str] = Field(
        default_factory=dict,
        description="CHP configuration label (e.g. 'backpressure', 'extraction')."
    )

    bus_out_2: Dict[U, Optional[BusId]] = Field(
        default_factory=dict,
        description="Second output bus, e.g. to represent heat output of CHP units."
    )

    carrier_out_2: Dict[U, Optional[BusId]] = Field(
        default_factory=dict,
        description="Carrier of second output, e.g. heat for CHP units."
    )   

    # -----------------------------------------------------------
    # Time series (profiles & full variable costs)
    # -----------------------------------------------------------
    p_t: Dict[UPY, float] = Field(
        default_factory=dict,
        description=(
            "Normalized profile (0–1) by (unit, period, year), typically for "
            "renewables or availability limits."
        ),
    )

    var_cost: Dict[UPY, float] = Field(
        default_factory=dict,
        description=(
            "Full variable cost [€/MWh_output] including fuel, by (unit, period, year). "
            "Typically built from fuel_price(fuel, t) and efficiency."
        ),
    )

    model_config = ConfigDict(frozen=True, extra="forbid")

    # -----------------------------------------------------------
    # Validators
    # -----------------------------------------------------------

    @field_validator(
        "system", "region",
        "tech", "fuel", "unit_type",
        "carrier_in", "carrier_out",
        "bus_in", "bus_out",
        "p_nom", "p_nom_max", "cap_factor",
        "capital_cost", "lifetime", 
        "decom_start_existing", "decom_start_new", "final_cap",
        "efficiency", "co2_intensity", "var_cost_no_fuel",
        "ramp_up_rate", "ramp_down_rate", "ramping_cost",
        mode="after",
    )
    def _keys_subset_of_units(cls, v, info):
        """
        Ensure all per-unit dicts only reference known units.

        Dicts are allowed to be sparse (keys ⊆ units), but may not contain unknown unit ids.
        """
        units = set(info.data.get("unit", []))
        extra = set(v) - units
        if extra:
            raise ValueError(f"{info.field_name} contains unknown units: {sorted(extra)}")
        return v

    @field_validator("p_t", "var_cost", "efficiency_ts", mode="after")
    def _upy_keys_have_string_unit(cls, v, info):
        """
        Ensure time-series keys are (unit:str, period:int, year:int).
        """
        for (u, p, y) in v:
            if not isinstance(u, str):
                raise TypeError(f"{info.field_name} key has non-str unit: {u!r}")
        return v

    @field_validator("efficiency", mode="after")
    def _efficiency_positive(cls, v):
        bad = [u for u, val in v.items() if not math.isfinite(val) or val <= 0]
        if bad:
            raise ValueError(f"efficiency must be > 0 for units: {sorted(bad)}")
        return v

    @field_validator("efficiency_ts", mode="after")
    def _efficiency_ts_positive(cls, v):
        bad = [k for k, val in v.items() if not math.isfinite(val) or val <= 0]
        if bad:
            raise ValueError(f"efficiency_ts must be > 0 for keys: {sorted(bad)}")
        return v


