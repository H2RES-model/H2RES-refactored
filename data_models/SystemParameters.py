from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict
from data_models.SystemSets import SystemSets
from data_models.Generators import Generators
from data_models.StorageUnits import StorageUnits
from data_models.Demand import Demand
from data_models.Bus import Bus
from data_models.Transport import Transport

# Index aliases
U = str
P = int
Y = int
UPY = Tuple[U, P, Y]

BusId = str
Carrier = str
Fuel = str
Sector = str

# ----------------------------------------------------------
# Placeholder market model
# ----------------------------------------------------------
class MarketParams(BaseModel):
    """
    Placeholder for market-related parameters
    (import/export limits, price series, market settings).
    """

    import_ntc: Dict[P, float] = Field(default_factory=dict)
    exports: Dict[P, float] = Field(default_factory=dict)

    exports_dat: bool = False
    import_price: Optional[float] = None
    import_price_increase: Optional[float] = None

    model_config = ConfigDict(frozen=True, extra="forbid")


# ----------------------------------------------------------
# Placeholder policy model
# ----------------------------------------------------------
class PolicyParams(BaseModel):
    """
    Placeholder for policy parameters:
    CO2 price, caps, RES targets, etc.
    """

    NPV: Dict[Y, float] = Field(default_factory=dict)
    rps: Dict[Y, float] = Field(default_factory=dict)
    CO2_price: Dict[Y, float] = Field(default_factory=dict)
    CO2_limit: Dict[Y, float] = Field(default_factory=dict)

    rps_inv: bool = False
    res_inv: bool = True
    ceep_limit: bool = False
    ceep_parameter: Optional[float] = None

    model_config = ConfigDict(frozen=True, extra="forbid")


class SystemParameters(BaseModel):
    """
    Top-level container for the H2RES data model.

    This brings together:
      - sets       : all index sets and subsets (SystemSets)
      - generators : all supply & conversion units (Generators)
      - storage_units : all storage units (StorageUnits)
      - demands    : demand by (carrier, bus, period, year)
      - market     : market and price-related parameters
      - policy     : policy and emissions-related parameters
    """

    sets: SystemSets = Field(
        description="Core index sets for the system.",
        json_schema_extra={"unit": "n.a.", "status": "mandatory"},
    )
    buses: Bus = Field(
        description="Network buses and carrier assignments.",
        json_schema_extra={"unit": "n.a.", "status": "mandatory"},
    )
    generators: Generators = Field(
        description="Power-converting units and converters.",
        json_schema_extra={"unit": "n.a.", "status": "mandatory"},
    )
    storage_units: StorageUnits = Field(
        description="Energy storage assets and parameters.",
        json_schema_extra={"unit": "n.a.", "status": "mandatory"},
    )
    demands: Demand = Field(
        description="Demand time series by carrier and bus.",
        json_schema_extra={"unit": "n.a.", "status": "mandatory"},
    )
    transport_units: Transport = Field(
        default_factory=Transport,
        description="Transport units and their time series.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    #TODO: Create Pydantic dataclasses and loader functions for market and policy parameters
    market: MarketParams = Field(
        description="Market and price-related parameters.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )
    policy: PolicyParams = Field(
        description="Policy and emissions-related parameters.",
        json_schema_extra={"unit": "n.a.", "status": "optional"},
    )

    model_config = ConfigDict(frozen=True, extra="forbid")
