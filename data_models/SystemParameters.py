from __future__ import annotations
from typing import Dict, List, Tuple, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict
from data_models.SystemSets import SystemSets
from data_models.Generators import Generators
from data_models.StorageUnits import StorageUnits
from data_models.Demand import Demand
from data_models.Bus import Bus

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
      - storage    : all storage units (StorageUnits)
      - demand     : demand by (carrier, bus, period, year)
      - market     : market and price-related parameters
      - policy     : policy and emissions-related parameters
    """

    sets: SystemSets
    bus: Bus
    generators: Generators
    storage: StorageUnits
    demand: Demand

    #TODO: Create Pydantic dataclasses and loader functions for market and policy parameters
    market: MarketParams
    policy:PolicyParams

    model_config = ConfigDict(frozen=True, extra="forbid")
