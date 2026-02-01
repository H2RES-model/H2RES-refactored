from __future__ import annotations
from typing import Dict, Tuple
from pydantic import BaseModel, Field, field_validator, ConfigDict

# Index aliases
P = int
Y = int

System = str
Region = str
BusId = str
Carrier = str



class Demand(BaseModel):
    """
    Generalised multi-carrier demand representation.

    p_t[(system, region, bus, carrier, period, year)] = value
    """

    p_t: Dict[Tuple[System, Region, BusId, Carrier, P, Y], float] = Field(
        default_factory=dict,
        description="Demand time series by (system, region, bus, carrier, period, year).",
        json_schema_extra={"unit": "MWh/period", "status": "input"},
    )

    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("p_t")
    def _non_negative(cls, v):
        for key, val in v.items():
            if val < 0:
                raise ValueError(f"Negative demand at {key}: {val}")
        return v

    @field_validator("p_t", mode="after")
    def _key_types(cls, v):
        for key in v:
            if not isinstance(key, tuple) or len(key) != 6:
                raise TypeError(f"Demand key must be (system, region, bus, carrier, period, year), got {key!r}")
        return v
