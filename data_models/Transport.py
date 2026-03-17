from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from data_models.table_schema import (
    ColumnSpec,
    TableSpec,
    empty_table,
    validate_table,
)


class Transport(BaseModel):
    """Internal canonical transport component with explicit pandas fields."""

    TABLE_SPECS: ClassVar[Dict[str, TableSpec]] = {
        "static": TableSpec(
            name="transport.static",
            description="Transport unit metadata indexed by unit.",
            index=("unit",),
            columns=(
                ColumnSpec("unit",                     "string", "Transport unit identifier.",                       status="mandatory"),
                ColumnSpec("system",                   "string", "System tag."),
                ColumnSpec("region",                   "string", "Region tag."),
                ColumnSpec("transport_segment",        "string", "Transport segment/group.",                         status="mandatory"),
                ColumnSpec("tech",                     "string", "Transport technology.",                            status="mandatory"),
                ColumnSpec("fuel_type",                "string", "Transport fuel label.",                            status="mandatory"),
                ColumnSpec("carrier_in",               "string", "System carrier consumed by the transport unit.",  status="mandatory"),
                ColumnSpec("bus_in",                   "string", "System bus supplying the transport unit."),
                ColumnSpec("efficiency_primary",       "float",  "Primary efficiency/conversion factor.",           status="mandatory"),
                ColumnSpec("fleet_units",              "float",  "Number of modeled vehicles/assets.",              status="mandatory"),
                ColumnSpec("battery_capacity_kwh",     "float",  "Per-unit onboard storage capacity.",              unit="kWh"),
                ColumnSpec("charge_rate_kw",           "float",  "Per-unit grid connection charge/discharge rate.", unit="kW"),
                ColumnSpec("grid_efficiency",          "float",  "Grid charging/discharging efficiency.",           unit="p.u."),
                ColumnSpec("storage_min_soc",          "float",  "Minimum usable state of charge.",                 unit="p.u."),
                ColumnSpec("v2g_cost",                 "float",  "Vehicle-to-grid specific power cost."),
                ColumnSpec("v2g_year_cost_variability","float",  "Yearly V2G cost variability factor."),
                ColumnSpec("lifetime",                 "int",    "Asset lifetime."),
                ColumnSpec("max_investment",           "float",  "Maximum fleet expansion."),
                ColumnSpec("supports_grid_connection", "bool",   "Whether the unit can connect to the modeled grid."),
                ColumnSpec("annual_demand_mwh",        "float",  "Annual demand allocated to the unit.",           unit="MWh"),
            ),
        ),
        "availability": TableSpec(
            name="transport.availability",
            description="Transport grid-connection availability time series.",
            columns=(
                ColumnSpec("unit",         "string", "Transport unit identifier.",                               status="mandatory"),
                ColumnSpec("period",       "int",    "Time period index.",                                       status="mandatory"),
                ColumnSpec("year",         "int",    "Model year.",                                              status="mandatory"),
                ColumnSpec("availability", "float",  "Fraction connected and available for charging/discharge.", status="mandatory"),
            ),
        ),
        "demand_profile": TableSpec(
            name="transport.demand_profile",
            description="Normalized hourly transport demand profile.",
            columns=(
                ColumnSpec("unit",           "string", "Transport unit identifier.",          status="mandatory"),
                ColumnSpec("period",         "int",    "Time period index.",                  status="mandatory"),
                ColumnSpec("year",           "int",    "Model year.",                         status="mandatory"),
                ColumnSpec("demand_profile", "float",  "Normalized temporal demand profile.", status="mandatory"),
            ),
        ),
        "demand": TableSpec(
            name="transport.demand",
            description="Hourly transport energy demand by unit.",
            columns=(
                ColumnSpec("unit",   "string", "Transport unit identifier.", status="mandatory"),
                ColumnSpec("period", "int",    "Time period index.",         status="mandatory"),
                ColumnSpec("year",   "int",    "Model year.",                status="mandatory"),
                ColumnSpec("demand", "float",  "Hourly transport demand.",   unit="MWh/period", status="mandatory"),
            ),
        ),
    }

    static: pd.DataFrame = Field(default_factory=lambda: empty_table(Transport.TABLE_SPECS["static"]))
    availability: pd.DataFrame = Field(default_factory=lambda: empty_table(Transport.TABLE_SPECS["availability"]))
    demand_profile: pd.DataFrame = Field(default_factory=lambda: empty_table(Transport.TABLE_SPECS["demand_profile"]))
    demand: pd.DataFrame = Field(default_factory=lambda: empty_table(Transport.TABLE_SPECS["demand"]))

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    @field_validator("static")
    @classmethod
    def _validate_static(cls, df: pd.DataFrame) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS["static"],
            keys=["unit"],
            non_negative=["fleet_units", "battery_capacity_kwh", "charge_rate_kw", "v2g_cost", "annual_demand_mwh"],
            index_col="unit",
        )

    @classmethod
    def _validate_timeseries(cls, df: pd.DataFrame, name: str) -> pd.DataFrame:
        table = validate_table(
            df,
            cls.TABLE_SPECS[name],
            keys=["unit", "period", "year"],
            non_negative=[] if name == "availability" else [name],
        )
        if name == "availability":
            bad = table[(table["availability"] < 0) | (table["availability"] > 1)]
            if not bad.empty:
                raise ValueError("transport.availability must be in [0,1].")
        return table

    @field_validator("availability")
    @classmethod
    def _validate_availability(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "availability")

    @field_validator("demand_profile")
    @classmethod
    def _validate_demand_profile(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "demand_profile")

    @field_validator("demand")
    @classmethod
    def _validate_demand(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "demand")

    @property
    def unit(self) -> List[str]:
        if self.static.empty:
            return []
        return self.static.index.astype(str).tolist()
