from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Tuple

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from data_models.table_schema import (
    ColumnSpec,
    TableSpec,
    dataframe_to_multiindex_dict,
    empty_table,
    validate_table,
)

U = str
P = int
Y = int
UPY = Tuple[U, P, Y]


class StorageUnits(BaseModel):
    """Storage inputs with explicit pandas fields."""

    TABLE_SPECS: ClassVar[Dict[str, TableSpec]] = {
        "static": TableSpec(
            name="storage.static",
            description="Static storage unit attributes indexed by unit.",
            index=("unit",),
            columns=(
                ColumnSpec("unit",                         "string", "Storage unit identifier.",                status="mandatory"),
                ColumnSpec("system",                       "string", "System/scenario tag."),
                ColumnSpec("region",                       "string", "Region/zone identifier."),
                ColumnSpec("tech",                         "string", "Storage technology."),
                ColumnSpec("carrier_in",                   "string", "Input carrier."),
                ColumnSpec("carrier_out",                  "string", "Output carrier."),
                ColumnSpec("bus_in",                       "string", "Charging bus."),
                ColumnSpec("bus_out",                      "string", "Discharging bus."),
                ColumnSpec("e_nom",                        "float",  "Existing energy capacity.",               unit="MWh"),
                ColumnSpec("e_min",                        "float",  "Minimum energy level.",                  unit="MWh"),
                ColumnSpec("e_nom_max",                    "float",  "Maximum energy capacity.",               unit="MWh"),
                ColumnSpec("p_charge_nom",                 "float",  "Charge power limit.",                   unit="MW"),
                ColumnSpec("p_charge_nom_max",             "float",  "Maximum charge power.",                 unit="MW"),
                ColumnSpec("p_discharge_nom",              "float",  "Discharge power limit.",                unit="MW"),
                ColumnSpec("p_discharge_nom_max",          "float",  "Maximum discharge power.",              unit="MW"),
                ColumnSpec("duration_charge",              "float",  "Charge duration.",                      unit="hours"),
                ColumnSpec("duration_discharge",           "float",  "Discharge duration.",                   unit="hours"),
                ColumnSpec("efficiency_charge",            "float",  "Charge efficiency.",                    unit="p.u."),
                ColumnSpec("efficiency_discharge",         "float",  "Discharge efficiency.",                 unit="p.u."),
                ColumnSpec("standby_loss",                 "float",  "Standing loss.",                        unit="p.u."),
                ColumnSpec("capital_cost_energy",          "float",  "Energy capacity capex.",                unit="EUR/MWh"),
                ColumnSpec("capital_cost_power_charge",    "float",  "Charge power capex.",                  unit="EUR/MW"),
                ColumnSpec("capital_cost_power_discharge", "float",  "Discharge power capex.",               unit="EUR/MW"),
                ColumnSpec("lifetime",                     "int",    "Technical/economic lifetime.",          unit="years"),
                ColumnSpec("spillage_cost",                "float",  "Spillage cost.",                        unit="EUR/MWh"),
            ),
        ),
        "inflow": TableSpec(
            name="storage.inflow",
            description="Storage inflow time series.",
            columns=(
                ColumnSpec("unit",   "string", "Storage unit identifier.", status="mandatory"),
                ColumnSpec("period", "int",    "Time period index.",       unit="index",      status="mandatory"),
                ColumnSpec("year",   "int",    "Model year.",              unit="year",       status="mandatory"),
                ColumnSpec("inflow", "float",  "Exogenous inflow.",        unit="MWh/period", status="mandatory"),
            ),
        ),
        "availability": TableSpec(
            name="storage.availability",
            description="Storage availability factor time series.",
            columns=(
                ColumnSpec("unit",         "string", "Storage unit identifier.", status="mandatory"),
                ColumnSpec("period",       "int",    "Time period index.",       unit="index", status="mandatory"),
                ColumnSpec("year",         "int",    "Model year.",              unit="year",  status="mandatory"),
                ColumnSpec("availability", "float",  "Availability factor.",     unit="p.u.",  status="mandatory"),
            ),
        ),
        "e_nom_ts": TableSpec(
            name="storage.e_nom_ts",
            description="Time-varying effective energy capacity.",
            columns=(
                ColumnSpec("unit",     "string", "Storage unit identifier.",                status="mandatory"),
                ColumnSpec("period",   "int",    "Time period index.",                      unit="index", status="mandatory"),
                ColumnSpec("year",     "int",    "Model year.",                             unit="year",  status="mandatory"),
                ColumnSpec("e_nom_ts", "float",  "Time-varying effective energy capacity.", unit="MWh",   status="mandatory"),
            ),
        ),
        "investment_costs": TableSpec(
            name="storage.investment_costs",
            description="Optional storage energy-capex overrides by unit and year.",
            columns=(
                ColumnSpec("unit",           "string", "Storage unit identifier.",         status="mandatory"),
                ColumnSpec("year",           "int",    "Model year.",                      unit="year",    status="mandatory"),
                ColumnSpec("e_nom_inv_cost", "float",  "Specific energy investment cost.", unit="EUR/MWh", status="mandatory"),
            ),
        ),
    }

    static: pd.DataFrame = Field(default_factory=lambda: empty_table(StorageUnits.TABLE_SPECS["static"]))
    inflow: pd.DataFrame = Field(default_factory=lambda: empty_table(StorageUnits.TABLE_SPECS["inflow"]))
    availability: pd.DataFrame = Field(default_factory=lambda: empty_table(StorageUnits.TABLE_SPECS["availability"]))
    e_nom_ts: pd.DataFrame = Field(default_factory=lambda: empty_table(StorageUnits.TABLE_SPECS["e_nom_ts"]))
    investment_costs: pd.DataFrame = Field(default_factory=lambda: empty_table(StorageUnits.TABLE_SPECS["investment_costs"]))

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    @field_validator("static")
    @classmethod
    def _validate_static(cls, df: pd.DataFrame) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS["static"],
            keys=["unit"],
            non_negative=[
                "e_nom",
                "e_min",
                "e_nom_max",
                "p_charge_nom",
                "p_charge_nom_max",
                "p_discharge_nom",
                "p_discharge_nom_max",
                "duration_charge",
                "duration_discharge",
                "capital_cost_energy",
                "capital_cost_power_charge",
                "capital_cost_power_discharge",
                "lifetime",
            ],
            index_col="unit",
        )

    @classmethod
    def _validate_timeseries(cls, df: pd.DataFrame, name: str) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS[name],
            keys=["unit", "period", "year"],
            non_negative=[name],
        )

    @field_validator("inflow")
    @classmethod
    def _validate_inflow(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "inflow")

    @field_validator("availability")
    @classmethod
    def _validate_availability(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "availability")

    @field_validator("e_nom_ts")
    @classmethod
    def _validate_e_nom_ts(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "e_nom_ts")

    @field_validator("investment_costs")
    @classmethod
    def _validate_investment_costs(cls, df: pd.DataFrame) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS["investment_costs"],
            keys=["unit", "year"],
            non_negative=["e_nom_inv_cost"],
        )

    @property
    def unit(self) -> List[U]:
        if self.static.empty:
            return []
        return self.static.index.astype(str).tolist()

    def _static_series(self, column: str) -> pd.Series:
        if self.static.empty or column not in self.static.columns:
            return pd.Series(dtype=object, name=column)
        return self.static[column]

    @property
    def system(self) -> pd.Series:
        return self._static_series("system")

    @property
    def region(self) -> pd.Series:
        return self._static_series("region")

    @property
    def tech(self) -> pd.Series:
        return self._static_series("tech")

    @property
    def carrier_in(self) -> pd.Series:
        return self._static_series("carrier_in")

    @property
    def carrier_out(self) -> pd.Series:
        return self._static_series("carrier_out")

    @property
    def bus_in(self) -> pd.Series:
        return self._static_series("bus_in")

    @property
    def bus_out(self) -> pd.Series:
        return self._static_series("bus_out")

    @property
    def e_nom(self) -> pd.Series:
        return self._static_series("e_nom")

    @property
    def e_min(self) -> pd.Series:
        return self._static_series("e_min")

    @property
    def e_nom_max(self) -> pd.Series:
        return self._static_series("e_nom_max")

    @property
    def p_charge_nom(self) -> pd.Series:
        return self._static_series("p_charge_nom")

    @property
    def p_charge_nom_max(self) -> pd.Series:
        return self._static_series("p_charge_nom_max")

    @property
    def p_discharge_nom(self) -> pd.Series:
        return self._static_series("p_discharge_nom")

    @property
    def p_discharge_nom_max(self) -> pd.Series:
        return self._static_series("p_discharge_nom_max")

    @property
    def duration_charge(self) -> pd.Series:
        return self._static_series("duration_charge")

    @property
    def duration_discharge(self) -> pd.Series:
        return self._static_series("duration_discharge")

    @property
    def efficiency_charge(self) -> pd.Series:
        return self._static_series("efficiency_charge")

    @property
    def efficiency_discharge(self) -> pd.Series:
        return self._static_series("efficiency_discharge")

    @property
    def standby_loss(self) -> pd.Series:
        return self._static_series("standby_loss")

    @property
    def capital_cost_energy(self) -> pd.Series:
        return self._static_series("capital_cost_energy")

    @property
    def capital_cost_power_charge(self) -> pd.Series:
        return self._static_series("capital_cost_power_charge")

    @property
    def capital_cost_power_discharge(self) -> pd.Series:
        return self._static_series("capital_cost_power_discharge")

    @property
    def lifetime(self) -> pd.Series:
        return self._static_series("lifetime")

    @property
    def spillage_cost(self) -> pd.Series:
        return self._static_series("spillage_cost")

    @property
    def e_nom_inv_cost(self) -> pd.DataFrame:
        if self.investment_costs.empty:
            return pd.DataFrame(columns=["unit", "year", "e_nom_inv_cost"])
        return self.investment_costs[["unit", "year", "e_nom_inv_cost"]].dropna(subset=["e_nom_inv_cost"]).reset_index(drop=True)

    def to_timeseries_table(self) -> pd.DataFrame:
        frames = [frame for frame in (self.inflow, self.availability, self.e_nom_ts) if not frame.empty]
        if not frames:
            return pd.DataFrame(columns=["unit", "period", "year", "inflow", "availability", "e_nom_ts"])
        out = frames[0].copy()
        for frame in frames[1:]:
            out = out.merge(frame, on=["unit", "period", "year"], how="outer")
        return out

    def static_dict(self, column: str) -> Dict[str, Any]:
        series = self._static_series(column)
        return {str(idx): value for idx, value in series.dropna().items()}

    def timeseries_dict(self, column: str) -> Dict[UPY, Any]:
        frame = getattr(self, column)
        if frame.empty:
            return {}
        indexed = frame.set_index(["unit", "period", "year"])
        return dataframe_to_multiindex_dict(indexed, column)

    def investment_costs_dict(self) -> Dict[Tuple[U, Y], float]:
        if self.investment_costs.empty:
            return {}
        indexed = self.investment_costs.set_index(["unit", "year"])
        return dataframe_to_multiindex_dict(indexed, "e_nom_inv_cost")

    def to_flat_tables(self) -> Dict[str, pd.DataFrame]:
        return {
            "static": self.static.copy(),
            "inflow": self.inflow.copy(),
            "availability": self.availability.copy(),
            "e_nom_ts": self.e_nom_ts.copy(),
            "investment_costs": self.investment_costs.copy(),
        }

    def to_legacy_dicts(self) -> Dict[str, Dict[Any, Any]]:
        data: Dict[str, Dict[Any, Any]] = {col: self.static_dict(col) for col in self.static.columns}
        data["inflow"] = self.timeseries_dict("inflow")
        data["availability"] = self.timeseries_dict("availability")
        data["e_nom_ts"] = self.timeseries_dict("e_nom_ts")
        data["e_nom_inv_cost"] = self.investment_costs_dict()
        return data
