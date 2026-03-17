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


class Generators(BaseModel):
    """Generator and converter inputs with explicit pandas fields."""

    TABLE_SPECS: ClassVar[Dict[str, TableSpec]] = {
        "static": TableSpec(
            name="generators.static",
            description="Static generator and converter attributes indexed by unit.",
            index=("unit",),
            columns=(
                ColumnSpec("unit",                  "string", "Generator/converter unit identifier.",      status="mandatory"),
                ColumnSpec("system",                "string", "System/scenario tag."),
                ColumnSpec("region",                "string", "Region/zone identifier."),
                ColumnSpec("tech",                  "string", "Technology label.",                          status="mandatory"),
                ColumnSpec("fuel",                  "string", "Fuel type.",                                 status="mandatory"),
                ColumnSpec("unit_type",             "string", "Supply/conversion role."),
                ColumnSpec("carrier_in",            "string", "Input carrier."),
                ColumnSpec("carrier_out",           "string", "Output carrier."),
                ColumnSpec("bus_in",                "string", "Input bus."),
                ColumnSpec("bus_out",               "string", "Output bus."),
                ColumnSpec("bus_out_2",             "string", "Secondary output bus."),
                ColumnSpec("carrier_out_2",         "string", "Secondary output carrier."),
                ColumnSpec("p_nom",                 "float",  "Existing output power capacity.",            unit="MW",                status="mandatory"),
                ColumnSpec("p_nom_max",             "float",  "Maximum output power capacity.",             unit="MW"),
                ColumnSpec("cap_factor",            "float",  "Capacity factor.",                           unit="p.u.",              status="mandatory"),
                ColumnSpec("capital_cost",          "float",  "Power investment cost.",                     unit="EUR/MW",            status="mandatory"),
                ColumnSpec("lifetime",              "int",    "Technical/economic lifetime.",               unit="years",             status="mandatory"),
                ColumnSpec("decom_start_existing",  "int",    "Existing decommissioning start year.",      unit="year",              status="mandatory"),
                ColumnSpec("decom_start_new",       "int",    "New-build decommissioning start year.",     unit="year",              status="mandatory"),
                ColumnSpec("final_cap",             "float",  "Residual power capacity at end of horizon.", unit="MW",                status="mandatory"),
                ColumnSpec("efficiency",            "float",  "Static efficiency.",                         unit="p.u.",              status="mandatory"),
                ColumnSpec("co2_intensity",         "float",  "CO2 intensity on output.",                   unit="tCO2/MWh_output",   status="mandatory"),
                ColumnSpec("var_cost_no_fuel",      "float",  "Non-fuel variable cost.",                   unit="EUR/MWh_output",    status="mandatory"),
                ColumnSpec("ramp_up_rate",          "float",  "Ramp-up rate.",                              unit="MW/period or p.u.", status="mandatory"),
                ColumnSpec("ramp_down_rate",        "float",  "Ramp-down rate.",                            unit="MW/period or p.u.", status="mandatory"),
                ColumnSpec("ramping_cost",          "float",  "Ramping cost coefficient."),
                ColumnSpec("chp_power_to_heat",     "float",  "CHP power-to-heat ratio.",                  unit="p.u."),
                ColumnSpec("chp_power_loss_factor", "float",  "CHP condensing-to-heat slope.",             unit="p.u."),
                ColumnSpec("chp_max_heat",          "float",  "Maximum CHP heat output.",                  unit="MW_heat"),
                ColumnSpec("chp_type",              "string", "CHP configuration type."),
            ),
        ),
        "p_t": TableSpec(
            name="generators.p_t",
            description="Generator availability/profile time series.",
            columns=(
                ColumnSpec("unit",   "string", "Generator/converter unit identifier.", status="mandatory"),
                ColumnSpec("period", "int",    "Time period index.",                    unit="index", status="mandatory"),
                ColumnSpec("year",   "int",    "Model year.",                           unit="year",  status="mandatory"),
                ColumnSpec("p_t",    "float",  "Availability/profile value.",           unit="p.u.",  status="mandatory"),
            ),
        ),
        "var_cost": TableSpec(
            name="generators.var_cost",
            description="Generator full variable cost time series.",
            columns=(
                ColumnSpec("unit",     "string", "Generator/converter unit identifier.", status="mandatory"),
                ColumnSpec("period",   "int",    "Time period index.",                    unit="index",          status="mandatory"),
                ColumnSpec("year",     "int",    "Model year.",                           unit="year",           status="mandatory"),
                ColumnSpec("var_cost", "float",  "Full variable cost.",                   unit="EUR/MWh_output", status="mandatory"),
            ),
        ),
        "efficiency_ts": TableSpec(
            name="generators.efficiency_ts",
            description="Generator time-varying efficiency.",
            columns=(
                ColumnSpec("unit",          "string", "Generator/converter unit identifier.", status="mandatory"),
                ColumnSpec("period",        "int",    "Time period index.",                    unit="index", status="mandatory"),
                ColumnSpec("year",          "int",    "Model year.",                           unit="year",  status="mandatory"),
                ColumnSpec("efficiency_ts", "float",  "Time-varying efficiency.",              unit="p.u.",  status="mandatory"),
            ),
        ),
    }

    static: pd.DataFrame = Field(default_factory=lambda: empty_table(Generators.TABLE_SPECS["static"]))
    p_t: pd.DataFrame = Field(default_factory=lambda: empty_table(Generators.TABLE_SPECS["p_t"]))
    var_cost: pd.DataFrame = Field(default_factory=lambda: empty_table(Generators.TABLE_SPECS["var_cost"]))
    efficiency_ts: pd.DataFrame = Field(default_factory=lambda: empty_table(Generators.TABLE_SPECS["efficiency_ts"]))

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    @field_validator("static")
    @classmethod
    def _validate_static(cls, df: pd.DataFrame) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS["static"],
            keys=["unit"],
            non_negative=["p_nom", "p_nom_max", "cap_factor", "capital_cost", "lifetime", "efficiency", "var_cost_no_fuel"],
            positive=["efficiency"],
            index_col="unit",
        )

    @classmethod
    def _validate_timeseries(cls, df: pd.DataFrame, name: str) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS[name],
            keys=["unit", "period", "year"],
            non_negative=[] if name == "efficiency_ts" else [name],
            positive=["efficiency_ts"] if name == "efficiency_ts" else [],
        )

    @field_validator("p_t")
    @classmethod
    def _validate_p_t(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "p_t")

    @field_validator("var_cost")
    @classmethod
    def _validate_var_cost(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "var_cost")

    @field_validator("efficiency_ts")
    @classmethod
    def _validate_efficiency_ts(cls, df: pd.DataFrame) -> pd.DataFrame:
        return cls._validate_timeseries(df, "efficiency_ts")

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
    def fuel(self) -> pd.Series:
        return self._static_series("fuel")

    @property
    def unit_type(self) -> pd.Series:
        return self._static_series("unit_type")

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
    def bus_out_2(self) -> pd.Series:
        return self._static_series("bus_out_2")

    @property
    def carrier_out_2(self) -> pd.Series:
        return self._static_series("carrier_out_2")

    @property
    def p_nom(self) -> pd.Series:
        return self._static_series("p_nom")

    @property
    def p_nom_max(self) -> pd.Series:
        return self._static_series("p_nom_max")

    @property
    def cap_factor(self) -> pd.Series:
        return self._static_series("cap_factor")

    @property
    def capital_cost(self) -> pd.Series:
        return self._static_series("capital_cost")

    @property
    def lifetime(self) -> pd.Series:
        return self._static_series("lifetime")

    @property
    def decom_start_existing(self) -> pd.Series:
        return self._static_series("decom_start_existing")

    @property
    def decom_start_new(self) -> pd.Series:
        return self._static_series("decom_start_new")

    @property
    def final_cap(self) -> pd.Series:
        return self._static_series("final_cap")

    @property
    def efficiency(self) -> pd.Series:
        return self._static_series("efficiency")

    @property
    def co2_intensity(self) -> pd.Series:
        return self._static_series("co2_intensity")

    @property
    def var_cost_no_fuel(self) -> pd.Series:
        return self._static_series("var_cost_no_fuel")

    @property
    def ramp_up_rate(self) -> pd.Series:
        return self._static_series("ramp_up_rate")

    @property
    def ramp_down_rate(self) -> pd.Series:
        return self._static_series("ramp_down_rate")

    @property
    def ramping_cost(self) -> pd.Series:
        return self._static_series("ramping_cost")

    @property
    def chp_power_to_heat(self) -> pd.Series:
        return self._static_series("chp_power_to_heat")

    @property
    def chp_power_loss_factor(self) -> pd.Series:
        return self._static_series("chp_power_loss_factor")

    @property
    def chp_max_heat(self) -> pd.Series:
        return self._static_series("chp_max_heat")

    @property
    def chp_type(self) -> pd.Series:
        return self._static_series("chp_type")

    def to_timeseries_table(self) -> pd.DataFrame:
        frames = [frame for frame in (self.p_t, self.var_cost, self.efficiency_ts) if not frame.empty]
        if not frames:
            return pd.DataFrame(columns=["unit", "period", "year", "p_t", "var_cost", "efficiency_ts"])
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

    def to_flat_tables(self) -> Dict[str, pd.DataFrame]:
        return {
            "static": self.static.copy(),
            "p_t": self.p_t.copy(),
            "var_cost": self.var_cost.copy(),
            "efficiency_ts": self.efficiency_ts.copy(),
        }

    def to_legacy_dicts(self) -> Dict[str, Dict[Any, Any]]:
        data: Dict[str, Dict[Any, Any]] = {col: self.static_dict(col) for col in self.static.columns}
        data["p_t"] = self.timeseries_dict("p_t")
        data["var_cost"] = self.timeseries_dict("var_cost")
        data["efficiency_ts"] = self.timeseries_dict("efficiency_ts")
        return data
