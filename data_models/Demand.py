from __future__ import annotations

from typing import ClassVar, Dict

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from data_models.table_schema import (
    ColumnSpec,
    TableSpec,
    empty_table,
    ensure_dataframe,
    normalize_dataframe,
    validate_non_negative,
    validate_unique_keys,
)


class Demand(BaseModel):
    """Generalized multi-carrier demand as one explicit pandas field."""

    TABLE_SPECS: ClassVar[Dict[str, TableSpec]] = {
        "p_t": TableSpec(
            name="demand.p_t",
            description="Demand time series by system, region, bus, carrier, period, and year.",
            columns=(
                ColumnSpec("system", "string", "System/scenario identifier."),
                ColumnSpec("region", "string", "Region/zone identifier."),
                ColumnSpec("bus", "string", "Demand bus identifier.", status="mandatory"),
                ColumnSpec("carrier", "string", "Demand carrier.", status="mandatory"),
                ColumnSpec("period", "int", "Time period index.", unit="index", status="mandatory"),
                ColumnSpec("year", "int", "Model year.", unit="year", status="mandatory"),
                ColumnSpec("p_t", "float", "Demand value.", unit="MWh/period", status="mandatory"),
            ),
        ),
    }

    p_t: pd.DataFrame = Field(default_factory=lambda: empty_table(Demand.TABLE_SPECS["p_t"]))

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    @field_validator("p_t")
    @classmethod
    def _validate_p_t(cls, df: pd.DataFrame) -> pd.DataFrame:
        spec = cls.TABLE_SPECS["p_t"]
        table = normalize_dataframe(ensure_dataframe(df, spec), spec, copy=True)
        validate_non_negative(table, ["p_t"], spec.name)
        validate_unique_keys(table, ["system", "region", "bus", "carrier", "period", "year"], spec.name)
        return table.reset_index(drop=True)

    def to_flat_tables(self) -> Dict[str, pd.DataFrame]:
        return {"p_t": self.p_t.copy()}
