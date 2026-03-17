from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator

from data_models.table_schema import (
    ColumnSpec,
    TableSpec,
    empty_table,
    validate_table,
)

BusId = str


class Bus(BaseModel):
    """Network buses and unit attachments."""

    TABLE_SPECS: ClassVar[Dict[str, TableSpec]] = {
        "static": TableSpec(
            name="bus.static",
            description="Static bus metadata indexed by bus identifier.",
            index=("bus",),
            columns=(
                ColumnSpec("bus",     "string", "Bus identifier.",                  status="mandatory"),
                ColumnSpec("system",  "string", "System/country tag for the bus."),
                ColumnSpec("region",  "string", "Region/zone tag for the bus."),
                ColumnSpec("carrier", "string", "Carrier assigned to the bus.",     status="mandatory"),
            ),
        ),
        "attachments": TableSpec(
            name="bus.attachments",
            description="Units attached to each bus by component role.",
            columns=(
                ColumnSpec("bus",       "string", "Bus identifier.",                     status="mandatory"),
                ColumnSpec("component", "string", "Attached component collection name.", status="mandatory"),
                ColumnSpec("unit",      "string", "Attached unit identifier.",           status="mandatory"),
                ColumnSpec("role",      "string", "Optional role label for the attachment."),
            ),
        ),
    }

    static: pd.DataFrame = Field(
        default_factory=lambda: empty_table(Bus.TABLE_SPECS["static"]),
        description="Indexed bus metadata table.",
        json_schema_extra={"unit": "table", "status": "mandatory", "table_spec": "static"},
    )
    attachments: pd.DataFrame = Field(
        default_factory=lambda: empty_table(Bus.TABLE_SPECS["attachments"]),
        description="Bus-to-unit attachment table.",
        json_schema_extra={"unit": "table", "status": "optional", "table_spec": "attachments"},
    )

    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    @field_validator("static")
    @classmethod
    def _validate_static(cls, df: pd.DataFrame) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS["static"],
            keys=["bus"],
            index_col="bus",
        )

    @field_validator("attachments")
    @classmethod
    def _validate_attachments(cls, df: pd.DataFrame) -> pd.DataFrame:
        return validate_table(
            df,
            cls.TABLE_SPECS["attachments"],
            keys=["bus", "component", "unit", "role"],
        )

    @property
    def name(self) -> List[BusId]:
        if self.static.empty:
            return []
        return self.static.index.astype(str).tolist()

    @property
    def system(self) -> pd.Series:
        if self.static.empty or "system" not in self.static.columns:
            return pd.Series(dtype=object, name="system")
        return self.static["system"]

    @property
    def region(self) -> pd.Series:
        if self.static.empty or "region" not in self.static.columns:
            return pd.Series(dtype=object, name="region")
        return self.static["region"]

    @property
    def carrier(self) -> pd.Series:
        if self.static.empty or "carrier" not in self.static.columns:
            return pd.Series(dtype=object, name="carrier")
        return self.static["carrier"]

    def units(
        self,
        bus: str,
        *,
        component: Optional[str] = None,
        role: Optional[str] = None,
    ) -> pd.DataFrame:
        """Return attachment rows for one bus, optionally filtered by component/role."""
        if self.attachments.empty:
            return pd.DataFrame(columns=["bus", "component", "unit", "role"])
        out = self.attachments[self.attachments["bus"].astype(str) == str(bus)]
        if component is not None:
            out = out[out["component"].astype(str) == str(component)]
        if role is not None:
            out = out[out["role"].astype(str) == str(role)]
        return out.reset_index(drop=True)

    def buses_for_carrier(self, carrier: str) -> List[str]:
        if self.static.empty or "carrier" not in self.static.columns:
            return []
        mask = self.static["carrier"].astype(str).str.lower() == str(carrier).strip().lower()
        return self.static.index[mask].astype(str).tolist()

    def carrier_of(self, bus: str) -> Optional[str]:
        if self.static.empty or bus not in self.static.index:
            return None
        value = self.static.at[bus, "carrier"] if "carrier" in self.static.columns else None
        if pd.isna(value):
            return None
        return str(value)

    def series_dict(self, column: str) -> Dict[str, Any]:
        if self.static.empty or column not in self.static.columns:
            return {}
        series = self.static[column].dropna()
        return {str(idx): value for idx, value in series.items()}

    def to_legacy_dicts(self) -> Dict[str, Dict[str, Any]]:
        return {
            "system": self.series_dict("system"),
            "region": self.series_dict("region"),
            "carrier": self.series_dict("carrier"),
        }

    @classmethod
    def from_csv(
        cls,
        buses_csv_path: str,
        *,
        generators_at_bus: Dict[BusId, List[str]] | None = None,
        storage_at_bus: Dict[BusId, List[str]] | None = None,
    ) -> "Bus":
        df = pd.read_csv(buses_csv_path)
        required = {"bus", "carrier"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"{buses_csv_path} is missing required columns: {sorted(missing)}")
        blank_carrier = df["carrier"].isna() | df["carrier"].astype(str).str.strip().eq("")
        if blank_carrier.any():
            buses = df.loc[blank_carrier, "bus"].astype(str).tolist()
            raise ValueError(f"{buses_csv_path} has missing required values in 'carrier': {buses}")

        static = df[["bus"]].copy()
        static["system"] = df["system"] if "system" in df.columns else ""
        static["region"] = df["region"] if "region" in df.columns else ""
        static["carrier"] = df["carrier"].astype(str)

        rows: list[dict[str, str]] = []
        for bus, units in (generators_at_bus or {}).items():
            rows.extend(
                {"bus": str(bus), "component": "generator", "unit": str(unit), "role": "output"}
                for unit in units
            )
        for bus, units in (storage_at_bus or {}).items():
            rows.extend(
                {"bus": str(bus), "component": "storage", "unit": str(unit), "role": "output"}
                for unit in units
            )
        attachments = pd.DataFrame(rows, columns=["bus", "component", "unit", "role"])
        return cls(static=static, attachments=attachments)
