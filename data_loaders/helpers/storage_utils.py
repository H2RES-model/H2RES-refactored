"""Shared storage helpers for building and validating StorageUnits."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Hashable, Iterable, List, Set, Tuple, TypeVar

from data_models.StorageUnits import StorageUnits

K = TypeVar("K", bound=Hashable)


def merge_no_overwrite(base: Dict[K, Any], new: Dict[K, Any]) -> Dict[K, Any]:
    """Merge dictionaries without clobbering existing keys.

    Args:
        base: Existing mapping (wins on conflicts).
        new: New mapping to merge in.

    Returns:
        Merged mapping with original values preserved.
    """
    merged = dict(base)
    for k, v in new.items():
        if k not in merged:
            merged[k] = v
    return merged


@dataclass
class StorageRecordStore:
    """Collect per-unit storage fields and preserve insertion order.

    Args:
        default_carrier: Default carrier to use when missing.
        default_bus: Default bus to use when missing.
    """
    default_carrier: str
    default_bus: str
    unit_order: List[str] = field(default_factory=list)
    _unit_seen: Set[str] = field(default_factory=set, init=False, repr=False)

    tech: Dict[str, str] = field(default_factory=dict)
    system: Dict[str, str] = field(default_factory=dict)
    region: Dict[str, str] = field(default_factory=dict)
    carrier_in: Dict[str, str] = field(default_factory=dict)
    carrier_out: Dict[str, str] = field(default_factory=dict)
    bus_in: Dict[str, str] = field(default_factory=dict)
    bus_out: Dict[str, str] = field(default_factory=dict)

    e_nom: Dict[str, float] = field(default_factory=dict)
    e_min: Dict[str, float] = field(default_factory=dict)
    e_nom_max: Dict[str, float] = field(default_factory=dict)

    p_charge_nom: Dict[str, float] = field(default_factory=dict)
    p_charge_nom_max: Dict[str, float] = field(default_factory=dict)
    p_discharge_nom: Dict[str, float] = field(default_factory=dict)
    p_discharge_nom_max: Dict[str, float] = field(default_factory=dict)
    duration_charge: Dict[str, float] = field(default_factory=dict)
    duration_discharge: Dict[str, float] = field(default_factory=dict)

    efficiency_charge: Dict[str, float] = field(default_factory=dict)
    efficiency_discharge: Dict[str, float] = field(default_factory=dict)
    standby_loss: Dict[str, float] = field(default_factory=dict)

    capital_cost_energy: Dict[str, float] = field(default_factory=dict)
    capital_cost_power_charge: Dict[str, float] = field(default_factory=dict)
    capital_cost_power_discharge: Dict[str, float] = field(default_factory=dict)
    lifetime: Dict[str, int] = field(default_factory=dict)
    spillage_cost: Dict[str, float] = field(default_factory=dict)

    def _track_unit(self, name: str) -> None:
        if name not in self._unit_seen:
            self._unit_seen.add(name)
            self.unit_order.append(name)

    def add_record(self, **kwargs: Any) -> None:
        """Add or update a single unit record.

        Args:
            **kwargs: Field values keyed by storage attribute name.
        """
        # Assign per-unit fields directly to dicts (last write wins).
        name = str(kwargs.get("unit", ""))
        if not name:
            return
        self._track_unit(name)
        self.tech[name] = str(kwargs.get("tech", ""))
        if "system" in kwargs:
            self.system[name] = str(kwargs.get("system", ""))
        if "region" in kwargs:
            self.region[name] = str(kwargs.get("region", ""))
        self.carrier_in[name] = str(kwargs.get("carrier_in", self.default_carrier))
        self.carrier_out[name] = str(kwargs.get("carrier_out", self.default_carrier))
        self.bus_in[name] = str(kwargs.get("bus_in", self.default_bus))
        self.bus_out[name] = str(kwargs.get("bus_out", self.default_bus))

        self.e_nom[name] = float(kwargs.get("e_nom", 0.0))
        self.e_min[name] = float(kwargs.get("e_min", 0.0))
        self.e_nom_max[name] = float(kwargs.get("e_nom_max", self.e_nom[name]))

        self.p_charge_nom[name] = float(kwargs.get("p_charge_nom", 0.0))
        self.p_charge_nom_max[name] = float(kwargs.get("p_charge_nom_max", self.p_charge_nom[name]))
        self.p_discharge_nom[name] = float(kwargs.get("p_discharge_nom", 0.0))
        self.p_discharge_nom_max[name] = float(kwargs.get("p_discharge_nom_max", self.p_discharge_nom[name]))

        if kwargs.get("duration_charge") is not None:
            self.duration_charge[name] = float(kwargs["duration_charge"])
        if kwargs.get("duration_discharge") is not None:
            self.duration_discharge[name] = float(kwargs["duration_discharge"])

        self.efficiency_charge[name] = float(kwargs.get("efficiency_charge", 1.0))
        self.efficiency_discharge[name] = float(kwargs.get("efficiency_discharge", 1.0))
        self.standby_loss[name] = float(kwargs.get("standby_loss", 0.0))

        self.capital_cost_energy[name] = float(kwargs.get("capital_cost_energy", 0.0))
        self.capital_cost_power_charge[name] = float(kwargs.get("capital_cost_power_charge", 0.0))
        self.capital_cost_power_discharge[name] = float(kwargs.get("capital_cost_power_discharge", 0.0))
        self.lifetime[name] = int(kwargs.get("lifetime", 0))
        self.spillage_cost[name] = float(kwargs.get("spillage_cost", 0.0))

def collect_units_from_storage(ex: StorageUnits) -> set[str]:
    """Collect all unit names referenced by a StorageUnits object.

    Args:
        ex: Existing StorageUnits object.

    Returns:
        Set of unit names referenced by any storage field or tuple-key mapping.
    """
    units = {str(u) for u in getattr(ex, "unit", [])}
    per_unit_attrs = [
        "tech",
        "system",
        "region",
        "carrier_in",
        "carrier_out",
        "bus_in",
        "bus_out",
        "e_nom",
        "e_min",
        "e_nom_max",
        "p_charge_nom",
        "p_charge_nom_max",
        "p_discharge_nom",
        "p_discharge_nom_max",
        "duration_charge",
        "duration_discharge",
        "efficiency_charge",
        "efficiency_discharge",
        "standby_loss",
        "capital_cost_energy",
        "capital_cost_power_charge",
        "capital_cost_power_discharge",
        "lifetime",
        "spillage_cost",
    ]
    for attr in per_unit_attrs:
        data = getattr(ex, attr, None)
        if isinstance(data, dict):
            units.update(str(k) for k in data.keys())
    for attr in ("e_nom_inv_cost", "inflow"):
        data = getattr(ex, attr, None)
        if isinstance(data, dict):
            for key in data.keys():
                if isinstance(key, tuple) and key:
                    units.add(str(key[0]))
    return units


def assert_unit_key_subset(
    units: Iterable[str],
    dicts: Iterable[Tuple[str, Dict[str, Any]]],
    tuple_dicts: Iterable[Tuple[str, Dict[Tuple[Any, ...], Any]]],
) -> None:
    """Assert that all dict keys reference known units.

    Args:
        units: Iterable of valid unit names.
        dicts: Sequence of (name, dict) pairs keyed by unit name.
        tuple_dicts: Sequence of (name, dict) pairs keyed by tuple with unit first.

    Raises:
        AssertionError: If any dict contains unknown unit keys. 
    """
    unit_set = {str(u) for u in units}

    for name, data in dicts:
        if not data:
            continue
        extra = {str(k) for k in data.keys()} - unit_set
        assert not extra, f"{name} has unknown units: {sorted(extra)}"

    for name, data in tuple_dicts:
        if not data:
            continue
        extra = {
            str(k[0]) for k in data.keys() if isinstance(k, tuple) and k
        } - unit_set
        assert not extra, f"{name} has unknown units: {sorted(extra)}"
