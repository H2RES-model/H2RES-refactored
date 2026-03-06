"""Shared default resolution helpers for loaders."""

from __future__ import annotations

from typing import Optional

from data_models.Bus import Bus
from data_models.SystemSets import SystemSets


def default_carrier(sets: SystemSets, fallback: str = "electricity") -> str:
    """Return the default carrier for a system. 

    Args:
        sets: SystemSets containing available carriers.
        fallback: Carrier to use when no carriers are defined.

    Returns:
        Default carrier name.
    """
    return sets.carriers[0] if getattr(sets, "carriers", None) else fallback


def default_electric_bus(
    sets: SystemSets, buses: Optional[Bus] = None, fallback: str = "SystemBus"
) -> str:
    """Return an electricity bus if available, else a default bus.

    Args:
        sets: SystemSets containing available buses.
        buses: Optional Bus model to search for an electricity carrier.
        fallback: Bus name to use when no buses are defined.

    Returns:
        Bus identifier for electricity or the fallback.
    """
    if buses is not None:
        carrier_series = getattr(buses, "carrier", None)
        if carrier_series is not None:
            for b, c in carrier_series.items():
                if str(c).lower() == "electricity":
                    return str(b)
    return sets.buses[0] if getattr(sets, "buses", None) else fallback
