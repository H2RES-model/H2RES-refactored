"""Optimization package — Gurobi models for the H2RES power sector.

This package currently ships a single model: the explicit (non-matrix)
electricity dispatch LP. It is intentionally written with plain
Python loops over ``(unit, period, year)`` tuples so that every
equation in the formulation maps one-to-one to a ``m.addConstr(...)``
call — much easier to read, debug and extend than a sparse-matrix
construction.
"""

from optimization.dispatch_electricity import (
    DispatchResults,
    ElectricityDispatchModel,
)

__all__ = [
    "DispatchResults",
    "ElectricityDispatchModel",
]
