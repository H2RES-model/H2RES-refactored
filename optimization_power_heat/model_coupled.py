"""Coupled power + heat model — solves both sectors together.

Power and heat are written in two separate files (:mod:`model_power`
and :mod:`model_heat`) to keep each sector readable. They are NEVER
solved on their own — this module is the only place where the Gurobi
model is actually created, combined and optimised.

==============================================================
How the two sectors talk to each other
==============================================================

The two sectors are coupled in two places:

1. **Electricity → heat.** Heat pumps and electric boilers draw
   electricity. For each such unit the heat sector builds a tiny
   linear expression ``p_elec_in[u,t,y] = q_out / COP``. These are
   summed per electricity bus into ``p_elec_load[(bus, t, y)]`` and
   handed to the power sector so its power balance subtracts them
   from the available supply.

2. **CHP heat byproduct.** CHP units are modelled as electricity
   generators in the power sector. Their heat output equals
   ``chp_p2h · p_out``. The heat sector reads the power sector's
   ``p_out`` variables and adds that term on the supply side of the
   heat balance.

Because each side needs the other's variables, we build in two phases
so the variables exist before any constraint references them::

    ┌──────────────── gp.Model (shared) ─────────────────┐
    │                                                    │
    │  1. power.add_variables(model)                     │
    │       → p_out, p_ch, p_dis, soc, cap_G, cap_E      │
    │                                                    │
    │  2. heat.add_variables(model)                      │
    │       → q_out, q_ch, q_dis, soc_h, cap_Gh, cap_Eh  │
    │       → also builds the p_elec_load LinExprs       │
    │                                                    │
    │  3. heat.add_constraints(model,                    │
    │         power_p_out=power.p_out)                   │
    │       → heat balance sees the CHP heat byproduct   │
    │                                                    │
    │  4. power.add_constraints(model,                   │
    │         extra_load=heat.p_elec_load)               │
    │       → power balance sees the heat-pump load      │
    │                                                    │
    │  5. setObjective(power.cost_expr + heat.cost_expr) │
    │                                                    │
    └────────────────────────────────────────────────────┘

No tricks (no ``chgCoeff`` on existing constraints, no placeholder
variables): each sector just references the other's variables by
name.

If the system has no heat layer at all (``heat_data.is_empty``), the
heat sector is a no-op: it creates no variables, adds no
constraints, and contributes 0 to the cost.

Usage
-----
>>> pdata  = PowerData.from_system(system, years=[2020], periods=list(range(1, 25)))
>>> hdata  = HeatData .from_system(system, years=[2020], periods=list(range(1, 25)))
>>> cmodel = CoupledModel(pdata, hdata)
>>> cmodel.build()
>>> result = cmodel.solve(OutputFlag=0)
>>> print(cmodel.summary(result))
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

import gurobipy as gp
from gurobipy import GRB

from optimization_2.data_prep_heat import HeatData
from optimization_2.data_prep_power import PowerData
from optimization_2.model_heat import HeatResults, HeatSector
from optimization_2.model_power import PowerResults, PowerSector


# -------------------------------------------------------------------
# Result bundle handed back by solve()
# -------------------------------------------------------------------
@dataclass
class CoupledResults:
    """Everything the coupled model can tell you after a solve."""
    status: int = 0
    objective: Optional[float] = None
    power: PowerResults = field(default_factory=PowerResults)
    heat: HeatResults = field(default_factory=HeatResults)


# ===================================================================
# CoupledModel
# ===================================================================
class CoupledModel:
    """Build and solve the power + heat model as one LP.

    The two sector classes (:class:`PowerSector`, :class:`HeatSector`)
    contribute variables, constraints and a cost expression; this
    class just orchestrates them on a single Gurobi model.
    """

    def __init__(
        self,
        power_data: PowerData,
        heat_data: HeatData,
        *,
        model_name: str = "H2RES_Coupled",
    ) -> None:
        self.power_data = power_data
        self.heat_data = heat_data
        self.model_name = model_name

        # Populated by build()
        self.model: Optional[gp.Model] = None
        self.power: Optional[PowerSector] = None
        self.heat: Optional[HeatSector] = None

    # ===============================================================
    # build() — create variables + constraints + objective
    # ===============================================================
    def build(self) -> gp.Model:
        """Assemble the whole LP on one Gurobi model.

        Follows the two-phase pattern described at the top of the
        file: first both sectors add their variables, then both
        sectors add their constraints with the other sector's
        variables already in scope.
        """
        shared = gp.Model(self.model_name)
        self.model = shared

        # ---- sector instances ------------------------------------
        self.power = PowerSector(self.power_data)
        self.heat = HeatSector(self.heat_data)

        # ---- phase 1: variables ----------------------------------
        # Creating variables first means that when we write the
        # constraints below, both p_out (power) and p_elec_load
        # (heat) already exist on the model.
        self.power.add_variables(shared)
        self.heat.add_variables(shared)

        # ---- phase 2: constraints --------------------------------
        # Heat balance first so it can reference the CHP units'
        # electricity output from the power sector.
        self.heat.add_constraints(shared, power_p_out=self.power.p_out)

        # Power balance last so it can subtract the heat sector's
        # electricity consumption (heat pumps, e-boilers).
        self.power.add_constraints(shared, extra_load=self.heat.p_elec_load)

        # ---- single combined objective ---------------------------
        cost = self.power.cost_expr
        if self.heat.cost_expr is not None:
            cost = cost + self.heat.cost_expr  # type: ignore[operator]
        shared.setObjective(cost, GRB.MINIMIZE)
        shared.update()
        return shared

    # ===============================================================
    # solve() — optimise and collect results
    # ===============================================================
    def solve(self, **gurobi_params) -> CoupledResults:
        """Run Gurobi and return a :class:`CoupledResults` bundle.

        Any keyword arguments are forwarded to ``model.setParam``:
            >>> cmodel.solve(OutputFlag=0, TimeLimit=60)
        """
        if self.model is None:
            self.build()
        assert self.model is not None
        assert self.power is not None
        assert self.heat is not None

        for k, v in gurobi_params.items():
            self.model.setParam(k, v)

        self.model.optimize()

        r = CoupledResults(status=self.model.status)
        if self.model.status == GRB.OPTIMAL:
            r.objective = float(self.model.objVal)

        # Each sector knows how to decode its own variables.
        r.power = self.power.read_results(self.model)
        r.heat = self.heat.read_results(self.model)
        return r

    # ===============================================================
    # summary() — quick human-friendly KPIs
    # ===============================================================
    def summary(self, r: CoupledResults) -> Dict[str, float]:
        """Return a small dict of headline numbers.

        Handy for printing after a solve. Keys are prefixed with
        ``power_`` or ``heat_`` so the two sectors don't clash.
        """
        assert self.power is not None
        assert self.heat is not None

        out: Dict[str, float] = {
            "coupled_total_cost_eur": r.objective or 0.0,
            "power_total_capex_eur":  r.power.total_capex_eur,
            "heat_total_capex_eur":   r.heat.total_capex_eur,
        }

        # Total generation (MWh) — handy rough number.
        d_p = self.power_data
        out["power_total_generation_mwh"] = sum(
            v for v in r.power.generation.values()
        ) * d_p.dt
        out["power_total_unserved_mwh"] = sum(
            v for v in r.power.unserved.values()
        ) * d_p.dt

        if not self.heat_data.is_empty:
            d_h = self.heat_data
            out["heat_total_generation_mwh"] = sum(
                v for v in r.heat.heat_gen.values()
            ) * d_h.dt
            out["heat_total_unserved_mwh"] = sum(
                v for v in r.heat.unserved_heat.values()
            ) * d_h.dt
            out["heat_total_elec_consumption_mwh"] = sum(
                v for v in r.heat.elec_consumption.values()
            ) * d_h.dt
        return out
