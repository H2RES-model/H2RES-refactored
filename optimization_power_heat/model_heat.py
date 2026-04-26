"""Heat sector — LP model (variables, constraints, cost).

Same split philosophy as :mod:`model_power`: this file only writes the
math of the heat sector. It does not solve anything. The coupled
runner in :mod:`model_coupled` combines this sector with the power
sector into one Gurobi model and a single objective.

==============================================================
What the heat model says, in words
==============================================================

For every heat bus (housing, District Heating, industry, ...), every
hour, every year:

    Heat from boilers (fuel / H2 / e-fuel, cost exogenous)
  + Heat from electric converters (heat pumps, e-boilers)
  + Heat released by CHP units (= chp_p2h · their electricity output)
  + Heat from TES (discharge − charge)
  + Unserved heat
  = Heat demand

Plus:

  - Every heat unit's output is capped by (installed + new MW).
  - TES follows a state-of-charge balance with charge/discharge
    efficiencies and a standby loss.
  - TES charge/discharge power is derived from energy capacity via
    duration ratios (same pattern as in the power sector).

Coupling to the power sector
----------------------------
Heat pumps and electric boilers CONSUME electricity. For each such
unit::

    p_elec_in[u, t, y]  =  q_out[u, t, y]  /  COP[u, t, y]

These p_elec_in expressions are summed per electricity bus into::

    p_elec_load[(elec_bus, t, y)]

and handed back to the power sector so that its power balance sees
the heat-sector's extra load. CHP heat works the other way: the power
sector's ``p_out[u, t, y]`` is multiplied by ``chp_p2h[u]`` and shows
up as heat supply at the CHP's heat bus.

Fuel pathway assumption
-----------------------
For boilers running on fuel, H2 or e-fuel the cost is picked up from
``gens.var_cost`` (or ``var_cost_no_fuel``). We do NOT yet model the
upstream chain that produces the H2 / e-fuel — that would be a future
extension. So these units simply pay an exogenous €/MWh_heat.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional, Tuple, Union

import gurobipy as gp
from gurobipy import GRB, quicksum

from optimization_2.data_prep_heat import (
    B, BTY, HeatData, P, U, UTY, Yr,
)
from optimization_2.model_power import annuity_factor


# -------------------------------------------------------------------
@dataclass
class HeatResults:
    status: int = 0
    heat_gen: Dict[UTY, float] = field(default_factory=dict)         # q_out    [MW_heat]
    tes_charge: Dict[UTY, float] = field(default_factory=dict)       # q_ch     [MW_heat]
    tes_discharge: Dict[UTY, float] = field(default_factory=dict)    # q_dis    [MW_heat]
    tes_soc: Dict[UTY, float] = field(default_factory=dict)          # MWh_heat
    unserved_heat: Dict[BTY, float] = field(default_factory=dict)
    heat_price: Dict[BTY, float] = field(default_factory=dict)       # €/MWh_heat
    heat_capacity_added: Dict[Tuple[U, Yr], float] = field(default_factory=dict)
    tes_energy_added: Dict[Tuple[U, Yr], float] = field(default_factory=dict)
    total_capex_eur: float = 0.0
    capex_by_tech: Dict[str, float] = field(default_factory=dict)
    elec_consumption: Dict[UTY, float] = field(default_factory=dict)


# ===================================================================
# HeatSector — builder class
# ===================================================================
class HeatSector:
    """Adds the heat-sector variables, constraints and cost onto a
    shared Gurobi model.

    If the system has no heat layer at all (empty HeatData), both
    :meth:`add_variables` and :meth:`add_constraints` are no-ops and
    the cost expression is simply 0.
    """

    def __init__(self, data: HeatData) -> None:
        self.data = data

        # Variables
        self.q_out: Dict[UTY, gp.Var] = {}
        self.q_ch: Dict[UTY, gp.Var] = {}
        self.q_dis: Dict[UTY, gp.Var] = {}
        self.soc_h: Dict[UTY, gp.Var] = {}
        self.ens_h: Dict[BTY, gp.Var] = {}
        self.cap_Gh: Dict[Tuple[U, Yr], gp.Var] = {}
        self.cap_Eh: Dict[Tuple[U, Yr], gp.Var] = {}

        # Electricity consumption of e→h converters, built on-the-fly
        # as linear expressions (no variable; it's q_out / COP).
        self.p_elec_in: Dict[UTY, gp.LinExpr] = {}
        self.p_elec_load: Dict[BTY, gp.LinExpr] = {}

        # Constraints & cost
        self.balance: Dict[BTY, gp.Constr] = {}
        self.cost_expr: Optional[gp.LinExpr] = 0.0  # type: ignore[assignment]
        self.capex_expr: Optional[gp.LinExpr] = None

    # ==============================================================
    # Step 1 — variables
    # ==============================================================
    def add_variables(self, model: gp.Model) -> None:
        """Create every heat-sector variable on ``model``."""
        d = self.data
        if d.is_empty:
            return

        periods, years = d.periods, d.years

        # ---- Upper bounds ------------------------------------------
        q_out_ub = {
            (u, t, y): d.p_nom_max[u]
            for u in d.G_h for t in periods for y in years
        }
        pc_ub = {
            s: max(d.pc_cap[s], d.e_nom_max[s] / d.dur_ch[s]) for s in d.S_h
        }
        pd_ub = {
            s: max(d.pd_cap[s], d.e_nom_max[s] / d.dur_dis[s]) for s in d.S_h
        }

        # ---- Variables ---------------------------------------------
        self.q_out = dict(model.addVars(
            [(u, t, y) for u in d.G_h for t in periods for y in years],
            lb=0.0, ub=q_out_ub, name="q_out",
        ))
        self.q_ch = dict(model.addVars(
            [(s, t, y) for s in d.S_h for t in periods for y in years],
            lb=0.0,
            ub={(s, t, y): pc_ub[s] for s in d.S_h for t in periods for y in years},
            name="q_ch",
        ))
        self.q_dis = dict(model.addVars(
            [(s, t, y) for s in d.S_h for t in periods for y in years],
            lb=0.0,
            ub={(s, t, y): pd_ub[s] for s in d.S_h for t in periods for y in years},
            name="q_dis",
        ))
        self.soc_h = dict(model.addVars(
            [(s, t, y) for s in d.S_h for t in periods for y in years],
            lb=0.0,
            ub={(s, t, y): d.e_nom_max[s]
                for s in d.S_h for t in periods for y in years},
            name="soc_h",
        ))
        self.ens_h = dict(model.addVars(
            [(b, t, y) for b in d.heat_buses for t in periods for y in years],
            lb=0.0, name="ens_h",
        ))

        # ---- Investment variables ----------------------------------
        for u in d.G_h:
            ub = max(0.0, d.p_nom_max[u] - d.p_nom[u])
            for y in years:
                self.cap_Gh[(u, y)] = model.addVar(
                    lb=0.0, ub=ub, name=f"cap_Gh[{u},{y}]"
                )
        for s in d.S_h:
            ub = max(0.0, d.e_nom_max[s] - d.e_nom[s])
            for y in years:
                self.cap_Eh[(s, y)] = model.addVar(
                    lb=0.0, ub=ub, name=f"cap_Eh[{s},{y}]"
                )

        # ---- Electricity consumption of e→h converters -------------
        # For a heat pump or electric boiler, the electricity drawn
        # at (t, y) equals heat produced divided by COP/efficiency.
        # We build these as linear expressions so the power sector
        # can add them into its balance without any new variables.
        for u in d.G_h_elec:
            for y in years:
                for t in periods:
                    cop = d.eff_at(u, t, y)
                    if cop <= 0:
                        cop = d.default_cop
                    self.p_elec_in[(u, t, y)] = self.q_out[(u, t, y)] * (1.0 / cop)

        # Aggregate per electricity bus, so the power balance can
        # look up "how much extra load at this bus at this hour".
        for eb, units in d.elec_consumers_at.items():
            for y in years:
                for t in periods:
                    total = quicksum(self.p_elec_in[(u, t, y)] for u in units)
                    self.p_elec_load[(eb, t, y)] = total

        model.update()

    # ==============================================================
    # Step 2 — constraints + cost
    # ==============================================================
    def add_constraints(
        self,
        model: gp.Model,
        *,
        power_p_out: Optional[Mapping[UTY, Union[gp.Var, gp.LinExpr]]] = None,
    ) -> None:
        """Add every heat-sector constraint and build the cost term.

        Args:
            model:       the shared Gurobi model.
            power_p_out: the power sector's electricity-output
                         variables. CHP units draw from this dict so
                         their heat byproduct appears on the heat
                         balance. When the power sector is missing or
                         empty, CHP contribution is just zero.
        """
        d = self.data
        if d.is_empty:
            self.cost_expr = 0.0  # type: ignore[assignment]
            return

        periods, years = d.periods, d.years
        dt = d.dt
        power_p_out = power_p_out or {}

        # ==========================================================
        # (H1) Heat balance at every heat bus and hour.
        #
        #   boilers + electric converters + TES (dis − ch)
        #           + CHP heat byproduct + unserved = demand
        # ==========================================================
        for b in d.heat_buses:
            for y in years:
                for t in periods:
                    supply = (
                          quicksum(self.q_out[u, t, y] for u in d.gens_at.get(b, []))
                        + quicksum(
                              self.q_dis[s, t, y] - self.q_ch[s, t, y]
                              for s in d.sto_at.get(b, [])
                          )
                        + self.ens_h[b, t, y]
                    )
                    # CHP heat = p2h_ratio · power_output, only if the
                    # power sector handed in its p_out dict.
                    chp_term: Union[float, gp.LinExpr] = 0.0
                    for u in d.chp_at.get(b, []):
                        p2h = d.chp_p2h.get(u, 0.0)
                        if p2h <= 0:
                            continue
                        pv = power_p_out.get((u, t, y))
                        if pv is None:
                            continue
                        chp_term = chp_term + p2h * pv

                    self.balance[(b, t, y)] = model.addConstr(
                        supply + chp_term == d.demand.get((b, t, y), 0.0),
                        name=f"heat_balance[{b},{t},{y}]",
                    )

        # ==========================================================
        # (H2) Heat-unit capacity.
        #     q_out ≤ existing MW + new MW
        # ==========================================================
        for u in d.G_h:
            for y in years:
                cap_total = d.p_nom[u] + self.cap_Gh[(u, y)]
                for t in periods:
                    model.addConstr(
                        self.q_out[u, t, y] <= cap_total,
                        name=f"cap_hgen[{u},{t},{y}]",
                    )

        # ==========================================================
        # (H3) TES capacity coupling.
        # ==========================================================
        for s in d.S_h:
            for y in years:
                e_total = d.e_nom[s] + self.cap_Eh[(s, y)]
                pc_lim = e_total / d.dur_ch[s]
                pd_lim = e_total / d.dur_dis[s]
                for t in periods:
                    model.addConstr(
                        self.soc_h[s, t, y] <= e_total,
                        name=f"soch_cap[{s},{t},{y}]",
                    )
                    model.addConstr(
                        self.q_ch[s, t, y] <= pc_lim,
                        name=f"qch_cap[{s},{t},{y}]",
                    )
                    model.addConstr(
                        self.q_dis[s, t, y] <= pd_lim,
                        name=f"qdis_cap[{s},{t},{y}]",
                    )

        # ==========================================================
        # (H4) TES state-of-charge balance (cyclic).
        # ==========================================================
        for s in d.S_h:
            for y in years:
                soc0 = 0.5 * (d.e_nom[s] + self.cap_Eh[(s, y)])
                for i, t in enumerate(periods):
                    soc_prev = soc0 if i == 0 else self.soc_h[s, periods[i - 1], y]
                    model.addConstr(
                        self.soc_h[s, t, y]
                        == (1.0 - d.loss[s]) * soc_prev
                        +  d.eta_ch[s]  * self.q_ch[s, t, y]  * dt
                        -  self.q_dis[s, t, y] / d.eta_dis[s] * dt,
                        name=f"soch_bal[{s},{t},{y}]",
                    )
                model.addConstr(
                    self.soc_h[s, periods[-1], y] == soc0,
                    name=f"soch_cyc[{s},{y}]",
                )

        # ==========================================================
        # Cost expression
        # ==========================================================
        # Variable cost (fuel + O&M) per MWh of heat produced. For
        # electricity converters, the electricity itself is paid for
        # inside the power sector's cost — we do NOT double-count it
        # here; their q_out only pays the (usually small) non-fuel
        # O&M cost.
        heat_var_cost = quicksum(
            d.var_cost_at(u, t, y) * self.q_out[u, t, y] * dt
            for u in d.G_h for t in periods for y in years
        )
        heat_ens_cost = quicksum(
            d.voll_heat * self.ens_h[b, t, y] * dt
            for b in d.heat_buses for t in periods for y in years
        )

        capex_scale = (
            (len(periods) * dt) / 8760.0
            if d.capex_scale is None
            else d.capex_scale
        )
        heat_gen_capex = quicksum(
            annuity_factor(d.discount_rate, d.gen_life[u])
            * d.gen_capex[u] * self.cap_Gh[(u, y)]
            for u in d.G_h for y in years
            if d.gen_capex[u] > 0
        )
        tes_capex = quicksum(
            annuity_factor(d.discount_rate, d.sto_life[s])
            * (
                d.cap_cost_E[s]
                + d.cap_cost_Pch[s]  / d.dur_ch[s]
                + d.cap_cost_Pdis[s] / d.dur_dis[s]
            )
            * self.cap_Eh[(s, y)]
            for s in d.S_h for y in years
            if (d.cap_cost_E[s] + d.cap_cost_Pch[s] + d.cap_cost_Pdis[s]) > 0
        )
        self.capex_expr = (heat_gen_capex + tes_capex) * capex_scale
        self.cost_expr = heat_var_cost + heat_ens_cost + self.capex_expr

    # ==============================================================
    # After solve — read results
    # ==============================================================
    def read_results(self, model: gp.Model) -> HeatResults:
        r = HeatResults(status=model.status)
        if self.data.is_empty:
            return r
        if model.status != GRB.OPTIMAL:
            return r

        r.heat_gen = {k: float(v.X) for k, v in self.q_out.items()}
        r.tes_charge = {k: float(v.X) for k, v in self.q_ch.items()}
        r.tes_discharge = {k: float(v.X) for k, v in self.q_dis.items()}
        r.tes_soc = {k: float(v.X) for k, v in self.soc_h.items()}
        r.unserved_heat = {k: float(v.X) for k, v in self.ens_h.items()}
        try:
            r.heat_price = {k: float(c.Pi) for k, c in self.balance.items()}
        except (AttributeError, gp.GurobiError):
            r.heat_price = {}
        r.heat_capacity_added = {k: float(v.X) for k, v in self.cap_Gh.items()}
        r.tes_energy_added = {k: float(v.X) for k, v in self.cap_Eh.items()}
        if self.capex_expr is not None:
            try:
                r.total_capex_eur = float(self.capex_expr.getValue())
            except (AttributeError, gp.GurobiError):
                r.total_capex_eur = 0.0
        r.capex_by_tech = self._capex_by_tech(r)
        # Numeric electricity consumption of e→h converters.
        for (u, t, y), expr in self.p_elec_in.items():
            try:
                r.elec_consumption[(u, t, y)] = float(expr.getValue())
            except (AttributeError, gp.GurobiError):
                pass
        return r

    def _capex_by_tech(self, r: HeatResults) -> Dict[str, float]:
        d = self.data
        scale = (
            (len(d.periods) * d.dt) / 8760.0
            if d.capex_scale is None
            else d.capex_scale
        )
        gens, sto = d.system.generators, d.system.storage
        by_tech: Dict[str, float] = {}
        for (u, _y), mw in r.heat_capacity_added.items():
            if mw <= 0:
                continue
            tech = str(gens.tech.get(u, "heat_gen"))
            crf = annuity_factor(d.discount_rate, d.gen_life[u])
            by_tech[tech] = by_tech.get(tech, 0.0) + crf * d.gen_capex[u] * mw * scale
        for (s, _y), mwh in r.tes_energy_added.items():
            if mwh <= 0:
                continue
            tech = str(sto.tech.get(s, "TES"))
            crf = annuity_factor(d.discount_rate, d.sto_life[s])
            per_mwh = (
                d.cap_cost_E[s]
                + d.cap_cost_Pch[s]  / d.dur_ch[s]
                + d.cap_cost_Pdis[s] / d.dur_dis[s]
            )
            by_tech[tech] = by_tech.get(tech, 0.0) + crf * per_mwh * mwh * scale
        return by_tech
