"""Power sector — LP model (variables, constraints, cost).

This file contains ONLY the maths of the power sector. It does not
read CSVs (that's :mod:`data_prep_power`), and it does not solve the
model on its own (that's :mod:`model_coupled`). Its job is to ADD the
power-sector variables and constraints onto a shared Gurobi model,
and to expose a cost expression that the coupled runner will combine
with the heat-sector cost into a single objective.

How sector files fit together
-----------------------------
::

    data_prep_power.py  ──►  PowerSector  ──┐
                                            ├── CoupledModel.build() + solve()
    data_prep_heat.py   ──►  HeatSector  ───┘

Usage (by the coupled runner — see :mod:`model_coupled`):

    1. create a shared ``gp.Model``,
    2. call :meth:`PowerSector.add_variables(model)`,
    3. call :meth:`HeatSector.add_variables(model)`,
    4. call :meth:`HeatSector.add_constraints(model, power_p_out=power.p_out)`,
    5. call :meth:`PowerSector.add_constraints(model, extra_load=heat.p_elec_load)`,
    6. ``setObjective(power.cost_expr + heat.cost_expr)``.

Splitting the build into "add_variables" and "add_constraints" lets
each sector see the other's variables BEFORE its own constraints are
written. That's how the power balance can subtract the heat sector's
electricity consumption (step 5) and the heat balance can add the CHP
units' heat output (step 4).

==============================================================
What the model says, in words
==============================================================

For every electricity bus, every hour, every year:

    Power produced
    + Power from storage (discharged)
    − Power into storage (charging)
    − Power drawn by heat-sector converters (heat pumps, e-boilers)
    + Unserved energy
    = Electricity demand

Plus:

    - Renewables (wind / solar / run-of-river) may produce up to
      ``(existing MW + new MW) × hourly availability``.
    - Thermal units (gas, coal, biomass, CHP) can ramp up or down
      only by a fraction of their capacity from one hour to the next.
    - Storage state-of-charge follows a physics-like balance with
      efficiency losses and standby leakage.
    - Hydro reservoirs get an exogenous water inflow; any overflow is
      "spilled" (at a penalty, if configured).
    - All capacity variables are capped by the data's ``*_nom_max``
      ceilings.

The cost we minimise adds up:

    - Variable cost (mostly fuel + O&M) × MWh of generation,
    - Unserved-energy penalty × MWh not delivered,
    - Hydro spillage penalty,
    - Annualised capex × MW / MWh of new capacity built (pro-rated
      to the slice of the year being simulated).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional, Tuple, Union

import gurobipy as gp
from gurobipy import GRB, quicksum

from optimization_2.data_prep_power import (
    B, BTY, P, PowerData, U, UTY, Yr,
)


# -------------------------------------------------------------------
# Capital Recovery Factor — spreads a lump-sum capex over a lifetime.
# Example: CRF(5%, 20 yr) ≈ 0.08, so 100 €/MW of capex costs 8 €/MW/yr.
# -------------------------------------------------------------------
def annuity_factor(discount_rate: float, lifetime: float) -> float:
    if lifetime <= 0:
        return 0.0
    if abs(discount_rate) < 1e-9:
        return 1.0 / lifetime
    f = (1.0 + discount_rate) ** lifetime
    return discount_rate * f / (f - 1.0)


# -------------------------------------------------------------------
# After solving, the coupled runner asks PowerSector for this bundle
# of number arrays — one entry per variable the model created.
# -------------------------------------------------------------------
@dataclass
class PowerResults:
    status: int = 0
    generation: Dict[UTY, float] = field(default_factory=dict)      # p_out    [MW]
    charge: Dict[UTY, float] = field(default_factory=dict)          # p_ch     [MW]
    discharge: Dict[UTY, float] = field(default_factory=dict)       # p_dis    [MW]
    soc: Dict[UTY, float] = field(default_factory=dict)             # SoC      [MWh]
    spill: Dict[UTY, float] = field(default_factory=dict)           # hydro    [MWh]
    unserved: Dict[BTY, float] = field(default_factory=dict)        # ENS      [MW]
    marginal_price: Dict[BTY, float] = field(default_factory=dict)  # €/MWh
    capacity_added: Dict[Tuple[U, Yr], float] = field(default_factory=dict)  # MW
    energy_added: Dict[Tuple[U, Yr], float] = field(default_factory=dict)    # MWh
    total_capex_eur: float = 0.0
    capex_by_tech: Dict[str, float] = field(default_factory=dict)


# ===================================================================
# PowerSector — builder class
# ===================================================================
class PowerSector:
    """Adds the power-sector variables, constraints and cost onto a
    shared Gurobi model. Does not solve anything on its own.

    Attributes populated after :meth:`add_variables`:
        p_out, p_ch, p_dis, soc, spill, ens, cap_G, cap_E

    Attributes populated after :meth:`add_constraints`:
        balance, cost_expr, capex_expr
    """

    def __init__(self, data: PowerData) -> None:
        self.data = data

        # Variables (filled by add_variables)
        self.p_out: Dict[UTY, gp.Var] = {}
        self.p_ch: Dict[UTY, gp.Var] = {}
        self.p_dis: Dict[UTY, gp.Var] = {}
        self.soc: Dict[UTY, gp.Var] = {}
        self.spill: Dict[UTY, gp.Var] = {}
        self.ens: Dict[BTY, gp.Var] = {}
        self.cap_G: Dict[Tuple[U, Yr], gp.Var] = {}  # new MW     per (unit, year)
        self.cap_E: Dict[Tuple[U, Yr], gp.Var] = {}  # new MWh    per (storage, year)

        # Constraints + expressions (filled by add_constraints)
        self.balance: Dict[BTY, gp.Constr] = {}
        self.cost_expr: Optional[gp.LinExpr] = None
        self.capex_expr: Optional[gp.LinExpr] = None

    # ==============================================================
    # Step 1 — create the decision variables
    # ==============================================================
    def add_variables(self, model: gp.Model) -> None:
        """Create all power-sector decision variables on ``model``.

        Upper bounds are derived from the data's investment ceilings,
        so Gurobi already knows: the most you can push a generator is
        ``p_nom_max``; the most you can install is ``p_nom_max − p_nom``.
        """
        d = self.data
        periods, years = d.periods, d.years
        S_charge = d.S_charge

        # ---- Generator output upper bound = the max buildable MW ----
        p_out_ub = {
            (u, t, y): d.p_nom_max[u]
            for u in d.G for t in periods for y in years
        }

        # ---- Storage charge/discharge ub = max of installed and ----
        # ---- what an investment up to e_nom_max could support.   ----
        pc_ub = {
            s: max(d.pc_cap_S.get(s, d.pc_cap_H.get(s, 0.0)),
                   d.e_nom_max[s] / d.dur_ch[s])
            for s in S_charge
        }
        # Plain hydro dams (HDAM) physically have no pump, so force 0.
        for s in d.H:
            if s not in d.hphs_set:
                pc_ub[s] = 0.0
        pd_ub = {
            s: max(d.pd_cap_S[s], d.e_nom_max[s] / d.dur_dis[s])
            for s in d.S
        }

        # ---- Variables -----------------------------------------------
        self.p_out = dict(model.addVars(
            [(u, t, y) for u in d.G for t in periods for y in years],
            lb=0.0, ub=p_out_ub, name="p_out",
        ))
        self.p_ch = dict(model.addVars(
            [(s, t, y) for s in S_charge for t in periods for y in years],
            lb=0.0,
            ub={(s, t, y): pc_ub[s]
                for s in S_charge for t in periods for y in years},
            name="p_ch",
        ))
        self.p_dis = dict(model.addVars(
            [(s, t, y) for s in d.S for t in periods for y in years],
            lb=0.0,
            ub={(s, t, y): pd_ub[s]
                for s in d.S for t in periods for y in years},
            name="p_dis",
        ))
        # State of charge. Lower bound is 0 (investment may start from
        # an empty installed base). Upper bound is the max buildable
        # energy capacity.
        self.soc = dict(model.addVars(
            [(s, t, y) for s in S_charge for t in periods for y in years],
            lb=0.0,
            ub={(s, t, y): d.e_nom_max[s]
                for s in S_charge for t in periods for y in years},
            name="soc",
        ))
        self.spill = dict(model.addVars(
            [(s, t, y) for s in d.H for t in periods for y in years],
            lb=0.0, name="spill",
        ))
        self.ens = dict(model.addVars(
            [(b, t, y) for b in d.elec_buses for t in periods for y in years],
            lb=0.0, name="ens",
        ))

        # ---- Investment variables -----------------------------------
        # cap_G[u,y]  = new MW of generator u to build in year y
        # cap_E[s,y]  = new MWh of storage  s to build in year y
        # Upper bound = headroom between the max ceiling and what's
        # already installed. If there's no headroom, the ub is 0 and
        # the variable contributes nothing.
        for u in d.G:
            ub = max(0.0, d.p_nom_max[u] - d.p_nom[u])
            for y in years:
                self.cap_G[(u, y)] = model.addVar(
                    lb=0.0, ub=ub, name=f"cap_G[{u},{y}]"
                )
        for s in S_charge:
            ub = max(0.0, d.e_nom_max[s] - d.e_nom[s])
            for y in years:
                self.cap_E[(s, y)] = model.addVar(
                    lb=0.0, ub=ub, name=f"cap_E[{s},{y}]"
                )

        model.update()

    # ==============================================================
    # Step 2 — add the constraints + build the cost expression
    # ==============================================================
    def add_constraints(
        self,
        model: gp.Model,
        *,
        extra_load: Optional[Mapping[BTY, Union[gp.Var, gp.LinExpr]]] = None,
    ) -> None:
        """Add every power-sector constraint and compute the cost.

        Args:
            model:      the shared Gurobi model.
            extra_load: electricity consumption coming from the heat
                        sector (heat pumps, e-boilers). Passed in by
                        the coupled runner as a dict
                        ``(bus, t, y) → LinExpr``.
        """
        d = self.data
        periods, years = d.periods, d.years
        dt = d.dt
        extra_load = extra_load or {}

        # ==========================================================
        # (1) Power balance at every electricity bus and hour.
        #
        #     generation + discharge − charging − heat-sector load
        #                + unserved  = demand
        # ==========================================================
        for b in d.elec_buses:
            for y in years:
                for t in periods:
                    lhs = (
                          quicksum(self.p_out[u, t, y] for u in d.gens_at.get(b, []))
                        + quicksum(self.p_dis[s, t, y] for s in d.S_dis_at.get(b, []))
                        - quicksum(self.p_ch[s, t, y] for s in d.S_ch_at.get(b, []))
                        - quicksum(self.p_ch[s, t, y] for s in d.H_ch_at.get(b, []))
                        + self.ens[b, t, y]
                    )
                    heat_sector_load = extra_load.get((b, t, y), 0.0)
                    self.balance[(b, t, y)] = model.addConstr(
                        lhs - heat_sector_load == d.demand.get((b, t, y), 0.0),
                        name=f"balance[{b},{t},{y}]",
                    )

        # ==========================================================
        # (2) Generator capacity / availability.
        #
        #     Renewables: p_out ≤ (p_nom + cap_G) × hourly profile
        #     Others:     p_out ≤ (p_nom + cap_G)
        # ==========================================================
        gens_p_t = d.system.generators.p_t
        G_res_set = set(d.G_res)
        for u in d.G:
            is_res = u in G_res_set
            for y in years:
                cap_total = d.p_nom[u] + self.cap_G[(u, y)]
                for t in periods:
                    profile = float(gens_p_t.get((u, t, y), 1.0)) if is_res else 1.0
                    model.addConstr(
                        self.p_out[u, t, y] <= cap_total * profile,
                        name=f"cap_gen[{u},{t},{y}]",
                    )

        # ==========================================================
        # (3) Ramping for thermal units (fossil, biomass, CHP).
        #
        #     Change in output between two consecutive hours can't
        #     exceed a fraction of (installed + new) capacity.
        # ==========================================================
        for u in d.G_ramp:
            ru, rd = d.ramp_up[u], d.ramp_dn[u]
            if ru <= 0 and rd <= 0:
                continue
            for y in years:
                cap_total = d.p_nom[u] + self.cap_G[(u, y)]
                for i in range(1, len(periods)):
                    t_prev, t = periods[i - 1], periods[i]
                    if ru > 0:
                        model.addConstr(
                            self.p_out[u, t, y] - self.p_out[u, t_prev, y]
                            <= ru * cap_total,
                            name=f"ramp_up[{u},{t},{y}]",
                        )
                    if rd > 0:
                        model.addConstr(
                            self.p_out[u, t_prev, y] - self.p_out[u, t, y]
                            <= rd * cap_total,
                            name=f"ramp_dn[{u},{t},{y}]",
                        )

        # ==========================================================
        # (4) Storage capacity coupling.
        #
        #     soc  ≤ existing MWh + new MWh
        #     p_ch ≤ (existing + new MWh) / duration_charge
        #     p_dis ≤ (existing + new MWh) / duration_discharge
        # ==========================================================
        hphs_set = d.hphs_set
        for s in d.S_charge:
            for y in years:
                e_total = d.e_nom[s] + self.cap_E[(s, y)]
                pc_limit = e_total / d.dur_ch[s]
                for t in periods:
                    model.addConstr(
                        self.soc[s, t, y] <= e_total,
                        name=f"soc_cap[{s},{t},{y}]",
                    )
                    # HDAM dams have no pump — charge is already pinned
                    # to 0 by the variable ub.
                    if s in d.H and s not in hphs_set:
                        continue
                    model.addConstr(
                        self.p_ch[s, t, y] <= pc_limit,
                        name=f"pch_cap[{s},{t},{y}]",
                    )
        for s in d.S:
            for y in years:
                pd_limit = (d.e_nom[s] + self.cap_E[(s, y)]) / d.dur_dis[s]
                for t in periods:
                    model.addConstr(
                        self.p_dis[s, t, y] <= pd_limit,
                        name=f"pdis_cap[{s},{t},{y}]",
                    )

        # ==========================================================
        # (5) Pure-storage state-of-charge balance.
        #
        #     soc[t] = (1 − standby_loss) · soc[t-1]
        #            + eta_ch · p_ch  · dt
        #            − p_dis  / eta_dis · dt
        #
        #     Initial SoC = half-full of (installed + new MWh).
        #     Terminal SoC is clamped back to the initial value so the
        #     storage budget is balanced over the slice.
        # ==========================================================
        for s in d.S:
            for y in years:
                soc0 = 0.5 * (d.e_nom[s] + self.cap_E[(s, y)])
                for i, t in enumerate(periods):
                    soc_prev = soc0 if i == 0 else self.soc[s, periods[i - 1], y]
                    model.addConstr(
                        self.soc[s, t, y]
                        == (1.0 - d.loss[s]) * soc_prev
                        +  d.eta_ch[s] * self.p_ch[s, t, y] * dt
                        -  self.p_dis[s, t, y] / d.eta_dis[s] * dt,
                        name=f"soc_bal[{s},{t},{y}]",
                    )
                model.addConstr(
                    self.soc[s, periods[-1], y] == soc0,
                    name=f"soc_cyc[{s},{y}]",
                )

        # ==========================================================
        # (6) Hydro reservoir balance (same as above + inflow − spill).
        #     Turbine output comes from p_out (not p_dis) because it's
        #     modelled as a generator in G, not a pure storage.
        # ==========================================================
        sto_inflow = d.system.storage.inflow
        for s in d.H:
            for y in years:
                soc0 = 0.5 * (d.e_nom[s] + self.cap_E[(s, y)])
                for i, t in enumerate(periods):
                    soc_prev = soc0 if i == 0 else self.soc[s, periods[i - 1], y]
                    inflow = float(sto_inflow.get((s, t, y), 0.0))
                    model.addConstr(
                        self.soc[s, t, y]
                        == (1.0 - d.loss[s]) * soc_prev
                        +  d.eta_ch[s] * self.p_ch[s, t, y] * dt
                        -  self.p_out[s, t, y] / d.eta_dis[s] * dt
                        +  inflow
                        -  self.spill[s, t, y],
                        name=f"res_bal[{s},{t},{y}]",
                    )
                model.addConstr(
                    self.soc[s, periods[-1], y] == soc0,
                    name=f"res_cyc[{s},{y}]",
                )

        # ==========================================================
        # (7) Cost expression — handed to the coupled runner which
        #     will add the heat-sector cost and set ONE objective.
        # ==========================================================
        gens_var_cost = d.system.generators.var_cost

        # Variable cost (€): fuel + O&M per MWh produced.
        gen_cost = quicksum(
            float(gens_var_cost.get((u, t, y), d.vc_no_fuel[u]))
            * self.p_out[u, t, y] * dt
            for u in d.G for t in periods for y in years
        )
        # Penalty for unserved electricity demand.
        ens_cost = quicksum(
            d.voll * self.ens[b, t, y] * dt
            for b in d.elec_buses for t in periods for y in years
        )
        # Penalty for hydro spillage (optional — 0 if not configured).
        spill_cost_expr = quicksum(
            d.spill_cost[s] * self.spill[s, t, y]
            for s in d.H for t in periods for y in years
            if d.spill_cost[s] > 0
        )

        # Annualised capex, pro-rated to the length of the slice so
        # that (say) a 24 h slice does not pay a full year's capex.
        capex_scale = (
            (len(periods) * dt) / 8760.0
            if d.capex_scale is None
            else d.capex_scale
        )
        gen_capex_expr = quicksum(
            annuity_factor(d.discount_rate, d.gen_life[u])
            * d.gen_capex[u] * self.cap_G[(u, y)]
            for u in d.G for y in years
            if d.gen_capex[u] > 0
        )
        sto_capex_expr = quicksum(
            annuity_factor(d.discount_rate, d.sto_life[s])
            * (
                d.cap_cost_E[s]
                + d.cap_cost_Pch[s]  / d.dur_ch[s]
                + d.cap_cost_Pdis[s] / d.dur_dis[s]
            )
            * self.cap_E[(s, y)]
            for s in d.S_charge for y in years
            if (d.cap_cost_E[s] + d.cap_cost_Pch[s] + d.cap_cost_Pdis[s]) > 0
        )
        self.capex_expr = (gen_capex_expr + sto_capex_expr) * capex_scale
        self.cost_expr = gen_cost + ens_cost + spill_cost_expr + self.capex_expr

    # ==============================================================
    # After solve — read the variable values into a PowerResults
    # ==============================================================
    def read_results(self, model: gp.Model) -> PowerResults:
        r = PowerResults(status=model.status)
        if model.status != GRB.OPTIMAL:
            return r
        r.generation = {k: float(v.X) for k, v in self.p_out.items()}
        r.charge = {k: float(v.X) for k, v in self.p_ch.items()}
        r.discharge = {k: float(v.X) for k, v in self.p_dis.items()}
        r.soc = {k: float(v.X) for k, v in self.soc.items()}
        r.spill = {k: float(v.X) for k, v in self.spill.items()}
        r.unserved = {k: float(v.X) for k, v in self.ens.items()}
        try:
            r.marginal_price = {k: float(c.Pi) for k, c in self.balance.items()}
        except (AttributeError, gp.GurobiError):
            r.marginal_price = {}
        r.capacity_added = {k: float(v.X) for k, v in self.cap_G.items()}
        r.energy_added = {k: float(v.X) for k, v in self.cap_E.items()}
        if self.capex_expr is not None:
            try:
                r.total_capex_eur = float(self.capex_expr.getValue())
            except (AttributeError, gp.GurobiError):
                r.total_capex_eur = 0.0
        r.capex_by_tech = self._capex_by_tech(r)
        return r

    def _capex_by_tech(self, r: PowerResults) -> Dict[str, float]:
        """Nice-to-have: group annualised capex by technology label."""
        d = self.data
        scale = (
            (len(d.periods) * d.dt) / 8760.0
            if d.capex_scale is None
            else d.capex_scale
        )
        gens, sto = d.system.generators, d.system.storage
        by_tech: Dict[str, float] = {}
        for (u, _y), mw in r.capacity_added.items():
            if mw <= 0:
                continue
            tech = str(gens.tech.get(u, "generator"))
            crf = annuity_factor(d.discount_rate, d.gen_life[u])
            by_tech[tech] = by_tech.get(tech, 0.0) + crf * d.gen_capex[u] * mw * scale
        for (s, _y), mwh in r.energy_added.items():
            if mwh <= 0:
                continue
            tech = str(sto.tech.get(s, "storage"))
            crf = annuity_factor(d.discount_rate, d.sto_life[s])
            per_mwh = (
                d.cap_cost_E[s]
                + d.cap_cost_Pch[s]  / d.dur_ch[s]
                + d.cap_cost_Pdis[s] / d.dur_dis[s]
            )
            by_tech[tech] = by_tech.get(tech, 0.0) + crf * per_mwh * mwh * scale
        return by_tech
