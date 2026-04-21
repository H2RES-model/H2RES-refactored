"""H2RES electricity dispatch — Gurobi LP (with optional investment).

A cost-minimising economic dispatch of the power sector, written as a
flat top-to-bottom build so the equations, variables and constraints
are easy to read.

When ``allow_investment=True``, the model also decides how much **new
generator capacity** and **new storage energy capacity** to build (one
decision per unit per year), bounded by the ``p_nom_max`` /
``e_nom_max`` columns in the data. The objective is augmented with an
annualised capital cost obtained from the unit's ``capital_cost``
columns, its ``lifetime`` and a configurable discount rate (the
standard Capital Recovery Factor formula). Capital cost is pro-rated
by ``capex_scale = NT·dt / 8760`` so it stays consistent with the
length of the optimised slice.

==============================================================
Mathematical formulation
==============================================================

Sets
    B              electricity buses
    T              periods (hours) in the time slice
    Y              years in the time slice
    G              generators producing electricity
      G_res  ⊆ G   RES / run-of-river units with an availability profile
      G_ramp ⊆ G   thermal units with ramp limits (fossil / biomass / CHP)
      G_hyd  ⊆ G   hydro turbines linked to a reservoir (HDAM + HPHS)
    S              pure storages (BESS, TES, H2 tanks, ...)
    H              hydro reservoirs (same names as G_hyd)
      H_pump ⊆ H   pumped hydro reservoirs with a real pump (HPHS)

Parameters
    p_nom[u]           MW         power capacity of unit u
    profile[u,t,y]     p.u.       RES availability factor (1 by default)
    ramp_up[u]         p.u.       max ramp-up rate (fraction of p_nom)
    ramp_dn[u]         p.u.       max ramp-down rate (fraction of p_nom)
    c[u,t,y]           €/MWh      generator variable cost (fuel + O&M)
    e_nom[s]           MWh        energy capacity
    e_min[s]           MWh        minimum SoC
    pc_cap[s]          MW         max charge power     (0 for HDAM)
    pd_cap[s]          MW         max discharge power  (only for S)
    eta_ch[s]          p.u.       charging efficiency
    eta_dis[s]         p.u.       discharging efficiency
    loss[s]            p.u.       standing loss per period
    inflow[s,t,y]      MWh/period exogenous hydro inflow
    gamma[s]           €/MWh      hydro spillage penalty
    demand[b,t,y]      MW         electricity demand at bus b
    VOLL               €/MWh      value of lost load
    dt                 h          length of one period

Decision variables (all ≥ 0)
    p_out[u,t,y]   MW    generator output                         u ∈ G
    p_ch[s,t,y]    MW    charging power                           s ∈ S ∪ H_pump
    p_dis[s,t,y]   MW    discharging power                        s ∈ S
    soc[s,t,y]     MWh   state of charge                          s ∈ S ∪ H
    spill[s,t,y]   MWh   hydro spillage                           s ∈ H
    ens[b,t,y]     MW    unserved energy at bus b
    -- with allow_investment=True --
    cap_G[u,y]     MW    new generator capacity                   u ∈ G
    cap_E[s,y]     MWh   new storage energy capacity              s ∈ S ∪ H

Objective
    min  Σ_{u,t,y}  c[u,t,y] · p_out[u,t,y] · dt        (gen variable cost)
       + Σ_{b,t,y}  VOLL     · ens[b,t,y]   · dt        (unserved energy penalty)
       + Σ_{s,t,y}  gamma[s] · spill[s,t,y]             (spillage penalty)
       + Σ_{u,y}    CRF(r,L_u) · cc_u · cap_G[u,y]  · capex_scale   (gen capex)
       + Σ_{s,y}    CRF(r,L_s) · cc_s · cap_E[s,y]  · capex_scale   (sto capex)

Constraints

    (1) Power balance        ∀ b, t, y
          Σ_{u at b} p_out[u,t,y]
        + Σ_{s ∈ S,  dis_bus=b} p_dis[s,t,y]
        − Σ_{s ∈ S,  ch_bus =b} p_ch [s,t,y]
        − Σ_{s ∈ H_pump, ch_bus=b} p_ch [s,t,y]
        + ens[b,t,y]                       =  demand[b,t,y]

    (2) Generator cap        ∀ u, t, y
          (dispatch-only)   p_out[u,t,y] ≤ p_nom[u] · profile[u,t,y]
          (with investment) p_out[u,t,y] ≤ (p_nom[u] + cap_G[u,y]) · profile[u,t,y]
        profile = 1 by default; <1 for G_res with a p_t availability series.
        With investment on, RES output becomes an inequality (curtailment
        allowed) instead of an equality.

    (3) Ramping              ∀ u ∈ G_ramp, t > t_first, y
          p_out[u,t,y]   − p_out[u,t-1,y] ≤ p_nom[u] · ramp_up[u]
          p_out[u,t-1,y] − p_out[u,t,y]   ≤ p_nom[u] · ramp_dn[u]

    (4) Pure-storage SoC     ∀ s ∈ S, t, y
          soc[s,t,y] = (1 − loss[s]) · soc[s,t-1,y]
                     + eta_ch[s]  · p_ch [s,t,y] · dt
                     − p_dis[s,t,y] / eta_dis[s] · dt

    (5) Hydro reservoir SoC  ∀ s ∈ H, t, y
          soc[s,t,y] = (1 − loss[s]) · soc[s,t-1,y]
                     + eta_ch[s]  · p_ch[s,t,y] · dt
                     − p_out[s,t,y] / eta_dis[s] · dt
                     + inflow[s,t,y] − spill[s,t,y]

    (6) Cyclic SoC           ∀ s ∈ S ∪ H, y
          soc[s, T_last, y] = soc_initial[s] = 0.5 · e_nom[s]

    (7) Capacity expansion (only when allow_investment=True)
          0 ≤ cap_G[u,y] ≤ max(0, p_nom_max[u] − p_nom[u])     ∀ u, y
          0 ≤ cap_E[s,y] ≤ max(0, e_nom_max[s] − e_nom[s])     ∀ s, y
          soc[s,t,y] ≤ e_nom[s] + cap_E[s,y]
          p_ch [s,t,y] ≤ (e_nom[s] + cap_E[s,y]) / duration_charge[s]
          p_dis[s,t,y] ≤ (e_nom[s] + cap_E[s,y]) / duration_discharge[s]

==============================================================
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

import gurobipy as gp
from gurobipy import GRB, quicksum

from data_models.SystemParameters import SystemParameters


# -------------------------------------------------------------------
# Index aliases
# -------------------------------------------------------------------
U = str   # unit id
P = int   # period index
Yr = int  # year index
B = str   # bus id

UTY = Tuple[U, P, Yr]
BTY = Tuple[B, P, Yr]


# -------------------------------------------------------------------
# Results container
# -------------------------------------------------------------------
@dataclass
class DispatchResults:
    """Container for dispatch optimisation results.

    Attributes:
        status:           Gurobi status code.
        objective:        Optimal total cost [EUR] (None if not solved).
        generation:       p_out values by (unit, period, year) [MW].
        charge:           p_ch  values by (storage, period, year) [MW].
        discharge:        p_dis values by (storage, period, year) [MW].
        soc:              State of charge by (storage, period, year) [MWh].
        spill:            Hydro spillage by (storage, period, year) [MWh].
        unserved:         ENS   by (bus, period, year) [MW].
        marginal_price:   Dual of the power balance by (bus, period, year) [€/MWh].
        capacity_added:   New generator capacity by (unit, year) [MW] —
                          only populated when allow_investment=True.
        energy_added:     New storage energy capacity by (unit, year) [MWh] —
                          only populated when allow_investment=True.
        total_capex_eur:  Annualised capital cost term in the objective
                          (pro-rated by capex_scale), 0 if no investment.
    """

    status: int = 0
    objective: Optional[float] = None
    generation: Dict[UTY, float] = field(default_factory=dict)
    charge: Dict[UTY, float] = field(default_factory=dict)
    discharge: Dict[UTY, float] = field(default_factory=dict)
    soc: Dict[UTY, float] = field(default_factory=dict)
    spill: Dict[UTY, float] = field(default_factory=dict)
    unserved: Dict[BTY, float] = field(default_factory=dict)
    marginal_price: Dict[BTY, float] = field(default_factory=dict)
    capacity_added: Dict[Tuple[U, Yr], float] = field(default_factory=dict)
    energy_added: Dict[Tuple[U, Yr], float] = field(default_factory=dict)
    total_capex_eur: float = 0.0


# -------------------------------------------------------------------
# Capital Recovery Factor
# -------------------------------------------------------------------
def annuity_factor(discount_rate: float, lifetime: float) -> float:
    """Capital Recovery Factor: CRF(r, n) = r·(1+r)^n / ((1+r)^n − 1).

    Used to spread a lump-sum capital cost over the asset's lifetime
    as a constant yearly payment. When r ≈ 0 the limit is 1/n.
    """
    if lifetime <= 0:
        return 0.0
    if abs(discount_rate) < 1e-9:
        return 1.0 / lifetime
    f = (1.0 + discount_rate) ** lifetime
    return discount_rate * f / (f - 1.0)


# -------------------------------------------------------------------
# Helper
# -------------------------------------------------------------------
def _clean_bus(value: object, fallback: str) -> str:
    """Return a valid bus id; fall back when value is NaN / None / ''."""
    if value is None:
        return fallback
    # NaN check without pulling in pandas
    try:
        if value != value:  # type: ignore[comparison-overlap]
            return fallback
    except Exception:
        pass
    text = str(value)
    return fallback if text in ("", "nan", "None") else text


# -------------------------------------------------------------------
# Dispatch model
# -------------------------------------------------------------------
class ElectricityDispatchModel:
    """Economic dispatch of the power sector using Gurobi.

    The whole formulation is built inside :meth:`build` as a single
    flat block. Each section below corresponds to one piece of the
    mathematical formulation in the module docstring.

    Usage:
        >>> model = ElectricityDispatchModel(system, years=[2020], voll=3000.0)
        >>> model.build()
        >>> results = model.solve(OutputFlag=0)
        >>> print(model.summary(results))
    """

    # -----------------------------------------------------------
    def __init__(
        self,
        system: SystemParameters,
        *,
        years: Optional[Iterable[int]] = None,
        periods: Optional[Iterable[int]] = None,
        dt: float = 1.0,
        voll: float = 3000.0,
        default_spillage_cost: float = 0.0,
        allow_investment: bool = False,
        discount_rate: float = 0.05,
        default_lifetime: float = 20.0,
        capex_scale: Optional[float] = None,
        model_name: str = "H2RES_ElectricityDispatch",
    ) -> None:
        self.system = system
        self.dt = float(dt)
        self.voll = float(voll)
        self.default_spillage_cost = float(default_spillage_cost)
        self.allow_investment = bool(allow_investment)
        self.discount_rate = float(discount_rate)
        self.default_lifetime = float(default_lifetime)
        # `capex_scale` pro-rates the annualised capex by the length of
        # the optimised slice: for a 24 h slice we pay 24/8760 of the
        # yearly payment, so capex and opex stay comparable. Pass an
        # explicit value to override (e.g. 1.0 for a full-year model).
        self.capex_scale: Optional[float] = (
            None if capex_scale is None else float(capex_scale)
        )
        self.model_name = model_name

        self.years: List[int] = (
            list(years) if years is not None else list(system.sets.years)
        )
        self.periods: List[int] = sorted(
            periods if periods is not None else system.sets.periods
        )

        # Populated by build()
        self.model: Optional[gp.Model] = None
        self.p_out: Dict[UTY, gp.Var] = {}
        self.p_ch: Dict[UTY, gp.Var] = {}
        self.p_dis: Dict[UTY, gp.Var] = {}
        self.soc: Dict[UTY, gp.Var] = {}
        self.spill: Dict[UTY, gp.Var] = {}
        self.ens: Dict[BTY, gp.Var] = {}
        self.cap_G: Dict[Tuple[U, Yr], gp.Var] = {}   # new gen capacity [MW]
        self.cap_E: Dict[Tuple[U, Yr], gp.Var] = {}   # new sto energy   [MWh]
        self.capex_expr: Optional[gp.LinExpr] = None  # for reporting
        self.balance: Dict[BTY, gp.Constr] = {}

    # ===========================================================
    # build()  — whole model in one flat sequence
    # ===========================================================
    def build(self) -> gp.Model:
        """Build variables, constraints and objective in one pass."""
        sys_ = self.system
        dt, voll = self.dt, self.voll
        periods, years = self.periods, self.years

        # ==========================================================
        # 1) SETS
        # ==========================================================
        gens, sto, sets = sys_.generators, sys_.storage, sys_.sets

        # Electricity buses
        elec_buses: List[B] = [
            b for b in sys_.bus.name
            if str(sys_.bus.carrier.get(b, "")).lower() == "electricity"
        ]
        bus_set = set(elec_buses)

        # Generators wired to the electricity grid
        G: List[U] = [
            u for u in gens.unit
            if str(gens.carrier_out.get(u, "")).lower() == "electricity"
            and gens.bus_out.get(u) in bus_set
        ]

        # Generator subsets
        hydro_set = set(sets.hydro_storage_units)
        hphs_set = set(sets.hphs_units)
        res_set = set(sets.wind_units) | set(sets.solar_units) | set(sets.hror_units)
        ramp_set = set(sets.fossil_units) | set(sets.biomass_units) | set(sets.chp_units)

        G_res = [u for u in G if u in res_set]
        G_ramp = [u for u in G if u in ramp_set]

        # Storage sets
        H: List[U] = [u for u in G if u in hydro_set and u in sto.unit]  # hydro reservoirs
        H_pump = [s for s in H if s in hphs_set]                          # HPHS pumps
        S: List[U] = [                                                    # pure storages
            s for s in sto.unit
            if s not in hydro_set
            and str(sto.carrier_out.get(s, "")).lower() == "electricity"
            and _clean_bus(sto.bus_out.get(s), "") in bus_set
        ]

        # Bus topology
        bus_of: Dict[U, B] = {u: str(gens.bus_out[u]) for u in G}
        dis_bus: Dict[U, B] = {}
        ch_bus: Dict[U, B] = {}

        for s in S:  # pure storages: bus_out for dis, bus_in for ch
            out_b = _clean_bus(sto.bus_out.get(s), "")
            dis_bus[s] = out_b
            ch_bus[s] = _clean_bus(sto.bus_in.get(s), out_b)
        for s in H:  # hydro: discharge = generator output; pump (HPHS) at turbine bus
            turbine_b = str(gens.bus_out.get(s, ""))
            dis_bus[s] = _clean_bus(sto.bus_out.get(s), turbine_b)
            ch_bus[s] = _clean_bus(sto.bus_in.get(s), turbine_b) if s in hphs_set else ""

        # ==========================================================
        # 2) PARAMETERS (pre-fetched with safe fallbacks)
        # ==========================================================
        p_nom = {u: float(gens.p_nom.get(u, 0.0)) for u in G}
        ramp_up = {u: float(gens.ramp_up_rate.get(u, 1.0)) for u in G}
        ramp_dn = {u: float(gens.ramp_down_rate.get(u, 1.0)) for u in G}
        vc_no_fuel = {u: float(gens.var_cost_no_fuel.get(u, 0.0)) for u in G}

        e_nom = {s: float(sto.e_nom.get(s, 0.0)) for s in S + H}
        e_min = {s: float(sto.e_min.get(s, 0.0)) for s in S + H}
        eta_ch = {s: float(sto.efficiency_charge.get(s, 1.0)) for s in S + H}
        eta_dis = {s: float(sto.efficiency_discharge.get(s, 1.0)) for s in S + H}
        loss = {s: float(sto.standby_loss.get(s, 0.0)) for s in S + H}

        pc_cap_S = {s: float(sto.p_charge_nom.get(s, 0.0)) for s in S}
        pd_cap_S = {s: float(sto.p_discharge_nom.get(s, 0.0)) for s in S}
        pc_cap_H = {  # HDAM has no pump
            s: float(sto.p_charge_nom.get(s, 0.0)) if s in hphs_set else 0.0
            for s in H
        }
        spill_cost = {
            s: float(sto.spillage_cost.get(s, self.default_spillage_cost)) for s in H
        }

        # ---- Investment parameters (only used when allow_investment=True) ----
        # Maximum buildable capacity: p_nom_max / e_nom_max (≥ existing p_nom / e_nom).
        # Fall back to existing capacity when the cap is missing or smaller than the
        # installed value (i.e. no room to expand).
        p_nom_max = {
            u: max(p_nom[u], float(gens.p_nom_max.get(u, p_nom[u]))) for u in G
        }
        e_nom_max = {
            s: max(e_nom[s], float(sto.e_nom_max.get(s, e_nom[s]))) for s in S + H
        }
        gen_life = {u: float(gens.lifetime.get(u, self.default_lifetime)) for u in G}
        gen_capex = {u: float(gens.capital_cost.get(u, 0.0)) for u in G}
        sto_life = {
            s: float(sto.lifetime.get(s, self.default_lifetime)) for s in S + H
        }
        cap_cost_E = {
            s: float(sto.capital_cost_energy.get(s, 0.0)) for s in S + H
        }
        cap_cost_Pch = {
            s: float(sto.capital_cost_power_charge.get(s, 0.0)) for s in S + H
        }
        cap_cost_Pdis = {
            s: float(sto.capital_cost_power_discharge.get(s, 0.0)) for s in S + H
        }
        # Duration in hours: e = dur · p. Fallback = 1 h (avoid div-by-zero).
        dur_ch = {
            s: max(1e-6, float(sto.duration_charge.get(s, 1.0))) for s in S + H
        }
        dur_dis = {
            s: max(1e-6, float(sto.duration_discharge.get(s, 1.0))) for s in S + H
        }

        # Pre-aggregate demand into demand[(b,t,y)]
        demand: Dict[BTY, float] = defaultdict(float)
        t_set, y_set = set(periods), set(years)
        for (_sysn, _reg, b, carrier, t, y), val in sys_.demand.p_t.items():
            if (
                str(carrier).lower() == "electricity"
                and b in bus_set
                and t in t_set
                and y in y_set
            ):
                demand[(b, t, y)] += float(val)

        # Units grouped by bus (for the power balance)
        gens_at: Dict[B, List[U]] = defaultdict(list)
        for u in G:
            gens_at[bus_of[u]].append(u)

        S_dis_at: Dict[B, List[U]] = defaultdict(list)
        S_ch_at: Dict[B, List[U]] = defaultdict(list)
        for s in S:
            S_dis_at[dis_bus[s]].append(s)
            if ch_bus[s] in bus_set:
                S_ch_at[ch_bus[s]].append(s)

        H_ch_at: Dict[B, List[U]] = defaultdict(list)
        for s in H_pump:
            if ch_bus[s] in bus_set:
                H_ch_at[ch_bus[s]].append(s)

        # ==========================================================
        # 3) MODEL + VARIABLES
        # ==========================================================
        m = gp.Model(self.model_name)
        self.model = m

        inv = self.allow_investment

        # Pre-compute variable upper bounds. Without investment we cap at
        # the installed values (p_nom, e_nom, pc_cap, pd_cap). With
        # investment we raise the ub to the *maximum* capacity so the new
        # cap_G / cap_E variables can push output/SoC beyond the existing
        # installed value. The explicit constraints added further below
        # tie those quantities to `p_nom + cap_G` / `e_nom + cap_E`.
        S_charge = S + H  # (HDAM gets pc_cap_H = 0, so variable is tight)
        pc_cap_all = {**pc_cap_S, **pc_cap_H}

        if inv:
            # New storage power derives from new energy via duration.
            pc_ub_all = {
                s: max(pc_cap_all[s], e_nom_max[s] / dur_ch[s])
                for s in S_charge
            }
            # HDAM has no pump → keep pc_cap_H[s] = 0 regardless.
            for s in H:
                if s not in hphs_set:
                    pc_ub_all[s] = 0.0
            pd_ub_S = {s: max(pd_cap_S[s], e_nom_max[s] / dur_dis[s]) for s in S}
        else:
            pc_ub_all = dict(pc_cap_all)
            pd_ub_S = dict(pd_cap_S)

        # Generator output — ub = installed capacity (no investment) or
        # maximum buildable capacity (with investment).
        p_out_ub = {
            (u, t, y): (p_nom_max[u] if inv else p_nom[u])
            for u in G for t in periods for y in years
        }
        p_out = m.addVars(
            [(u, t, y) for u in G for t in periods for y in years],
            lb=0.0,
            ub=p_out_ub,
            name="p_out",
        )

        # Charging power  p_ch[s,t,y] — for pure storage AND HPHS pumps
        p_ch = m.addVars(
            [(s, t, y) for s in S_charge for t in periods for y in years],
            lb=0.0,
            ub={
                (s, t, y): pc_ub_all[s]
                for s in S_charge for t in periods for y in years
            },
            name="p_ch",
        )

        # Discharging power  p_dis[s,t,y]  — pure storage only
        p_dis = m.addVars(
            [(s, t, y) for s in S for t in periods for y in years],
            lb=0.0,
            ub={(s, t, y): pd_ub_S[s] for s in S for t in periods for y in years},
            name="p_dis",
        )

        # State of charge  e_min[s] ≤ soc[s,t,y] ≤ e_nom[s] (or e_nom_max with inv).
        # Note: when investment is on we also relax e_min proportionally to avoid
        # infeasibility when cap_E=0 and the installed e_nom=0 (typical for new
        # technologies without pre-installed capacity).
        soc_lb_val = (
            {s: 0.0 for s in S_charge}
            if inv
            else {s: e_min[s] for s in S_charge}
        )
        soc_ub_val = {s: (e_nom_max[s] if inv else e_nom[s]) for s in S_charge}
        soc = m.addVars(
            [(s, t, y) for s in S_charge for t in periods for y in years],
            lb={(s, t, y): soc_lb_val[s] for s in S_charge for t in periods for y in years},
            ub={(s, t, y): soc_ub_val[s] for s in S_charge for t in periods for y in years},
            name="soc",
        )

        # Hydro spillage
        spill = m.addVars(
            [(s, t, y) for s in H for t in periods for y in years],
            lb=0.0, name="spill",
        )

        # Unserved energy
        ens = m.addVars(
            [(b, t, y) for b in elec_buses for t in periods for y in years],
            lb=0.0, name="ens",
        )

        # ---- Investment variables ----
        # cap_G[u,y] — new MW of generator u built in year y, bounded by
        # (p_nom_max − p_nom). cap_E[s,y] — new MWh of storage s built
        # in year y, bounded by (e_nom_max − e_nom). Always created as
        # {} when allow_investment=False, so callers can safely inspect
        # them.
        cap_G: Dict[Tuple[U, Yr], gp.Var] = {}
        cap_E: Dict[Tuple[U, Yr], gp.Var] = {}
        if inv:
            for u in G:
                ub = max(0.0, p_nom_max[u] - p_nom[u])
                for y in years:
                    cap_G[(u, y)] = m.addVar(
                        lb=0.0, ub=ub, name=f"cap_G[{u},{y}]"
                    )
            for s in S_charge:
                ub = max(0.0, e_nom_max[s] - e_nom[s])
                for y in years:
                    cap_E[(s, y)] = m.addVar(
                        lb=0.0, ub=ub, name=f"cap_E[{s},{y}]"
                    )

        # Expose as plain dicts on self
        self.p_out = dict(p_out)
        self.p_ch = dict(p_ch)
        self.p_dis = dict(p_dis)
        self.soc = dict(soc)
        self.spill = dict(spill)
        self.ens = dict(ens)
        self.cap_G = cap_G
        self.cap_E = cap_E

        # ==========================================================
        # 4) POWER BALANCE   — Eq. (1)
        #
        #   Σ p_out(u∈G@b) + Σ p_dis(s∈S@b) − Σ p_ch(s∈S@b)
        #                                  − Σ p_ch(s∈H_pump@b)
        #   + ens(b) = demand(b)
        # ==========================================================
        for b in elec_buses:
            for y in years:
                for t in periods:
                    lhs = (
                          quicksum(p_out[u, t, y] for u in gens_at[b])
                        + quicksum(p_dis[s, t, y] for s in S_dis_at[b])
                        - quicksum(p_ch[s, t, y] for s in S_ch_at[b])
                        - quicksum(p_ch[s, t, y] for s in H_ch_at[b])
                        + ens[b, t, y]
                    )
                    self.balance[(b, t, y)] = m.addConstr(
                        lhs == demand[(b, t, y)],
                        name=f"balance[{b},{t},{y}]",
                    )

        # ==========================================================
        # 5) GENERATOR AVAILABILITY   — Eq. (2)
        #
        #   Dispatch only:
        #     p_out[u,t,y] == p_nom[u] · profile[u,t,y]    ∀ u ∈ G_res
        #     (variable ub handles non-RES units)
        #
        #   With investment (curtailment allowed for RES):
        #     p_out[u,t,y] ≤ (p_nom[u] + cap_G[u,y]) · profile[u,t,y]   ∀ u ∈ G_res
        #     p_out[u,t,y] ≤  p_nom[u] + cap_G[u,y]                     ∀ u ∉ G_res
        # ==========================================================
        if inv:
            G_res_set = set(G_res)
            for u in G:
                is_res = u in G_res_set
                for y in years:
                    pn_plus = p_nom[u] + cap_G[(u, y)]
                    for t in periods:
                        profile = float(gens.p_t.get((u, t, y), 1.0)) if is_res else 1.0
                        m.addConstr(
                            p_out[u, t, y] <= pn_plus * profile,
                            name=f"cap_gen[{u},{t},{y}]",
                        )
        else:
            for u in G_res:
                for y in years:
                    for t in periods:
                        profile = float(gens.p_t.get((u, t, y), 1.0))
                        m.addConstr(
                            p_out[u, t, y] == p_nom[u] * profile,
                            name=f"avail[{u},{t},{y}]",
                        )

        # ==========================================================
        # 6) RAMPING   — Eq. (3)
        #
        #   p_out[u,t,y] − p_out[u,t-1,y] ≤ (p_nom[u] + cap_G[u,y])·ramp_up[u]
        #   p_out[u,t-1,y] − p_out[u,t,y] ≤ (p_nom[u] + cap_G[u,y])·ramp_dn[u]
        # (cap_G term drops out when allow_investment=False)
        # ==========================================================
        for u in G_ramp:
            ru_rate = ramp_up[u]
            rd_rate = ramp_dn[u]
            if ru_rate <= 0 and rd_rate <= 0:
                continue
            for y in years:
                if inv:
                    cap_expr = p_nom[u] + cap_G[(u, y)]
                    ru_rhs = ru_rate * cap_expr if ru_rate > 0 else None
                    rd_rhs = rd_rate * cap_expr if rd_rate > 0 else None
                else:
                    ru_rhs = ru_rate * p_nom[u] if ru_rate > 0 else None
                    rd_rhs = rd_rate * p_nom[u] if rd_rate > 0 else None
                for i in range(1, len(periods)):
                    t_prev, t = periods[i - 1], periods[i]
                    if ru_rhs is not None:
                        m.addConstr(
                            p_out[u, t, y] - p_out[u, t_prev, y] <= ru_rhs,
                            name=f"ramp_up[{u},{t},{y}]",
                        )
                    if rd_rhs is not None:
                        m.addConstr(
                            p_out[u, t_prev, y] - p_out[u, t, y] <= rd_rhs,
                            name=f"ramp_dn[{u},{t},{y}]",
                        )

        # ==========================================================
        # 6b) STORAGE CAPACITY COUPLING (only with investment) — Eq. (7)
        #
        #   soc[s,t,y]  ≤ e_nom[s] + cap_E[s,y]
        #   p_ch [s,t,y] ≤ (e_nom[s] + cap_E[s,y]) / duration_charge[s]
        #   p_dis[s,t,y] ≤ (e_nom[s] + cap_E[s,y]) / duration_discharge[s]
        # (pure storage and hydro reservoirs; HDAM has no pump, so its
        #  p_ch is already pinned to 0 via the variable ub).
        # ==========================================================
        if inv:
            for s in S_charge:
                for y in years:
                    e_total = e_nom[s] + cap_E[(s, y)]
                    pc_cap_expr = e_total / dur_ch[s]
                    for t in periods:
                        m.addConstr(
                            soc[s, t, y] <= e_total,
                            name=f"soc_cap[{s},{t},{y}]",
                        )
                        # Skip the charge-power constraint for HDAM (no pump).
                        if s in H and s not in hphs_set:
                            continue
                        m.addConstr(
                            p_ch[s, t, y] <= pc_cap_expr,
                            name=f"pch_cap[{s},{t},{y}]",
                        )
            # Discharge coupling only for pure storages (hydro uses p_out,
            # which is coupled to cap_G via the generator capacity block).
            for s in S:
                for y in years:
                    pd_cap_expr = (e_nom[s] + cap_E[(s, y)]) / dur_dis[s]
                    for t in periods:
                        m.addConstr(
                            p_dis[s, t, y] <= pd_cap_expr,
                            name=f"pdis_cap[{s},{t},{y}]",
                        )

        # ==========================================================
        # 7) PURE STORAGE SoC BALANCE   — Eq. (4)
        #
        #   soc[s,t] = (1 − loss) · soc[s,t-1]
        #            + eta_ch · p_ch  · dt
        #            − p_dis / eta_dis · dt
        #   soc[s, T_last] = soc_initial  (cyclic, Eq. 6)
        #   soc_initial = 0.5 · (e_nom + cap_E)  when investment is on
        # ==========================================================
        for s in S:
            for y in years:
                # Initial SoC = half-full of the *available* capacity.
                if inv:
                    soc0 = 0.5 * (e_nom[s] + cap_E[(s, y)])
                else:
                    soc0 = 0.5 * e_nom[s]
                for i, t in enumerate(periods):
                    soc_prev = soc0 if i == 0 else soc[s, periods[i - 1], y]
                    m.addConstr(
                        soc[s, t, y]
                        == (1.0 - loss[s]) * soc_prev
                        +  eta_ch[s]  * p_ch[s, t, y]  * dt
                        -  p_dis[s, t, y] / eta_dis[s] * dt,
                        name=f"soc_bal[{s},{t},{y}]",
                    )
                m.addConstr(soc[s, periods[-1], y] == soc0, name=f"soc_cyc[{s},{y}]")

        # ==========================================================
        # 8) HYDRO RESERVOIR SoC BALANCE   — Eq. (5)
        #
        #   soc[s,t] = (1 − loss) · soc[s,t-1]
        #            + eta_ch · p_ch   · dt        (HPHS pump, 0 for HDAM)
        #            − p_out / eta_dis · dt        (turbine = discharge)
        #            + inflow[s,t] − spill[s,t]
        #   cyclic terminal condition
        # ==========================================================
        for s in H:
            for y in years:
                if inv:
                    soc0 = 0.5 * (e_nom[s] + cap_E[(s, y)])
                else:
                    soc0 = 0.5 * e_nom[s]
                for i, t in enumerate(periods):
                    soc_prev = soc0 if i == 0 else soc[s, periods[i - 1], y]
                    inflow = float(sto.inflow.get((s, t, y), 0.0))
                    m.addConstr(
                        soc[s, t, y]
                        == (1.0 - loss[s]) * soc_prev
                        +  eta_ch[s]  * p_ch[s, t, y]  * dt
                        -  p_out[s, t, y] / eta_dis[s] * dt
                        +  inflow
                        -  spill[s, t, y],
                        name=f"res_bal[{s},{t},{y}]",
                    )
                m.addConstr(soc[s, periods[-1], y] == soc0, name=f"res_cyc[{s},{y}]")

        # ==========================================================
        # 9) OBJECTIVE — generator cost + ENS penalty + spillage (+ capex)
        #
        # The capex term — only active with allow_investment — takes a
        # lump-sum investment cost per MW (or per MWh) and converts it
        # into an annuity via the Capital Recovery Factor (CRF). It is
        # then pro-rated by ``capex_scale = NT·dt / 8760`` so the annual
        # payment stays on the same scale as the slice-length opex. For
        # storage, the lump-sum per MWh of new energy capacity is the
        # sum of the energy capex plus the power capex divided by the
        # duration, so one MWh of storage pays for its associated
        # charge/discharge power at the duration ratio.
        # ==========================================================
        gen_cost = quicksum(
            float(gens.var_cost.get((u, t, y), vc_no_fuel[u])) * p_out[u, t, y] * dt
            for u in G for t in periods for y in years
        )
        ens_cost = quicksum(
            voll * ens[b, t, y] * dt
            for b in elec_buses for t in periods for y in years
        )
        spill_cost_expr = quicksum(
            spill_cost[s] * spill[s, t, y]
            for s in H for t in periods for y in years
            if spill_cost[s] > 0
        )

        capex_cost = 0.0  # type: ignore[assignment]
        if inv:
            # Pro-rate annualised capex by the length of the slice.
            if self.capex_scale is None:
                capex_scale = (len(periods) * dt) / 8760.0
            else:
                capex_scale = self.capex_scale

            # Generator capex: CRF · capital_cost · cap_G [€/MW · MW → €/yr]
            gen_capex_expr = quicksum(
                annuity_factor(self.discount_rate, gen_life[u])
                * gen_capex[u]
                * cap_G[(u, y)]
                for u in G for y in years
                if gen_capex[u] > 0
            )
            # Storage capex: annualise energy + power capex (power capex
            # scaled by 1/duration to convert from €/MW back to €/MWh).
            sto_capex_expr = quicksum(
                annuity_factor(self.discount_rate, sto_life[s])
                * (
                    cap_cost_E[s]
                    + cap_cost_Pch[s]  / dur_ch[s]
                    + cap_cost_Pdis[s] / dur_dis[s]
                )
                * cap_E[(s, y)]
                for s in S_charge for y in years
                if (cap_cost_E[s] + cap_cost_Pch[s] + cap_cost_Pdis[s]) > 0
            )
            capex_cost = (gen_capex_expr + sto_capex_expr) * capex_scale
            self.capex_expr = capex_cost  # type: ignore[assignment]
        else:
            self.capex_expr = None

        m.setObjective(
            gen_cost + ens_cost + spill_cost_expr + capex_cost,
            GRB.MINIMIZE,
        )

        m.update()
        return m

    # ===========================================================
    # solve() — run Gurobi and collect results
    # ===========================================================
    def solve(self, **gurobi_params) -> DispatchResults:
        """Solve the model and return a :class:`DispatchResults`."""
        if self.model is None:
            self.build()
        assert self.model is not None

        for k, v in gurobi_params.items():
            self.model.setParam(k, v)

        self.model.optimize()

        r = DispatchResults(status=self.model.status)
        if self.model.status != GRB.OPTIMAL:
            return r

        r.objective = float(self.model.objVal)
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

        # Investment results (empty dicts / 0.0 when allow_investment=False)
        r.capacity_added = {k: float(v.X) for k, v in self.cap_G.items()}
        r.energy_added = {k: float(v.X) for k, v in self.cap_E.items()}
        if self.capex_expr is not None:
            try:
                r.total_capex_eur = float(self.capex_expr.getValue())
            except (AttributeError, gp.GurobiError):
                r.total_capex_eur = 0.0
        else:
            r.total_capex_eur = 0.0

        return r

    # ===========================================================
    # summary() — compact KPI dictionary
    # ===========================================================
    def summary(self, r: DispatchResults) -> Dict[str, float]:
        """Return a compact dictionary of KPIs from a solved dispatch."""
        total_gen = sum(r.generation.values()) * self.dt
        total_ens = sum(r.unserved.values()) * self.dt
        avg_price = (
            sum(r.marginal_price.values()) / len(r.marginal_price)
            if r.marginal_price else float("nan")
        )
        out = {
            "total_cost_eur": r.objective or 0.0,
            "total_generation_mwh": total_gen,
            "total_unserved_mwh": total_ens,
            "avg_marginal_price_eur_per_mwh": avg_price,
        }
        if self.allow_investment:
            out["new_gen_capacity_mw"] = sum(r.capacity_added.values())
            out["new_storage_energy_mwh"] = sum(r.energy_added.values())
            out["annualised_capex_eur"] = r.total_capex_eur
        return out
