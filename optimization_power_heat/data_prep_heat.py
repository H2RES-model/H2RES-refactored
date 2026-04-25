"""Heat sector — data preparation.

Mirror of :mod:`data_prep_power`, but for the heat layer.

Heat covers space heating for housing, District Heating, industrial
process heat, etc. From the model's point of view these are just
different heat buses with different demand profiles. This file:

  1. Finds every heat bus (``bus.carrier == "heat"``).
  2. Groups heat-producing units into three buckets:
       - **Fuel boilers**  (gas / biomass / H2 / e-fuel → heat).
         Their fuel cost is treated as exogenous — we don't yet
         model where the H2 or e-fuel came from.
       - **Electric converters**  (heat pump, electric boiler,
         ETES pump). These DRAW electricity, so they will show up
         as extra load in the power balance.
       - **CHP units**  already live in the POWER sector (they
         produce electricity). Their heat byproduct appears here
         as a supply term at their heat bus.
  3. Finds heat storage (TES) and pre-fetches all its numbers.
  4. Aggregates heat demand to the heat buses.

Investment is always on: the model will always create new-capacity
variables. If the CSV has no room to expand (``p_nom_max == p_nom``),
the variable's upper bound is just 0.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from data_models.SystemParameters import SystemParameters


# -------------------------------------------------------------------
# Same index aliases as the power module for consistency.
# -------------------------------------------------------------------
U = str
P = int
Yr = int
B = str

UTY = Tuple[U, P, Yr]
BTY = Tuple[B, P, Yr]


def _clean_bus(value: object, fallback: str) -> str:
    if value is None:
        return fallback
    try:
        if value != value:  # NaN
            return fallback
    except Exception:
        pass
    text = str(value)
    return fallback if text in ("", "nan", "None") else text


def _lower_str(value: object) -> str:
    if value is None:
        return ""
    try:
        if value != value:  # NaN
            return ""
    except Exception:
        pass
    return str(value).lower()


# ===================================================================
# HeatData
# ===================================================================
@dataclass
class HeatData:
    """Container of everything the heat model needs."""

    # -- Raw reference + settings ------------------------------------
    system: SystemParameters
    periods: List[P]
    years: List[Yr]
    dt: float = 1.0
    voll_heat: float = 3000.0        # penalty for unserved heat [€/MWh]
    discount_rate: float = 0.05
    default_lifetime: float = 20.0
    capex_scale: Optional[float] = None
    default_efficiency: float = 1.0  # fall back for missing boiler eff.
    default_cop: float = 3.0         # fall back for missing heat-pump COP

    # -- Sets --------------------------------------------------------
    heat_buses: List[B] = field(default_factory=list)
    G_h: List[U] = field(default_factory=list)        # all heat generators
    G_h_fuel: List[U] = field(default_factory=list)   # boilers (fuel or H2/e-fuel)
    G_h_elec: List[U] = field(default_factory=list)   # electricity → heat
    S_h: List[U] = field(default_factory=list)        # heat storage (TES)
    G_chp: List[U] = field(default_factory=list)      # CHP units with heat output

    # -- Who sits where ----------------------------------------------
    bus_of: Dict[U, B] = field(default_factory=dict)           # heat gen → heat bus
    elec_bus_of: Dict[U, B] = field(default_factory=dict)      # elec converter → elec bus it draws from
    chp_bus_of: Dict[U, B] = field(default_factory=dict)       # CHP → heat bus it supplies
    sto_bus_of: Dict[U, B] = field(default_factory=dict)       # TES → heat bus
    gens_at: Dict[B, List[U]] = field(default_factory=dict)    # heat bus → heat gens
    chp_at: Dict[B, List[U]] = field(default_factory=dict)     # heat bus → CHPs that deposit heat there
    sto_at: Dict[B, List[U]] = field(default_factory=dict)     # heat bus → TES
    elec_consumers_at: Dict[B, List[U]] = field(default_factory=dict)  # elec bus → e→h converters

    # -- Heat-generator scalars --------------------------------------
    p_nom: Dict[U, float] = field(default_factory=dict)
    p_nom_max: Dict[U, float] = field(default_factory=dict)
    efficiency: Dict[U, float] = field(default_factory=dict)   # boiler eff OR HP COP (static fallback)
    vc_no_fuel: Dict[U, float] = field(default_factory=dict)
    gen_life: Dict[U, float] = field(default_factory=dict)
    gen_capex: Dict[U, float] = field(default_factory=dict)

    # CHP power-to-heat ratio. Heat supplied by a CHP at (t, y) is
    #   chp_p2h[u] × (electricity output p_out of that unit from the power model).
    chp_p2h: Dict[U, float] = field(default_factory=dict)

    # -- Heat-storage scalars (TES) ----------------------------------
    e_nom: Dict[U, float] = field(default_factory=dict)
    e_min: Dict[U, float] = field(default_factory=dict)
    e_nom_max: Dict[U, float] = field(default_factory=dict)
    eta_ch: Dict[U, float] = field(default_factory=dict)
    eta_dis: Dict[U, float] = field(default_factory=dict)
    loss: Dict[U, float] = field(default_factory=dict)
    pc_cap: Dict[U, float] = field(default_factory=dict)
    pd_cap: Dict[U, float] = field(default_factory=dict)
    dur_ch: Dict[U, float] = field(default_factory=dict)
    dur_dis: Dict[U, float] = field(default_factory=dict)
    sto_life: Dict[U, float] = field(default_factory=dict)
    cap_cost_E: Dict[U, float] = field(default_factory=dict)
    cap_cost_Pch: Dict[U, float] = field(default_factory=dict)
    cap_cost_Pdis: Dict[U, float] = field(default_factory=dict)

    # -- Heat demand, aggregated to heat buses -----------------------
    demand: Dict[BTY, float] = field(default_factory=dict)

    # -----------------------------------------------------------
    @property
    def is_empty(self) -> bool:
        """True if there is no heat layer to model at all."""
        return not (self.heat_buses and (self.G_h or self.G_chp or self.S_h))

    def eff_at(self, u: U, t: P, y: Yr) -> float:
        """Return the efficiency (or COP) of unit ``u`` at (t, y).

        Uses the time-varying value if present, otherwise the static
        fallback; guards against zero/negative values.
        """
        ts = self.system.generators.efficiency_ts
        val = ts.get((u, t, y))
        if val is None or val != val or val <= 0:
            return self.efficiency[u]
        return float(val)

    def var_cost_at(self, u: U, t: P, y: Yr) -> float:
        """Return €/MWh_heat for unit ``u`` at (t, y) (fuel + O&M, exogenous)."""
        vc_ts = self.system.generators.var_cost.get((u, t, y))
        if vc_ts is not None and vc_ts == vc_ts:
            return float(vc_ts)
        return float(self.vc_no_fuel.get(u, 0.0))

    # ===============================================================
    # Factory
    # ===============================================================
    @classmethod
    def from_system(
        cls,
        system: SystemParameters,
        *,
        years: Optional[Iterable[int]] = None,
        periods: Optional[Iterable[int]] = None,
        dt: float = 1.0,
        voll_heat: float = 3000.0,
        discount_rate: float = 0.05,
        default_lifetime: float = 20.0,
        capex_scale: Optional[float] = None,
        default_efficiency: float = 1.0,
        default_cop: float = 3.0,
    ) -> "HeatData":
        """Build the :class:`HeatData` container.

        If the system has no heat bus / heat data at all, the returned
        container is empty (``is_empty == True``) and the heat model
        becomes a no-op. This is useful when you want to run a
        power-only system in ``optimization_2`` without having to
        import a different module.
        """
        years_list = list(years) if years is not None else list(system.sets.years)
        periods_list = sorted(
            periods if periods is not None else system.sets.periods
        )

        gens, sto = system.generators, system.storage

        # ----- Buses: heat and electricity -------------------------
        heat_buses = [
            b for b in system.bus.name
            if _lower_str(system.bus.carrier.get(b, "")) == "heat"
        ]
        hbus = set(heat_buses)
        elec_buses = [
            b for b in system.bus.name
            if _lower_str(system.bus.carrier.get(b, "")) == "electricity"
        ]
        ebus = set(elec_buses)

        # ----- Heat generators -------------------------------------
        # A "heat generator" here is any unit whose output is heat
        # and whose output bus is a heat bus.
        G_h: List[U] = []
        G_h_fuel: List[U] = []
        G_h_elec: List[U] = []
        elec_bus_of: Dict[U, B] = {}
        for u in gens.unit:
            if _lower_str(gens.carrier_out.get(u, "")) != "heat":
                continue
            bo = _clean_bus(gens.bus_out.get(u, ""), "")
            if bo not in hbus:
                continue
            G_h.append(u)
            # Electric-input converter (heat pump, e-boiler): it draws
            # electricity from an electricity bus.
            if _lower_str(gens.carrier_in.get(u, "")) == "electricity":
                bi = _clean_bus(gens.bus_in.get(u, ""), "")
                if bi in ebus:
                    G_h_elec.append(u)
                    elec_bus_of[u] = bi
                else:
                    G_h_fuel.append(u)  # fallback if elec bus is unclear
            else:
                G_h_fuel.append(u)  # fuel / H2 / e-fuel boiler

        # ----- CHP units (they live in the power sector) -----------
        # Identify CHPs whose heat byproduct is wired to a heat bus
        # via the ``bus_out_2`` and ``carrier_out_2`` columns.
        G_chp: List[U] = []
        chp_bus_of: Dict[U, B] = {}
        chp_p2h: Dict[U, float] = {}
        chp_set = set(system.sets.chp_units)
        for u in gens.unit:
            if u not in chp_set:
                continue
            if _lower_str(gens.carrier_out_2.get(u, "")) != "heat":
                continue
            bo2 = _clean_bus(gens.bus_out_2.get(u, ""), "")
            if bo2 not in hbus:
                continue
            G_chp.append(u)
            chp_bus_of[u] = bo2
            chp_p2h[u] = float(gens.chp_power_to_heat.get(u, 0.0))

        # ----- Heat storage (TES) ----------------------------------
        S_h: List[U] = []
        sto_bus_of: Dict[U, B] = {}
        for s in sto.unit:
            if _lower_str(sto.carrier_out.get(s, "")) != "heat":
                continue
            bo = _clean_bus(sto.bus_out.get(s), "")
            if bo not in hbus:
                continue
            S_h.append(s)
            sto_bus_of[s] = bo

        # ----- Bus → unit lookups ----------------------------------
        bus_of: Dict[U, B] = {u: str(gens.bus_out[u]) for u in G_h}
        gens_at: Dict[B, List[U]] = defaultdict(list)
        for u in G_h:
            gens_at[bus_of[u]].append(u)
        chp_at: Dict[B, List[U]] = defaultdict(list)
        for u in G_chp:
            chp_at[chp_bus_of[u]].append(u)
        sto_at: Dict[B, List[U]] = defaultdict(list)
        for s in S_h:
            sto_at[sto_bus_of[s]].append(s)
        elec_consumers_at: Dict[B, List[U]] = defaultdict(list)
        for u in G_h_elec:
            elec_consumers_at[elec_bus_of[u]].append(u)

        # ----- Scalar parameters: heat generators ------------------
        p_nom = {u: float(gens.p_nom.get(u, 0.0)) for u in G_h}
        p_nom_max = {
            u: max(p_nom[u], float(gens.p_nom_max.get(u, p_nom[u]))) for u in G_h
        }
        G_h_elec_set = set(G_h_elec)
        efficiency: Dict[U, float] = {}
        for u in G_h:
            val = float(gens.efficiency.get(u, 0.0))
            if val <= 0:
                val = default_cop if u in G_h_elec_set else default_efficiency
            efficiency[u] = val
        vc_no_fuel = {u: float(gens.var_cost_no_fuel.get(u, 0.0)) for u in G_h}
        gen_life = {u: float(gens.lifetime.get(u, default_lifetime)) for u in G_h}
        gen_capex = {u: float(gens.capital_cost.get(u, 0.0)) for u in G_h}

        # ----- Scalar parameters: heat storage (TES) ---------------
        e_nom = {s: float(sto.e_nom.get(s, 0.0)) for s in S_h}
        e_min = {s: float(sto.e_min.get(s, 0.0)) for s in S_h}
        e_nom_max = {
            s: max(e_nom[s], float(sto.e_nom_max.get(s, e_nom[s]))) for s in S_h
        }
        eta_ch = {s: float(sto.efficiency_charge.get(s, 1.0)) for s in S_h}
        eta_dis = {s: float(sto.efficiency_discharge.get(s, 1.0)) for s in S_h}
        loss = {s: float(sto.standby_loss.get(s, 0.0)) for s in S_h}
        pc_cap = {s: float(sto.p_charge_nom.get(s, 0.0)) for s in S_h}
        pd_cap = {s: float(sto.p_discharge_nom.get(s, 0.0)) for s in S_h}
        dur_ch = {
            s: max(1e-6, float(sto.duration_charge.get(s, 1.0))) for s in S_h
        }
        dur_dis = {
            s: max(1e-6, float(sto.duration_discharge.get(s, 1.0))) for s in S_h
        }
        sto_life = {s: float(sto.lifetime.get(s, default_lifetime)) for s in S_h}
        cap_cost_E = {s: float(sto.capital_cost_energy.get(s, 0.0)) for s in S_h}
        cap_cost_Pch = {
            s: float(sto.capital_cost_power_charge.get(s, 0.0)) for s in S_h
        }
        cap_cost_Pdis = {
            s: float(sto.capital_cost_power_discharge.get(s, 0.0)) for s in S_h
        }

        # ----- Heat demand ----------------------------------------
        demand: Dict[BTY, float] = defaultdict(float)
        t_set, y_set = set(periods_list), set(years_list)
        for (_sysn, _reg, b, carrier, t, y), val in system.demand.p_t.items():
            if (
                _lower_str(carrier) == "heat"
                and b in hbus
                and t in t_set
                and y in y_set
            ):
                demand[(b, t, y)] += float(val)

        return cls(
            system=system,
            periods=periods_list,
            years=years_list,
            dt=float(dt),
            voll_heat=float(voll_heat),
            discount_rate=float(discount_rate),
            default_lifetime=float(default_lifetime),
            capex_scale=None if capex_scale is None else float(capex_scale),
            default_efficiency=float(default_efficiency),
            default_cop=float(default_cop),
            heat_buses=heat_buses,
            G_h=G_h, G_h_fuel=G_h_fuel, G_h_elec=G_h_elec,
            S_h=S_h, G_chp=G_chp,
            bus_of=bus_of,
            elec_bus_of=elec_bus_of,
            chp_bus_of=chp_bus_of,
            sto_bus_of=sto_bus_of,
            gens_at=dict(gens_at),
            chp_at=dict(chp_at),
            sto_at=dict(sto_at),
            elec_consumers_at=dict(elec_consumers_at),
            p_nom=p_nom, p_nom_max=p_nom_max,
            efficiency=efficiency,
            vc_no_fuel=vc_no_fuel,
            gen_life=gen_life, gen_capex=gen_capex,
            chp_p2h=chp_p2h,
            e_nom=e_nom, e_min=e_min, e_nom_max=e_nom_max,
            eta_ch=eta_ch, eta_dis=eta_dis, loss=loss,
            pc_cap=pc_cap, pd_cap=pd_cap,
            dur_ch=dur_ch, dur_dis=dur_dis,
            sto_life=sto_life,
            cap_cost_E=cap_cost_E,
            cap_cost_Pch=cap_cost_Pch,
            cap_cost_Pdis=cap_cost_Pdis,
            demand=dict(demand),
        )
