"""Power sector — data preparation.

This file's job: read the SystemParameters object (which was built
from CSVs by the ``data_loaders`` package) and produce a tidy
:class:`PowerData` container that the power-sector model can consume
directly. No Gurobi is imported here. No LP math. Just data plumbing.

Why a separate file for this?
-----------------------------
Each H2RES sector has two ingredients:

  1) **The data** — which generators exist, what buses, what demand,
     what costs. A lot of it needs filtering and cross-referencing
     (e.g. "the list of renewable generators connected to an
     electricity bus").
  2) **The model** — equations (power balance, storage dynamics,
     ramping, ...) that the optimiser solves.

Mixing (1) and (2) in the same file made the old ``dispatch_electricity.py``
hard to read: you had to skip 200 lines of data wrangling before you
saw the first constraint. Splitting them lets a non-coder read the
equations without being distracted by Pandas-like plumbing.

What this file produces
-----------------------
A :class:`PowerData` dataclass with:

  * the sets (which units belong to which group),
  * the topology (which units sit on which bus),
  * the numbers (capacity, cost, efficiency, ...),
  * the demand and renewable profiles,
  * the investment upper bounds.

Investment decisions are *always* enabled in ``optimization_2`` — so
there is no ``allow_investment`` flag to fiddle with. If a unit has
no room to expand (``p_nom_max == p_nom``), the investment variable
is still created but its upper bound is zero, so Gurobi simply
ignores it.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from data_models.SystemParameters import SystemParameters


# -------------------------------------------------------------------
# Short aliases for the kinds of indices we use everywhere
# -------------------------------------------------------------------
U = str   # unit id          — "CCGT_DE", "WindPP_FR", ...
P = int   # period index     — 1..8760 (hours in a year)
Yr = int  # year index       — 2020, 2025, 2030, ...
B = str   # bus id           — "elec_DE", "heat_DH_DE", ...

UTY = Tuple[U, P, Yr]   # (unit, period, year)
BTY = Tuple[B, P, Yr]   # (bus,  period, year)


# -------------------------------------------------------------------
# Tiny helper: clean up bus names coming from CSVs
#
# Pandas sometimes gives us NaN (a float that is "not a number") when
# a cell was empty. We never want NaN to flow into a bus map — so we
# substitute a fallback string.
# -------------------------------------------------------------------
def _clean_bus(value: object, fallback: str) -> str:
    if value is None:
        return fallback
    try:
        if value != value:  # float NaN is the only thing != itself
            return fallback
    except Exception:
        pass
    text = str(value)
    return fallback if text in ("", "nan", "None") else text


# ===================================================================
# PowerData — everything the power-sector model needs, in one place
# ===================================================================
@dataclass
class PowerData:
    """A plain container of sets, parameters and demand for the power LP.

    Think of it as a "spreadsheet view" of the system, pre-filtered to
    the electricity layer and ready to plug into the model.

    You normally do not build this by hand — use :meth:`from_system`.
    """

    # -- Raw reference plus time/economics settings ------------------
    system: SystemParameters
    periods: List[P]           # time periods to optimise (e.g. 1..24)
    years: List[Yr]            # years to optimise
    dt: float = 1.0            # hours per period (1 h by default)
    voll: float = 3000.0       # € / MWh penalty for unserved load
    default_spillage_cost: float = 0.0
    discount_rate: float = 0.05
    default_lifetime: float = 20.0
    # Annualised capex is pro-rated by this factor so that a 24 h slice
    # does not "pay" a full year's capex. When None, we use
    #   capex_scale = (periods · dt) / 8760
    capex_scale: Optional[float] = None

    # -- Sets (filled by the factory) --------------------------------
    elec_buses: List[B] = field(default_factory=list)
    G: List[U] = field(default_factory=list)        # power generators on an elec bus
    G_res: List[U] = field(default_factory=list)    # wind / solar / run-of-river
    G_ramp: List[U] = field(default_factory=list)   # thermal + CHP (have ramp limits)
    H: List[U] = field(default_factory=list)        # hydro reservoirs
    H_pump: List[U] = field(default_factory=list)   # pumped-hydro reservoirs
    S: List[U] = field(default_factory=list)        # pure storage (BESS, H2 tanks, ...)

    # -- Who sits where ----------------------------------------------
    bus_of: Dict[U, B] = field(default_factory=dict)     # generator → its bus
    dis_bus: Dict[U, B] = field(default_factory=dict)    # storage   → discharge bus
    ch_bus: Dict[U, B] = field(default_factory=dict)     # storage   → charge bus
    gens_at: Dict[B, List[U]] = field(default_factory=dict)
    S_dis_at: Dict[B, List[U]] = field(default_factory=dict)
    S_ch_at: Dict[B, List[U]] = field(default_factory=dict)
    H_ch_at: Dict[B, List[U]] = field(default_factory=dict)

    # -- Techno-economic numbers (per unit) --------------------------
    p_nom: Dict[U, float] = field(default_factory=dict)         # installed MW
    p_nom_max: Dict[U, float] = field(default_factory=dict)     # MW cap on p_nom + new build
    ramp_up: Dict[U, float] = field(default_factory=dict)
    ramp_dn: Dict[U, float] = field(default_factory=dict)
    vc_no_fuel: Dict[U, float] = field(default_factory=dict)    # €/MWh (non-fuel)

    e_nom: Dict[U, float] = field(default_factory=dict)         # installed MWh
    e_min: Dict[U, float] = field(default_factory=dict)
    e_nom_max: Dict[U, float] = field(default_factory=dict)     # MWh upper bound
    eta_ch: Dict[U, float] = field(default_factory=dict)        # charge efficiency
    eta_dis: Dict[U, float] = field(default_factory=dict)       # discharge efficiency
    loss: Dict[U, float] = field(default_factory=dict)          # standby loss per period
    pc_cap_S: Dict[U, float] = field(default_factory=dict)      # charge MW   (pure storage)
    pd_cap_S: Dict[U, float] = field(default_factory=dict)      # discharge MW (pure storage)
    pc_cap_H: Dict[U, float] = field(default_factory=dict)      # pump MW     (HPHS only)
    spill_cost: Dict[U, float] = field(default_factory=dict)    # €/MWh hydro spillage

    # Duration-based sizing: MWh of storage = duration * MW of power.
    dur_ch: Dict[U, float] = field(default_factory=dict)
    dur_dis: Dict[U, float] = field(default_factory=dict)

    # -- Investment numbers ------------------------------------------
    gen_life: Dict[U, float] = field(default_factory=dict)
    gen_capex: Dict[U, float] = field(default_factory=dict)     # €/MW
    sto_life: Dict[U, float] = field(default_factory=dict)
    cap_cost_E: Dict[U, float] = field(default_factory=dict)    # €/MWh
    cap_cost_Pch: Dict[U, float] = field(default_factory=dict)  # €/MW (charge)
    cap_cost_Pdis: Dict[U, float] = field(default_factory=dict) # €/MW (discharge)

    # -- Demand already aggregated to the electricity buses ----------
    demand: Dict[BTY, float] = field(default_factory=dict)

    # -----------------------------------------------------------
    # Convenience sets used by the model
    # -----------------------------------------------------------
    @property
    def S_charge(self) -> List[U]:
        """Anything with a state-of-charge (pure storage + hydro reservoirs)."""
        return self.S + self.H

    @property
    def hphs_set(self) -> set:
        """Set of pumped-hydro reservoir IDs (= those that actually have a pump)."""
        return set(self.system.sets.hphs_units)

    # ===============================================================
    # Factory — the one-stop way to build a PowerData from a system
    # ===============================================================
    @classmethod
    def from_system(
        cls,
        system: SystemParameters,
        *,
        years: Optional[Iterable[int]] = None,
        periods: Optional[Iterable[int]] = None,
        dt: float = 1.0,
        voll: float = 3000.0,
        default_spillage_cost: float = 0.0,
        discount_rate: float = 0.05,
        default_lifetime: float = 20.0,
        capex_scale: Optional[float] = None,
    ) -> "PowerData":
        """Read the system and produce a ready-to-use :class:`PowerData`.

        This function does three jobs, one after the other:

          1. **Figure out who is who** — which buses are electricity
             buses, which generators are renewables, which storages are
             hydro, etc.
          2. **Fill the lookup tables** — for each unit: what bus, what
             cost, what capacity, what investment ceiling.
          3. **Aggregate demand** — the system stores demand keyed by
             (system, region, bus, carrier, t, y); we keep only the
             electricity entries on the buses and time steps we care
             about.
        """
        years_list = list(years) if years is not None else list(system.sets.years)
        periods_list = sorted(
            periods if periods is not None else system.sets.periods
        )

        gens, sto, sets = system.generators, system.storage, system.sets

        # --- 1a) Electricity buses ---------------------------------
        elec_buses: List[B] = [
            b for b in system.bus.name
            if str(system.bus.carrier.get(b, "")).lower() == "electricity"
        ]
        bus_set = set(elec_buses)

        # --- 1b) Generators wired to the electricity grid ----------
        G: List[U] = [
            u for u in gens.unit
            if str(gens.carrier_out.get(u, "")).lower() == "electricity"
            and gens.bus_out.get(u) in bus_set
        ]

        # Useful subsets used by different constraints.
        hydro_set = set(sets.hydro_storage_units)
        hphs_set = set(sets.hphs_units)
        res_set = set(sets.wind_units) | set(sets.solar_units) | set(sets.hror_units)
        ramp_set = (
            set(sets.fossil_units) | set(sets.biomass_units) | set(sets.chp_units)
        )

        G_res = [u for u in G if u in res_set]
        G_ramp = [u for u in G if u in ramp_set]

        # --- 1c) Storage sets --------------------------------------
        # A hydro reservoir is a storage that has a matching generator
        # (a turbine) producing electricity. HDAM = dam only, HPHS =
        # dam + pump.
        H: List[U] = [u for u in G if u in hydro_set and u in sto.unit]
        H_pump = [s for s in H if s in hphs_set]

        # Pure storage: anything that isn't a hydro reservoir and whose
        # output is electricity.
        S: List[U] = [
            s for s in sto.unit
            if s not in hydro_set
            and str(sto.carrier_out.get(s, "")).lower() == "electricity"
            and _clean_bus(sto.bus_out.get(s), "") in bus_set
        ]

        # --- 2) Topology: who sits on which bus --------------------
        bus_of: Dict[U, B] = {u: str(gens.bus_out[u]) for u in G}

        dis_bus: Dict[U, B] = {}
        ch_bus: Dict[U, B] = {}

        for s in S:
            # Pure storage sends its output at bus_out and takes its
            # input at bus_in (default: same bus).
            out_b = _clean_bus(sto.bus_out.get(s), "")
            dis_bus[s] = out_b
            ch_bus[s] = _clean_bus(sto.bus_in.get(s), out_b)

        for s in H:
            # For hydro, the turbine's bus is the generator's bus_out.
            # Pumped-hydro takes power FROM the grid at that same bus;
            # plain dams (HDAM) have no pump.
            turbine_b = str(gens.bus_out.get(s, ""))
            dis_bus[s] = _clean_bus(sto.bus_out.get(s), turbine_b)
            ch_bus[s] = (
                _clean_bus(sto.bus_in.get(s), turbine_b) if s in hphs_set else ""
            )

        # Bus → list-of-units lookups (used by the power balance)
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

        # --- 3) Pre-fetch every scalar with a safe fallback --------
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
        pc_cap_H = {
            s: float(sto.p_charge_nom.get(s, 0.0)) if s in hphs_set else 0.0
            for s in H
        }
        spill_cost = {
            s: float(sto.spillage_cost.get(s, default_spillage_cost)) for s in H
        }

        # Investment ceilings. If no p_nom_max is given in the data we
        # default to p_nom (= no room to grow; investment variable ub = 0).
        p_nom_max = {
            u: max(p_nom[u], float(gens.p_nom_max.get(u, p_nom[u]))) for u in G
        }
        e_nom_max = {
            s: max(e_nom[s], float(sto.e_nom_max.get(s, e_nom[s]))) for s in S + H
        }
        gen_life = {u: float(gens.lifetime.get(u, default_lifetime)) for u in G}
        gen_capex = {u: float(gens.capital_cost.get(u, 0.0)) for u in G}
        sto_life = {s: float(sto.lifetime.get(s, default_lifetime)) for s in S + H}
        cap_cost_E = {s: float(sto.capital_cost_energy.get(s, 0.0)) for s in S + H}
        cap_cost_Pch = {
            s: float(sto.capital_cost_power_charge.get(s, 0.0)) for s in S + H
        }
        cap_cost_Pdis = {
            s: float(sto.capital_cost_power_discharge.get(s, 0.0)) for s in S + H
        }

        # Duration ratio: 1 MWh of storage ↔ (1/duration) MW of power.
        # A minimum of 1e-6 avoids divide-by-zero if the CSV is empty.
        dur_ch = {
            s: max(1e-6, float(sto.duration_charge.get(s, 1.0))) for s in S + H
        }
        dur_dis = {
            s: max(1e-6, float(sto.duration_discharge.get(s, 1.0))) for s in S + H
        }

        # --- 4) Demand: keep only electricity, aggregated by bus ---
        demand: Dict[BTY, float] = defaultdict(float)
        t_set, y_set = set(periods_list), set(years_list)
        for (_sysn, _reg, b, carrier, t, y), val in system.demand.p_t.items():
            if (
                str(carrier).lower() == "electricity"
                and b in bus_set
                and t in t_set
                and y in y_set
            ):
                demand[(b, t, y)] += float(val)

        # --- Done --------------------------------------------------
        return cls(
            system=system,
            periods=periods_list,
            years=years_list,
            dt=float(dt),
            voll=float(voll),
            default_spillage_cost=float(default_spillage_cost),
            discount_rate=float(discount_rate),
            default_lifetime=float(default_lifetime),
            capex_scale=None if capex_scale is None else float(capex_scale),
            elec_buses=elec_buses,
            G=G, G_res=G_res, G_ramp=G_ramp,
            H=H, H_pump=H_pump, S=S,
            bus_of=bus_of, dis_bus=dis_bus, ch_bus=ch_bus,
            gens_at=dict(gens_at),
            S_dis_at=dict(S_dis_at),
            S_ch_at=dict(S_ch_at),
            H_ch_at=dict(H_ch_at),
            p_nom=p_nom, p_nom_max=p_nom_max,
            ramp_up=ramp_up, ramp_dn=ramp_dn,
            vc_no_fuel=vc_no_fuel,
            e_nom=e_nom, e_min=e_min, e_nom_max=e_nom_max,
            eta_ch=eta_ch, eta_dis=eta_dis, loss=loss,
            pc_cap_S=pc_cap_S, pd_cap_S=pd_cap_S, pc_cap_H=pc_cap_H,
            spill_cost=spill_cost,
            dur_ch=dur_ch, dur_dis=dur_dis,
            gen_life=gen_life, gen_capex=gen_capex,
            sto_life=sto_life,
            cap_cost_E=cap_cost_E,
            cap_cost_Pch=cap_cost_Pch,
            cap_cost_Pdis=cap_cost_Pdis,
            demand=dict(demand),
        )
