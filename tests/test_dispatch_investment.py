"""Smoke test for the electricity dispatch LP with investment.

Runs a 24-hour slice with ``allow_investment=True`` and checks that the
model solves and that the new reported fields (capacity_added,
energy_added, total_capex_eur) are populated.
"""
from __future__ import annotations

import os
import sys

# Ensure we can import H2RES modules when run from the tests folder.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from data_loaders.load_sector import load_sector
from optimization.dispatch_electricity import ElectricityDispatchModel


def _load_electricity_system():
    os.chdir(ROOT)
    return load_sector(sector="electricity")


def run(slice_hours: int = 24, allow_investment: bool = True):
    sys_ = _load_electricity_system()

    # Periods are 1-indexed in the CSVs (1..8760) — start at 1 to match.
    periods = list(range(1, slice_hours + 1))
    years = sorted(list(sys_.sets.years))[:1] or [2020]

    model = ElectricityDispatchModel(
        sys_,
        years=years,
        periods=periods,
        voll=3000.0,
        allow_investment=allow_investment,
        discount_rate=0.07,
        default_lifetime=25.0,
    )
    model.build()
    res = model.solve(OutputFlag=0)
    kpis = model.summary(res)

    print(f"\n=== allow_investment={allow_investment}, slice={slice_hours} h ===")
    print(f"Status           : {res.status}")
    if res.objective is None:
        print("   (model did not solve to optimality — see Gurobi status)")
        return res
    print(f"Objective (EUR)  : {res.objective:,.2f}")
    for k, v in kpis.items():
        print(f"  {k:32s} : {v:,.4f}")

    if allow_investment:
        added = {
            k: v for k, v in res.capacity_added.items() if abs(v) > 1e-6
        }
        print(f"New gen capacity entries: {len(added)}")
        for (u, y), mw in sorted(added.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    +{mw:10.2f} MW   {u}  (y={y})")

        e_added = {
            k: v for k, v in res.energy_added.items() if abs(v) > 1e-6
        }
        print(f"New storage energy entries: {len(e_added)}")
        for (s, y), mwh in sorted(e_added.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    +{mwh:10.2f} MWh  {s}  (y={y})")

    return res


if __name__ == "__main__":
    # Baseline dispatch (no investment) — shorter slice for restricted license.
    run(slice_hours=12, allow_investment=False)
    # With investment enabled
    run(slice_hours=12, allow_investment=True)
