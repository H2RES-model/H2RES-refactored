"""H2RES optimization package (v2) — cleaner split & heat sector.

What's new in ``optimization_2`` vs. the original ``optimization``
------------------------------------------------------------------
1. **Data preparation is separated from model building.**
   All the bus/subset discovery, parameter fetching, demand
   aggregation and investment-bound logic lives in
   :mod:`data_prep_power` / :mod:`data_prep_heat` (returning a
   :class:`PowerData` / :class:`HeatData` snapshot). Model
   construction lives in :mod:`model_power` / :mod:`model_heat`. Each
   file stays short, focused and easy to read.

2. **Investment decisions are always on.** The model ALWAYS creates
   new-capacity variables (``cap_G`` for generators, ``cap_E`` for
   storage energy, ``cap_Gh``/``cap_Eh`` on the heat side). If the
   CSV data has no room to expand, the variable's upper bound is
   just 0 and it contributes nothing.

3. **New heat-supply layer.** :mod:`data_prep_heat` and
   :mod:`model_heat` model heat production for housing, District
   Heating and Industry. Heat can come from fuel / H2 / e-fuel
   boilers (fuel cost exogenous — no upstream pathway modelled yet),
   from electricity-to-heat converters (heat pumps, e-boilers), or
   from CHP heat byproduct. Heat storage (TES) is included.

4. **Power and heat are ALWAYS solved together.** The sector classes
   (:class:`PowerSector`, :class:`HeatSector`) only know how to add
   variables and constraints to a shared Gurobi model. Only
   :class:`CoupledModel` in :mod:`model_coupled` actually creates a
   model and runs Gurobi — so the two sectors are always optimised
   jointly as a single LP.

Recommended entry point
-----------------------
>>> from optimization_2 import CoupledModel, PowerData, HeatData
>>> pdata  = PowerData.from_system(system, years=[2020], periods=list(range(1, 25)))
>>> hdata  = HeatData .from_system(system, years=[2020], periods=list(range(1, 25)))
>>> cmodel = CoupledModel(pdata, hdata)
>>> cmodel.build()
>>> result = cmodel.solve(OutputFlag=0)
>>> print(cmodel.summary(result))
"""

from optimization_2.data_prep_power import PowerData
from optimization_2.data_prep_heat import HeatData
from optimization_2.model_power import PowerResults, PowerSector
from optimization_2.model_heat import HeatResults, HeatSector
from optimization_2.model_coupled import CoupledModel, CoupledResults

__all__ = [
    # data containers
    "PowerData",
    "HeatData",
    # sector builders (used only by CoupledModel)
    "PowerSector",
    "HeatSector",
    # result dataclasses
    "PowerResults",
    "HeatResults",
    "CoupledResults",
    # single-entry-point coupled runner
    "CoupledModel",
]
