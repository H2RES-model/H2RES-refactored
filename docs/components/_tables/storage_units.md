| Table | Index | Column | Type | Unit | Status | Description |
| --- | --- | --- | --- | --- | --- | --- |
| `static` | unit | `unit` | string | n.a. | mandatory | Storage unit identifier. |
| `static` | unit | `system` | string | n.a. | optional | System/scenario tag. |
| `static` | unit | `region` | string | n.a. | optional | Region/zone identifier. |
| `static` | unit | `tech` | string | n.a. | optional | Storage technology. |
| `static` | unit | `carrier_in` | string | n.a. | optional | Input carrier. |
| `static` | unit | `carrier_out` | string | n.a. | optional | Output carrier. |
| `static` | unit | `bus_in` | string | n.a. | optional | Charging bus. |
| `static` | unit | `bus_out` | string | n.a. | optional | Discharging bus. |
| `static` | unit | `e_nom` | float | MWh | optional | Existing energy capacity. |
| `static` | unit | `e_min` | float | MWh | optional | Minimum energy level. |
| `static` | unit | `e_nom_max` | float | MWh | optional | Maximum energy capacity. |
| `static` | unit | `p_charge_nom` | float | MW | optional | Charge power limit. |
| `static` | unit | `p_charge_nom_max` | float | MW | optional | Maximum charge power. |
| `static` | unit | `p_discharge_nom` | float | MW | optional | Discharge power limit. |
| `static` | unit | `p_discharge_nom_max` | float | MW | optional | Maximum discharge power. |
| `static` | unit | `duration_charge` | float | hours | optional | Charge duration. |
| `static` | unit | `duration_discharge` | float | hours | optional | Discharge duration. |
| `static` | unit | `efficiency_charge` | float | p.u. | optional | Charge efficiency. |
| `static` | unit | `efficiency_discharge` | float | p.u. | optional | Discharge efficiency. |
| `static` | unit | `standby_loss` | float | p.u. | optional | Standing loss. |
| `static` | unit | `capital_cost_energy` | float | EUR/MWh | optional | Energy capacity capex. |
| `static` | unit | `capital_cost_power_charge` | float | EUR/MW | optional | Charge power capex. |
| `static` | unit | `capital_cost_power_discharge` | float | EUR/MW | optional | Discharge power capex. |
| `static` | unit | `lifetime` | int | years | optional | Technical/economic lifetime. |
| `static` | unit | `spillage_cost` | float | EUR/MWh | optional | Spillage cost. |
| `inflow` | - | `unit` | string | n.a. | mandatory | Storage unit identifier. |
| `inflow` | - | `period` | int | index | mandatory | Time period index. |
| `inflow` | - | `year` | int | year | mandatory | Model year. |
| `inflow` | - | `inflow` | float | MWh/period | mandatory | Exogenous inflow. |
| `availability` | - | `unit` | string | n.a. | mandatory | Storage unit identifier. |
| `availability` | - | `period` | int | index | mandatory | Time period index. |
| `availability` | - | `year` | int | year | mandatory | Model year. |
| `availability` | - | `availability` | float | p.u. | mandatory | Availability factor. |
| `e_nom_ts` | - | `unit` | string | n.a. | mandatory | Storage unit identifier. |
| `e_nom_ts` | - | `period` | int | index | mandatory | Time period index. |
| `e_nom_ts` | - | `year` | int | year | mandatory | Model year. |
| `e_nom_ts` | - | `e_nom_ts` | float | MWh | mandatory | Time-varying effective energy capacity. |
| `investment_costs` | - | `unit` | string | n.a. | mandatory | Storage unit identifier. |
| `investment_costs` | - | `year` | int | year | mandatory | Model year. |
| `investment_costs` | - | `e_nom_inv_cost` | float | EUR/MWh | mandatory | Specific energy investment cost. |
| `field` | - | `dynamic` | Dict[str, DataFrame] | table-map | optional | Separate time-varying storage tables keyed by attribute name. |
