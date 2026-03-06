| Table | Index | Column | Type | Unit | Status | Description |
| --- | --- | --- | --- | --- | --- | --- |
| `static` | unit | `unit` | string | n.a. | mandatory | Generator/converter unit identifier. |
| `static` | unit | `system` | string | n.a. | optional | System/scenario tag. |
| `static` | unit | `region` | string | n.a. | optional | Region/zone identifier. |
| `static` | unit | `tech` | string | n.a. | mandatory | Technology label. |
| `static` | unit | `fuel` | string | n.a. | mandatory | Fuel type. |
| `static` | unit | `unit_type` | string | n.a. | optional | Supply/conversion role. |
| `static` | unit | `carrier_in` | string | n.a. | optional | Input carrier. |
| `static` | unit | `carrier_out` | string | n.a. | optional | Output carrier. |
| `static` | unit | `bus_in` | string | n.a. | optional | Input bus. |
| `static` | unit | `bus_out` | string | n.a. | optional | Output bus. |
| `static` | unit | `bus_out_2` | string | n.a. | optional | Secondary output bus. |
| `static` | unit | `carrier_out_2` | string | n.a. | optional | Secondary output carrier. |
| `static` | unit | `p_nom` | float | MW | mandatory | Existing output power capacity. |
| `static` | unit | `p_nom_max` | float | MW | optional | Maximum output power capacity. |
| `static` | unit | `cap_factor` | float | p.u. | mandatory | Capacity factor. |
| `static` | unit | `capital_cost` | float | EUR/MW | mandatory | Power investment cost. |
| `static` | unit | `lifetime` | int | years | mandatory | Technical/economic lifetime. |
| `static` | unit | `decom_start_existing` | int | year | mandatory | Existing decommissioning start year. |
| `static` | unit | `decom_start_new` | int | year | mandatory | New-build decommissioning start year. |
| `static` | unit | `final_cap` | float | MW | mandatory | Residual power capacity at end of horizon. |
| `static` | unit | `efficiency` | float | p.u. | mandatory | Static efficiency. |
| `static` | unit | `co2_intensity` | float | tCO2/MWh_output | mandatory | CO2 intensity on output. |
| `static` | unit | `var_cost_no_fuel` | float | EUR/MWh_output | mandatory | Non-fuel variable cost. |
| `static` | unit | `ramp_up_rate` | float | MW/period or p.u. | mandatory | Ramp-up rate. |
| `static` | unit | `ramp_down_rate` | float | MW/period or p.u. | mandatory | Ramp-down rate. |
| `static` | unit | `ramping_cost` | float | n.a. | optional | Ramping cost coefficient. |
| `static` | unit | `chp_power_to_heat` | float | p.u. | optional | CHP power-to-heat ratio. |
| `static` | unit | `chp_power_loss_factor` | float | p.u. | optional | CHP condensing-to-heat slope. |
| `static` | unit | `chp_max_heat` | float | MW_heat | optional | Maximum CHP heat output. |
| `static` | unit | `chp_type` | string | n.a. | optional | CHP configuration type. |
| `p_t` | - | `unit` | string | n.a. | mandatory | Generator/converter unit identifier. |
| `p_t` | - | `period` | int | index | mandatory | Time period index. |
| `p_t` | - | `year` | int | year | mandatory | Model year. |
| `p_t` | - | `p_t` | float | p.u. | mandatory | Availability/profile value. |
| `var_cost` | - | `unit` | string | n.a. | mandatory | Generator/converter unit identifier. |
| `var_cost` | - | `period` | int | index | mandatory | Time period index. |
| `var_cost` | - | `year` | int | year | mandatory | Model year. |
| `var_cost` | - | `var_cost` | float | EUR/MWh_output | mandatory | Full variable cost. |
| `efficiency_ts` | - | `unit` | string | n.a. | mandatory | Generator/converter unit identifier. |
| `efficiency_ts` | - | `period` | int | index | mandatory | Time period index. |
| `efficiency_ts` | - | `year` | int | year | mandatory | Model year. |
| `efficiency_ts` | - | `efficiency_ts` | float | p.u. | mandatory | Time-varying efficiency. |
| `field` | - | `dynamic` | Dict[str, DataFrame] | table-map | optional | Separate time-varying generator tables keyed by attribute name. |
