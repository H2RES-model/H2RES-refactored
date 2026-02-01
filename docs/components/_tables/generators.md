| Attribute | Type | Unit | Description | Status |
| --- | --- | --- | --- | --- |
| `unit` | List[str] | n.a. | All generator / converter units in the system. | mandatory |
| `system` | Dict[str, str] | n.a. | System/scenario tag (column 'system' in powerplants.csv), e.g. country code. | optional |
| `region` | Dict[str, str] | n.a. | Region/zone for the unit (column 'region' in powerplants.csv). | optional |
| `tech` | Dict[str, str] | n.a. | Technology label (e.g. 'CCGT', 'WindPP', 'PV', 'CHP', 'HDAM_turbine', 'HPHS_pump'). | mandatory |
| `fuel` | Dict[str, str] | n.a. | Fuel type (e.g. 'Gas', 'Coal', 'Wind', 'Solar', 'Water', 'Electricity'). | mandatory |
| `unit_type` | Dict[str, typing.Literal['supply', 'conversion']] | n.a. | Modelling role of the unit: 'supply' = fuel→output with implicit input carrier; 'conversion' = explicit carrier_in→carrier_out (e.g. HP, ETES pump, PHS pump). | optional |
| `carrier_in` | Dict[str, Union[str, NoneType]] | n.a. | Input carrier for conversion units (e.g. 'Electricity' for HP, ETES pumps). | optional |
| `carrier_out` | Dict[str, str] | n.a. | Output carrier (e.g. 'Electricity', 'Heat', 'H2'). | optional |
| `bus_in` | Dict[str, Union[str, NoneType]] | n.a. | Bus where input power is drawn (for conversion units). | optional |
| `bus_out` | Dict[str, str] | n.a. | Bus where output power is injected. | optional |
| `p_nom` | Dict[str, float] | MW | Existing/committed nominal output power capacity [MW]. | mandatory |
| `p_nom_max` | Dict[str, float] | MW | Maximum allowed power capacity [MW] (upper bound on investment). | optional |
| `cap_factor` | Dict[str, float] | p.u. | Capacity factor of unit. | mandatory |
| `capital_cost` | Dict[str, float] | EUR/MW | Investment cost per unit of power capacity [€/MW]. | mandatory |
| `lifetime` | Dict[str, int] | years | Technical/economic lifetime of the power asset [years]. | mandatory |
| `decom_start_existing` | Dict[str, int] | year | Year when existing capacity starts decommissioning. | mandatory |
| `decom_start_new` | Dict[str, int] | year | Year when newly built capacity starts decommissioning. | mandatory |
| `final_cap` | Dict[str, float] | MW | Residual power capacity at end of horizon [MW]. | mandatory |
| `efficiency` | Dict[str, float] | p.u. | Static efficiency per unit (output/input). For fuel-based generators, fuel→power; for HP, power→heat, etc. | mandatory |
| `efficiency_ts` | Dict[Tuple[str, int, int], float] | p.u. | Time-varying efficiency (e.g. COP) by (unit, period, year). | optional |
| `co2_intensity` | Dict[str, float] | tCO2/MWh_output | CO2 intensity attributed to output [tCO2/MWh_output]. | mandatory |
| `var_cost_no_fuel` | Dict[str, float] | EUR/MWh_output | Non-fuel variable O&M cost [€/MWh_output]. | mandatory |
| `ramp_up_rate` | Dict[str, float] | MW/period or p.u. | Maximum ramp-up rate [MW/period or pu]. | mandatory |
| `ramp_down_rate` | Dict[str, float] | MW/period or p.u. | Maximum ramp-down rate [MW/period or pu]. | mandatory |
| `ramping_cost` | Dict[str, float] | n.a. | Ramping cost coefficient (optional). | optional |
| `chp_power_to_heat` | Dict[str, float] | p.u. | Back-pressure power-to-heat ratio for CHP units. | optional |
| `chp_power_loss_factor` | Dict[str, float] | p.u. | Slope of condensing-to-heat trade-off for extraction CHP. | optional |
| `chp_max_heat` | Dict[str, float] | MW_heat | Maximum thermal output [MW_heat] for CHP units. | optional |
| `chp_type` | Dict[str, str] | n.a. | CHP configuration label (e.g. 'backpressure', 'extraction'). | optional |
| `bus_out_2` | Dict[str, Union[str, NoneType]] | n.a. | Second output bus, e.g. to represent heat output of CHP units. | optional |
| `carrier_out_2` | Dict[str, Union[str, NoneType]] | n.a. | Carrier of second output, e.g. heat for CHP units. | optional |
| `p_t` | Dict[Tuple[str, int, int], float] | p.u. | Normalized profile (0–1) by (unit, period, year), typically for renewables or availability limits. | optional |
| `var_cost` | Dict[Tuple[str, int, int], float] | EUR/MWh_output | Full variable cost [€/MWh_output] including fuel, by (unit, period, year). Typically built from fuel_price(fuel, t) and efficiency. | optional |
