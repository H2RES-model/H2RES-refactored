| Attribute | Type | Unit | Description | Status |
| --- | --- | --- | --- | --- |
| `unit` | List[str] | n.a. | Name of the storage assets (hydro reservoirs, PHS, batteries, TES, H2 tanks, ...). | optional |
| `system` | Dict[str, str] | n.a. | System/scenario tag (column 'system' in storage_units.csv), e.g. country code. | optional |
| `region` | Dict[str, str] | n.a. | Region/zone for the storage asset (column 'region' in storage_units.csv). | optional |
| `tech` | Dict[str, str] | n.a. | Storage technology label (e.g. 'HDAM', 'HPHS', 'BESS', 'TES', 'H2_tank'). | optional |
| `carrier_in` | Dict[str, str] | n.a. | Input energy carrier (e.g. 'Electricity', 'Heat', 'H2'). | optional |
| `carrier_out` | Dict[str, str] | n.a. | Output energy carrier (e.g. 'Electricity', 'Heat', 'H2'). | optional |
| `bus_in` | Dict[str, str] | n.a. | Bus where storage charge is connected (e.g. 'SystemBus'). | optional |
| `bus_out` | Dict[str, str] | n.a. | Bus where storage discharge is connected (e.g. 'SystemBus'). | optional |
| `e_nom` | Dict[str, float] | MWh | Existing/committed energy capacity [MWh]. | optional |
| `e_min` | Dict[str, float] | MWh | Minimum allowed energy level during operation[MWh]. | optional |
| `e_nom_max` | Dict[str, float] | MWh | Maximum allowed energy capacity to be installed [MWh] (e.g. = e_nom for non-expandable hydro). | optional |
| `p_charge_nom` | Dict[str, float] | MW | Maximum charging power [MW] into storage (physical limit). | optional |
| `p_charge_nom_max` | Dict[str, float] | MW | Maximum charging capacity[MW] that can be installed. | optional |
| `p_discharge_nom` | Dict[str, float] | MW | Maximum discharging power [MW] from storage (physical limit). | optional |
| `p_discharge_nom_max` | Dict[str, float] | MW | Maximum discharging capacity [MW] that can be installed. | optional |
| `duration_charge` | Dict[str, float] | hours | Charge duration [h]; combined with e_nom to derive p_charge_nom when templates use duration. | optional |
| `duration_discharge` | Dict[str, float] | hours | Discharge duration [h]; combined with e_nom to derive p_discharge_nom when templates use duration. | optional |
| `efficiency_charge` | Dict[str, float] | p.u. | Charging efficiency (fraction of input power stored). | optional |
| `efficiency_discharge` | Dict[str, float] | p.u. | Discharging efficiency (fraction of stored energy delivered). | optional |
| `standby_loss` | Dict[str, float] | p.u. | Fractional standing loss of stored energy per model period. | optional |
| `capital_cost_energy` | Dict[str, float] | EUR/MWh | Investment cost per unit of energy capacity [€/MWh]. | optional |
| `capital_cost_power_charge` | Dict[str, float] | EUR/MW | Investment cost per unit of charge power [€/MW]. | optional |
| `capital_cost_power_discharge` | Dict[str, float] | EUR/MW | Investment cost per unit of discharge power [€/MW]. | optional |
| `lifetime` | Dict[str, int] | years | Technical/economic lifetime of storage asset [years]. | optional |
| `inflow` | Dict[Tuple[str, int, int], float] | MWh/period | Exogenous inflow to storage [MWh/period] by (unit, period, year), e.g. hydro inflows. | optional |
| `spillage_cost` | Dict[str, float] | EUR/MWh | Penalty cost for spilling energy [€/MWh], if modelled. | optional |
| `e_nom_inv_cost` | Dict[Tuple[str, int], float] | EUR/MWh | Specific investment cost for energy capacity [€/MWh] per (unit, year). | optional |
