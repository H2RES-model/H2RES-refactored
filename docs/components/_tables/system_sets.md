| Attribute | Type | Unit | Description | Status |
| --- | --- | --- | --- | --- |
| `years` | List[int] | year | Model years in the planning horizon. | mandatory |
| `periods` | List[int] | index | Time periods within a year (e.g., hours). | mandatory |
| `carriers` | List[str] | n.a. | Energy carriers (e.g. Electricity, Heat, H2). | optional |
| `buses` | List[str] | n.a. | Network buses; single-node version uses one bus. | optional |
| `units` | List[str] | n.a. | All generator / converter units (CCGT, WindPP, PV, CHP, HP, etc.). | optional |
| `storage_units` | List[str] | n.a. | All storage units (battery, TES, H2, HDAM, HPHS, ...). | optional |
| `fossil_units` | List[str] | n.a. |  | optional |
| `biomass_units` | List[str] | n.a. |  | optional |
| `hror_units` | List[str] | n.a. |  | optional |
| `wind_units` | List[str] | n.a. |  | optional |
| `solar_units` | List[str] | n.a. |  | optional |
| `chp_units` | List[str] | n.a. |  | optional |
| `ncre_units` | List[str] | n.a. | Optional group for non-conventional renewables (e.g. wind, solar, biomass, RoR hydro). | optional |
| `disp_units` | List[str] | n.a. |  | optional |
| `nondisp_units` | List[str] | n.a. |  | optional |
| `hydro_storage_units` | List[str] | n.a. | Hydro storage assets (HDAM, HPHS, etc.). | optional |
| `hdam_units` | List[str] | n.a. | Hydro dam storage units (tech == 'HDAM'). | optional |
| `hphs_units` | List[str] | n.a. | Pumped hydro storage units (tech == 'HPHS'). | optional |
| `battery_units` | List[str] | n.a. |  | optional |
| `tes_units` | List[str] | n.a. |  | optional |
| `hydrogen_storage_units` | List[str] | n.a. |  | optional |
| `demand_sectors` | List[str] | n.a. | Logical demand sectors (e.g. Electricity, Industry, Transport). | optional |
