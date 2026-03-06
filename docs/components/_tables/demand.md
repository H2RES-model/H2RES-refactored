| Table | Index | Column | Type | Unit | Status | Description |
| --- | --- | --- | --- | --- | --- | --- |
| `timeseries` | - | `system` | string | n.a. | optional | System/scenario identifier. |
| `timeseries` | - | `region` | string | n.a. | optional | Region/zone identifier. |
| `timeseries` | - | `bus` | string | n.a. | mandatory | Demand bus identifier. |
| `timeseries` | - | `carrier` | string | n.a. | mandatory | Demand carrier. |
| `timeseries` | - | `period` | int | index | mandatory | Time period index. |
| `timeseries` | - | `year` | int | year | mandatory | Model year. |
| `timeseries` | - | `p_t` | float | MWh/period | mandatory | Demand value. |
