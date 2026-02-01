| Attribute | Type | Unit | Description | Status |
| --- | --- | --- | --- | --- |
| `name` | List[str] | n.a. | All buses in the system. | optional |
| `system` | Dict[str, str] | n.a. | System/country tag for each bus. | optional |
| `region` | Dict[str, str] | n.a. | Region/zone for each bus. | optional |
| `carrier` | Dict[str, str] | n.a. | Carrier assigned to each bus. | optional |
| `generators_at_bus` | Dict[str, List[str]] | n.a. | Generator/converter units on each bus. | optional |
| `storage_at_bus` | Dict[str, List[str]] | n.a. | Storage units on each bus. | optional |
