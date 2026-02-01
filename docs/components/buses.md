# Buses

The `Bus` model represents network nodes and their assigned energy carrier.
Buses also keep track of which generator and storage units are connected.

## Where it lives

- Model: `data_models/Bus.py`
- Loader: `data_loaders/load_bus.py`

## Key fields

- `name`: list of all bus IDs.
- `system`: optional system/country tag for each bus.
- `region`: optional region/zone for each bus.
- `carrier`: carrier assigned to each bus (e.g. Electricity, Heat).
- `generators_at_bus`: mapping of bus -> list of generator/converter units.
- `storage_at_bus`: mapping of bus -> list of storage units.

## Validation rules

- All dictionary keys must be known bus IDs.
- `generators_at_bus` and `storage_at_bus` values must be lists.

## Typical source files

- `data/buses.csv`
- Demand headers (to infer buses)

## Example usage

```python
from data_models.Bus import Bus

bus = Bus.from_csv("data/buses.csv")
print(bus.name[:5])
```

## Field summary

--8<-- "components/_tables/buses.md"
