# Buses

The `Bus` model represents network nodes and their assigned energy carrier.
Buses also keep track of which generator and storage units are connected.

## Where it lives

- Model: `data_models/Bus.py`
- Loader: `data_loaders/load_bus.py`

## Key fields

- `static`: indexed bus metadata table.
- `attachments`: long table of bus-to-unit links.
- `name`: list of all bus IDs.
- `system`: bus-level system/country series.
- `region`: bus-level region/zone series.
- `carrier`: bus-level carrier series.

## Query helpers

- `units(bus, component=None, role=None)`: return attachment rows for a bus.
- `buses_for_carrier(carrier)`: return bus IDs for one carrier.
- `carrier_of(bus)`: return the carrier assigned to one bus.

## Typical source files

- `data/buses.csv`
- Demand headers (to infer buses)

## Example usage

```python
from data_models.Bus import Bus

bus = Bus.from_csv("data/buses.csv")
print(bus.name[:5])
print(bus.units("HR_EL", component="generator").head())
```

## Field summary

--8<-- "components/_tables/buses.md"
