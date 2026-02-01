# System Parameters

`SystemParameters` is the top-level container that bundles all components
into one object.

## Where it lives

- Model: `data_models/SystemParameters.py`
- Assembled by: `data_loaders/load_sector.py` and `data_loaders/load_system.py`

## Contains

- `sets`: `SystemSets`
- `bus`: `Bus`
- `generators`: `Generators`
- `storage`: `StorageUnits`
- `demand`: `Demand`
- `market`: `MarketParams` (placeholder model)
- `policy`: `PolicyParams` (placeholder model)

This is the object you should pass to downstream solvers or analyses.

## Field summary

--8<-- "components/_tables/system_parameters.md"
