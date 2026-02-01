# System Sets

`SystemSets` defines the index sets that describe the system structure.
These sets are used to build all other components consistently.

## Where it lives

- Model: `data_models/SystemSets.py`
- Loader: `data_loaders/load_sets.py`

## Time sets

- `years`: model years.
- `periods`: time periods (e.g., hours).

## Carriers and buses

- `carriers`: list of carriers (default: Electricity).
- `buses`: list of buses (default: SystemBus).

## Unit sets

- `units`: all generator/converter units.
- `storage_units`: all storage assets.

## Subsets (examples)

Generator subsets:

- `fossil_units`, `biomass_units`, `wind_units`, `solar_units`, `chp_units`...

Storage subsets:

- `hydro_storage_units`, `battery_units`, `tes_units`, `hydrogen_storage_units`...

## Validation rules

- All generator subsets must be subsets of `units`.
- All storage subsets must be subsets of `storage_units`.

## Field summary

--8<-- "components/_tables/system_sets.md"
