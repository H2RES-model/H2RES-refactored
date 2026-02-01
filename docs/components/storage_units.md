# Storage Units

`StorageUnits` stores energy storage assets such as batteries, hydro
reservoirs, thermal storage, and hydrogen tanks.

## Where it lives

- Model: `data_models/StorageUnits.py`
- Loader: `data_loaders/load_storage.py`

## What it represents

Storage units hold **energy-side** attributes such as:

- energy capacity (`e_nom`)
- charging/discharging power limits
- efficiencies and losses
- inflows (e.g., hydro)

Power-side parameters (turbine capacities, conversion units) are stored in
`Generators` when relevant.

## Typical source files

- `data/electricity/storage_units.csv`
- `data/heating/storage_units.csv`
- `data/cooling/storage_units.csv`

## Field summary

--8<-- "components/_tables/storage_units.md"
