# Storage Units

`StorageUnits` stores energy storage assets such as batteries, hydro
reservoirs, thermal storage, and hydrogen tanks.

## Where it lives

- Model: `data_models/StorageUnits.py`
- Loader: `data_loaders/load_storage.py`

## What it represents

Storage units hold **power-side** and **energy-side** attributes such as:

- nominal (dis)charging power (`p_charge_nom`, `p_discharge_nom` etc.)
- energy capacity (`e_nom`, `e_nom_max` etc.)
- charging/discharging power limits
- efficiencies and losses
- inflows (e.g., hydro)


## Typical source files

- `data/electricity/storage_units.csv`
- `data/heating/storage_units.csv`
- `data/cooling/storage_units.csv`

## Field summary

--8<-- "components/_tables/storage_units.md"
