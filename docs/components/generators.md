# Generators

`Generators` stores all power-converting units and converters in the system.
This includes conventional plants, renewables, and conversion technologies
(e.g. heat pumps).

## Where it lives

- Model: `data_models/Generators.py`
- Loader: `data_loaders/load_generators.py`

## What it represents

Generators hold **power-side** attributes such as:

- installed power capacity
- efficiency (static or time-varying)
- variable costs and CO2 intensity
- ramping constraints
- carrier and bus connections

Storage-related parameters (energy capacity, SOC, inflows) are **not** here.
Those live in `StorageUnits`.

## Typical source files

- `data/electricity/powerplants.csv`
- `data/heating/converters.csv`
- optional efficiency time series (e.g. `data/heating/COP.csv`)

## Field summary

--8<-- "components/_tables/generators.md"
