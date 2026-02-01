# Getting Started

H2RES Refactored reads input tables, validates them with Pydantic, and
assembles a single `SystemParameters` object for downstream modeling.

## Prerequisites

Requirements:
- Python 3.10+ installed
- A terminal (PowerShell on Windows is fine)

Optional:
- A code editor (VS Code, PyCharm, etc.)

## Install

Create a virtual environment and install dependencies.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r docs/requirements.txt
```

## Preview the docs

MkDocs provides a live-reload server.

```bash
mkdocs serve
```

Open the local URL printed by MkDocs.

## Build the docs

This builds a static site under `site/`.

```bash
mkdocs build
```

## Core data model

- `Generators`: power-converting units and converters.
- `StorageUnits`: energy storage assets.
- `Demand`: time-series demand by carrier and bus.
- `Bus`: network buses and carrier connections.
- `SystemSets`: index sets (years, periods, unit lists, subsets).
- `SystemParameters`: top-level container that ties everything together.

## Understanding the input files

Inputs live in the `data/` folder. There is one subfolder per sector:

- `data/electricity/`
- `data/heating/`
- `data/cooling/`

Each sector has template tables. For example:

- `powerplants.csv` (generators)
- `storage_units.csv` (storage assets)
- `electricity_demand.csv` or `heat_demand.csv` (demand series)
- `res_profile.csv` (renewable profiles)
- `scaled_inflows.csv` (hydro inflows)

There are also shared tables:

- `data/buses.csv`
- `data/fuel_cost.csv`

## Load a single sector

```python
from data_loaders.load_sector import load_sector

system = load_sector(sector="electricity")
print(system.sets.years)
print(system.generators.unit[:5])
```

The loader uses default files under `data/electricity/`, validates them, and
returns a `SystemParameters` object.

## Load multiple sectors

To load electricity + heating together, provide explicit paths:

```python
from data_loaders.load_system import load_system

electricity_paths = {
    "powerplants_path": "data/electricity/powerplants.csv",
    "storage_path": "data/electricity/storage_units.csv",
    "renewable_profiles_path": "data/electricity/res_profile.csv",
    "inflow_path": "data/electricity/scaled_inflows.csv",
    "electricity_demand_path": "data/electricity/electricity_demand.csv",
}

heating_paths = {
    "powerplants_path": "data/heating/converters.csv",
    "storage_path": "data/heating/storage_units.csv",
    "efficiency_ts_path": "data/heating/COP.csv",
    "heating_demand_path": "data/heating/heat_demand.csv",
}

system = load_system(
    sectors=["electricity", "heating"],
    electricity_paths=electricity_paths,
    heating_paths=heating_paths,
    buses_path="data/buses.csv",
    fuel_cost_path="data/fuel_cost.csv",
)
```

## Common issues

1. **Wrong file names**: use the default names or pass explicit paths.
2. **Missing columns**: start from the provided templates.
3. **Mixed formats**: CSV/Parquet/Feather are supported; base names resolve automatically.

## Project layout

- `data_models`: Pydantic models for system data.
- `data_loaders`: loader functions that build `SystemParameters`.
- `data`: example input tables.
- `docs`: documentation source files.

## Next steps

- Read the Tutorials section for step-by-step walkthroughs.
- Check the API Reference to see every loader and model.
- Use the example tables in `data/` to build your own dataset.
