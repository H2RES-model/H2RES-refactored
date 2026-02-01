# Design Overview

This page summarizes how H2RES Refactored is structured and how the
core objects fit together.

## The big picture

H2RES Refactored is a data model and loader pipeline:

1. **Input tables** (CSV/Parquet/Feather) live under `data/`.
2. **Loaders** in `data_loaders/` read and validate those tables.
3. **Pydantic models** in `data_models/` hold typed, validated data.
4. Everything is assembled into one `SystemParameters` object.

`SystemParameters` is the single, consistent container for a model run.

## Core objects

- `SystemParameters`: the top-level container for everything.
- `SystemSets`: index sets (years, periods, units, storage units, subsets).
- `Bus`: network buses and carrier assignments.
- `Generators`: power-converting units and converters (power-side data).
- `StorageUnits`: energy storage assets (energy-side data).
- `Demand`: demand time series by system, region, bus, and carrier.

## Loader flow

1. `load_sets` builds `SystemSets`.
2. `load_bus` builds `Bus`.
3. `load_generators` builds `Generators`.
4. `load_storage` builds `StorageUnits`.
5. `load_demand` builds `Demand`.
6. `load_sector` assembles `SystemParameters`.
7. `load_system` merges multiple sectors.

## Conceptual separation: power vs energy

The model cleanly separates:

- **Generators**: power conversion (capacity, efficiency, ramping).
- **StorageUnits**: energy storage (energy capacity, SOC, inflows, losses).

This keeps power-side and energy-side parameters independent.

## Sectors and data folders

Supported sectors today are:

- `electricity`
- `heating`
- `cooling`

Each sector has its own folder under `data/`. Defaults are used when you pass
`sector="electricity"` (or similar).

## Extending the model

To add new sectors or new parameters:

1. Add new input tables under `data/<sector>/`.
2. Extend `load_sector` defaults and required inputs.
3. Extend `load_system` to allow the new sector name.
4. Add new fields to `data_models` as needed.
5. Update docs to keep the reference accurate.
