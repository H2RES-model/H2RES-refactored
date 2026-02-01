# Quickstart

Load a single sector using built-in defaults.

```python
from data_loaders.load_sector import load_sector

system = load_sector(sector="electricity")
print(system.sets.years)
print(system.generators.unit[:5])
```

Load multiple sectors with explicit paths.

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
