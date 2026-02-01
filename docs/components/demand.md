# Demand

`Demand` stores time-series demand values for one or more carriers.

## Where it lives

- Model: `data_models/Demand.py`
- Loader: `data_loaders/load_demand.py`

## Data structure

Demand is stored as:

```
p_t[(system, region, bus, carrier, period, year)] = value
```

## Validation rules

- No negative demand values.
- Keys must be 6-tuples: `(system, region, bus, carrier, period, year)`.

## Typical source files

- `data/electricity/electricity_demand.csv`
- `data/heating/heat_demand.csv`
- `data/cooling/cooling_demand.csv`

## Field summary

--8<-- "components/_tables/demand.md"
