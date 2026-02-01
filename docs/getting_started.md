# Getting Started

This project provides a data model (`data_models`) and loaders
(`data_loaders`) for energy system inputs.

## Install

Create a virtual environment and install dependencies.

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r docs/requirements.txt
```

## Build docs
The H2RES model can be installed using the following command. 
# TODO: Write command 
```bash
python 
```

## Loading input data
Users can prepare an H2RES model using the template CSV files. The model consists of different predefined, which enable the representation of different physical elements (generators, storage units, loads, converter etc.). Three component types are used to represent the physical elements of an energy system: `Generators`, `StorageUnits`, `Demand`. All additional information abouut a system are stored in the `Bus`, `SystemSets`, `SystemParameters` classes. To ensure consistent, typed and validated input data, Pydantic datacalsses are. 

# TODO: Improve text, making it compelling for beginners who know little about programming.

Loader functions read input data from the template CSV files and create instances of the `data_models` classes.    

- `data_models`: Pydantic models for system data (sets, buses, generators, storage, demand).
- `data_loaders`: CSV and time-series loaders that assemble `SystemParameters`.
- `data`: Example input tables for electricity, heating, and cooling sectors.
