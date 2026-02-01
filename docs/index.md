# H2RES Refactored

H2RES Refactored is a data model and loader pipeline for energy system inputs.
It turns structured input tables into a validated, typed `SystemParameters`
object that downstream models can consume.

## Quick start

```bash
pip install -r docs/requirements.txt
mkdocs serve
```

Open the local site in your browser (the terminal will show the URL).

## Where to begin

- **Design Overview** explains how the data model is structured and how the
  loaders assemble a system.
- **Components** provides a dedicated page for each core data class
  (buses, generators, storage, demand, and system parameters).
- **Tutorials** walk you through loading data and extending the pipeline.
- **API Reference** lists the loaders, models, and helper modules.

## Project snapshot

- `data/` contains example input tables by sector.
- `data_loaders/` implements the loader pipeline.
- `data_models/` defines all Pydantic data classes.
