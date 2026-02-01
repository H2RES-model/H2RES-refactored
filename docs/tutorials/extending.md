# Extending the Pipeline

This project is organized so you can extend it with new sectors or new input
columns without rewriting the pipeline.

## Checklist for adding a sector

1. Add a new `data/<sector>` folder with the required tables.
2. Extend `data_loaders.load_sector` to map the new sector key and defaults.
3. Update `data_loaders.load_system` to allow the new sector name.
4. Add new fields to the Pydantic models in `data_models` if needed.
5. Update the API docs to include any new modules.
