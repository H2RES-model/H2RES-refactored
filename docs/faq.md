# FAQ

## Which sectors are supported?

The loader pipeline supports `electricity`, `heating`, and `cooling`.
Other sector names will raise a `ValueError` unless you extend the loaders.

## What files are required for electricity?

At minimum you must provide:

- powerplants
- storage units
- RES profiles
- inflows
- electricity demand

## What file types can I use?

CSV, Parquet, and Feather are supported. The loader tries to resolve a
matching extension if only a base name is provided.

## Why does Sphinx fail to import modules?

Install project dependencies (notably `pandas` and `pydantic`), or adjust
`autodoc_mock_imports` in `docs/conf.py`.
