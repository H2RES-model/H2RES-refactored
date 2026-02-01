Loading Data
============

Supported table formats include CSV, Parquet, and Feather. The loader will
resolve a file by extension or try a matching alternative if only a base name
is provided.

Examples:

.. code-block:: python

   from data_loaders.helpers.io import read_table

   df_csv = read_table("data/electricity/powerplants.csv")
   df_parquet = read_table("data/electricity/powerplants.parquet")
   df_auto = read_table("data/electricity/powerplants")

When using ``load_sector(sector=...)``, any missing paths are filled from the
standard ``data/<sector>`` layout. When using ``load_system(...)``, you must
pass a full mapping for the electricity sector and any additional sectors you
want to include.
