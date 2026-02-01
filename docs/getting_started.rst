Getting Started
===============

This project provides a data model (``data_models``) and loaders
(``data_loaders``) for energy system inputs.

Install
-------

Create a virtual environment and install dependencies.

.. code-block:: bash

   python -m venv .venv
   .venv\Scripts\activate
   pip install -r docs/requirements.txt

Build docs
----------

.. code-block:: bash

   sphinx-build -b html docs docs/_build/html

Project layout
--------------

- ``data_models``: Pydantic models for system data (sets, buses, generators, storage, demand).
- ``data_loaders``: CSV and time-series loaders that assemble ``SystemParameters``.
- ``data``: Example input tables for electricity, heating, and cooling sectors.
