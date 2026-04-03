# motorcycle-growth

Python project for studying motorcycle fleet growth, trauma hospitalizations, mortality, and emergency care capacity in Brazil.

## Current status

This repository currently contains the initial project scaffold:

- `src`-based Python package
- Poetry configuration
- Initial data pipeline dependencies
- Data, output, notebook, and test directories
- VS Code launch settings for future pipeline and dashboard entry points
- A first data-ingestion planning layer with a documented data catalog

## Initial setup

1. Install Poetry if it is not available on your system.
2. Create the virtual environment and install dependencies:

```bash
poetry install
```

3. Activate the environment:

```bash
poetry shell
```

4. Run the basic paths CLI:

```bash
poetry run motorcycle-growth check-project
```

5. Inspect the planned data sources:

```bash
poetry run motorcycle-growth show-data-catalog
```

6. Check raw data acquisition status without downloading:

```bash
poetry run motorcycle-growth acquire-raw-data --check-only
```

7. Run automated raw data acquisition:

```bash
poetry run motorcycle-growth acquire-raw-data
```

8. Request one specific SIH/SUS competence file conservatively:

```bash
poetry run motorcycle-growth acquire-raw-data --sih-year 2025 --sih-month 1 --sih-uf SP
```

## Project structure

```text
src/
data/raw/
data/interim/
data/processed/
notebooks/
outputs/figures/
outputs/tables/
tests/
.vscode/
```

## Data ingestion planning

The repository now includes:

- [`data_catalog.md`](data_catalog.md): source-by-source planning for data intake
- [`data/raw/README.md`](data/raw/README.md): rules for organizing original files
- `motorcycle_growth.data_catalog`: reusable Python metadata for planned sources
- `motorcycle_growth.raw_data`: first raw ingestion layer for downloads and manual checks

The raw ingestion CLI now supports:

- light public-page discovery for SENATRAN and IBGE
- public dataset-page discovery for SIM
- official transfer-flow discovery for one SIH/SUS file at a time
- retry with backoff and a small local discovery cache
