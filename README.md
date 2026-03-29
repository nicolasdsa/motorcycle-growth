# motorcycle-growth

Python project for studying motorcycle fleet growth, trauma hospitalizations, mortality, and emergency care capacity in Brazil.

## Current status

This repository currently contains the initial project scaffold:

- `src`-based Python package
- Poetry configuration
- Initial data pipeline dependencies
- Data, output, notebook, and test directories
- VS Code launch settings for future pipeline and dashboard entry points

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
