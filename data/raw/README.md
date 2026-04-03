# Raw Data Intake Guide

Place only original source files in this folder.

## Source folders

- `senatran_motorcycle_fleet/`: SENATRAN motorcycle fleet tables
- `ibge_population/`: IBGE population tables
- `sih_sus/`: SIH/SUS hospitalization extracts and supporting dictionaries
- `sim_mortality/`: SIM mortality files and supporting dictionaries
- `cnes_establishments/`: CNES establishment files
- `cnes_hospital_beds/`: CNES hospital and bed files

## Rules

- Do not overwrite official filenames.
- Do not place transformed parquet, csv, or analysis-ready files here.
- Keep layouts, dictionaries, and download notes near the corresponding raw files.
- Move cleaned outputs to `data/interim/` or `data/processed/` in later pipeline steps.
