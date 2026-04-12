"""Tests for the analytical panel builder."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from motorcycle_growth.panel_builder import (
    PanelDataQualityError,
    run_panel_build,
)


def test_run_panel_build_uses_outer_joins_and_derives_rates(tmp_path: Path) -> None:
    """The panel step should preserve non-overlapping municipality-year rows."""
    population_path = tmp_path / "population.parquet"
    senatran_path = tmp_path / "senatran.parquet"
    sih_path = tmp_path / "sih.parquet"
    sim_path = tmp_path / "sim.parquet"
    cnes_path = tmp_path / "cnes.parquet"
    output_path = tmp_path / "processed" / "panel.parquet"
    metadata_path = tmp_path / "processed" / "panel_metadata.json"

    pd.DataFrame(
        {
            "CO_IBGE": ["1100150", "1100230"],
            "municipality_name": ["Alta Floresta D'Oeste", "Ariquemes"],
            "year": [2025, 2025],
            "population": [1000, 2000],
        }
    ).to_parquet(population_path, index=False)
    pd.DataFrame(
        {
            "CO_IBGE": ["1100150", "1100230"],
            "municipality_name": ["Alta Floresta D'Oeste", "Ariquemes"],
            "year": [2025, 2026],
            "motorcycles_total": [500.0, 800.0],
            "motocicleta": [400.0, 600.0],
            "motoneta": [90.0, 170.0],
            "ciclomotor": [5.0, 20.0],
            "triciclo": [3.0, 5.0],
            "quadriciclo": [2.0, 5.0],
            "side_car": [0.0, 0.0],
            "months_observed": [1, 2],
            "source_months": ["02", "01,02"],
            "is_partial_year": [True, True],
            "aggregation_rule": [
                "yearly_average_of_available_monthly_stocks",
                "yearly_average_of_available_monthly_stocks",
            ],
        }
    ).to_parquet(senatran_path, index=False)
    pd.DataFrame(
        {
            "municipality_code": ["110015"],
            "year": [2025],
            "motorcycle_hospitalizations": [10],
            "total_hospitalization_cost": [5000.0],
            "total_length_of_stay_days": [20],
            "mean_length_of_stay_days": [2.0],
            "mean_hospitalization_cost": [500.0],
            "municipality_scope": ["residence"],
            "source_municipality_column": ["MUNIC_RES"],
        }
    ).to_parquet(sih_path, index=False)
    pd.DataFrame(
        {
            "municipality_code": ["110015"],
            "year": [2024],
            "motorcycle_deaths": [2],
            "municipality_scope": ["residence"],
            "source_municipality_column": ["CODMUNRES"],
        }
    ).to_parquet(sim_path, index=False)
    pd.DataFrame(
        {
            "municipality_code": ["110015", "999999"],
            "municipality_name": ["ALTA FLORESTA D'OESTE", "MUNICIPIO TESTE"],
            "year": [2025, 2026],
            "has_samu": [True, False],
            "probable_samu_establishment_count": [1, 0],
            "mobile_emergency_establishment_count": [1, 0],
            "icu_beds_existing": [5, 1],
            "icu_beds_sus": [3, 1],
            "total_hospital_beds_existing": [20, 4],
            "total_hospital_beds_sus": [18, 4],
            "observed_bed_competence_count": [1, 1],
            "has_bed_snapshot": [True, True],
            "has_establishment_snapshot": [True, True],
        }
    ).to_parquet(cnes_path, index=False)

    result = run_panel_build(
        population_path=population_path,
        senatran_path=senatran_path,
        sih_path=sih_path,
        sim_path=sim_path,
        cnes_path=cnes_path,
        output_path=output_path,
        metadata_path=metadata_path,
    )

    panel_frame = pd.read_parquet(result.output_path)
    metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))

    assert panel_frame["municipality_key"].tolist() == [
        "MU6:110015",
        "CO7:1100230",
        "MU6:110015",
        "CO7:1100230",
        "MU6:999999",
    ]
    assert panel_frame["year"].tolist() == [2024, 2025, 2025, 2026, 2026]
    assert panel_frame.loc[0, "municipality_code"] == "110015"
    assert pd.isna(panel_frame.loc[1, "municipality_code"])
    assert panel_frame.loc[2, "municipality_code"] == "110015"
    assert pd.isna(panel_frame.loc[3, "municipality_code"])
    assert panel_frame.loc[4, "municipality_code"] == "999999"

    alta_floresta_2025 = panel_frame.loc[
        (panel_frame["municipality_key"] == "MU6:110015")
        & (panel_frame["year"] == 2025)
    ].iloc[0]
    municipio_teste_2026 = panel_frame.loc[
        (panel_frame["municipality_key"] == "MU6:999999")
        & (panel_frame["year"] == 2026)
    ].iloc[0]
    ariquemes_2025 = panel_frame.loc[
        (panel_frame["municipality_key"] == "CO7:1100230")
        & (panel_frame["year"] == 2025)
    ].iloc[0]

    assert alta_floresta_2025["CO_IBGE"] == "1100150"
    assert bool(alta_floresta_2025["has_population_data"]) is True
    assert bool(alta_floresta_2025["has_senatran_data"]) is True
    assert bool(alta_floresta_2025["has_sih_data"]) is True
    assert bool(alta_floresta_2025["has_sim_data"]) is False
    assert bool(alta_floresta_2025["has_cnes_data"]) is True
    assert alta_floresta_2025["motorcycles_per_1000_inhabitants"] == 500.0
    assert alta_floresta_2025["motorcycle_hospitalizations_per_100k"] == 1000.0
    assert pd.isna(alta_floresta_2025["motorcycle_deaths_per_100k"])
    assert alta_floresta_2025["icu_beds_per_100k"] == 500.0

    assert pd.isna(municipio_teste_2026["CO_IBGE"])
    assert municipio_teste_2026["municipality_name"] == "MUNICIPIO TESTE"
    assert bool(municipio_teste_2026["has_cnes_data"]) is True
    assert bool(municipio_teste_2026["has_population_data"]) is False
    assert pd.isna(municipio_teste_2026["icu_beds_per_100k"])

    assert ariquemes_2025["CO_IBGE"] == "1100230"
    assert pd.isna(ariquemes_2025["municipality_code"])
    assert ariquemes_2025["municipality_name"] == "Ariquemes"

    assert metadata["merge_strategy"]["join_type"] == "outer"
    assert metadata["merge_steps"][0]["dataset_name"] == "senatran"
    assert metadata["missingness"]["motorcycle_deaths"]["missing_count"] == 4
    assert metadata["summary"]["row_count"] == 5


def test_run_panel_build_raises_for_duplicate_municipality_year_rows(
    tmp_path: Path,
) -> None:
    """The panel step should fail loudly when one source is not unique."""
    population_path = tmp_path / "population.parquet"
    senatran_path = tmp_path / "senatran.parquet"
    sih_path = tmp_path / "sih.parquet"
    sim_path = tmp_path / "sim.parquet"
    cnes_path = tmp_path / "cnes.parquet"

    pd.DataFrame(
        {
            "CO_IBGE": ["1100150", "1100150"],
            "municipality_name": ["Alta Floresta D'Oeste", "Alta Floresta D'Oeste"],
            "year": [2025, 2025],
            "population": [1000, 1000],
        }
    ).to_parquet(population_path, index=False)
    pd.DataFrame(
        {
            "CO_IBGE": ["1100150"],
            "municipality_name": ["Alta Floresta D'Oeste"],
            "year": [2025],
            "motorcycles_total": [500.0],
            "motocicleta": [400.0],
            "motoneta": [90.0],
            "ciclomotor": [5.0],
            "triciclo": [3.0],
            "quadriciclo": [2.0],
            "side_car": [0.0],
            "months_observed": [1],
            "source_months": ["02"],
            "is_partial_year": [True],
            "aggregation_rule": ["yearly_average_of_available_monthly_stocks"],
        }
    ).to_parquet(senatran_path, index=False)
    pd.DataFrame(
        {
            "municipality_code": ["110015"],
            "year": [2025],
            "motorcycle_hospitalizations": [10],
            "total_hospitalization_cost": [5000.0],
            "total_length_of_stay_days": [20],
            "mean_length_of_stay_days": [2.0],
            "mean_hospitalization_cost": [500.0],
            "municipality_scope": ["residence"],
            "source_municipality_column": ["MUNIC_RES"],
        }
    ).to_parquet(sih_path, index=False)
    pd.DataFrame(
        {
            "municipality_code": ["110015"],
            "year": [2025],
            "motorcycle_deaths": [2],
            "municipality_scope": ["residence"],
            "source_municipality_column": ["CODMUNRES"],
        }
    ).to_parquet(sim_path, index=False)
    pd.DataFrame(
        {
            "municipality_code": ["110015"],
            "municipality_name": ["ALTA FLORESTA D'OESTE"],
            "year": [2025],
            "has_samu": [True],
            "probable_samu_establishment_count": [1],
            "mobile_emergency_establishment_count": [1],
            "icu_beds_existing": [5],
            "icu_beds_sus": [3],
            "total_hospital_beds_existing": [20],
            "total_hospital_beds_sus": [18],
            "observed_bed_competence_count": [1],
            "has_bed_snapshot": [True],
            "has_establishment_snapshot": [True],
        }
    ).to_parquet(cnes_path, index=False)

    with pytest.raises(PanelDataQualityError, match="duplicate municipality-year rows"):
        run_panel_build(
            population_path=population_path,
            senatran_path=senatran_path,
            sih_path=sih_path,
            sim_path=sim_path,
            cnes_path=cnes_path,
        )
