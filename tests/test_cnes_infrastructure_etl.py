"""Tests for the CNES infrastructure ETL module."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from motorcycle_growth.cnes_infrastructure_etl import (
    aggregate_cnes_bed_indicators,
    aggregate_cnes_establishment_indicators,
    resolve_cnes_bed_input_paths,
    run_cnes_infrastructure_etl,
    standardize_cnes_bed_frame,
    standardize_cnes_establishment_frame,
)


def test_standardize_cnes_establishment_frame_detects_samu_without_samuel_false_positive() -> None:
    """The establishment ETL should match SAMU conservatively."""
    raw_frame = pd.DataFrame(
        {
            "CO_IBGE": ["260290", "260290", "260290", "260290"],
            "CO_CNES": ["27", "28", "29", "30"],
            "NO_FANTASIA": [
                "SAMU 192 CABO",
                "AMBULANCIA PRIVADA",
                "CONSULTORIO DR SAMUEL",
                pd.NA,
            ],
            "NO_RAZAO_SOCIAL": [
                "PREFEITURA MUNICIPAL",
                "MEDVIDAS SERVICOS DE AMBULANCIA",
                "SAMUEL ERMELINDO",
                "SERVICO DE ATENDIMENTO MOVEL DE URGENCIA",
            ],
            "TP_UNIDADE": ["42", "42", "22", pd.NA],
        }
    )

    standardized_frame = standardize_cnes_establishment_frame(
        raw_frame,
        year=2026,
        source_file_name="cnes_estabelecimentos.csv",
    )
    aggregated_frame = aggregate_cnes_establishment_indicators(standardized_frame)

    assert standardized_frame["is_probable_samu"].tolist() == [True, False, False, True]
    assert aggregated_frame.to_dict(orient="records") == [
        {
            "municipality_code": "260290",
            "year": 2026,
            "probable_samu_establishment_count": 2,
            "mobile_emergency_establishment_count": 2,
            "has_samu": True,
        }
    ]


def test_aggregate_cnes_bed_indicators_keeps_maximum_stock_within_year() -> None:
    """The annual bed indicators should not sum competences across the same year."""
    raw_frame = pd.DataFrame(
        {
            "COMP": ["202601", "202601", "202602", "202602"],
            "CO_IBGE": ["260290", "260290", "260290", "260290"],
            "MUNICIPIO": [
                "CABO DE SANTO AGOSTINHO",
                "CABO DE SANTO AGOSTINHO",
                "CABO DE SANTO AGOSTINHO",
                "CABO DE SANTO AGOSTINHO",
            ],
            "LEITOS_EXISTENTES": ["10", "5", "11", "6"],
            "LEITOS_SUS": ["8", "5", "9", "6"],
            "UTI_TOTAL_EXIST": ["3", "2", "4", "2"],
            "UTI_TOTAL_SUS": ["2", "1", "3", "1"],
        }
    )

    standardized_frame = standardize_cnes_bed_frame(
        raw_frame,
        source_file_name="Leitos_2026.csv",
    )
    aggregated_frame = aggregate_cnes_bed_indicators(standardized_frame)

    assert aggregated_frame.to_dict(orient="records") == [
        {
            "municipality_code": "260290",
            "year": 2026,
            "municipality_name": "CABO DE SANTO AGOSTINHO",
            "total_hospital_beds_existing": 17,
            "total_hospital_beds_sus": 15,
            "icu_beds_existing": 6,
            "icu_beds_sus": 4,
            "observed_bed_competence_count": 2,
        }
    ]


def test_resolve_cnes_bed_input_paths_prefers_extracted_tabular_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The raw-bed discovery should avoid loading both a zip and its extracted CSV."""
    raw_dir = tmp_path / "cnes_hospital_beds"
    extracted_dir = raw_dir / "Leitos_csv_2026"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    archive_path = raw_dir / "Leitos_csv_2026.zip"
    csv_path = extracted_dir / "Leitos_2026.csv"

    archive_path.write_bytes(b"PK\x03\x04placeholder")
    csv_path.write_text("COMP;CO_IBGE\n202601;260290\n", encoding="utf-8")

    monkeypatch.setattr(
        "motorcycle_growth.cnes_infrastructure_etl.CNES_BEDS_RAW_DIR",
        raw_dir,
    )

    assert resolve_cnes_bed_input_paths() == [csv_path.resolve()]


def test_run_cnes_infrastructure_etl_writes_outputs_and_uses_bed_year_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The ETL should write outputs and resolve establishment year from beds when needed."""
    establishment_input_path = tmp_path / "cnes_estabelecimentos.csv"
    bed_input_path = tmp_path / "Leitos_2026.csv"
    establishment_input_path.write_text("placeholder", encoding="utf-8")
    bed_input_path.write_text("placeholder", encoding="utf-8")

    establishment_frame = pd.DataFrame(
        {
            "CO_IBGE": ["260290", "260290", "355030"],
            "CO_CNES": ["27", "28", "99"],
            "NO_FANTASIA": ["SAMU 192 CABO", "AMBULANCIA PRIVADA", "CONSULTORIO DR SAMUEL"],
            "NO_RAZAO_SOCIAL": [
                "PREFEITURA MUNICIPAL",
                "MEDVIDAS SERVICOS DE AMBULANCIA",
                "SAMUEL ERMELINDO",
            ],
            "TP_UNIDADE": ["42", "42", "22"],
        }
    )
    bed_frame = pd.DataFrame(
        {
            "COMP": ["202601", "202601"],
            "CO_IBGE": ["260290", "355030"],
            "MUNICIPIO": ["CABO DE SANTO AGOSTINHO", "SAO PAULO"],
            "LEITOS_EXISTENTES": ["15", "30"],
            "LEITOS_SUS": ["10", "20"],
            "UTI_TOTAL_EXIST": ["5", "8"],
            "UTI_TOTAL_SUS": ["3", "4"],
        }
    )

    monkeypatch.setattr(
        "motorcycle_growth.cnes_infrastructure_etl.load_cnes_raw_frame",
        lambda path: establishment_frame
        if path.name == establishment_input_path.name
        else bed_frame,
    )

    output_path = tmp_path / "cnes" / "cnes_infrastructure.parquet"
    metadata_path = tmp_path / "cnes" / "cnes_infrastructure_metadata.json"
    result = run_cnes_infrastructure_etl(
        establishment_input_paths=[establishment_input_path],
        bed_input_paths=[bed_input_path],
        output_path=output_path,
        metadata_path=metadata_path,
    )

    saved_frame = pd.read_parquet(result.output_path)
    saved_metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))

    assert result.output_path == output_path.resolve()
    assert result.metadata_path == metadata_path.resolve()
    assert result.summary.establishment_year_resolution == "bed_year_fallback"
    assert saved_frame.to_dict(orient="records") == [
        {
            "municipality_code": "260290",
            "municipality_name": "CABO DE SANTO AGOSTINHO",
            "year": 2026,
            "has_samu": True,
            "probable_samu_establishment_count": 1,
            "mobile_emergency_establishment_count": 2,
            "icu_beds_existing": 5,
            "icu_beds_sus": 3,
            "total_hospital_beds_existing": 15,
            "total_hospital_beds_sus": 10,
            "observed_bed_competence_count": 1,
            "has_bed_snapshot": True,
            "has_establishment_snapshot": True,
        },
        {
            "municipality_code": "355030",
            "municipality_name": "SAO PAULO",
            "year": 2026,
            "has_samu": False,
            "probable_samu_establishment_count": 0,
            "mobile_emergency_establishment_count": 0,
            "icu_beds_existing": 8,
            "icu_beds_sus": 4,
            "total_hospital_beds_existing": 30,
            "total_hospital_beds_sus": 20,
            "observed_bed_competence_count": 1,
            "has_bed_snapshot": True,
            "has_establishment_snapshot": True,
        },
    ]
    assert saved_metadata["summary"]["observed_bed_years"] == [2026]
    assert saved_metadata["summary"]["establishment_year_resolution"] == "bed_year_fallback"
