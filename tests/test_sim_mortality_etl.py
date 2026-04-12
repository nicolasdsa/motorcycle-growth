"""Tests for the SIM mortality ETL module."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from motorcycle_growth.sim_mortality_etl import (
    SimMortalitySchemaError,
    aggregate_sim_mortality,
    resolve_sim_input_paths,
    run_sim_mortality_etl,
    standardize_sim_mortality_frame,
)


def test_standardize_sim_mortality_frame_filters_v20_v29_deaths() -> None:
    """The ETL should keep only motorcycle-related SIM deaths."""
    raw_frame = pd.DataFrame(
        {
            "CODMUNRES": ["3550308", "3550308", "3304557"],
            "DTOBITO": ["08112024", "09112024", "10112024"],
            "CAUSABAS": ["V230", "A419", "Y100"],
            "LINHAA": ["T149", "V210", "W189"],
        }
    )

    standardized_frame, schema, matched_columns = standardize_sim_mortality_frame(
        raw_frame,
        source_file_name="DO24OPEN.csv",
    )

    assert schema.municipality.scope == "residence"
    assert schema.municipality.column_name == "CODMUNRES"
    assert schema.year.column_name == "DTOBITO"
    assert matched_columns == ("CAUSABAS",)
    assert standardized_frame.to_dict(orient="records") == [
        {
            "municipality_code": "3550308",
            "year": 2024,
            "motorcycle_deaths": 1,
            "municipality_scope": "residence",
            "source_municipality_column": "CODMUNRES",
            "source_file_name": "DO24OPEN.csv",
        },
    ]


def test_aggregate_sim_mortality_computes_requested_metrics() -> None:
    """The ETL should aggregate record-level SIM data to municipality-year."""
    record_frame = pd.DataFrame(
        {
            "municipality_code": ["3550308", "3550308"],
            "year": [2024, 2024],
            "motorcycle_deaths": [1, 1],
            "municipality_scope": ["residence", "residence"],
            "source_municipality_column": ["CODMUNRES", "CODMUNRES"],
            "source_file_name": ["DO24OPEN.csv", "DO24OPEN.csv"],
        }
    )

    aggregated_frame = aggregate_sim_mortality(record_frame)

    assert aggregated_frame.to_dict(orient="records") == [
        {
            "municipality_code": "3550308",
            "year": 2024,
            "motorcycle_deaths": 2,
            "municipality_scope": "residence",
            "source_municipality_column": "CODMUNRES",
        }
    ]


def test_standardize_sim_mortality_frame_raises_without_cause_columns() -> None:
    """The ETL should fail when the raw layout cannot support CID filtering."""
    raw_frame = pd.DataFrame(
        {
            "CODMUNRES": ["3550308"],
            "DTOBITO": ["08112024"],
        }
    )

    with pytest.raises(SimMortalitySchemaError, match="cause-of-death"):
        standardize_sim_mortality_frame(
            raw_frame,
            source_file_name="DO24OPEN.csv",
        )


def test_standardize_sim_mortality_frame_falls_back_when_basic_cause_is_missing() -> None:
    """The ETL should use line-level causes only when the basic-cause field is absent."""
    raw_frame = pd.DataFrame(
        {
            "CODMUNOCOR": ["3304557", "3304557"],
            "DTOBITO": ["10112024", "11112024"],
            "LINHAA": ["V210", "A419"],
            "LINHAB": ["T149", "V290"],
        }
    )

    standardized_frame, schema, matched_columns = standardize_sim_mortality_frame(
        raw_frame,
        source_file_name="DO24OPEN.csv",
    )

    assert schema.municipality.scope == "occurrence"
    assert matched_columns == ("LINHAA", "LINHAB")
    assert standardized_frame["municipality_code"].tolist() == ["3304557", "3304557"]


def test_resolve_sim_input_paths_detects_open_csv_zip_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The raw SIM discovery should include the current OpenDataSUS ZIP pattern."""
    raw_dir = tmp_path / "sim_mortality"
    raw_dir.mkdir(parents=True, exist_ok=True)
    zip_path = raw_dir / "DO24OPEN_csv.zip"
    zip_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        "motorcycle_growth.sim_mortality_etl.SIM_RAW_DIR",
        raw_dir,
    )

    assert resolve_sim_input_paths() == [zip_path]


def test_run_sim_mortality_etl_writes_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ETL should write the aggregated parquet and metadata json outputs."""
    input_path = tmp_path / "DO24OPEN.csv"
    input_path.write_text("placeholder", encoding="utf-8")
    output_path = tmp_path / "sim" / "sim_motorcycle_mortality.parquet"
    metadata_path = tmp_path / "sim" / "sim_motorcycle_mortality_metadata.json"

    raw_frame = pd.DataFrame(
        {
            "CODMUNRES": ["3550308", "3550308", "3304557"],
            "DTOBITO": ["08112024", "09112024", "10112024"],
            "CAUSABAS": ["V230", "A419", "V290"],
            "LINHAA": ["T149", "V210", "S060"],
        }
    )

    monkeypatch.setattr(
        "motorcycle_growth.sim_mortality_etl.load_sim_raw_frame",
        lambda _path: raw_frame,
    )

    result = run_sim_mortality_etl(
        input_paths=[input_path],
        output_path=output_path,
        metadata_path=metadata_path,
    )

    saved_frame = pd.read_parquet(result.output_path)
    saved_metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))

    assert result.output_path == output_path.resolve()
    assert result.metadata_path == metadata_path.resolve()
    assert saved_frame.to_dict(orient="records") == [
        {
            "municipality_code": "3304557",
            "year": 2024,
            "motorcycle_deaths": 1,
            "municipality_scope": "residence",
            "source_municipality_column": "CODMUNRES",
        },
        {
            "municipality_code": "3550308",
            "year": 2024,
            "motorcycle_deaths": 1,
            "municipality_scope": "residence",
            "source_municipality_column": "CODMUNRES",
        },
    ]
    assert saved_metadata["summary"]["municipality_scope"] == "residence"
    assert saved_metadata["summary"]["cause_columns_with_matches"] == [
        "CAUSABAS",
    ]
