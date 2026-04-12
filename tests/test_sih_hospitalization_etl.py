"""Tests for the SIH hospitalization ETL module."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import types

import pandas as pd
import pytest

from motorcycle_growth.sih_hospitalization_etl import (
    SihHospitalizationDependencyError,
    SihHospitalizationSchemaError,
    aggregate_sih_hospitalizations,
    load_sih_raw_frame,
    _load_dbc_frame,
    resolve_sih_input_paths,
    run_sih_hospitalization_etl,
    standardize_sih_hospitalization_frame,
)


def test_standardize_sih_hospitalization_frame_filters_v20_v29_cases() -> None:
    """The ETL should keep only motorcycle-related hospitalizations."""
    raw_frame = pd.DataFrame(
        {
            "MUNIC_RES": ["3550308", "3550308", "3304557"],
            "MES_CMPT": ["202501", "202501", "202502"],
            "DIAS_PERM": ["5", "3", "2"],
            "VAL_TOT": ["1200.50", "800.00", "200.00"],
            "DIAG_PRINC": ["S720", "V210", "S060"],
            "DIAG_SECUN": ["T149", "S000", "Z000"],
            "CAUSAEXT": ["V234", "W189", "W189"],
        }
    )

    standardized_frame, schema, matched_columns = standardize_sih_hospitalization_frame(
        raw_frame,
        source_file_name="RDSP2501.csv",
    )

    assert schema.municipality.scope == "residence"
    assert schema.municipality.column_name == "MUNIC_RES"
    assert schema.year.column_name == "MES_CMPT"
    assert schema.cost.source_columns == ("VAL_TOT",)
    assert matched_columns == ("DIAG_PRINC", "CAUSAEXT")
    assert standardized_frame.to_dict(orient="records") == [
        {
            "municipality_code": "3550308",
            "year": 2025,
            "hospitalization_cost": 1200.5,
            "length_of_stay_days": 5,
            "municipality_scope": "residence",
            "source_municipality_column": "MUNIC_RES",
            "source_file_name": "RDSP2501.csv",
        },
        {
            "municipality_code": "3550308",
            "year": 2025,
            "hospitalization_cost": 800.0,
            "length_of_stay_days": 3,
            "municipality_scope": "residence",
            "source_municipality_column": "MUNIC_RES",
            "source_file_name": "RDSP2501.csv",
        },
    ]


def test_aggregate_sih_hospitalizations_computes_requested_metrics() -> None:
    """The ETL should aggregate record-level SIH data to municipality-year."""
    record_frame = pd.DataFrame(
        {
            "municipality_code": ["3550308", "3550308"],
            "year": [2025, 2025],
            "hospitalization_cost": [1200.5, 800.0],
            "length_of_stay_days": [5, 3],
            "municipality_scope": ["residence", "residence"],
            "source_municipality_column": ["MUNIC_RES", "MUNIC_RES"],
            "source_file_name": ["RDSP2501.csv", "RDSP2501.csv"],
        }
    )

    aggregated_frame = aggregate_sih_hospitalizations(record_frame)

    assert aggregated_frame.to_dict(orient="records") == [
        {
            "municipality_code": "3550308",
            "year": 2025,
            "motorcycle_hospitalizations": 2,
            "total_hospitalization_cost": 2000.5,
            "total_length_of_stay_days": 8,
            "mean_length_of_stay_days": 4.0,
            "mean_hospitalization_cost": 1000.25,
            "municipality_scope": "residence",
            "source_municipality_column": "MUNIC_RES",
        }
    ]


def test_standardize_sih_hospitalization_frame_raises_without_diagnosis_columns() -> None:
    """The ETL should fail when the raw layout cannot support CID filtering."""
    raw_frame = pd.DataFrame(
        {
            "MUNIC_RES": ["3550308"],
            "MES_CMPT": ["202501"],
            "DIAS_PERM": ["5"],
            "VAL_TOT": ["1200.50"],
        }
    )

    with pytest.raises(SihHospitalizationSchemaError, match="diagnosis/external-cause"):
        standardize_sih_hospitalization_frame(
            raw_frame,
            source_file_name="RDSP2501.csv",
        )


def test_load_sih_raw_frame_reads_dbc_via_datasus_library(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ETL should route DBC files through the DATASUS decompressor."""
    input_path = tmp_path / "RDSP2501.dbc"
    input_path.write_bytes(b"compressed-dbc")

    fake_module = types.SimpleNamespace(decompress_bytes=lambda payload: b"dbf-bytes")
    monkeypatch.setitem(sys.modules, "datasus_dbc", fake_module)
    monkeypatch.setattr(
        "motorcycle_growth.sih_hospitalization_etl._load_dbf_frame",
        lambda path: pd.DataFrame({"loaded_from": [path.suffix]}),
    )

    loaded_frame = load_sih_raw_frame(input_path)

    assert loaded_frame.to_dict(orient="records") == [{"loaded_from": ".dbf"}]


def test_load_dbc_frame_raises_when_datasus_dependency_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ETL should explain the missing dependency for DBC support."""
    input_path = tmp_path / "RDSP2501.dbc"
    input_path.write_bytes(b"compressed-dbc")

    real_import = __import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "datasus_dbc":
            raise ImportError("missing datasus_dbc")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)

    with pytest.raises(SihHospitalizationDependencyError, match="datasus-dbc"):
        _load_dbc_frame(input_path)


def test_resolve_sih_input_paths_detects_dbc_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The raw SIH discovery should include DATASUS DBC files."""
    raw_dir = tmp_path / "sih_sus"
    raw_dir.mkdir(parents=True, exist_ok=True)
    dbc_path = raw_dir / "RDSP2501.dbc"
    dbc_path.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(
        "motorcycle_growth.sih_hospitalization_etl.SIH_RAW_DIR",
        raw_dir,
    )

    assert resolve_sih_input_paths() == [dbc_path]


def test_run_sih_hospitalization_etl_writes_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ETL should write the aggregated parquet and metadata json outputs."""
    input_path = tmp_path / "RDSP2501.csv"
    input_path.write_text("placeholder", encoding="utf-8")
    output_path = tmp_path / "sih" / "sih_motorcycle_hospitalizations.parquet"
    metadata_path = tmp_path / "sih" / "sih_motorcycle_hospitalizations_metadata.json"

    raw_frame = pd.DataFrame(
        {
            "MUNIC_RES": ["3550308", "3550308"],
            "MES_CMPT": ["202501", "202501"],
            "DIAS_PERM": ["5", "3"],
            "VAL_SH": ["900.00", "500.00"],
            "VAL_SP": ["300.00", "100.00"],
            "DIAG_PRINC": ["S720", "V210"],
            "CAUSAEXT": ["V234", "W189"],
        }
    )

    monkeypatch.setattr(
        "motorcycle_growth.sih_hospitalization_etl.load_sih_raw_frame",
        lambda _path: raw_frame,
    )

    result = run_sih_hospitalization_etl(
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
            "municipality_code": "3550308",
            "year": 2025,
            "motorcycle_hospitalizations": 2,
            "total_hospitalization_cost": 1800.0,
            "total_length_of_stay_days": 8,
            "mean_length_of_stay_days": 4.0,
            "mean_hospitalization_cost": 900.0,
            "municipality_scope": "residence",
            "source_municipality_column": "MUNIC_RES",
        }
    ]
    assert saved_metadata["summary"]["municipality_scope"] == "residence"
    assert saved_metadata["summary"]["source_cost_type"] == "sum_of_service_components"
    assert saved_metadata["summary"]["diagnosis_columns_with_matches"] == [
        "CAUSAEXT",
        "DIAG_PRINC",
    ]
