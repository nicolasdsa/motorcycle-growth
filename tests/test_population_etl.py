"""Tests for the population ETL module."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from motorcycle_growth.population_etl import (
    PopulationSchemaError,
    infer_population_year,
    load_population_raw_frame,
    run_population_etl,
    standardize_population_frame,
)


def test_standardize_population_frame_builds_expected_columns() -> None:
    """The ETL should keep municipality rows and build CO_IBGE correctly."""
    raw_frame = pd.DataFrame(
        {
            "COD. UF": [None, 35, "33"],
            "COD. MUNIC": [None, 50308, "04557"],
            "Municípios": ["Brasil", "São Paulo", "Rio de Janeiro"],
            "POPULAÇÃO ESTIMADA": [212583750, 11895893, 6211223],
            "Ignored": ["x", "y", "z"],
        }
    )

    standardized_frame, summary = standardize_population_frame(raw_frame, year=2025)

    assert list(standardized_frame.columns) == [
        "CO_IBGE",
        "municipality_name",
        "year",
        "population",
    ]
    assert standardized_frame.to_dict(orient="records") == [
        {
            "CO_IBGE": "3304557",
            "municipality_name": "Rio de Janeiro",
            "year": 2025,
            "population": 6211223,
        },
        {
            "CO_IBGE": "3550308",
            "municipality_name": "São Paulo",
            "year": 2025,
            "population": 11895893,
        },
    ]
    assert summary.year == 2025
    assert summary.raw_row_count == 3
    assert summary.kept_row_count == 2
    assert summary.dropped_non_municipality_rows == 1
    assert summary.unique_municipality_count == 2


def test_standardize_population_frame_raises_for_missing_required_columns() -> None:
    """The ETL should fail loudly when raw required columns are missing."""
    raw_frame = pd.DataFrame(
        {
            "COD. UF": [35],
            "Municípios": ["São Paulo"],
            "POPULAÇÃO ESTIMADA": [11895893],
        }
    )

    with pytest.raises(PopulationSchemaError, match="municipality_code"):
        standardize_population_frame(raw_frame, year=2025)


def test_infer_population_year_prefers_pop_prefix_over_release_date() -> None:
    """The ETL should infer the reference year from the POP-prefixed filename."""
    input_path = Path("POP2025_20260113.xls")

    inferred_year = infer_population_year(input_path)

    assert inferred_year == 2025


def test_load_population_raw_frame_selects_the_municipality_sheet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loader should ignore non-municipal worksheets and select the right one."""

    class FakeExcelFile:
        def __init__(self) -> None:
            self.sheet_names = ["BRASIL E UFs", "Municípios"]

    frames = {
        "BRASIL E UFs": pd.DataFrame(
            {
                "BRASIL E UNIDADES DA FEDERAÇÃO": ["Brasil"],
                "POPULAÇÃO ESTIMADA": [213421037],
            }
        ),
        "Municípios": pd.DataFrame(
            {
                "COD. UF": [35],
                "COD. MUNIC": [50308],
                "NOME DO MUNICÍPIO": ["São Paulo"],
                "POPULAÇÃO ESTIMADA": [11895893],
            }
        ),
    }

    monkeypatch.setattr(
        "motorcycle_growth.population_etl.pd.ExcelFile",
        lambda *args, **kwargs: FakeExcelFile(),
    )
    monkeypatch.setattr(
        "motorcycle_growth.population_etl.pd.read_excel",
        lambda _excel_file, *, sheet_name, header: frames[sheet_name],
    )

    loaded_frame = load_population_raw_frame(Path("POP2025.xlsx"))

    assert list(loaded_frame.columns) == [
        "COD. UF",
        "COD. MUNIC",
        "NOME DO MUNICÍPIO",
        "POPULAÇÃO ESTIMADA",
    ]


def test_run_population_etl_writes_parquet_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ETL should write the cleaned population dataset to parquet."""
    input_path = tmp_path / "POP2025_20260113.xls"
    input_path.write_text("placeholder", encoding="utf-8")
    output_path = tmp_path / "population" / "population_2025.parquet"

    raw_frame = pd.DataFrame(
        {
            "COD. UF": [35],
            "COD. MUNIC": [50308],
            "Municípios": ["São Paulo"],
            "POPULAÇÃO ESTIMADA": [11895893],
        }
    )

    monkeypatch.setattr(
        "motorcycle_growth.population_etl.load_population_raw_frame",
        lambda _: raw_frame,
    )

    result = run_population_etl(
        input_path=input_path,
        output_path=output_path,
    )

    saved_frame = pd.read_parquet(result.output_path)

    assert result.output_path == output_path.resolve()
    assert saved_frame.to_dict(orient="records") == [
        {
            "CO_IBGE": "3550308",
            "municipality_name": "São Paulo",
            "year": 2025,
            "population": 11895893,
        }
    ]
