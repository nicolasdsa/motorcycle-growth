"""Tests for the SENATRAN fleet ETL module."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from motorcycle_growth.senatran_fleet_etl import (
    SenatranPeriod,
    aggregate_monthly_to_year,
    apply_municipality_alias,
    build_frame_from_sheet_rows,
    infer_senatran_period,
    run_senatran_fleet_etl,
    standardize_senatran_monthly_frame,
)


def test_build_frame_from_sheet_rows_uses_the_detected_header_row() -> None:
    """The loader should use the SENATRAN header row and drop repeated headers."""
    rows = [
        ["Fleet title"],
        [None, None, "12345"],
        [
            "UF",
            "MUNICIPIO",
            "TOTAL",
            "CICLOMOTOR",
            "MOTOCICLETA",
            "MOTONETA",
            "QUADRICICLO",
            "SIDE-CAR",
            "TRICICLO",
        ],
        [
            "UF",
            "MUNICIPIO",
            "TOTAL",
            "CICLOMOTOR",
            "MOTOCICLETA",
            "MOTONETA",
            "QUADRICICLO",
            "SIDE-CAR",
            "TRICICLO",
        ],
        ["SP", "SAO PAULO", "10", "1", "2", "3", "0", "0", "0"],
    ]

    frame = build_frame_from_sheet_rows(rows)

    assert frame.to_dict(orient="records") == [
        {
            "UF": "SP",
            "MUNICIPIO": "SAO PAULO",
            "TOTAL": "10",
            "CICLOMOTOR": "1",
            "MOTOCICLETA": "2",
            "MOTONETA": "3",
            "QUADRICICLO": "0",
            "SIDE-CAR": "0",
            "TRICICLO": "0",
        }
    ]


def test_infer_senatran_period_prefers_sheet_name() -> None:
    """The ETL should infer the month-year period from the worksheet name."""
    raw_frame = pd.DataFrame(columns=["UF"])

    period = infer_senatran_period(
        input_path=Path("frotapormunicipioetipojaneiro2025.xlsx"),
        sheet_name="FEVEREIRO_2026",
        raw_frame=raw_frame,
    )

    assert period == SenatranPeriod(year=2026, month=2, month_label="FEVEREIRO")


def test_apply_municipality_alias_maps_known_senatran_variants() -> None:
    """The linkage should fix known historical or orthographic variants explicitly."""
    assert apply_municipality_alias("RJ", "Parati") == "PARATY"
    assert apply_municipality_alias("PB", "Santarém") == "JOCA CLAUDINO"
    assert apply_municipality_alias("SP", "São Paulo") == "SAO PAULO"


def test_standardize_senatran_monthly_frame_links_municipality_codes() -> None:
    """The ETL should recover CO_IBGE and build motorcycles_total from raw columns."""
    raw_frame = pd.DataFrame(
        {
            "UF": ["RJ", "SP", "SP"],
            "MUNICIPIO": ["PARATI", "SAO PAULO", "MUNICIPIO NAO INFORMADO"],
            "TOTAL": ["15", "30", "99"],
            "CICLOMOTOR": ["1", "2", "0"],
            "MOTOCICLETA": ["5", "10", "0"],
            "MOTONETA": ["4", "6", "0"],
            "QUADRICICLO": ["0", "1", "0"],
            "SIDE-CAR": ["0", "0", "0"],
            "TRICICLO": ["1", "2", "0"],
        }
    )
    population_crosswalk = pd.DataFrame(
        {
            "CO_IBGE": ["3338070", "3550308"],
            "uf": ["RJ", "SP"],
            "municipality_name": ["Paraty", "São Paulo"],
            "municipality_lookup_key": ["PARATY", "SAO PAULO"],
        }
    )

    frame = standardize_senatran_monthly_frame(
        raw_frame,
        period=SenatranPeriod(year=2026, month=2, month_label="FEVEREIRO"),
        source_file_name="fleet_feb_2026.xlsx",
        population_crosswalk=population_crosswalk,
    )

    assert frame.to_dict(orient="records") == [
        {
            "CO_IBGE": "3338070",
            "municipality_name": "Paraty",
            "uf": "RJ",
            "year": 2026,
            "month": 2,
            "month_label": "FEVEREIRO",
            "ciclomotor": 1,
            "motocicleta": 5,
            "motoneta": 4,
            "quadriciclo": 0,
            "side_car": 0,
            "triciclo": 1,
            "motorcycles_total": 11,
            "source_file_name": "fleet_feb_2026.xlsx",
            "municipality_name_source": "PARATI",
        },
        {
            "CO_IBGE": "3550308",
            "municipality_name": "São Paulo",
            "uf": "SP",
            "year": 2026,
            "month": 2,
            "month_label": "FEVEREIRO",
            "ciclomotor": 2,
            "motocicleta": 10,
            "motoneta": 6,
            "quadriciclo": 1,
            "side_car": 0,
            "triciclo": 2,
            "motorcycles_total": 21,
            "source_file_name": "fleet_feb_2026.xlsx",
            "municipality_name_source": "SAO PAULO",
        },
    ]


def test_aggregate_monthly_to_year_computes_yearly_average_and_flags_partial_year() -> None:
    """Annualization should average the available monthly stock observations."""
    monthly_frame = pd.DataFrame(
        {
            "CO_IBGE": ["3550308", "3550308"],
            "municipality_name": ["São Paulo", "São Paulo"],
            "year": [2026, 2026],
            "month": [2, 3],
            "month_label": ["FEVEREIRO", "MARCO"],
            "ciclomotor": [10, 14],
            "motocicleta": [100, 104],
            "motoneta": [50, 54],
            "quadriciclo": [1, 1],
            "side_car": [0, 0],
            "triciclo": [2, 4],
            "motorcycles_total": [163, 177],
            "source_file_name": ["fleet_feb.xlsx", "fleet_mar.xlsx"],
            "municipality_name_source": ["SAO PAULO", "SAO PAULO"],
        }
    )

    annual_frame = aggregate_monthly_to_year(monthly_frame)

    assert annual_frame.to_dict(orient="records") == [
        {
            "CO_IBGE": "3550308",
            "municipality_name": "São Paulo",
            "year": 2026,
            "motorcycles_total": 170.0,
            "motocicleta": 102.0,
            "motoneta": 52.0,
            "ciclomotor": 12.0,
            "triciclo": 3.0,
            "quadriciclo": 1.0,
            "side_car": 0.0,
            "months_observed": 2,
            "source_months": "02,03",
            "is_partial_year": True,
            "aggregation_rule": "yearly_average_of_available_monthly_stocks",
        }
    ]


def test_run_senatran_fleet_etl_writes_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ETL should write the annual parquet output and metadata json."""
    senatran_input_path = tmp_path / "frotapormunicipioetipofevereiro2026.xlsx"
    senatran_input_path.write_text("placeholder", encoding="utf-8")
    population_path = tmp_path / "population_reference.parquet"
    pd.DataFrame(
        {
            "CO_IBGE": ["3550308"],
            "municipality_name": ["São Paulo"],
        }
    ).to_parquet(population_path, index=False)
    output_path = tmp_path / "senatran" / "senatran_motorcycles.parquet"
    metadata_path = tmp_path / "senatran" / "senatran_motorcycles_metadata.json"

    raw_frame = pd.DataFrame(
        {
            "UF": ["SP"],
            "MUNICIPIO": ["SAO PAULO"],
            "TOTAL": ["25"],
            "CICLOMOTOR": ["2"],
            "MOTOCICLETA": ["10"],
            "MOTONETA": ["8"],
            "QUADRICICLO": ["1"],
            "SIDE-CAR": ["0"],
            "TRICICLO": ["1"],
        }
    )

    monkeypatch.setattr(
        "motorcycle_growth.senatran_fleet_etl.load_senatran_raw_frame",
        lambda _path: (raw_frame, "FEVEREIRO_2026"),
    )

    result = run_senatran_fleet_etl(
        input_paths=[senatran_input_path],
        output_path=output_path,
        metadata_path=metadata_path,
        population_path=population_path,
    )

    saved_frame = pd.read_parquet(result.output_path)
    saved_metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))

    assert result.output_path == output_path.resolve()
    assert result.metadata_path == metadata_path.resolve()
    assert saved_frame.to_dict(orient="records") == [
        {
            "CO_IBGE": "3550308",
            "municipality_name": "São Paulo",
            "year": 2026,
            "motorcycles_total": 22.0,
            "motocicleta": 10.0,
            "motoneta": 8.0,
            "ciclomotor": 2.0,
            "triciclo": 1.0,
            "quadriciclo": 1.0,
            "side_car": 0.0,
            "months_observed": 1,
            "source_months": "02",
            "is_partial_year": True,
            "aggregation_rule": "yearly_average_of_available_monthly_stocks",
        }
    ]
    assert saved_metadata["summary"]["annual_row_count"] == 1
    assert saved_metadata["input_files"] == [
        {
            "path": str(senatran_input_path.resolve()),
            "year": 2026,
            "month": 2,
            "month_label": "FEVEREIRO",
        }
    ]
