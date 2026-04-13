"""ETL helpers for municipality-level population data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from motorcycle_growth.config import INTERIM_DATA_DIR, RAW_DATA_DIR
from motorcycle_growth.etl_utils import (
    assert_mask_empty,
    assert_no_duplicate_keys,
    build_normalized_column_map,
    clean_numeric_code,
    find_column_by_aliases,
    normalize_co_ibge_like_code,
    normalize_label,
    resolve_output_path,
    save_parquet_frame,
)
from motorcycle_growth.logging_utils import get_logger


LOGGER = get_logger(__name__)

POPULATION_RAW_DIR = RAW_DATA_DIR / "ibge_population"
POPULATION_INTERIM_DIR = INTERIM_DATA_DIR / "population"
DEFAULT_OUTPUT_FILE_NAME_TEMPLATE = "population_municipality_year_{year}.parquet"


class PopulationEtlError(RuntimeError):
    """Base error for the population ETL flow."""


class PopulationDependencyError(PopulationEtlError):
    """Raised when a required optional dependency is missing."""


class PopulationSchemaError(PopulationEtlError):
    """Raised when the raw population file does not match expectations."""


class PopulationDataQualityError(PopulationEtlError):
    """Raised when the cleaned population dataset is not fit for downstream use."""


@dataclass(frozen=True)
class PopulationQualitySummary:
    """Small quality summary emitted after the ETL succeeds."""

    year: int
    raw_row_count: int
    kept_row_count: int
    dropped_non_municipality_rows: int
    unique_municipality_count: int


@dataclass(frozen=True)
class PopulationEtlResult:
    """Result metadata for one population ETL execution."""

    input_path: Path
    output_path: Path
    summary: PopulationQualitySummary


def resolve_population_input_path(input_path: Path | None = None) -> Path:
    """Resolve one raw population file from the expected input location."""
    if input_path is not None:
        resolved_path = input_path.expanduser().resolve()
        if not resolved_path.exists():
            msg = f"Population raw file not found: {resolved_path}"
            raise FileNotFoundError(msg)
        return resolved_path

    candidates = sorted(
        path
        for pattern in ("*.xls", "*.xlsx", "*.ods")
        for path in POPULATION_RAW_DIR.glob(pattern)
        if path.is_file()
    )

    if not candidates:
        msg = (
            "No raw population file was found under "
            f"{POPULATION_RAW_DIR}. Place the original IBGE file there or pass "
            "--input-path explicitly."
        )
        raise FileNotFoundError(msg)

    if len(candidates) > 1:
        candidate_list = ", ".join(path.name for path in candidates)
        msg = (
            "Multiple raw population files were found. Pass --input-path to choose "
            f"one file explicitly. Candidates: {candidate_list}"
        )
        raise PopulationEtlError(msg)

    return candidates[0]


def infer_population_year(input_path: Path, explicit_year: int | None = None) -> int:
    """Infer the reference year from the raw filename when not provided."""
    if explicit_year is not None:
        return explicit_year

    preferred_match = re.search(r"POP(?P<year>20\d{2})", input_path.stem.upper())
    if preferred_match is not None:
        return int(preferred_match.group("year"))

    year_matches = re.findall(r"(?<!\d)(20\d{2})(?!\d)", input_path.stem)
    unique_matches = sorted(set(year_matches))
    if len(unique_matches) == 1:
        return int(unique_matches[0])

    msg = (
        "Could not infer the population year from the raw filename. Pass --year "
        f"explicitly. File name: {input_path.name}"
    )
    raise PopulationEtlError(msg)


def load_population_raw_frame(input_path: Path) -> pd.DataFrame:
    """Load the raw population spreadsheet from disk."""
    LOGGER.info("Loading raw population file from %s", input_path)

    excel_engine: str | None = None
    if input_path.suffix.lower() == ".xls":
        try:
            import xlrd  # noqa: F401
        except ImportError as exc:
            msg = (
                "Reading the official IBGE .xls file requires the optional "
                "dependency 'xlrd'. Install project dependencies with Poetry "
                "before running this ETL step."
            )
            raise PopulationDependencyError(msg) from exc

        excel_engine = "xlrd"

    excel_file = pd.ExcelFile(input_path, engine=excel_engine)

    # Assumptions isolated here: the current IBGE workbook stores the title in the
    # first row and the actual headers in the second row, and the municipality data
    # may live in a dedicated worksheet such as "Municípios" instead of the first tab.
    for sheet_name in excel_file.sheet_names:
        candidate_frame = pd.read_excel(excel_file, sheet_name=sheet_name, header=1)
        try:
            resolve_source_columns(candidate_frame)
        except PopulationSchemaError:
            continue

        LOGGER.info("Selected population worksheet: %s", sheet_name)
        return candidate_frame

    available_sheets = ", ".join(excel_file.sheet_names)
    msg = (
        "Could not find a worksheet with municipality-level population columns. "
        f"Available worksheets: {available_sheets}"
    )
    raise PopulationSchemaError(msg)


def resolve_source_columns(raw_frame: pd.DataFrame) -> dict[str, str]:
    """Resolve the raw columns required to build the standard output."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)

    columns = {
        "uf_code": find_column_by_aliases(normalized_to_original, "COD. UF", "COD UF"),
        "municipality_code": find_column_by_aliases(
            normalized_to_original,
            "COD. MUNIC",
            "COD MUNIC",
        ),
        "municipality_name": find_column_by_aliases(
            normalized_to_original,
            "Municípios",
            "Municipios",
            "NOME DO MUNIC",
            "NOME DO MUNICIPIO",
        ),
        "population": find_column_by_aliases(
            normalized_to_original,
            "POPULAÇÃO",
            "POPULACAO",
            "POPULAÇÃO ESTIMADA",
            "POPULACAO ESTIMADA",
            allow_prefix="POPULA",
        ),
    }

    missing = [
        column_name
        for column_name, original_name in columns.items()
        if original_name is None
    ]
    if missing:
        available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
        msg = (
            "Population raw file is missing required columns for the ETL: "
            f"{', '.join(missing)}. Available columns: {available_columns}"
        )
        raise PopulationSchemaError(msg)

    return {
        canonical_name: original_name
        for canonical_name, original_name in columns.items()
        if original_name is not None
    }


def standardize_population_frame(
    raw_frame: pd.DataFrame,
    *,
    year: int,
) -> tuple[pd.DataFrame, PopulationQualitySummary]:
    """Validate and standardize the municipality population dataset."""
    resolved_columns = resolve_source_columns(raw_frame)
    working_frame = raw_frame.rename(
        columns={
            resolved_columns["uf_code"]: "uf_code",
            resolved_columns["municipality_code"]: "municipality_code",
            resolved_columns["municipality_name"]: "municipality_name",
            resolved_columns["population"]: "population",
        }
    )[
        ["uf_code", "municipality_code", "municipality_name", "population"]
    ].copy()

    raw_row_count = len(working_frame)

    working_frame["municipality_name"] = (
        working_frame["municipality_name"].astype("string").str.strip()
    )
    working_frame["uf_code"] = working_frame["uf_code"].map(
        lambda value: clean_numeric_code(value, width=2)
    )
    working_frame["municipality_code"] = working_frame["municipality_code"].map(
        lambda value: clean_numeric_code(value, width=5)
    )
    working_frame["population"] = pd.to_numeric(
        working_frame["population"],
        errors="coerce",
    )

    municipality_row_mask = (
        working_frame["uf_code"].notna()
        & working_frame["municipality_code"].notna()
        & working_frame["municipality_name"].notna()
        & working_frame["municipality_name"].ne("")
    )

    dropped_non_municipality_rows = int((~municipality_row_mask).sum())
    working_frame = working_frame.loc[municipality_row_mask].copy()

    assert_mask_empty(
        working_frame["population"].isna(),
        error_cls=PopulationDataQualityError,
        message="Population ETL found municipality rows with missing or invalid population values",
    )

    working_frame["population"] = working_frame["population"].round().astype("Int64")
    working_frame["CO_IBGE"] = (
        working_frame["uf_code"] + working_frame["municipality_code"]
    ).map(normalize_co_ibge_like_code)
    working_frame["year"] = int(year)

    assert_mask_empty(
        ~working_frame["CO_IBGE"].str.fullmatch(r"\d{7}"),
        error_cls=PopulationDataQualityError,
        message="Population ETL generated invalid municipality codes in CO_IBGE",
    )
    assert_no_duplicate_keys(
        working_frame,
        subset=["CO_IBGE", "year"],
        error_cls=PopulationDataQualityError,
        message="Population ETL found duplicate municipality-year keys in the cleaned dataset",
    )

    standardized_frame = (
        working_frame[["CO_IBGE", "municipality_name", "year", "population"]]
        .sort_values(["year", "CO_IBGE"])
        .reset_index(drop=True)
    )

    summary = PopulationQualitySummary(
        year=int(year),
        raw_row_count=raw_row_count,
        kept_row_count=len(standardized_frame),
        dropped_non_municipality_rows=dropped_non_municipality_rows,
        unique_municipality_count=int(standardized_frame["CO_IBGE"].nunique()),
    )
    return standardized_frame, summary


def build_default_output_path(*, year: int) -> Path:
    """Return the default parquet path for a cleaned population dataset."""
    return POPULATION_INTERIM_DIR / DEFAULT_OUTPUT_FILE_NAME_TEMPLATE.format(year=year)


def run_population_etl(
    *,
    input_path: Path | None = None,
    output_path: Path | None = None,
    year: int | None = None,
) -> PopulationEtlResult:
    """Execute the population ETL from raw IBGE file to interim parquet."""
    resolved_input_path = resolve_population_input_path(input_path)
    resolved_year = infer_population_year(resolved_input_path, explicit_year=year)
    raw_frame = load_population_raw_frame(resolved_input_path)
    population_frame, summary = standardize_population_frame(raw_frame, year=resolved_year)

    resolved_output_path = resolve_output_path(
        output_path=output_path,
        default_path=build_default_output_path(year=resolved_year),
    )
    saved_output_path = save_parquet_frame(
        population_frame,
        output_path=resolved_output_path,
    )

    LOGGER.info(
        (
            "Population ETL summary: year=%s raw_rows=%s kept_rows=%s "
            "dropped_non_municipality_rows=%s unique_municipalities=%s"
        ),
        summary.year,
        summary.raw_row_count,
        summary.kept_row_count,
        summary.dropped_non_municipality_rows,
        summary.unique_municipality_count,
    )
    LOGGER.info("Clean population dataset saved to %s", saved_output_path)

    return PopulationEtlResult(
        input_path=resolved_input_path,
        output_path=saved_output_path,
        summary=summary,
    )
