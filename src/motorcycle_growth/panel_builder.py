"""Build the municipality-year analytical panel from interim datasets."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re

import pandas as pd

from motorcycle_growth.config import INTERIM_DATA_DIR, PROCESSED_DATA_DIR
from motorcycle_growth.etl_utils import (
    assert_mask_empty,
    assert_no_duplicate_keys,
    normalize_lookup_text,
    resolve_output_path,
    save_json_payload,
    save_parquet_frame,
)
from motorcycle_growth.logging_utils import get_logger


LOGGER = get_logger(__name__)

PANEL_PROCESSED_DIR = PROCESSED_DATA_DIR / "panel"

DEFAULT_POPULATION_PATH = (
    INTERIM_DATA_DIR / "population" / "population_municipality_year_2025.parquet"
)
DEFAULT_SENATRAN_PATH = (
    INTERIM_DATA_DIR / "senatran" / "senatran_motorcycles_municipality_year.parquet"
)
DEFAULT_SIH_PATH = (
    INTERIM_DATA_DIR / "sih" / "sih_motorcycle_hospitalizations_municipality_year.parquet"
)
DEFAULT_SIM_PATH = (
    INTERIM_DATA_DIR / "sim" / "sim_motorcycle_mortality_municipality_year.parquet"
)
DEFAULT_CNES_PATH = (
    INTERIM_DATA_DIR / "cnes" / "cnes_infrastructure_municipality_year.parquet"
)

DEFAULT_OUTPUT_FILE_NAME = "analytical_panel_municipality_year.parquet"
DEFAULT_METADATA_FILE_NAME = "analytical_panel_municipality_year_metadata.json"

PANEL_KEY_COLUMNS = ("municipality_key", "year")
PANEL_MERGE_REASON = (
    "Outer joins preserve municipality-year rows that exist in one interim dataset "
    "but not in others. This is required because the current interim extracts do "
    "not share the same year coverage or completeness, and an inner join would "
    "silently discard valid analytical observations."
)
SOURCE_JOIN_TYPE = "outer"
SOURCE_PRESENCE_COLUMNS = (
    "has_population_data",
    "has_senatran_data",
    "has_sih_data",
    "has_sim_data",
    "has_cnes_data",
)


class PanelBuildError(RuntimeError):
    """Base error for the panel-building flow."""


class PanelSchemaError(PanelBuildError):
    """Raised when an interim dataset does not contain the required columns."""


class PanelDataQualityError(PanelBuildError):
    """Raised when the analytical panel fails one or more quality checks."""


@dataclass(frozen=True)
class PanelInputPaths:
    """Resolved input paths used to build the analytical panel."""

    population_path: Path
    senatran_path: Path
    sih_path: Path
    sim_path: Path
    cnes_path: Path


@dataclass(frozen=True)
class MergeStepSummary:
    """Diagnostics for one outer-join step in the panel construction."""

    dataset_name: str
    join_type: str
    left_row_count: int
    right_row_count: int
    output_row_count: int
    matched_row_count: int
    left_only_row_count: int
    right_only_row_count: int


@dataclass(frozen=True)
class PanelBuildSummary:
    """High-level summary of the analytical panel output."""

    row_count: int
    unique_municipality_count: int
    unique_year_count: int
    missing_co_ibge_count: int
    missing_municipality_name_count: int
    rows_with_any_outcome_data: int
    rows_with_any_capacity_data: int


@dataclass(frozen=True)
class PanelBuildResult:
    """Result metadata for one analytical-panel execution."""

    input_paths: PanelInputPaths
    output_path: Path
    metadata_path: Path
    summary: PanelBuildSummary


def build_default_output_path() -> Path:
    """Return the default parquet output path for the analytical panel."""
    return PANEL_PROCESSED_DIR / DEFAULT_OUTPUT_FILE_NAME


def build_default_metadata_path(output_path: Path) -> Path:
    """Return the default metadata path alongside the parquet output."""
    return output_path.with_name(DEFAULT_METADATA_FILE_NAME)


def resolve_input_path(
    input_path: Path | None,
    *,
    default_path: Path,
    dataset_label: str,
) -> Path:
    """Resolve one required interim input path."""
    resolved_path = (
        input_path.expanduser().resolve()
        if input_path is not None
        else default_path.resolve()
    )
    if not resolved_path.exists():
        msg = (
            f"The required {dataset_label} interim dataset was not found: "
            f"{resolved_path}"
        )
        raise FileNotFoundError(msg)
    return resolved_path


def resolve_panel_input_paths(
    *,
    population_path: Path | None = None,
    senatran_path: Path | None = None,
    sih_path: Path | None = None,
    sim_path: Path | None = None,
    cnes_path: Path | None = None,
) -> PanelInputPaths:
    """Resolve all required interim input paths for the analytical panel."""
    return PanelInputPaths(
        population_path=resolve_input_path(
            population_path,
            default_path=DEFAULT_POPULATION_PATH,
            dataset_label="population",
        ),
        senatran_path=resolve_input_path(
            senatran_path,
            default_path=DEFAULT_SENATRAN_PATH,
            dataset_label="SENATRAN fleet",
        ),
        sih_path=resolve_input_path(
            sih_path,
            default_path=DEFAULT_SIH_PATH,
            dataset_label="SIH hospitalization",
        ),
        sim_path=resolve_input_path(
            sim_path,
            default_path=DEFAULT_SIM_PATH,
            dataset_label="SIM mortality",
        ),
        cnes_path=resolve_input_path(
            cnes_path,
            default_path=DEFAULT_CNES_PATH,
            dataset_label="CNES infrastructure",
        ),
    )


def extract_numeric_code_digits(value: object) -> str | None:
    """Extract digits from one municipality-code-like value."""
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    integer_like_match = re.fullmatch(r"(?P<digits>\d+)\.0+", text)
    if integer_like_match is not None:
        digits_only = integer_like_match.group("digits")
    else:
        digits_only = re.sub(r"\D", "", text)

    return digits_only or None


def normalize_panel_municipality_code(value: object) -> str | None:
    """Normalize municipality codes to a 6-digit merge key."""
    digits_only = extract_numeric_code_digits(value)
    if digits_only is None:
        return None
    if len(digits_only) == 7:
        return digits_only[:6]
    if len(digits_only) == 6:
        return digits_only
    if len(digits_only) < 6:
        return digits_only.zfill(6)
    return None


def normalize_co_ibge_code(value: object) -> str | None:
    """Normalize one CO_IBGE value to a 7-digit identifier."""
    digits_only = extract_numeric_code_digits(value)
    if digits_only is None:
        return None
    if len(digits_only) == 7:
        return digits_only
    return None


def build_official_municipality_key(value: object) -> str | None:
    """Build the canonical municipality key from one official 6-digit code."""
    municipality_code = normalize_panel_municipality_code(value)
    if municipality_code is None:
        return None
    return f"MU6:{municipality_code}"


def build_co_ibge_municipality_key(value: object) -> str | None:
    """Build the canonical municipality key from one CO_IBGE code."""
    co_ibge_code = normalize_co_ibge_code(value)
    if co_ibge_code is None:
        return None
    return f"CO7:{co_ibge_code}"


def require_columns(
    frame: pd.DataFrame,
    *,
    required_columns: tuple[str, ...],
    dataset_label: str,
) -> None:
    """Raise when one frame does not expose the required columns."""
    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in frame.columns
    ]
    if missing_columns:
        available_columns = ", ".join(str(column_name) for column_name in frame.columns)
        msg = (
            f"{dataset_label} is missing required columns: "
            f"{', '.join(missing_columns)}. Available columns: {available_columns}"
        )
        raise PanelSchemaError(msg)


def validate_source_frame(
    frame: pd.DataFrame,
    *,
    dataset_label: str,
    key_columns: tuple[str, ...] = PANEL_KEY_COLUMNS,
) -> None:
    """Validate one standardized source frame before it enters the panel merge."""
    missing_key_mask = frame.loc[:, list(key_columns)].isna().any(axis=1)
    assert_mask_empty(
        missing_key_mask,
        error_cls=PanelDataQualityError,
        message=f"{dataset_label} contains rows with missing municipality-year keys",
    )
    assert_no_duplicate_keys(
        frame,
        subset=key_columns,
        error_cls=PanelDataQualityError,
        message=f"{dataset_label} contains duplicate municipality-year rows",
    )

    numeric_columns = [
        column_name
        for column_name in frame.columns
        if pd.api.types.is_numeric_dtype(frame[column_name])
        and column_name != "year"
        and not pd.api.types.is_bool_dtype(frame[column_name])
    ]
    if not numeric_columns:
        return

    negative_mask = frame.loc[:, numeric_columns].lt(0).any(axis=1)
    assert_mask_empty(
        negative_mask,
        error_cls=PanelDataQualityError,
        message=f"{dataset_label} contains impossible negative values",
    )


def build_cnes_official_code_lookup(cnes_frame: pd.DataFrame) -> pd.DataFrame:
    """Build an official municipality-code lookup from CNES names and UF prefixes."""
    official_code_lookup = cnes_frame.loc[
        :,
        ["municipality_code", "municipality_name"],
    ].drop_duplicates(subset=["municipality_code"])
    missing_name_count = int(official_code_lookup["municipality_name"].isna().sum())
    if missing_name_count > 0:
        LOGGER.info(
            "CNES official municipality lookup skipped %s rows without municipality names",
            missing_name_count,
        )
    official_code_lookup = official_code_lookup.loc[
        official_code_lookup["municipality_name"].notna()
    ].copy()
    official_code_lookup["uf_code"] = official_code_lookup["municipality_code"].str[:2]
    official_code_lookup["municipality_name_lookup"] = official_code_lookup[
        "municipality_name"
    ].map(lambda value: normalize_lookup_text(value) if not pd.isna(value) else pd.NA)
    assert_no_duplicate_keys(
        official_code_lookup,
        subset=("uf_code", "municipality_name_lookup"),
        error_cls=PanelDataQualityError,
        message=(
            "CNES official municipality lookup contains duplicated UF-name pairs and "
            "cannot support deterministic panel key mapping"
        ),
    )
    return official_code_lookup.rename(
        columns={"municipality_code": "official_municipality_code"}
    ).loc[
        :,
        ["uf_code", "municipality_name_lookup", "official_municipality_code"],
    ]


def remap_population_like_codes(
    frame: pd.DataFrame,
    *,
    official_code_lookup: pd.DataFrame,
    dataset_label: str,
) -> pd.DataFrame:
    """Map population-like municipality identifiers to the official 6-digit code."""
    frame["uf_code"] = frame["CO_IBGE"].str[:2]
    frame["municipality_name_lookup"] = frame["municipality_name"].map(
        lambda value: normalize_lookup_text(value) if not pd.isna(value) else pd.NA
    )
    mapped_frame = frame.merge(
        official_code_lookup,
        on=["uf_code", "municipality_name_lookup"],
        how="left",
        validate="many_to_one",
    )
    mapped_frame["municipality_code"] = mapped_frame["official_municipality_code"]
    mapped_frame["municipality_key"] = mapped_frame["municipality_code"].map(
        build_official_municipality_key
    )
    unresolved_mask = mapped_frame["municipality_key"].isna()
    mapped_frame.loc[unresolved_mask, "municipality_key"] = mapped_frame.loc[
        unresolved_mask,
        "CO_IBGE",
    ].map(build_co_ibge_municipality_key)

    matched_row_count = int(mapped_frame["official_municipality_code"].notna().sum())
    fallback_row_count = int(mapped_frame["official_municipality_code"].isna().sum())
    LOGGER.info(
        "%s municipality-code remapping: matched_by_cnes_name=%s fallback=%s",
        dataset_label,
        matched_row_count,
        fallback_row_count,
    )
    return mapped_frame.drop(
        columns=[
            "uf_code",
            "municipality_name_lookup",
            "official_municipality_code",
        ]
    )


def standardize_population_panel_frame(
    frame: pd.DataFrame,
    *,
    official_code_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the population interim dataset for panel merges."""
    require_columns(
        frame,
        required_columns=("CO_IBGE", "municipality_name", "year", "population"),
        dataset_label="Population interim dataset",
    )
    standardized_frame = frame.loc[
        :,
        ["CO_IBGE", "municipality_name", "year", "population"],
    ].copy()
    standardized_frame["CO_IBGE"] = standardized_frame["CO_IBGE"].map(
        normalize_co_ibge_code
    )
    standardized_frame = remap_population_like_codes(
        standardized_frame,
        official_code_lookup=official_code_lookup,
        dataset_label="Population",
    )
    standardized_frame["year"] = pd.to_numeric(
        standardized_frame["year"],
        errors="coerce",
    ).astype("Int64")
    standardized_frame["population"] = pd.to_numeric(
        standardized_frame["population"],
        errors="coerce",
    )
    standardized_frame["municipality_name"] = standardized_frame[
        "municipality_name"
    ].astype("string")
    standardized_frame["has_population_data"] = True
    standardized_frame = standardized_frame.loc[
        :,
        [
            "municipality_key",
            "municipality_code",
            "year",
            "population",
            "CO_IBGE",
            "municipality_name",
            "has_population_data",
        ],
    ]
    validate_source_frame(
        standardized_frame,
        dataset_label="Population panel source",
    )
    return standardized_frame


def standardize_senatran_panel_frame(
    frame: pd.DataFrame,
    *,
    official_code_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the SENATRAN interim dataset for panel merges."""
    require_columns(
        frame,
        required_columns=(
            "CO_IBGE",
            "municipality_name",
            "year",
            "motorcycles_total",
            "motocicleta",
            "motoneta",
            "ciclomotor",
            "triciclo",
            "quadriciclo",
            "side_car",
            "months_observed",
            "source_months",
            "is_partial_year",
            "aggregation_rule",
        ),
        dataset_label="SENATRAN interim dataset",
    )
    standardized_frame = frame.copy()
    standardized_frame["CO_IBGE"] = standardized_frame["CO_IBGE"].map(
        normalize_co_ibge_code
    )
    standardized_frame = remap_population_like_codes(
        standardized_frame,
        official_code_lookup=official_code_lookup,
        dataset_label="SENATRAN",
    )
    standardized_frame["year"] = pd.to_numeric(
        standardized_frame["year"],
        errors="coerce",
    ).astype("Int64")
    standardized_frame["municipality_name"] = standardized_frame[
        "municipality_name"
    ].astype("string")
    numeric_columns = [
        "motorcycles_total",
        "motocicleta",
        "motoneta",
        "ciclomotor",
        "triciclo",
        "quadriciclo",
        "side_car",
        "months_observed",
    ]
    for column_name in numeric_columns:
        standardized_frame[column_name] = pd.to_numeric(
            standardized_frame[column_name],
            errors="coerce",
        )
    standardized_frame["is_partial_year"] = standardized_frame[
        "is_partial_year"
    ].astype("boolean")
    standardized_frame["has_senatran_data"] = True
    standardized_frame = standardized_frame.loc[
        :,
        [
            "municipality_key",
            "municipality_code",
            "year",
            "motorcycles_total",
            "motocicleta",
            "motoneta",
            "ciclomotor",
            "triciclo",
            "quadriciclo",
            "side_car",
            "months_observed",
            "source_months",
            "is_partial_year",
            "aggregation_rule",
            "CO_IBGE",
            "municipality_name",
            "has_senatran_data",
        ],
    ]
    validate_source_frame(
        standardized_frame,
        dataset_label="SENATRAN panel source",
    )
    return standardized_frame


def standardize_sih_panel_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Standardize the SIH interim dataset for panel merges."""
    require_columns(
        frame,
        required_columns=(
            "municipality_code",
            "year",
            "motorcycle_hospitalizations",
            "total_hospitalization_cost",
            "total_length_of_stay_days",
            "mean_length_of_stay_days",
            "mean_hospitalization_cost",
            "municipality_scope",
            "source_municipality_column",
        ),
        dataset_label="SIH interim dataset",
    )
    standardized_frame = frame.rename(
        columns={
            "municipality_scope": "sih_municipality_scope",
            "source_municipality_column": "sih_source_municipality_column",
        }
    ).copy()
    standardized_frame["municipality_code"] = standardized_frame[
        "municipality_code"
    ].map(normalize_panel_municipality_code)
    standardized_frame["municipality_key"] = standardized_frame["municipality_code"].map(
        build_official_municipality_key
    )
    standardized_frame["year"] = pd.to_numeric(
        standardized_frame["year"],
        errors="coerce",
    ).astype("Int64")
    numeric_columns = [
        "motorcycle_hospitalizations",
        "total_hospitalization_cost",
        "total_length_of_stay_days",
        "mean_length_of_stay_days",
        "mean_hospitalization_cost",
    ]
    for column_name in numeric_columns:
        standardized_frame[column_name] = pd.to_numeric(
            standardized_frame[column_name],
            errors="coerce",
        )
    standardized_frame["sih_municipality_scope"] = standardized_frame[
        "sih_municipality_scope"
    ].astype("string")
    standardized_frame["sih_source_municipality_column"] = standardized_frame[
        "sih_source_municipality_column"
    ].astype("string")
    standardized_frame["has_sih_data"] = True
    standardized_frame = standardized_frame.loc[
        :,
        [
            "municipality_key",
            "municipality_code",
            "year",
            "motorcycle_hospitalizations",
            "total_hospitalization_cost",
            "total_length_of_stay_days",
            "mean_length_of_stay_days",
            "mean_hospitalization_cost",
            "sih_municipality_scope",
            "sih_source_municipality_column",
            "has_sih_data",
        ],
    ]
    validate_source_frame(
        standardized_frame,
        dataset_label="SIH panel source",
    )
    return standardized_frame


def standardize_sim_panel_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Standardize the SIM interim dataset for panel merges."""
    require_columns(
        frame,
        required_columns=(
            "municipality_code",
            "year",
            "motorcycle_deaths",
            "municipality_scope",
            "source_municipality_column",
        ),
        dataset_label="SIM interim dataset",
    )
    standardized_frame = frame.rename(
        columns={
            "municipality_scope": "sim_municipality_scope",
            "source_municipality_column": "sim_source_municipality_column",
        }
    ).copy()
    standardized_frame["municipality_code"] = standardized_frame[
        "municipality_code"
    ].map(normalize_panel_municipality_code)
    standardized_frame["municipality_key"] = standardized_frame["municipality_code"].map(
        build_official_municipality_key
    )
    standardized_frame["year"] = pd.to_numeric(
        standardized_frame["year"],
        errors="coerce",
    ).astype("Int64")
    standardized_frame["motorcycle_deaths"] = pd.to_numeric(
        standardized_frame["motorcycle_deaths"],
        errors="coerce",
    )
    standardized_frame["sim_municipality_scope"] = standardized_frame[
        "sim_municipality_scope"
    ].astype("string")
    standardized_frame["sim_source_municipality_column"] = standardized_frame[
        "sim_source_municipality_column"
    ].astype("string")
    standardized_frame["has_sim_data"] = True
    standardized_frame = standardized_frame.loc[
        :,
        [
            "municipality_key",
            "municipality_code",
            "year",
            "motorcycle_deaths",
            "sim_municipality_scope",
            "sim_source_municipality_column",
            "has_sim_data",
        ],
    ]
    validate_source_frame(
        standardized_frame,
        dataset_label="SIM panel source",
    )
    return standardized_frame


def standardize_cnes_panel_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Standardize the CNES interim dataset for panel merges."""
    require_columns(
        frame,
        required_columns=(
            "municipality_code",
            "municipality_name",
            "year",
            "has_samu",
            "probable_samu_establishment_count",
            "mobile_emergency_establishment_count",
            "icu_beds_existing",
            "icu_beds_sus",
            "total_hospital_beds_existing",
            "total_hospital_beds_sus",
            "observed_bed_competence_count",
            "has_bed_snapshot",
            "has_establishment_snapshot",
        ),
        dataset_label="CNES interim dataset",
    )
    standardized_frame = frame.copy()
    standardized_frame["municipality_code"] = standardized_frame[
        "municipality_code"
    ].map(normalize_panel_municipality_code)
    standardized_frame["municipality_key"] = standardized_frame["municipality_code"].map(
        build_official_municipality_key
    )
    standardized_frame["year"] = pd.to_numeric(
        standardized_frame["year"],
        errors="coerce",
    ).astype("Int64")
    standardized_frame["municipality_name"] = standardized_frame[
        "municipality_name"
    ].astype("string")
    numeric_columns = [
        "probable_samu_establishment_count",
        "mobile_emergency_establishment_count",
        "icu_beds_existing",
        "icu_beds_sus",
        "total_hospital_beds_existing",
        "total_hospital_beds_sus",
        "observed_bed_competence_count",
    ]
    for column_name in numeric_columns:
        standardized_frame[column_name] = pd.to_numeric(
            standardized_frame[column_name],
            errors="coerce",
        )
    for column_name in (
        "has_samu",
        "has_bed_snapshot",
        "has_establishment_snapshot",
    ):
        standardized_frame[column_name] = standardized_frame[column_name].astype(
            "boolean"
        )
    standardized_frame["has_cnes_data"] = True
    standardized_frame = standardized_frame.loc[
        :,
        [
            "municipality_key",
            "municipality_code",
            "year",
            "has_samu",
            "probable_samu_establishment_count",
            "mobile_emergency_establishment_count",
            "icu_beds_existing",
            "icu_beds_sus",
            "total_hospital_beds_existing",
            "total_hospital_beds_sus",
            "observed_bed_competence_count",
            "has_bed_snapshot",
            "has_establishment_snapshot",
            "municipality_name",
            "has_cnes_data",
        ],
    ]
    validate_source_frame(
        standardized_frame,
        dataset_label="CNES panel source",
    )
    return standardized_frame


def build_identity_crosswalk(
    *,
    population_frame: pd.DataFrame,
    senatran_frame: pd.DataFrame,
    cnes_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Build a municipality identity crosswalk for display columns."""
    identity_records = pd.concat(
        [
            population_frame.loc[
                :,
                ["municipality_key", "municipality_code", "CO_IBGE", "municipality_name"],
            ].assign(source_priority=1, name_source="population"),
            senatran_frame.loc[
                :,
                ["municipality_key", "municipality_code", "CO_IBGE", "municipality_name"],
            ].assign(source_priority=2, name_source="senatran"),
            cnes_frame.loc[
                :,
                ["municipality_key", "municipality_code", "municipality_name"],
            ].assign(CO_IBGE=pd.NA, source_priority=3, name_source="cnes"),
        ],
        ignore_index=True,
    )

    co_ibge_records = identity_records.loc[identity_records["CO_IBGE"].notna()].copy()
    conflicting_co_ibge = co_ibge_records.groupby("municipality_key")["CO_IBGE"].nunique()
    conflicting_co_ibge = conflicting_co_ibge.loc[conflicting_co_ibge.gt(1)]
    if not conflicting_co_ibge.empty:
        conflicting_codes = ", ".join(conflicting_co_ibge.index.tolist()[:10])
        msg = (
            "The identity crosswalk found conflicting CO_IBGE mappings for the same "
            f"municipality key. Examples: {conflicting_codes}"
        )
        raise PanelDataQualityError(msg)

    identity_records["municipality_name_lookup"] = identity_records["municipality_name"].map(
        lambda value: normalize_lookup_text(value) if not pd.isna(value) else pd.NA
    )
    identity_records = identity_records.sort_values(
        by=["source_priority", "municipality_code"],
        kind="stable",
    )

    crosswalk = identity_records.drop_duplicates(
        subset=["municipality_key"],
        keep="first",
    ).loc[
        :,
        [
            "municipality_key",
            "municipality_code",
            "CO_IBGE",
            "municipality_name",
            "municipality_name_lookup",
            "name_source",
        ],
    ]
    assert_no_duplicate_keys(
        crosswalk,
        subset=("municipality_key",),
        error_cls=PanelDataQualityError,
        message="Identity crosswalk contains duplicate municipality keys",
    )
    return crosswalk


def outer_merge_panel_step(
    left_frame: pd.DataFrame,
    right_frame: pd.DataFrame,
    *,
    dataset_name: str,
) -> tuple[pd.DataFrame, MergeStepSummary]:
    """Outer-join one source dataset into the current panel frame."""
    merged_frame = left_frame.merge(
        right_frame,
        on=list(PANEL_KEY_COLUMNS),
        how=SOURCE_JOIN_TYPE,
        indicator=True,
        validate="one_to_one",
    )
    merge_counts = merged_frame["_merge"].value_counts()
    step_summary = MergeStepSummary(
        dataset_name=dataset_name,
        join_type=SOURCE_JOIN_TYPE,
        left_row_count=len(left_frame),
        right_row_count=len(right_frame),
        output_row_count=len(merged_frame),
        matched_row_count=int(merge_counts.get("both", 0)),
        left_only_row_count=int(merge_counts.get("left_only", 0)),
        right_only_row_count=int(merge_counts.get("right_only", 0)),
    )
    LOGGER.info(
        (
            "Outer-joined %s into panel: left=%s right=%s output=%s matched=%s "
            "left_only=%s right_only=%s"
        ),
        dataset_name,
        step_summary.left_row_count,
        step_summary.right_row_count,
        step_summary.output_row_count,
        step_summary.matched_row_count,
        step_summary.left_only_row_count,
        step_summary.right_only_row_count,
    )
    return merged_frame.drop(columns="_merge"), step_summary


def compute_rate(
    numerator: pd.Series,
    denominator: pd.Series,
    *,
    scale: int,
) -> pd.Series:
    """Compute one safe rate with missing output when the denominator is invalid."""
    numerator_values = pd.to_numeric(numerator, errors="coerce")
    denominator_values = pd.to_numeric(denominator, errors="coerce")
    valid_mask = denominator_values.notna() & denominator_values.gt(0)

    result = pd.Series(pd.NA, index=numerator.index, dtype="Float64")
    result.loc[valid_mask] = (
        numerator_values.loc[valid_mask] / denominator_values.loc[valid_mask]
    ) * scale
    return result


def fill_presence_flags(frame: pd.DataFrame) -> pd.DataFrame:
    """Replace missing source-presence indicators with False."""
    for column_name in SOURCE_PRESENCE_COLUMNS:
        frame[column_name] = frame[column_name].fillna(False).astype("boolean")
    return frame


def finalize_panel_frame(
    merged_frame: pd.DataFrame,
    *,
    identity_crosswalk: pd.DataFrame,
) -> pd.DataFrame:
    """Add identity columns, derived variables, and final ordering."""
    panel_frame = merged_frame.merge(
        identity_crosswalk,
        on="municipality_key",
        how="left",
        validate="many_to_one",
    )
    panel_frame = fill_presence_flags(panel_frame)
    panel_frame["available_source_count"] = (
        panel_frame.loc[:, list(SOURCE_PRESENCE_COLUMNS)]
        .astype(int)
        .sum(axis=1)
        .astype("Int64")
    )
    panel_frame["motorcycles_per_1000_inhabitants"] = compute_rate(
        panel_frame["motorcycles_total"],
        panel_frame["population"],
        scale=1000,
    )
    panel_frame["motorcycle_hospitalizations_per_100k"] = compute_rate(
        panel_frame["motorcycle_hospitalizations"],
        panel_frame["population"],
        scale=100000,
    )
    panel_frame["motorcycle_deaths_per_100k"] = compute_rate(
        panel_frame["motorcycle_deaths"],
        panel_frame["population"],
        scale=100000,
    )
    panel_frame["icu_beds_per_100k"] = compute_rate(
        panel_frame["icu_beds_existing"],
        panel_frame["population"],
        scale=100000,
    )

    ordered_columns = [
        "municipality_key",
        "municipality_code",
        "CO_IBGE",
        "municipality_name",
        "municipality_name_lookup",
        "name_source",
        "year",
        "available_source_count",
        "has_population_data",
        "has_senatran_data",
        "has_sih_data",
        "has_sim_data",
        "has_cnes_data",
        "population",
        "motorcycles_total",
        "motocicleta",
        "motoneta",
        "ciclomotor",
        "triciclo",
        "quadriciclo",
        "side_car",
        "motorcycle_hospitalizations",
        "total_hospitalization_cost",
        "total_length_of_stay_days",
        "mean_length_of_stay_days",
        "mean_hospitalization_cost",
        "motorcycle_deaths",
        "has_samu",
        "probable_samu_establishment_count",
        "mobile_emergency_establishment_count",
        "icu_beds_existing",
        "icu_beds_sus",
        "total_hospital_beds_existing",
        "total_hospital_beds_sus",
        "observed_bed_competence_count",
        "has_bed_snapshot",
        "has_establishment_snapshot",
        "motorcycles_per_1000_inhabitants",
        "motorcycle_hospitalizations_per_100k",
        "motorcycle_deaths_per_100k",
        "icu_beds_per_100k",
        "months_observed",
        "source_months",
        "is_partial_year",
        "aggregation_rule",
        "sih_municipality_scope",
        "sih_source_municipality_column",
        "sim_municipality_scope",
        "sim_source_municipality_column",
    ]
    panel_frame = panel_frame.loc[:, ordered_columns].sort_values(
        by=["year", "municipality_key"],
        kind="stable",
    )
    return panel_frame.reset_index(drop=True)


def validate_final_panel_frame(frame: pd.DataFrame) -> None:
    """Run the required final quality checks for the analytical panel."""
    assert_mask_empty(
        frame.loc[:, list(PANEL_KEY_COLUMNS)].isna().any(axis=1),
        error_cls=PanelDataQualityError,
        message="Analytical panel contains rows with missing municipality-year keys",
    )
    assert_no_duplicate_keys(
        frame,
        subset=PANEL_KEY_COLUMNS,
        error_cls=PanelDataQualityError,
        message="Analytical panel contains duplicate municipality-year rows",
    )

    numeric_columns = [
        column_name
        for column_name in frame.columns
        if pd.api.types.is_numeric_dtype(frame[column_name])
        and column_name != "year"
        and not pd.api.types.is_bool_dtype(frame[column_name])
    ]
    negative_mask = frame.loc[:, numeric_columns].lt(0).any(axis=1)
    assert_mask_empty(
        negative_mask,
        error_cls=PanelDataQualityError,
        message="Analytical panel contains impossible negative values",
    )


def build_missingness_summary(frame: pd.DataFrame) -> dict[str, dict[str, float]]:
    """Build a missingness summary for every column in the final panel."""
    row_count = len(frame)
    missingness_summary: dict[str, dict[str, float]] = {}
    for column_name in frame.columns:
        missing_count = int(frame[column_name].isna().sum())
        missing_share = float(missing_count / row_count) if row_count else 0.0
        missingness_summary[column_name] = {
            "missing_count": missing_count,
            "missing_share": missing_share,
        }
    return missingness_summary


def log_missingness_summary(
    missingness_summary: dict[str, dict[str, float]],
) -> None:
    """Log a compact missingness report for the most relevant analytical fields."""
    report_columns = (
        "CO_IBGE",
        "municipality_name",
        "population",
        "motorcycles_total",
        "motorcycle_hospitalizations",
        "motorcycle_deaths",
        "icu_beds_existing",
        "motorcycles_per_1000_inhabitants",
        "motorcycle_hospitalizations_per_100k",
        "motorcycle_deaths_per_100k",
        "icu_beds_per_100k",
    )
    for column_name in report_columns:
        summary = missingness_summary[column_name]
        LOGGER.info(
            "Post-merge missingness: %s missing=%s share=%.4f",
            column_name,
            summary["missing_count"],
            summary["missing_share"],
        )


def build_panel_summary(frame: pd.DataFrame) -> PanelBuildSummary:
    """Build the high-level summary exposed by the ETL result."""
    return PanelBuildSummary(
        row_count=len(frame),
        unique_municipality_count=int(frame["municipality_key"].nunique()),
        unique_year_count=int(frame["year"].nunique()),
        missing_co_ibge_count=int(frame["CO_IBGE"].isna().sum()),
        missing_municipality_name_count=int(frame["municipality_name"].isna().sum()),
        rows_with_any_outcome_data=int(
            frame[
                [
                    "motorcycle_hospitalizations",
                    "motorcycle_deaths",
                ]
            ]
            .notna()
            .any(axis=1)
            .sum()
        ),
        rows_with_any_capacity_data=int(
            frame[
                [
                    "icu_beds_existing",
                    "total_hospital_beds_existing",
                ]
            ]
            .notna()
            .any(axis=1)
            .sum()
        ),
    )


def build_metadata_payload(
    *,
    input_paths: PanelInputPaths,
    summary: PanelBuildSummary,
    merge_steps: list[MergeStepSummary],
    source_row_counts: dict[str, int],
    missingness_summary: dict[str, dict[str, float]],
) -> dict[str, object]:
    """Build the machine-readable metadata payload for the panel output."""
    return {
        "input_paths": {
            "population_path": str(input_paths.population_path),
            "senatran_path": str(input_paths.senatran_path),
            "sih_path": str(input_paths.sih_path),
            "sim_path": str(input_paths.sim_path),
            "cnes_path": str(input_paths.cnes_path),
        },
        "merge_strategy": {
            "join_type": SOURCE_JOIN_TYPE,
            "join_keys": list(PANEL_KEY_COLUMNS),
            "reasoning": PANEL_MERGE_REASON,
        },
        "source_row_counts": source_row_counts,
        "merge_steps": [asdict(step) for step in merge_steps],
        "summary": asdict(summary),
        "missingness": missingness_summary,
        "quality_checks": {
            "duplicate_municipality_year_rows": "passed",
            "missing_key_columns": "passed",
            "impossible_negative_values": "passed",
        },
        "derived_variables": [
            "motorcycles_per_1000_inhabitants",
            "motorcycle_hospitalizations_per_100k",
            "motorcycle_deaths_per_100k",
            "icu_beds_per_100k",
        ],
    }


def run_panel_build(
    *,
    population_path: Path | None = None,
    senatran_path: Path | None = None,
    sih_path: Path | None = None,
    sim_path: Path | None = None,
    cnes_path: Path | None = None,
    output_path: Path | None = None,
    metadata_path: Path | None = None,
) -> PanelBuildResult:
    """Build and save the municipality-year analytical panel."""
    input_paths = resolve_panel_input_paths(
        population_path=population_path,
        senatran_path=senatran_path,
        sih_path=sih_path,
        sim_path=sim_path,
        cnes_path=cnes_path,
    )
    resolved_output_path = resolve_output_path(
        output_path=output_path,
        default_path=build_default_output_path(),
    )
    resolved_metadata_path = resolve_output_path(
        output_path=metadata_path,
        default_path=build_default_metadata_path(resolved_output_path),
    )

    cnes_panel = standardize_cnes_panel_frame(pd.read_parquet(input_paths.cnes_path))
    official_code_lookup = build_cnes_official_code_lookup(cnes_panel)
    population_panel = standardize_population_panel_frame(
        pd.read_parquet(input_paths.population_path),
        official_code_lookup=official_code_lookup,
    )
    senatran_panel = standardize_senatran_panel_frame(
        pd.read_parquet(input_paths.senatran_path),
        official_code_lookup=official_code_lookup,
    )
    sih_panel = standardize_sih_panel_frame(pd.read_parquet(input_paths.sih_path))
    sim_panel = standardize_sim_panel_frame(pd.read_parquet(input_paths.sim_path))

    identity_crosswalk = build_identity_crosswalk(
        population_frame=population_panel,
        senatran_frame=senatran_panel,
        cnes_frame=cnes_panel,
    )

    panel_frame = population_panel.drop(
        columns=["municipality_code", "CO_IBGE", "municipality_name"]
    )
    merge_steps: list[MergeStepSummary] = []

    panel_frame, step_summary = outer_merge_panel_step(
        panel_frame,
        senatran_panel.drop(columns=["municipality_code", "CO_IBGE", "municipality_name"]),
        dataset_name="senatran",
    )
    merge_steps.append(step_summary)

    panel_frame, step_summary = outer_merge_panel_step(
        panel_frame,
        sih_panel.drop(columns=["municipality_code"]),
        dataset_name="sih",
    )
    merge_steps.append(step_summary)

    panel_frame, step_summary = outer_merge_panel_step(
        panel_frame,
        sim_panel.drop(columns=["municipality_code"]),
        dataset_name="sim",
    )
    merge_steps.append(step_summary)

    panel_frame, step_summary = outer_merge_panel_step(
        panel_frame,
        cnes_panel.drop(columns=["municipality_code", "municipality_name"]),
        dataset_name="cnes",
    )
    merge_steps.append(step_summary)

    final_panel = finalize_panel_frame(
        panel_frame,
        identity_crosswalk=identity_crosswalk,
    )
    validate_final_panel_frame(final_panel)

    summary = build_panel_summary(final_panel)
    missingness_summary = build_missingness_summary(final_panel)
    log_missingness_summary(missingness_summary)

    saved_output_path = save_parquet_frame(final_panel, output_path=resolved_output_path)
    metadata_payload = build_metadata_payload(
        input_paths=input_paths,
        summary=summary,
        merge_steps=merge_steps,
        source_row_counts={
            "population": len(population_panel),
            "senatran": len(senatran_panel),
            "sih": len(sih_panel),
            "sim": len(sim_panel),
            "cnes": len(cnes_panel),
        },
        missingness_summary=missingness_summary,
    )
    saved_metadata_path = save_json_payload(
        metadata_payload,
        output_path=resolved_metadata_path,
    )

    LOGGER.info("Analytical panel saved to %s", saved_output_path)
    LOGGER.info("Analytical panel metadata saved to %s", saved_metadata_path)

    return PanelBuildResult(
        input_paths=input_paths,
        output_path=saved_output_path,
        metadata_path=saved_metadata_path,
        summary=summary,
    )
