"""ETL helpers for municipality-year SIM mortality data."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
import csv
import gzip
import re
import zipfile

import pandas as pd

from motorcycle_growth.config import INTERIM_DATA_DIR, RAW_DATA_DIR
from motorcycle_growth.etl_utils import (
    assert_mask_empty,
    assert_no_duplicate_keys,
    build_normalized_column_map,
    find_column_by_aliases,
    resolve_output_path,
    save_json_payload,
    save_parquet_frame,
)
from motorcycle_growth.logging_utils import get_logger


LOGGER = get_logger(__name__)

SIM_RAW_DIR = RAW_DATA_DIR / "sim_mortality"
SIM_INTERIM_DIR = INTERIM_DATA_DIR / "sim"

DEFAULT_OUTPUT_FILE_NAME = "sim_motorcycle_mortality_municipality_year.parquet"
DEFAULT_METADATA_FILE_NAME = "sim_motorcycle_mortality_municipality_year_metadata.json"

MOTORCYCLE_CID_PREFIXES = frozenset(f"V{index}" for index in range(20, 30))
SUPPORTED_TABULAR_SUFFIXES = (
    ".parquet",
    ".csv",
    ".txt",
    ".zip",
    ".gz",
)

DELIMITER_CANDIDATES = ";,\t|"
SIM_PANEL_EXTRACT_REQUIRED_COLUMNS = {
    "municipality_code",
    "year",
    "month",
    "motorcycle_deaths",
}


class SimMortalityEtlError(RuntimeError):
    """Base error for the SIM mortality ETL flow."""


class SimMortalitySchemaError(SimMortalityEtlError):
    """Raised when the SIM raw files do not match the ETL expectations."""


class SimMortalityDataQualityError(SimMortalityEtlError):
    """Raised when cleaned SIM data is not fit for downstream use."""


@dataclass(frozen=True)
class MunicipalityColumnResolution:
    """The municipality column selected for aggregation."""

    column_name: str
    scope: str


@dataclass(frozen=True)
class YearResolution:
    """The raw column used to derive the annual aggregation key."""

    column_name: str
    source_type: str


@dataclass(frozen=True)
class SimSchemaResolution:
    """Schema fields resolved from one SIM raw frame."""

    municipality: MunicipalityColumnResolution
    year: YearResolution
    cause_columns: tuple[str, ...]


@dataclass(frozen=True)
class SimQualitySummary:
    """Small quality summary emitted after the ETL succeeds."""

    source_layout: str
    raw_file_count: int
    raw_row_count: int
    filtered_row_count: int
    aggregated_row_count: int
    unique_municipality_count: int
    unique_year_count: int
    municipality_scope: str
    source_municipality_column: str
    source_year_column: str
    source_year_type: str
    cause_columns_considered: tuple[str, ...]
    cause_columns_with_matches: tuple[str, ...]


@dataclass(frozen=True)
class SimMortalityEtlResult:
    """Result metadata for one SIM mortality ETL execution."""

    input_paths: tuple[Path, ...]
    output_path: Path
    metadata_path: Path
    summary: SimQualitySummary


def resolve_sim_input_paths(input_paths: list[Path] | None = None) -> list[Path]:
    """Resolve one or more raw SIM files from the expected input location."""
    if input_paths:
        resolved_paths = []
        for input_path in input_paths:
            resolved_path = input_path.expanduser().resolve()
            if not resolved_path.exists():
                msg = f"SIM raw file not found: {resolved_path}"
                raise FileNotFoundError(msg)
            resolved_paths.append(resolved_path)
        return sorted(resolved_paths)

    if not SIM_RAW_DIR.exists():
        msg = (
            "The expected SIM raw directory does not exist yet: "
            f"{SIM_RAW_DIR}. Create it and place the original files there, or pass "
            "--input-path explicitly."
        )
        raise FileNotFoundError(msg)

    candidates = sorted(
        path
        for path in SIM_RAW_DIR.iterdir()
        if path.is_file()
        and not path.name.startswith(".")
        and path.suffix.lower() in SUPPORTED_TABULAR_SUFFIXES
    )
    if not candidates:
        msg = (
            "No raw SIM file was found under "
            f"{SIM_RAW_DIR}. Place the original SIM files there or pass --input-path "
            "explicitly."
        )
        raise FileNotFoundError(msg)

    return candidates


def _detect_delimiter(sample_text: str) -> str | None:
    """Infer one CSV delimiter from a small text sample when possible."""
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=DELIMITER_CANDIDATES)
    except csv.Error:
        return None
    return dialect.delimiter


def _build_read_csv_kwargs(sample_text: str) -> dict[str, object]:
    """Build pandas CSV options with a fast delimiter path and safe fallback."""
    delimiter = _detect_delimiter(sample_text)
    if delimiter is not None:
        return {
            "sep": delimiter,
            "low_memory": False,
        }

    return {
        "sep": None,
        "engine": "python",
    }


def _read_text_sample(input_path: Path, *, compression: str | None = None) -> str:
    """Read a small text sample from one delimited file for delimiter sniffing."""
    if compression == "gzip":
        with gzip.open(input_path, "rt", encoding="utf-8-sig", errors="ignore") as input_file:
            return input_file.read(8192)

    with input_path.open("r", encoding="utf-8-sig", errors="ignore") as input_file:
        return input_file.read(8192)


def _load_delimited_frame(input_path: Path) -> pd.DataFrame:
    """Read one CSV-like SIM file with delimiter sniffing."""
    sample_text = _read_text_sample(input_path)
    return pd.read_csv(
        input_path,
        **_build_read_csv_kwargs(sample_text),
    )


def _load_zip_member_frame(input_path: Path) -> pd.DataFrame:
    """Read one supported tabular file stored inside a ZIP archive."""
    with zipfile.ZipFile(input_path) as archive:
        supported_members = [
            name
            for name in archive.namelist()
            if not name.endswith("/")
            and not Path(name).name.startswith(".")
            and Path(name).suffix.lower() in {".parquet", ".csv", ".txt"}
        ]

        if not supported_members:
            msg = (
                "The SIM zip file does not contain a supported tabular file. "
                f"Archive: {input_path.name}"
            )
            raise SimMortalitySchemaError(msg)

        if len(supported_members) > 1:
            member_names = ", ".join(sorted(supported_members))
            msg = (
                "The SIM zip file contains multiple supported members. Extract the "
                f"desired file and pass it explicitly. Members: {member_names}"
            )
            raise SimMortalitySchemaError(msg)

        member_name = supported_members[0]
        member_suffix = Path(member_name).suffix.lower()

        if member_suffix == ".parquet":
            return pd.read_parquet(BytesIO(archive.read(member_name)))

        member_bytes = archive.read(member_name)
        sample_text = member_bytes[:8192].decode("utf-8-sig", errors="ignore")
        return pd.read_csv(
            BytesIO(member_bytes),
            **_build_read_csv_kwargs(sample_text),
        )


def load_sim_raw_frame(input_path: Path) -> pd.DataFrame:
    """Load one raw SIM file from disk."""
    LOGGER.info("Loading raw SIM file from %s", input_path)

    suffixes = tuple(part.lower() for part in input_path.suffixes)
    if suffixes[-2:] in {(".csv", ".gz"), (".txt", ".gz")}:
        sample_text = _read_text_sample(input_path, compression="gzip")
        return pd.read_csv(
            input_path,
            compression="gzip",
            **_build_read_csv_kwargs(sample_text),
        )

    suffix = input_path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(input_path)
    if suffix in {".csv", ".txt"}:
        return _load_delimited_frame(input_path)
    if suffix == ".zip":
        return _load_zip_member_frame(input_path)

    msg = (
        "Unsupported SIM raw file format. Supported formats are .parquet, .csv, "
        ".csv.gz, .txt, .txt.gz, and .zip with one supported member. "
        f"Received file: {input_path.name}"
    )
    raise SimMortalitySchemaError(msg)


def resolve_municipality_column(raw_frame: pd.DataFrame) -> MunicipalityColumnResolution:
    """Resolve the municipality code column and its analytical meaning."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)
    candidates = (
        (
            "residence",
            (
                "CODMUNRES",
                "COD MUN RES",
                "MUNICRES",
                "MUN_RES",
                "CO MUNICIPIO RESIDENCIA",
                "COD MUNICIPIO RESIDENCIA",
            ),
        ),
        (
            "occurrence",
            (
                "CODMUNOCOR",
                "COD MUN OCOR",
                "MUNICOCOR",
                "MUN_OCOR",
                "CO MUNICIPIO OCORRENCIA",
                "COD MUNICIPIO OCORRENCIA",
            ),
        ),
    )

    for scope, aliases in candidates:
        column_name = find_column_by_aliases(normalized_to_original, *aliases)
        if column_name is not None:
            return MunicipalityColumnResolution(column_name=column_name, scope=scope)

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIM raw file is missing a recognized municipality code column. Supported "
        "aliases cover residence and occurrence fields. "
        f"Available columns: {available_columns}"
    )
    raise SimMortalitySchemaError(msg)


def _parse_year_series(raw_values: pd.Series) -> pd.Series:
    """Parse a year series from SIM date or year columns."""
    string_values = raw_values.astype("string").str.strip()
    year_values = pd.Series(pd.NA, index=raw_values.index, dtype="Int64")

    four_digit_mask = string_values.str.fullmatch(r"20\d{2}", na=False)
    year_values.loc[four_digit_mask] = string_values.loc[four_digit_mask].astype("Int64")

    ddmmyyyy_mask = string_values.str.fullmatch(r"\d{8}", na=False)
    ddmmyyyy_dates = pd.to_datetime(
        string_values.loc[ddmmyyyy_mask],
        format="%d%m%Y",
        errors="coerce",
    )
    valid_ddmmyyyy_mask = ddmmyyyy_dates.notna()
    if valid_ddmmyyyy_mask.any():
        year_values.loc[ddmmyyyy_dates.index[valid_ddmmyyyy_mask]] = ddmmyyyy_dates.loc[
            valid_ddmmyyyy_mask
        ].dt.year.astype("Int64")

    seven_digit_mask = string_values.str.fullmatch(r"\d{7}", na=False)
    prefixed_dates = pd.to_datetime(
        "0" + string_values.loc[seven_digit_mask],
        format="%d%m%Y",
        errors="coerce",
    )
    valid_seven_digit_mask = prefixed_dates.notna()
    if valid_seven_digit_mask.any():
        year_values.loc[prefixed_dates.index[valid_seven_digit_mask]] = prefixed_dates.loc[
            valid_seven_digit_mask
        ].dt.year.astype("Int64")

    missing_mask = year_values.isna()
    if missing_mask.any():
        parsed_dates = pd.to_datetime(
            string_values.loc[missing_mask],
            errors="coerce",
            dayfirst=True,
            format="mixed",
        )
        valid_date_mask = parsed_dates.notna()
        if valid_date_mask.any():
            year_values.loc[parsed_dates.index[valid_date_mask]] = parsed_dates.loc[
                valid_date_mask
            ].dt.year.astype("Int64")

    return year_values


def resolve_year_column(raw_frame: pd.DataFrame) -> YearResolution:
    """Resolve the raw column used to derive the annual aggregation key."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)
    column_groups = (
        (
            "date",
            (
                "DTOBITO",
                "DATA OBITO",
                "DT OBITO",
                "DT_OCORRENCIA",
                "DATA EVENTO",
            ),
        ),
        (
            "year",
            (
                "ANOOBITO",
                "ANO OBITO",
                "ANO",
                "ANO_OCOR",
            ),
        ),
    )

    for source_type, aliases in column_groups:
        column_name = find_column_by_aliases(normalized_to_original, *aliases)
        if column_name is not None:
            return YearResolution(column_name=column_name, source_type=source_type)

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIM raw file is missing a recognized death date/year column, so the ETL "
        "cannot aggregate to municipality-year. "
        f"Available columns: {available_columns}"
    )
    raise SimMortalitySchemaError(msg)


def resolve_cause_columns(raw_frame: pd.DataFrame) -> tuple[str, ...]:
    """Resolve the cause-of-death columns available for CID filtering."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)
    basic_cause_aliases = (
        "CAUSABAS",
        "CAUSABASICA",
        "CAUSABAS_O",
        "CB_ALT",
    )
    for alias in basic_cause_aliases:
        column_name = find_column_by_aliases(normalized_to_original, alias)
        if column_name is not None:
            return (column_name,)

    fallback_columns: list[str] = []
    fallback_aliases = {
        "LINHAA",
        "LINHAB",
        "LINHAC",
        "LINHAD",
        "LINHAII",
    }

    for normalized_name, original_name in normalized_to_original.items():
        compact_name = normalized_name.replace(" ", "")
        if compact_name in fallback_aliases or compact_name.startswith("CAUSA"):
            fallback_columns.append(original_name)

    deduplicated_columns = tuple(dict.fromkeys(fallback_columns))
    if deduplicated_columns:
        return deduplicated_columns

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIM raw file does not expose recognized cause-of-death columns. Without "
        "those fields the ETL cannot apply the CID-10 V20-V29 case definition. "
        f"Available columns: {available_columns}"
    )
    raise SimMortalitySchemaError(msg)


def resolve_sim_schema(raw_frame: pd.DataFrame) -> SimSchemaResolution:
    """Resolve all raw columns required by the SIM ETL."""
    return SimSchemaResolution(
        municipality=resolve_municipality_column(raw_frame),
        year=resolve_year_column(raw_frame),
        cause_columns=resolve_cause_columns(raw_frame),
    )


def is_sim_panel_extract(raw_frame: pd.DataFrame) -> bool:
    """Return whether one raw frame matches the SIM panel monthly extract layout."""
    normalized_columns = {str(column_name).strip() for column_name in raw_frame.columns}
    return SIM_PANEL_EXTRACT_REQUIRED_COLUMNS.issubset(normalized_columns)


def clean_municipality_code(value: object) -> str | None:
    """Normalize one municipality code without assuming a universal code width."""
    if pd.isna(value):
        return None

    digits_only = re.sub(r"\D", "", str(value).strip())
    if len(digits_only) < 6 or set(digits_only) == {"0"}:
        return None

    return digits_only


def normalize_cid_code(value: object) -> str | None:
    """Normalize one potential CID code for range filtering."""
    if pd.isna(value):
        return None

    normalized = re.sub(r"[^A-Za-z0-9]", "", str(value).upper())
    if len(normalized) < 3:
        return None

    return normalized


def is_motorcycle_occupant_cid(value: object) -> bool:
    """Return whether one code falls inside CID-10 V20-V29."""
    normalized_code = normalize_cid_code(value)
    if normalized_code is None:
        return False

    return normalized_code[:3] in MOTORCYCLE_CID_PREFIXES


def standardize_sim_panel_extract_frame(
    raw_frame: pd.DataFrame,
    *,
    source_file_name: str,
) -> tuple[pd.DataFrame, SimSchemaResolution, tuple[str, ...]]:
    """Validate and standardize one SIM panel monthly extract."""
    working_frame = raw_frame[
        ["municipality_code", "year", "month", "motorcycle_deaths"]
    ].copy()

    working_frame["municipality_code"] = working_frame["municipality_code"].map(
        clean_municipality_code
    )
    working_frame["year"] = pd.to_numeric(working_frame["year"], errors="coerce")
    working_frame["month"] = pd.to_numeric(working_frame["month"], errors="coerce")
    working_frame["motorcycle_deaths"] = pd.to_numeric(
        working_frame["motorcycle_deaths"],
        errors="coerce",
    )

    assert_mask_empty(
        working_frame["municipality_code"].isna(),
        error_cls=SimMortalityDataQualityError,
        message="SIM panel extract contains invalid municipality codes",
    )
    assert_mask_empty(
        working_frame["year"].isna(),
        error_cls=SimMortalityDataQualityError,
        message="SIM panel extract contains invalid year values",
    )
    assert_mask_empty(
        working_frame["month"].isna(),
        error_cls=SimMortalityDataQualityError,
        message="SIM panel extract contains invalid month values",
    )
    assert_mask_empty(
        working_frame["motorcycle_deaths"].isna(),
        error_cls=SimMortalityDataQualityError,
        message="SIM panel extract contains invalid motorcycle death counts",
    )
    assert_mask_empty(
        ~working_frame["month"].between(1, 12),
        error_cls=SimMortalityDataQualityError,
        message="SIM panel extract contains months outside the 1-12 range",
    )
    assert_mask_empty(
        working_frame["motorcycle_deaths"].lt(0),
        error_cls=SimMortalityDataQualityError,
        message="SIM panel extract contains negative motorcycle death counts",
    )

    working_frame["year"] = working_frame["year"].astype("int64")
    working_frame["month"] = working_frame["month"].astype("int64")
    working_frame["motorcycle_deaths"] = working_frame["motorcycle_deaths"].astype("int64")
    working_frame["municipality_scope"] = "residence"
    working_frame["source_municipality_column"] = "municipality_code"
    working_frame["source_file_name"] = source_file_name

    schema = SimSchemaResolution(
        municipality=MunicipalityColumnResolution(
            column_name="municipality_code",
            scope="residence",
        ),
        year=YearResolution(column_name="year", source_type="year"),
        cause_columns=("panel_indicator_v20_v29",),
    )
    return (
        working_frame[
            [
                "municipality_code",
                "year",
                "motorcycle_deaths",
                "municipality_scope",
                "source_municipality_column",
                "source_file_name",
            ]
        ].reset_index(drop=True),
        schema,
        ("panel_indicator_v20_v29",),
    )


def standardize_sim_mortality_frame(
    raw_frame: pd.DataFrame,
    *,
    source_file_name: str,
) -> tuple[pd.DataFrame, SimSchemaResolution, tuple[str, ...]]:
    """Validate and standardize one SIM raw frame to death-record metrics."""
    if is_sim_panel_extract(raw_frame):
        return standardize_sim_panel_extract_frame(
            raw_frame,
            source_file_name=source_file_name,
        )

    schema = resolve_sim_schema(raw_frame)

    selected_columns = [
        schema.municipality.column_name,
        schema.year.column_name,
        *schema.cause_columns,
    ]
    rename_map = {
        schema.municipality.column_name: "municipality_code",
        schema.year.column_name: "year_source_value",
    }
    working_frame = raw_frame[selected_columns].rename(columns=rename_map).copy()

    working_frame["municipality_code"] = working_frame["municipality_code"].map(
        clean_municipality_code
    )
    municipality_mask = working_frame["municipality_code"].notna()
    working_frame = working_frame.loc[municipality_mask].copy()

    year_values = _parse_year_series(working_frame["year_source_value"])
    assert_mask_empty(
        year_values.isna(),
        error_cls=SimMortalityDataQualityError,
        message="SIM ETL found rows with an invalid death date/year value for annual aggregation",
    )
    working_frame["year"] = year_values.astype("Int64")

    cause_match_frame = working_frame.loc[:, schema.cause_columns].apply(
        lambda column: column.map(is_motorcycle_occupant_cid)
    )
    cause_columns_with_matches = tuple(
        column_name
        for column_name in schema.cause_columns
        if bool(cause_match_frame[column_name].any())
    )
    motorcycle_case_mask = cause_match_frame.any(axis=1)
    filtered_frame = working_frame.loc[motorcycle_case_mask].copy()

    filtered_frame["motorcycle_deaths"] = 1
    filtered_frame["source_file_name"] = source_file_name
    filtered_frame["municipality_scope"] = schema.municipality.scope
    filtered_frame["source_municipality_column"] = schema.municipality.column_name

    standardized_frame = filtered_frame[
        [
            "municipality_code",
            "year",
            "motorcycle_deaths",
            "municipality_scope",
            "source_municipality_column",
            "source_file_name",
        ]
    ].reset_index(drop=True)
    return standardized_frame, schema, cause_columns_with_matches


def aggregate_sim_mortality(record_frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate record-level SIM mortality data to municipality-year."""
    if record_frame.empty:
        return pd.DataFrame(
            columns=[
                "municipality_code",
                "year",
                "motorcycle_deaths",
                "municipality_scope",
                "source_municipality_column",
            ]
        )

    aggregated_frame = (
        record_frame.groupby(
            [
                "municipality_code",
                "year",
                "municipality_scope",
                "source_municipality_column",
            ],
            as_index=False,
        )
        .agg(motorcycle_deaths=("motorcycle_deaths", "sum"))
        .sort_values(["year", "municipality_code"])
        .reset_index(drop=True)
    )

    assert_no_duplicate_keys(
        aggregated_frame,
        subset=["municipality_code", "year"],
        error_cls=SimMortalityDataQualityError,
        message="SIM ETL produced duplicate municipality-year keys",
    )

    return aggregated_frame[
        [
            "municipality_code",
            "year",
            "motorcycle_deaths",
            "municipality_scope",
            "source_municipality_column",
        ]
    ]


def build_default_output_path() -> Path:
    """Return the default parquet path for the SIM municipality-year dataset."""
    return SIM_INTERIM_DIR / DEFAULT_OUTPUT_FILE_NAME


def build_default_metadata_path() -> Path:
    """Return the default JSON metadata path for the SIM municipality-year dataset."""
    return SIM_INTERIM_DIR / DEFAULT_METADATA_FILE_NAME


def run_sim_mortality_etl(
    *,
    input_paths: list[Path] | None = None,
    output_path: Path | None = None,
    metadata_path: Path | None = None,
) -> SimMortalityEtlResult:
    """Execute the SIM ETL from raw files to an interim municipality-year parquet."""
    resolved_input_paths = resolve_sim_input_paths(input_paths)

    standardized_frames: list[pd.DataFrame] = []
    raw_row_count = 0
    cause_columns_with_matches: set[str] = set()
    resolved_schema: SimSchemaResolution | None = None
    source_layout: str | None = None

    for input_path in resolved_input_paths:
        raw_frame = load_sim_raw_frame(input_path)
        raw_row_count += len(raw_frame)
        current_layout = (
            "panel_api_monthly_extract"
            if is_sim_panel_extract(raw_frame)
            else "record_level_sim"
        )
        standardized_frame, schema, matched_columns = standardize_sim_mortality_frame(
            raw_frame,
            source_file_name=input_path.name,
        )
        standardized_frames.append(standardized_frame)
        cause_columns_with_matches.update(matched_columns)
        if source_layout is None:
            source_layout = current_layout
        elif source_layout != current_layout:
            msg = (
                "The SIM ETL found mixed raw layouts across input files. Do not mix "
                "record-level SIM files with panel API extracts in the same execution."
            )
            raise SimMortalitySchemaError(msg)

        if resolved_schema is None:
            resolved_schema = schema
            continue

        if schema != resolved_schema:
            msg = (
                "The SIM ETL found inconsistent schemas across input files. Ensure "
                "all files use the same municipality, time, and cause-of-death layout "
                "before aggregating them together."
            )
            raise SimMortalitySchemaError(msg)

    if resolved_schema is None:
        raise SimMortalitySchemaError("The SIM ETL did not load any raw frame.")

    combined_frame = pd.concat(standardized_frames, ignore_index=True)
    aggregated_frame = aggregate_sim_mortality(combined_frame)

    resolved_output_path = resolve_output_path(
        output_path=output_path,
        default_path=build_default_output_path(),
    )
    saved_output_path = save_parquet_frame(
        aggregated_frame,
        output_path=resolved_output_path,
    )

    summary = SimQualitySummary(
        source_layout=source_layout or "unknown",
        raw_file_count=len(resolved_input_paths),
        raw_row_count=raw_row_count,
        filtered_row_count=len(combined_frame),
        aggregated_row_count=len(aggregated_frame),
        unique_municipality_count=int(aggregated_frame["municipality_code"].nunique()),
        unique_year_count=int(aggregated_frame["year"].nunique()),
        municipality_scope=resolved_schema.municipality.scope,
        source_municipality_column=resolved_schema.municipality.column_name,
        source_year_column=resolved_schema.year.column_name,
        source_year_type=resolved_schema.year.source_type,
        cause_columns_considered=resolved_schema.cause_columns,
        cause_columns_with_matches=tuple(sorted(cause_columns_with_matches)),
    )

    resolved_metadata_path = resolve_output_path(
        output_path=metadata_path,
        default_path=build_default_metadata_path(),
    )
    saved_metadata_path = save_json_payload(
        {
            "input_files": [str(path) for path in resolved_input_paths],
            "summary": asdict(summary),
            "notes": [
                (
                    "municipality_scope describes whether aggregation used residence "
                    "or occurrence municipality according to the raw columns available."
                ),
                (
                    "When source_layout is record_level_sim, CID-10 filtering uses "
                    "the cause-of-death columns listed in cause_columns_considered "
                    "and keeps records with any V20-V29 code found there."
                ),
                (
                    "This ETL prioritizes CODMUNRES when available, so the default "
                    "municipality-year mortality panel is residence-based unless the "
                    "raw layout only exposes occurrence municipality."
                ),
                (
                    "When source_layout is panel_api_monthly_extract, the raw file is "
                    "already filtered to the official panel indicator for CID-10 "
                    "V20-V29 and is treated as a preliminary residence-based extract "
                    "when the official panel marks the year with *."
                ),
                (
                    "Operational note: the repository rechecks this panel extract on a "
                    "roughly bimonthly cadence, but the official page text verified in "
                    "code only confirms the preliminary * marker and the extraction "
                    "reference date shown on the panel."
                ),
            ],
        },
        output_path=resolved_metadata_path,
    )

    LOGGER.info(
        (
            "SIM ETL summary: raw_files=%s raw_rows=%s filtered_rows=%s "
            "aggregated_rows=%s municipalities=%s years=%s municipality_scope=%s"
        ),
        summary.raw_file_count,
        summary.raw_row_count,
        summary.filtered_row_count,
        summary.aggregated_row_count,
        summary.unique_municipality_count,
        summary.unique_year_count,
        summary.municipality_scope,
    )
    LOGGER.info("Clean SIM municipality-year dataset saved to %s", saved_output_path)
    LOGGER.info("SIM ETL metadata saved to %s", saved_metadata_path)

    return SimMortalityEtlResult(
        input_paths=tuple(resolved_input_paths),
        output_path=saved_output_path,
        metadata_path=saved_metadata_path,
        summary=summary,
    )
