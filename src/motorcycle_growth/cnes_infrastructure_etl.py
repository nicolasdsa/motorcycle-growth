"""ETL helpers for municipality-year CNES emergency infrastructure indicators."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
import csv
import re
import tempfile
import zipfile

import pandas as pd

from motorcycle_growth.config import INTERIM_DATA_DIR, RAW_DATA_DIR
from motorcycle_growth.etl_utils import (
    assert_mask_empty,
    assert_no_duplicate_keys,
    build_normalized_column_map,
    clean_numeric_code,
    find_column_by_aliases,
    normalize_lookup_text,
    resolve_output_path,
    save_json_payload,
    save_parquet_frame,
)
from motorcycle_growth.logging_utils import get_logger


LOGGER = get_logger(__name__)

CNES_ESTABLISHMENTS_RAW_DIR = RAW_DATA_DIR / "cnes_establishments"
CNES_BEDS_RAW_DIR = RAW_DATA_DIR / "cnes_hospital_beds"
CNES_INTERIM_DIR = INTERIM_DATA_DIR / "cnes"

DEFAULT_OUTPUT_FILE_NAME = "cnes_infrastructure_municipality_year.parquet"
DEFAULT_METADATA_FILE_NAME = "cnes_infrastructure_municipality_year_metadata.json"

SUPPORTED_TABULAR_SUFFIXES = (".parquet", ".csv", ".txt", ".dbf", ".dbc", ".zip")
DELIMITER_CANDIDATES = ";,\t|"
SAMU_KEYWORD_PATTERN = re.compile(r"\bSAMU\b")


class CnesInfrastructureEtlError(RuntimeError):
    """Base error for the CNES infrastructure ETL flow."""


class CnesInfrastructureSchemaError(CnesInfrastructureEtlError):
    """Raised when one CNES raw file does not match the ETL expectations."""


class CnesInfrastructureDataQualityError(CnesInfrastructureEtlError):
    """Raised when cleaned CNES data is not fit for downstream use."""


class CnesInfrastructureDependencyError(CnesInfrastructureEtlError):
    """Raised when one optional CNES reader dependency is missing."""


@dataclass(frozen=True)
class CnesInfrastructureQualitySummary:
    """Small quality summary emitted after the ETL succeeds."""

    raw_establishment_file_count: int
    raw_bed_file_count: int
    raw_establishment_row_count: int
    raw_bed_row_count: int
    municipality_year_row_count: int
    unique_municipality_count: int
    unique_year_count: int
    municipality_years_with_samu: int
    probable_samu_establishment_count: int
    observed_bed_years: tuple[int, ...]
    establishment_year_resolution: str
    bed_year_aggregation_rule: str


@dataclass(frozen=True)
class CnesInfrastructureEtlResult:
    """Result metadata for one CNES infrastructure ETL execution."""

    establishment_input_paths: tuple[Path, ...]
    bed_input_paths: tuple[Path, ...]
    output_path: Path
    metadata_path: Path
    summary: CnesInfrastructureQualitySummary


def _is_supported_tabular_file(path: Path) -> bool:
    """Return whether one path should be considered as CNES tabular input."""
    if not path.is_file():
        return False
    if path.name.startswith(".") or path.name.startswith(".~lock."):
        return False
    if ":Zone.Identifier" in path.name:
        return False
    return path.suffix.lower() in SUPPORTED_TABULAR_SUFFIXES


def _resolve_input_paths(
    *,
    input_paths: list[Path] | None,
    raw_directory: Path,
    dataset_label: str,
) -> list[Path]:
    """Resolve one or more CNES input files from the expected raw location."""
    if input_paths:
        resolved_paths = []
        for input_path in input_paths:
            resolved_path = input_path.expanduser().resolve()
            if not resolved_path.exists():
                msg = f"{dataset_label} raw file not found: {resolved_path}"
                raise FileNotFoundError(msg)
            if not _is_supported_tabular_file(resolved_path):
                msg = f"Unsupported {dataset_label} raw file: {resolved_path.name}"
                raise CnesInfrastructureEtlError(msg)
            resolved_paths.append(resolved_path)
        return sorted(resolved_paths)

    if not raw_directory.exists():
        msg = (
            f"The expected {dataset_label} raw directory does not exist yet: "
            f"{raw_directory}. Create it and place the original files there, or "
            "pass the input paths explicitly."
        )
        raise FileNotFoundError(msg)

    candidates = sorted(
        path.resolve()
        for path in raw_directory.rglob("*")
        if _is_supported_tabular_file(path)
    )
    non_archive_candidates = [
        path for path in candidates if path.suffix.lower() != ".zip"
    ]
    if non_archive_candidates:
        candidates = non_archive_candidates

    if not candidates:
        msg = (
            f"No raw {dataset_label} file was found under {raw_directory}. Place the "
            "original CNES files there or pass explicit input paths."
        )
        raise FileNotFoundError(msg)

    return candidates


def resolve_cnes_establishment_input_paths(
    input_paths: list[Path] | None = None,
) -> list[Path]:
    """Resolve the establishment input paths for the CNES ETL."""
    return _resolve_input_paths(
        input_paths=input_paths,
        raw_directory=CNES_ESTABLISHMENTS_RAW_DIR,
        dataset_label="CNES establishment",
    )


def resolve_cnes_bed_input_paths(input_paths: list[Path] | None = None) -> list[Path]:
    """Resolve the bed input paths for the CNES ETL."""
    return _resolve_input_paths(
        input_paths=input_paths,
        raw_directory=CNES_BEDS_RAW_DIR,
        dataset_label="CNES bed",
    )


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
        return {"sep": delimiter, "low_memory": False}

    return {"sep": None, "engine": "python"}


def _read_delimited_bytes(payload: bytes) -> pd.DataFrame:
    """Read one delimited file payload with a small encoding fallback."""
    sample_text = payload[:8192].decode("utf-8-sig", errors="ignore")
    read_kwargs = _build_read_csv_kwargs(sample_text)

    for encoding in ("utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(
                BytesIO(payload),
                encoding=encoding,
                encoding_errors="ignore",
                **read_kwargs,
            )
        except UnicodeDecodeError:
            continue

    return pd.read_csv(
        BytesIO(payload),
        encoding="utf-8-sig",
        encoding_errors="ignore",
        **read_kwargs,
    )


def _load_delimited_frame(input_path: Path) -> pd.DataFrame:
    """Read one CSV-like CNES file with delimiter sniffing."""
    payload = input_path.read_bytes()
    return _read_delimited_bytes(payload)


def _load_dbf_frame(input_path: Path) -> pd.DataFrame:
    """Read one DBF CNES extract using an optional dependency."""
    try:
        from dbfread import DBF
    except ImportError as exc:
        msg = (
            "Reading CNES .dbf files requires the optional dependency 'dbfread'. "
            "Install project dependencies with Poetry before running this ETL step."
        )
        raise CnesInfrastructureDependencyError(msg) from exc

    records = DBF(str(input_path), load=True, char_decode_errors="ignore")
    return pd.DataFrame(iter(records))


def _load_dbc_frame(input_path: Path) -> pd.DataFrame:
    """Read one DBC CNES extract by decompressing it to a temporary DBF file."""
    try:
        import datasus_dbc
    except ImportError as exc:
        msg = (
            "Reading CNES .dbc files requires the runtime dependency "
            "'datasus-dbc'. Install project dependencies with Poetry before running "
            "this ETL step."
        )
        raise CnesInfrastructureDependencyError(msg) from exc

    dbf_bytes = datasus_dbc.decompress_bytes(input_path.read_bytes())
    with tempfile.NamedTemporaryFile(suffix=".dbf", delete=True) as temp_file:
        temp_file.write(dbf_bytes)
        temp_file.flush()
        return _load_dbf_frame(Path(temp_file.name))


def _load_zip_member_frame(input_path: Path) -> pd.DataFrame:
    """Read one supported tabular file stored inside a ZIP archive."""
    with zipfile.ZipFile(input_path) as archive:
        supported_members = [
            name
            for name in archive.namelist()
            if not name.endswith("/")
            and not Path(name).name.startswith(".")
            and Path(name).suffix.lower() in {".parquet", ".csv", ".txt", ".dbf", ".dbc"}
        ]

        if not supported_members:
            msg = (
                "The CNES zip file does not contain a supported tabular file. "
                f"Archive: {input_path.name}"
            )
            raise CnesInfrastructureSchemaError(msg)

        if len(supported_members) > 1:
            member_names = ", ".join(sorted(supported_members))
            msg = (
                "The CNES zip file contains multiple supported members. Extract the "
                f"desired file and pass it explicitly. Members: {member_names}"
            )
            raise CnesInfrastructureSchemaError(msg)

        member_name = supported_members[0]
        member_suffix = Path(member_name).suffix.lower()

        if member_suffix == ".parquet":
            return pd.read_parquet(BytesIO(archive.read(member_name)))
        if member_suffix == ".dbf":
            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=True) as temp_file:
                temp_file.write(archive.read(member_name))
                temp_file.flush()
                return _load_dbf_frame(Path(temp_file.name))
        if member_suffix == ".dbc":
            with tempfile.NamedTemporaryFile(suffix=".dbc", delete=True) as temp_file:
                temp_file.write(archive.read(member_name))
                temp_file.flush()
                return _load_dbc_frame(Path(temp_file.name))

        return _read_delimited_bytes(archive.read(member_name))


def load_cnes_raw_frame(input_path: Path) -> pd.DataFrame:
    """Load one CNES raw file from disk."""
    LOGGER.info("Loading raw CNES file from %s", input_path)

    suffix = input_path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(input_path)
    if suffix in {".csv", ".txt"}:
        return _load_delimited_frame(input_path)
    if suffix == ".dbf":
        return _load_dbf_frame(input_path)
    if suffix == ".dbc":
        return _load_dbc_frame(input_path)
    if suffix == ".zip":
        return _load_zip_member_frame(input_path)

    msg = f"Unsupported CNES input format: {input_path.name}"
    raise CnesInfrastructureEtlError(msg)


def infer_year_from_path(input_path: Path) -> int | None:
    """Infer a reference year from one file path when possible."""
    candidates = re.findall(r"(?<!\d)(20\d{2})(?!\d)", str(input_path))
    unique_candidates = sorted(set(candidates))
    if len(unique_candidates) == 1:
        return int(unique_candidates[0])
    return None


def resolve_establishment_reference_years(
    *,
    input_paths: list[Path],
    explicit_year: int | None,
    fallback_years: tuple[int, ...],
) -> tuple[dict[Path, int], str]:
    """Resolve the reference year for each establishment file."""
    if explicit_year is not None:
        return {path: explicit_year for path in input_paths}, "explicit_cli_argument"

    resolved_years: dict[Path, int] = {}
    unresolved_paths: list[Path] = []

    for input_path in input_paths:
        inferred_year = infer_year_from_path(input_path)
        if inferred_year is None:
            unresolved_paths.append(input_path)
            continue
        resolved_years[input_path] = inferred_year

    if not unresolved_paths:
        return resolved_years, "file_path"

    if len(fallback_years) == 1:
        fallback_year = fallback_years[0]
        for input_path in unresolved_paths:
            resolved_years[input_path] = fallback_year
        return resolved_years, "bed_year_fallback"

    unresolved_names = ", ".join(path.name for path in unresolved_paths)
    msg = (
        "Could not infer the establishment reference year from the file path, and "
        "the bed inputs do not expose a single fallback year. Pass "
        "--establishments-year explicitly. Files: "
        f"{unresolved_names}"
    )
    raise CnesInfrastructureEtlError(msg)


def resolve_cnes_establishment_columns(raw_frame: pd.DataFrame) -> dict[str, str]:
    """Resolve the raw columns required for establishment-level indicators."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)

    municipality_code = find_column_by_aliases(
        normalized_to_original,
        "CO_IBGE",
        "CODIGO MUNICIPIO",
        "CO_MUNICIPIO_GESTOR",
    )
    cnes_code = find_column_by_aliases(
        normalized_to_original,
        "CNES",
        "CO_CNES",
    )
    unit_name = find_column_by_aliases(
        normalized_to_original,
        "NO_FANTASIA",
        "NOME ESTABELECIMENTO",
        "NOME_ESTABELECIMENTO",
    )
    legal_name = find_column_by_aliases(
        normalized_to_original,
        "NO_RAZAO_SOCIAL",
        "RAZAO SOCIAL",
        "RAZAO_SOCIAL",
    )
    unit_type_code = find_column_by_aliases(
        normalized_to_original,
        "TP_UNIDADE",
        "CO_TIPO_UNIDADE",
    )

    missing = []
    if municipality_code is None:
        missing.append("municipality_code")
    if cnes_code is None:
        missing.append("cnes_code")
    if unit_name is None and legal_name is None:
        missing.append("unit_name_or_legal_name")

    if missing:
        available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
        msg = (
            "CNES establishment raw file is missing required columns for the ETL: "
            f"{', '.join(missing)}. Available columns: {available_columns}"
        )
        raise CnesInfrastructureSchemaError(msg)

    resolved_columns = {
        "municipality_code": municipality_code,
        "cnes_code": cnes_code,
    }
    if unit_name is not None:
        resolved_columns["unit_name"] = unit_name
    if legal_name is not None:
        resolved_columns["legal_name"] = legal_name
    if unit_type_code is not None:
        resolved_columns["unit_type_code"] = unit_type_code
    return resolved_columns


def _coerce_numeric_capacity_series(
    series: pd.Series,
    *,
    column_label: str,
) -> pd.Series:
    """Convert one capacity column to numeric while keeping blanks as zero."""
    text_series = series.astype("string").str.strip()
    numeric_series = pd.to_numeric(text_series, errors="coerce")

    invalid_mask = text_series.notna() & text_series.ne("") & numeric_series.isna()
    assert_mask_empty(
        invalid_mask,
        error_cls=CnesInfrastructureDataQualityError,
        message=f"CNES ETL found invalid numeric values in {column_label}",
    )

    numeric_series = numeric_series.fillna(0)
    assert_mask_empty(
        numeric_series.lt(0),
        error_cls=CnesInfrastructureDataQualityError,
        message=f"CNES ETL found negative values in {column_label}",
    )
    return numeric_series.astype("int64")


def standardize_cnes_establishment_frame(
    raw_frame: pd.DataFrame,
    *,
    year: int,
    source_file_name: str,
) -> pd.DataFrame:
    """Validate and standardize the establishment subset needed for SAMU proxies."""
    resolved_columns = resolve_cnes_establishment_columns(raw_frame)

    working_frame = pd.DataFrame(
        {
            "municipality_code": raw_frame[resolved_columns["municipality_code"]],
            "cnes_code": raw_frame[resolved_columns["cnes_code"]],
            "unit_name": raw_frame[resolved_columns["unit_name"]]
            if "unit_name" in resolved_columns
            else pd.Series(pd.NA, index=raw_frame.index),
            "legal_name": raw_frame[resolved_columns["legal_name"]]
            if "legal_name" in resolved_columns
            else pd.Series(pd.NA, index=raw_frame.index),
            "unit_type_code": raw_frame[resolved_columns["unit_type_code"]]
            if "unit_type_code" in resolved_columns
            else pd.Series(pd.NA, index=raw_frame.index),
        }
    ).copy()

    working_frame["municipality_code"] = working_frame["municipality_code"].map(
        lambda value: clean_numeric_code(value, width=6)
    )
    working_frame["cnes_code"] = working_frame["cnes_code"].map(
        lambda value: clean_numeric_code(value, width=7)
    )
    working_frame["unit_name"] = working_frame["unit_name"].astype("string").str.strip()
    working_frame["legal_name"] = working_frame["legal_name"].astype("string").str.strip()
    working_frame["unit_type_code"] = (
        working_frame["unit_type_code"].astype("string").str.strip()
    )
    working_frame["year"] = year
    working_frame["source_file_name"] = source_file_name

    municipality_mask = working_frame["municipality_code"].notna()
    cnes_mask = working_frame["cnes_code"].notna()
    name_mask = (
        working_frame["unit_name"].fillna("").ne("")
        | working_frame["legal_name"].fillna("").ne("")
    )

    assert_mask_empty(
        ~municipality_mask,
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES establishment ETL found rows without municipality code",
    )
    assert_mask_empty(
        ~cnes_mask,
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES establishment ETL found rows without CNES code",
    )
    assert_mask_empty(
        ~name_mask,
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES establishment ETL found rows without usable establishment name",
    )

    combined_name = (
        working_frame["unit_name"].fillna("")
        + " "
        + working_frame["legal_name"].fillna("")
    ).map(normalize_lookup_text)
    keyword_match = combined_name.str.contains(SAMU_KEYWORD_PATTERN, regex=True)
    full_phrase_match = combined_name.str.contains(
        "SERVICO DE ATENDIMENTO MOVEL DE URGENCIA",
        regex=False,
    )
    working_frame["samu_keyword_match"] = keyword_match | full_phrase_match
    working_frame["is_mobile_emergency_unit"] = working_frame["unit_type_code"].eq("42")
    working_frame["is_probable_samu"] = working_frame["samu_keyword_match"] & (
        working_frame["unit_type_code"].isna()
        | working_frame["is_mobile_emergency_unit"]
    )

    return working_frame[
        [
            "municipality_code",
            "year",
            "cnes_code",
            "unit_type_code",
            "samu_keyword_match",
            "is_mobile_emergency_unit",
            "is_probable_samu",
            "source_file_name",
        ]
    ].copy()


def aggregate_cnes_establishment_indicators(
    standardized_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate establishment-level SAMU proxies to municipality-year."""
    deduplicated_frame = standardized_frame.drop_duplicates(
        subset=["municipality_code", "year", "cnes_code"]
    ).copy()

    aggregated_frame = (
        deduplicated_frame.groupby(["municipality_code", "year"], as_index=False)
        .agg(
            probable_samu_establishment_count=("is_probable_samu", "sum"),
            mobile_emergency_establishment_count=("is_mobile_emergency_unit", "sum"),
        )
        .copy()
    )
    aggregated_frame["probable_samu_establishment_count"] = aggregated_frame[
        "probable_samu_establishment_count"
    ].astype("int64")
    aggregated_frame["mobile_emergency_establishment_count"] = aggregated_frame[
        "mobile_emergency_establishment_count"
    ].astype("int64")
    aggregated_frame["has_samu"] = aggregated_frame[
        "probable_samu_establishment_count"
    ].gt(0)
    return aggregated_frame


def _resolve_bed_capacity_columns(
    normalized_to_original: dict[str, str],
) -> dict[str, tuple[str, ...]]:
    """Resolve the bed-capacity columns used by the first CNES ETL version."""
    total_beds_existing = find_column_by_aliases(
        normalized_to_original,
        "LEITOS_EXISTENTES",
    )
    total_beds_sus = find_column_by_aliases(
        normalized_to_original,
        "LEITOS_SUS",
    )
    icu_total_existing = find_column_by_aliases(
        normalized_to_original,
        "UTI_TOTAL_EXIST",
        "UTI TOTAL EXIST",
    )
    icu_total_sus = find_column_by_aliases(
        normalized_to_original,
        "UTI_TOTAL_SUS",
        "UTI TOTAL SUS",
    )

    if icu_total_existing is not None:
        icu_existing_columns = (icu_total_existing,)
    else:
        icu_existing_columns = tuple(
            column_name
            for alias in (
                "UTI_ADULTO_EXIST",
                "UTI_PEDIATRICO_EXIST",
                "UTI_NEONATAL_EXIST",
                "UTI_QUEIMADO_EXIST",
                "UTI_CORONARIANA_EXIST",
            )
            if (column_name := find_column_by_aliases(normalized_to_original, alias))
            is not None
        )

    if icu_total_sus is not None:
        icu_sus_columns = (icu_total_sus,)
    else:
        icu_sus_columns = tuple(
            column_name
            for alias in (
                "UTI_ADULTO_SUS",
                "UTI_PEDIATRICO_SUS",
                "UTI_NEONATAL_SUS",
                "UTI_QUEIMADO_SUS",
                "UTI_CORONARIANA_SUS",
            )
            if (column_name := find_column_by_aliases(normalized_to_original, alias))
            is not None
        )

    missing = []
    if total_beds_existing is None:
        missing.append("total_hospital_beds_existing")
    if total_beds_sus is None:
        missing.append("total_hospital_beds_sus")
    if not icu_existing_columns:
        missing.append("icu_beds_existing")
    if not icu_sus_columns:
        missing.append("icu_beds_sus")

    if missing:
        available_columns = ", ".join(sorted(normalized_to_original.values()))
        msg = (
            "CNES bed raw file is missing required columns for the ETL: "
            f"{', '.join(missing)}. Available columns: {available_columns}"
        )
        raise CnesInfrastructureSchemaError(msg)

    return {
        "total_hospital_beds_existing": (total_beds_existing,),
        "total_hospital_beds_sus": (total_beds_sus,),
        "icu_beds_existing": icu_existing_columns,
        "icu_beds_sus": icu_sus_columns,
    }


def resolve_cnes_bed_columns(raw_frame: pd.DataFrame) -> dict[str, object]:
    """Resolve the raw columns required for bed-capacity indicators."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)

    municipality_code = find_column_by_aliases(
        normalized_to_original,
        "CO_IBGE",
        "CODIGO MUNICIPIO",
    )
    municipality_name = find_column_by_aliases(
        normalized_to_original,
        "MUNICIPIO",
        "NO_MUNICIPIO",
        "MUNICIPIO_NOME",
    )
    competence = find_column_by_aliases(
        normalized_to_original,
        "COMP",
        "COMPETENCIA",
    )
    year = find_column_by_aliases(
        normalized_to_original,
        "ANO",
    )

    missing = []
    if municipality_code is None:
        missing.append("municipality_code")
    if municipality_name is None:
        missing.append("municipality_name")
    if competence is None and year is None:
        missing.append("competence_or_year")

    if missing:
        available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
        msg = (
            "CNES bed raw file is missing required columns for the ETL: "
            f"{', '.join(missing)}. Available columns: {available_columns}"
        )
        raise CnesInfrastructureSchemaError(msg)

    resolved_columns: dict[str, object] = {
        "municipality_code": municipality_code,
        "municipality_name": municipality_name,
        "capacity": _resolve_bed_capacity_columns(normalized_to_original),
    }
    if competence is not None:
        resolved_columns["competence"] = competence
    if year is not None:
        resolved_columns["year"] = year
    return resolved_columns


def standardize_cnes_bed_frame(
    raw_frame: pd.DataFrame,
    *,
    source_file_name: str,
) -> pd.DataFrame:
    """Validate and standardize the bed-capacity subset needed for the first ETL."""
    resolved_columns = resolve_cnes_bed_columns(raw_frame)
    capacity_columns = resolved_columns["capacity"]

    working_frame = pd.DataFrame(
        {
            "municipality_code": raw_frame[resolved_columns["municipality_code"]],
            "municipality_name": raw_frame[resolved_columns["municipality_name"]],
        }
    ).copy()

    if "competence" in resolved_columns:
        competence_series = raw_frame[resolved_columns["competence"]].map(
            lambda value: clean_numeric_code(value, width=6)
        )
        year_series = competence_series.str[:4]
    else:
        year_series = raw_frame[resolved_columns["year"]].map(
            lambda value: clean_numeric_code(value, width=4)
        )
        competence_series = year_series + "01"

    working_frame["municipality_code"] = working_frame["municipality_code"].map(
        lambda value: clean_numeric_code(value, width=6)
    )
    working_frame["municipality_name"] = (
        working_frame["municipality_name"].astype("string").str.strip()
    )
    working_frame["competence"] = competence_series
    working_frame["year"] = pd.to_numeric(year_series, errors="coerce")
    working_frame["source_file_name"] = source_file_name

    for output_column, source_columns in capacity_columns.items():
        series = pd.Series(0, index=raw_frame.index, dtype="int64")
        for source_column in source_columns:
            series = series.add(
                _coerce_numeric_capacity_series(
                    raw_frame[source_column],
                    column_label=source_column,
                ),
                fill_value=0,
            )
        working_frame[output_column] = series.astype("int64")

    assert_mask_empty(
        working_frame["municipality_code"].isna(),
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES bed ETL found rows without municipality code",
    )
    assert_mask_empty(
        working_frame["municipality_name"].isna() | working_frame["municipality_name"].eq(""),
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES bed ETL found rows without municipality name",
    )
    assert_mask_empty(
        working_frame["competence"].isna(),
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES bed ETL found rows without competence",
    )
    assert_mask_empty(
        working_frame["year"].isna(),
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES bed ETL found rows without year",
    )

    working_frame["year"] = working_frame["year"].astype("int64")

    return working_frame[
        [
            "municipality_code",
            "municipality_name",
            "competence",
            "year",
            "total_hospital_beds_existing",
            "total_hospital_beds_sus",
            "icu_beds_existing",
            "icu_beds_sus",
            "source_file_name",
        ]
    ].copy()


def aggregate_cnes_bed_indicators(standardized_frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate bed-capacity snapshots to municipality-year."""
    municipality_competence_frame = (
        standardized_frame.groupby(
            ["municipality_code", "municipality_name", "year", "competence"],
            as_index=False,
        )
        .agg(
            total_hospital_beds_existing=("total_hospital_beds_existing", "sum"),
            total_hospital_beds_sus=("total_hospital_beds_sus", "sum"),
            icu_beds_existing=("icu_beds_existing", "sum"),
            icu_beds_sus=("icu_beds_sus", "sum"),
        )
        .copy()
    )

    aggregated_frame = (
        municipality_competence_frame.groupby(
            ["municipality_code", "year"],
            as_index=False,
        )
        .agg(
            municipality_name=("municipality_name", "last"),
            total_hospital_beds_existing=("total_hospital_beds_existing", "max"),
            total_hospital_beds_sus=("total_hospital_beds_sus", "max"),
            icu_beds_existing=("icu_beds_existing", "max"),
            icu_beds_sus=("icu_beds_sus", "max"),
            observed_bed_competence_count=("competence", "nunique"),
        )
        .copy()
    )

    numeric_columns = [
        "total_hospital_beds_existing",
        "total_hospital_beds_sus",
        "icu_beds_existing",
        "icu_beds_sus",
        "observed_bed_competence_count",
    ]
    for column_name in numeric_columns:
        aggregated_frame[column_name] = aggregated_frame[column_name].astype("int64")

    return aggregated_frame


def run_cnes_infrastructure_etl(
    *,
    establishment_input_paths: list[Path] | None = None,
    bed_input_paths: list[Path] | None = None,
    output_path: Path | None = None,
    metadata_path: Path | None = None,
    establishments_year: int | None = None,
) -> CnesInfrastructureEtlResult:
    """Run the first municipality-year CNES infrastructure ETL."""
    resolved_establishment_paths = resolve_cnes_establishment_input_paths(
        establishment_input_paths
    )
    resolved_bed_paths = resolve_cnes_bed_input_paths(bed_input_paths)

    standardized_bed_frames: list[pd.DataFrame] = []
    raw_bed_row_count = 0
    for input_path in resolved_bed_paths:
        raw_frame = load_cnes_raw_frame(input_path)
        raw_bed_row_count += len(raw_frame)
        standardized_bed_frames.append(
            standardize_cnes_bed_frame(
                raw_frame,
                source_file_name=input_path.name,
            )
        )

    combined_bed_frame = pd.concat(standardized_bed_frames, ignore_index=True)
    aggregated_bed_frame = aggregate_cnes_bed_indicators(combined_bed_frame)
    observed_bed_years = tuple(sorted(aggregated_bed_frame["year"].unique().tolist()))

    establishment_years, establishment_year_resolution = (
        resolve_establishment_reference_years(
            input_paths=resolved_establishment_paths,
            explicit_year=establishments_year,
            fallback_years=observed_bed_years,
        )
    )

    standardized_establishment_frames: list[pd.DataFrame] = []
    raw_establishment_row_count = 0
    for input_path in resolved_establishment_paths:
        raw_frame = load_cnes_raw_frame(input_path)
        raw_establishment_row_count += len(raw_frame)
        standardized_establishment_frames.append(
            standardize_cnes_establishment_frame(
                raw_frame,
                year=establishment_years[input_path],
                source_file_name=input_path.name,
            )
        )

    combined_establishment_frame = pd.concat(
        standardized_establishment_frames,
        ignore_index=True,
    )
    aggregated_establishment_frame = aggregate_cnes_establishment_indicators(
        combined_establishment_frame
    )

    final_frame = aggregated_bed_frame.merge(
        aggregated_establishment_frame,
        on=["municipality_code", "year"],
        how="outer",
    )
    final_frame["has_bed_snapshot"] = final_frame["icu_beds_existing"].notna()
    final_frame["has_establishment_snapshot"] = final_frame[
        "probable_samu_establishment_count"
    ].notna()

    numeric_fill_columns = [
        "total_hospital_beds_existing",
        "total_hospital_beds_sus",
        "icu_beds_existing",
        "icu_beds_sus",
        "observed_bed_competence_count",
        "probable_samu_establishment_count",
        "mobile_emergency_establishment_count",
    ]
    for column_name in numeric_fill_columns:
        final_frame[column_name] = final_frame[column_name].fillna(0).astype("int64")

    final_frame["has_samu"] = final_frame["has_samu"].fillna(False).astype(bool)
    final_frame["municipality_name"] = final_frame["municipality_name"].astype("string")

    final_frame = final_frame[
        [
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
        ]
    ].sort_values(["year", "municipality_code"], ignore_index=True)

    assert_no_duplicate_keys(
        final_frame,
        subset=("municipality_code", "year"),
        error_cls=CnesInfrastructureDataQualityError,
        message="CNES infrastructure ETL produced duplicate municipality-year rows",
    )

    resolved_output_path = resolve_output_path(
        output_path=output_path,
        default_path=CNES_INTERIM_DIR / DEFAULT_OUTPUT_FILE_NAME,
    )
    resolved_metadata_path = resolve_output_path(
        output_path=metadata_path,
        default_path=CNES_INTERIM_DIR / DEFAULT_METADATA_FILE_NAME,
    )

    save_parquet_frame(final_frame, output_path=resolved_output_path)

    summary = CnesInfrastructureQualitySummary(
        raw_establishment_file_count=len(resolved_establishment_paths),
        raw_bed_file_count=len(resolved_bed_paths),
        raw_establishment_row_count=raw_establishment_row_count,
        raw_bed_row_count=raw_bed_row_count,
        municipality_year_row_count=len(final_frame),
        unique_municipality_count=int(final_frame["municipality_code"].nunique()),
        unique_year_count=int(final_frame["year"].nunique()),
        municipality_years_with_samu=int(final_frame["has_samu"].sum()),
        probable_samu_establishment_count=int(
            final_frame["probable_samu_establishment_count"].sum()
        ),
        observed_bed_years=observed_bed_years,
        establishment_year_resolution=establishment_year_resolution,
        bed_year_aggregation_rule="municipality-year uses the maximum observed municipality-competence stock within each year",
    )
    metadata_payload = {
        "summary": asdict(summary),
        "establishment_input_paths": [str(path) for path in resolved_establishment_paths],
        "bed_input_paths": [str(path) for path in resolved_bed_paths],
        "methodology": {
            "samu_indicator": (
                "has_samu equals 1 when at least one establishment in the "
                "municipality-year matches a conservative SAMU keyword heuristic "
                "based on establishment names, optionally constrained by mobile "
                "unit type code 42 when that field is available."
            ),
            "icu_capacity_indicator": (
                "icu_beds_existing and icu_beds_sus sum establishment-level ICU "
                "capacity within each competence and then keep the maximum observed "
                "municipality stock within the year."
            ),
            "known_limitations": [
                "CNES establishment inputs used here do not expose a clear reference year in the current file layout, so the ETL may assign the year from the bed inputs when only one bed year is available.",
                "The SAMU proxy is intentionally conservative and may undercount units that do not mention SAMU in establishment names.",
                "Municipality-year bed counts are treated as stock snapshots, not annual flows.",
                "This first version does not yet model service habilitations, equipment, staff, or finer ICU subtypes beyond the documented totals.",
            ],
        },
    }
    save_json_payload(metadata_payload, output_path=resolved_metadata_path)

    return CnesInfrastructureEtlResult(
        establishment_input_paths=tuple(resolved_establishment_paths),
        bed_input_paths=tuple(resolved_bed_paths),
        output_path=resolved_output_path,
        metadata_path=resolved_metadata_path,
        summary=summary,
    )
