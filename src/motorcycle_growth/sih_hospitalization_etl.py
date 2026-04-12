"""ETL helpers for municipality-year SIH/SUS hospitalization data."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
import csv
import gzip
import re
import tempfile
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

SIH_RAW_DIR = RAW_DATA_DIR / "sih_sus"
SIH_INTERIM_DIR = INTERIM_DATA_DIR / "sih"

DEFAULT_OUTPUT_FILE_NAME = "sih_motorcycle_hospitalizations_municipality_year.parquet"
DEFAULT_METADATA_FILE_NAME = "sih_motorcycle_hospitalizations_municipality_year_metadata.json"

MOTORCYCLE_CID_PREFIXES = frozenset(f"V{index}" for index in range(20, 30))
SUPPORTED_TABULAR_SUFFIXES = (
    ".parquet",
    ".csv",
    ".txt",
    ".dbf",
    ".dbc",
    ".zip",
    ".gz",
)
COMPONENT_COST_COLUMN_GROUPS = (
    ("hospital_service_cost", ("VAL_SH", "VL_SH", "VAL SERV HOSP", "VALOR SH")),
    ("professional_service_cost", ("VAL_SP", "VL_SP", "VAL SERV PROF", "VALOR SP")),
)
DELIMITER_CANDIDATES = ";,\t|"


class SihHospitalizationEtlError(RuntimeError):
    """Base error for the SIH hospitalization ETL flow."""


class SihHospitalizationDependencyError(SihHospitalizationEtlError):
    """Raised when a required optional dependency is missing."""


class SihHospitalizationSchemaError(SihHospitalizationEtlError):
    """Raised when the SIH raw files do not match the ETL expectations."""


class SihHospitalizationDataQualityError(SihHospitalizationEtlError):
    """Raised when cleaned SIH data is not fit for downstream use."""


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
class CostResolution:
    """The raw cost columns used to produce hospitalization cost metrics."""

    source_type: str
    source_columns: tuple[str, ...]


@dataclass(frozen=True)
class SihSchemaResolution:
    """Schema fields resolved from one SIH raw frame."""

    municipality: MunicipalityColumnResolution
    year: YearResolution
    length_of_stay_column: str
    diagnosis_columns: tuple[str, ...]
    cost: CostResolution


@dataclass(frozen=True)
class SihQualitySummary:
    """Small quality summary emitted after the ETL succeeds."""

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
    source_length_of_stay_column: str
    source_cost_type: str
    source_cost_columns: tuple[str, ...]
    diagnosis_columns_considered: tuple[str, ...]
    diagnosis_columns_with_matches: tuple[str, ...]


@dataclass(frozen=True)
class SihHospitalizationEtlResult:
    """Result metadata for one SIH hospitalization ETL execution."""

    input_paths: tuple[Path, ...]
    output_path: Path
    metadata_path: Path
    summary: SihQualitySummary


def resolve_sih_input_paths(input_paths: list[Path] | None = None) -> list[Path]:
    """Resolve one or more raw SIH files from the expected input location."""
    if input_paths:
        resolved_paths = []
        for input_path in input_paths:
            resolved_path = input_path.expanduser().resolve()
            if not resolved_path.exists():
                msg = f"SIH raw file not found: {resolved_path}"
                raise FileNotFoundError(msg)
            resolved_paths.append(resolved_path)
        return sorted(resolved_paths)

    if not SIH_RAW_DIR.exists():
        msg = (
            "The expected SIH raw directory does not exist yet: "
            f"{SIH_RAW_DIR}. Create it and place the original files there, or pass "
            "--input-path explicitly."
        )
        raise FileNotFoundError(msg)

    candidates = sorted(
        path
        for path in SIH_RAW_DIR.iterdir()
        if path.is_file()
        and not path.name.startswith(".")
        and path.suffix.lower() in SUPPORTED_TABULAR_SUFFIXES
    )
    if not candidates:
        msg = (
            "No raw SIH file was found under "
            f"{SIH_RAW_DIR}. Place the original SIH files there or pass --input-path "
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
    """Read one CSV-like SIH file with delimiter sniffing."""
    sample_text = _read_text_sample(input_path)
    return pd.read_csv(
        input_path,
        **_build_read_csv_kwargs(sample_text),
    )


def _load_dbf_frame(input_path: Path) -> pd.DataFrame:
    """Read one DBF SIH extract using an optional dependency."""
    try:
        from dbfread import DBF
    except ImportError as exc:
        msg = (
            "Reading SIH .dbf files requires the optional dependency 'dbfread'. "
            "Install it with Poetry before running this ETL step."
        )
        raise SihHospitalizationDependencyError(msg) from exc

    records = DBF(str(input_path), load=True, char_decode_errors="ignore")
    return pd.DataFrame(iter(records))


def _load_dbc_frame(input_path: Path) -> pd.DataFrame:
    """Read one DBC SIH extract by decompressing it to a temporary DBF file."""
    try:
        import datasus_dbc
    except ImportError as exc:
        msg = (
            "Reading SIH .dbc files requires the runtime dependency "
            "'datasus-dbc'. Install project dependencies with Poetry before running "
            "this ETL step."
        )
        raise SihHospitalizationDependencyError(msg) from exc

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
                "The SIH zip file does not contain a supported tabular file. "
                f"Archive: {input_path.name}"
            )
            raise SihHospitalizationSchemaError(msg)

        if len(supported_members) > 1:
            member_names = ", ".join(sorted(supported_members))
            msg = (
                "The SIH zip file contains multiple supported members. Extract the "
                f"desired file and pass it explicitly. Members: {member_names}"
            )
            raise SihHospitalizationSchemaError(msg)

        member_name = supported_members[0]
        member_suffix = Path(member_name).suffix.lower()

        if member_suffix == ".parquet":
            return pd.read_parquet(BytesIO(archive.read(member_name)))

        if member_suffix in {".csv", ".txt"}:
            member_bytes = archive.read(member_name)
            sample_text = member_bytes[:8192].decode("utf-8-sig", errors="ignore")
            return pd.read_csv(
                BytesIO(member_bytes),
                **_build_read_csv_kwargs(sample_text),
            )

        if member_suffix == ".dbc":
            try:
                import datasus_dbc
            except ImportError as exc:
                msg = (
                    "Reading SIH .dbc files requires the runtime dependency "
                    "'datasus-dbc'. Install project dependencies with Poetry before "
                    "running this ETL step."
                )
                raise SihHospitalizationDependencyError(msg) from exc

            dbf_bytes = datasus_dbc.decompress_bytes(archive.read(member_name))
            with tempfile.NamedTemporaryFile(suffix=".dbf", delete=True) as temp_file:
                temp_file.write(dbf_bytes)
                temp_file.flush()
                return _load_dbf_frame(Path(temp_file.name))

        with tempfile.NamedTemporaryFile(suffix=member_suffix, delete=True) as temp_file:
            temp_file.write(archive.read(member_name))
            temp_file.flush()
            return _load_dbf_frame(Path(temp_file.name))


def load_sih_raw_frame(input_path: Path) -> pd.DataFrame:
    """Load one raw SIH file from disk."""
    LOGGER.info("Loading raw SIH file from %s", input_path)

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
    if suffix == ".dbf":
        return _load_dbf_frame(input_path)
    if suffix == ".zip":
        return _load_zip_member_frame(input_path)
    if suffix == ".dbc":
        return _load_dbc_frame(input_path)

    msg = (
        "Unsupported SIH raw file format. Supported formats are .parquet, .csv, "
        ".csv.gz, .txt, .txt.gz, .dbf, and .zip with one supported member. "
        f"Received file: {input_path.name}"
    )
    raise SihHospitalizationSchemaError(msg)


def resolve_municipality_column(raw_frame: pd.DataFrame) -> MunicipalityColumnResolution:
    """Resolve the municipality code column and its analytical meaning."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)
    candidates = (
        (
            "residence",
            (
                "MUNIC_RES",
                "MUN_RES",
                "CODMUNRES",
                "COD MUN RES",
                "CO MUNICIPIO RESIDENCIA",
                "COD MUNICIPIO RESIDENCIA",
            ),
        ),
        (
            "occurrence",
            (
                "MUNIC_OCOR",
                "MUN_OCOR",
                "CODMUNOCOR",
                "COD MUN OCOR",
                "CO MUNICIPIO OCORRENCIA",
                "COD MUNICIPIO OCORRENCIA",
            ),
        ),
        (
            "hospital_location",
            (
                "MUNIC_MOV",
                "MUN_MOV",
                "CODMUNMOV",
                "COD MUN MOV",
                "CO MUNICIPIO HOSPITAL",
                "CO MUNICIPIO ESTABELECIMENTO",
                "COD MUNICIPIO HOSPITAL",
            ),
        ),
    )

    for scope, aliases in candidates:
        column_name = find_column_by_aliases(normalized_to_original, *aliases)
        if column_name is not None:
            return MunicipalityColumnResolution(column_name=column_name, scope=scope)

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIH raw file is missing a recognized municipality code column. Supported "
        "aliases cover residence, occurrence, and hospital-location fields. "
        f"Available columns: {available_columns}"
    )
    raise SihHospitalizationSchemaError(msg)


def _parse_year_series(raw_values: pd.Series) -> pd.Series:
    """Parse a year series from SIH competence or date columns."""
    string_values = raw_values.astype("string").str.strip()
    year_values = pd.Series(pd.NA, index=raw_values.index, dtype="Int64")

    four_digit_mask = string_values.str.fullmatch(r"20\d{2}", na=False)
    year_values.loc[four_digit_mask] = string_values.loc[four_digit_mask].astype("Int64")

    yyyymm_mask = string_values.str.fullmatch(r"20\d{2}(0[1-9]|1[0-2])(\d{2})?", na=False)
    year_values.loc[yyyymm_mask] = string_values.loc[yyyymm_mask].str[:4].astype("Int64")

    month_year_mask = string_values.str.fullmatch(
        r"(0?[1-9]|1[0-2])[/-]20\d{2}",
        na=False,
    )
    year_values.loc[month_year_mask] = string_values.loc[month_year_mask].str[-4:].astype("Int64")

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
            "year",
            (
                "ANO CMPT",
                "ANO COMPETENCIA",
                "ANO",
                "ANO INTERNACAO",
            ),
        ),
        (
            "competence",
            (
                "MES CMPT",
                "COMPETEN",
                "COMPETENCIA",
                "COMPET",
            ),
        ),
        (
            "date",
            (
                "DT INTER",
                "DT SAIDA",
                "DATA INTERNACAO",
                "DATA SAIDA",
            ),
        ),
    )

    for source_type, aliases in column_groups:
        column_name = find_column_by_aliases(normalized_to_original, *aliases)
        if column_name is not None:
            return YearResolution(column_name=column_name, source_type=source_type)

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIH raw file is missing a recognized competence/date column, so the ETL "
        "cannot aggregate to municipality-year. "
        f"Available columns: {available_columns}"
    )
    raise SihHospitalizationSchemaError(msg)


def resolve_length_of_stay_column(raw_frame: pd.DataFrame) -> str:
    """Resolve the raw column used for hospital length of stay."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)
    column_name = find_column_by_aliases(
        normalized_to_original,
        "DIAS PERM",
        "PERMANENCIA",
        "QT DIARIAS",
        "DIAS INTERNADO",
    )
    if column_name is not None:
        return column_name

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIH raw file is missing a recognized length-of-stay column required for "
        f"the requested metrics. Available columns: {available_columns}"
    )
    raise SihHospitalizationSchemaError(msg)


def resolve_cost_columns(raw_frame: pd.DataFrame) -> CostResolution:
    """Resolve how hospitalization cost should be built from the raw layout."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)
    total_cost_column = find_column_by_aliases(
        normalized_to_original,
        "VAL TOT",
        "VAL TOTAL",
        "VALOR TOTAL",
        "VL TOTAL",
        "VL TOT",
    )
    if total_cost_column is not None:
        return CostResolution(
            source_type="single_total_column",
            source_columns=(total_cost_column,),
        )

    component_columns = []
    for _canonical_name, aliases in COMPONENT_COST_COLUMN_GROUPS:
        column_name = find_column_by_aliases(normalized_to_original, *aliases)
        if column_name is None:
            break
        component_columns.append(column_name)

    if len(component_columns) == len(COMPONENT_COST_COLUMN_GROUPS):
        return CostResolution(
            source_type="sum_of_service_components",
            source_columns=tuple(component_columns),
        )

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIH raw file is missing a recognized hospitalization-cost field. The ETL "
        "supports one total-cost column or the pair VAL_SH and VAL_SP (or close "
        f"aliases). Available columns: {available_columns}"
    )
    raise SihHospitalizationSchemaError(msg)


def resolve_diagnosis_columns(raw_frame: pd.DataFrame) -> tuple[str, ...]:
    """Resolve the diagnosis/external-cause columns available for CID filtering."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)
    columns: list[str] = []
    known_aliases = {
        "DIAGPRINC",
        "DIAGSECUN",
        "DIAGSEC1",
        "DIAGSEC2",
        "DIAGSEC3",
        "DIAGSEC4",
        "DIAGSEC5",
        "CIDPRINC",
        "CIDSECUN",
        "CIDSEC1",
        "CIDSEC2",
        "CIDSEC3",
        "CIDSEC4",
        "CIDSEC5",
        "CAUSAEXT",
        "CAUSAEXTERNA",
        "CAUSABAS",
        "CAUSABASICA",
    }

    for normalized_name, original_name in normalized_to_original.items():
        compact_name = normalized_name.replace(" ", "")
        if compact_name.startswith("DIAG") or compact_name in known_aliases:
            columns.append(original_name)

    deduplicated_columns = tuple(dict.fromkeys(columns))
    if deduplicated_columns:
        return deduplicated_columns

    available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
    msg = (
        "SIH raw file does not expose recognized diagnosis/external-cause columns. "
        "Without those fields the ETL cannot apply the CID-10 V20-V29 case "
        f"definition. Available columns: {available_columns}"
    )
    raise SihHospitalizationSchemaError(msg)


def resolve_sih_schema(raw_frame: pd.DataFrame) -> SihSchemaResolution:
    """Resolve all raw columns required by the SIH ETL."""
    return SihSchemaResolution(
        municipality=resolve_municipality_column(raw_frame),
        year=resolve_year_column(raw_frame),
        length_of_stay_column=resolve_length_of_stay_column(raw_frame),
        diagnosis_columns=resolve_diagnosis_columns(raw_frame),
        cost=resolve_cost_columns(raw_frame),
    )


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


def standardize_sih_hospitalization_frame(
    raw_frame: pd.DataFrame,
    *,
    source_file_name: str,
) -> tuple[pd.DataFrame, SihSchemaResolution, tuple[str, ...]]:
    """Validate and standardize one SIH raw frame to record-level metrics."""
    schema = resolve_sih_schema(raw_frame)

    selected_columns = [
        schema.municipality.column_name,
        schema.year.column_name,
        schema.length_of_stay_column,
        *schema.cost.source_columns,
        *schema.diagnosis_columns,
    ]
    rename_map = {
        schema.municipality.column_name: "municipality_code",
        schema.year.column_name: "year_source_value",
        schema.length_of_stay_column: "length_of_stay_days",
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
        error_cls=SihHospitalizationDataQualityError,
        message=(
            "SIH ETL found rows with an invalid competence/date value for annual aggregation"
        ),
    )
    working_frame["year"] = year_values.astype("Int64")

    working_frame["length_of_stay_days"] = pd.to_numeric(
        working_frame["length_of_stay_days"],
        errors="coerce",
    )
    assert_mask_empty(
        working_frame["length_of_stay_days"].isna(),
        error_cls=SihHospitalizationDataQualityError,
        message="SIH ETL found rows with missing or invalid length of stay",
    )
    assert_mask_empty(
        working_frame["length_of_stay_days"] < 0,
        error_cls=SihHospitalizationDataQualityError,
        message="SIH ETL found rows with negative length of stay",
    )

    if schema.cost.source_type == "single_total_column":
        working_frame["hospitalization_cost"] = pd.to_numeric(
            raw_frame.loc[working_frame.index, schema.cost.source_columns[0]],
            errors="coerce",
        )
    else:
        component_costs = raw_frame.loc[working_frame.index, list(schema.cost.source_columns)].apply(
            pd.to_numeric,
            errors="coerce",
        )
        assert_mask_empty(
            component_costs.isna().any(axis=1),
            error_cls=SihHospitalizationDataQualityError,
            message="SIH ETL found rows with missing or invalid hospitalization-cost components",
        )
        working_frame["hospitalization_cost"] = component_costs.sum(axis=1)

    assert_mask_empty(
        working_frame["hospitalization_cost"].isna(),
        error_cls=SihHospitalizationDataQualityError,
        message="SIH ETL found rows with missing or invalid hospitalization cost",
    )
    assert_mask_empty(
        working_frame["hospitalization_cost"] < 0,
        error_cls=SihHospitalizationDataQualityError,
        message="SIH ETL found rows with negative hospitalization cost",
    )

    diagnosis_match_frame = working_frame.loc[:, schema.diagnosis_columns].apply(
        lambda column: column.map(is_motorcycle_occupant_cid)
    )
    diagnosis_columns_with_matches = tuple(
        column_name
        for column_name in schema.diagnosis_columns
        if bool(diagnosis_match_frame[column_name].any())
    )
    motorcycle_case_mask = diagnosis_match_frame.any(axis=1)
    filtered_frame = working_frame.loc[motorcycle_case_mask].copy()

    filtered_frame["source_file_name"] = source_file_name
    filtered_frame["municipality_scope"] = schema.municipality.scope
    filtered_frame["source_municipality_column"] = schema.municipality.column_name

    standardized_frame = filtered_frame[
        [
            "municipality_code",
            "year",
            "hospitalization_cost",
            "length_of_stay_days",
            "municipality_scope",
            "source_municipality_column",
            "source_file_name",
        ]
    ].reset_index(drop=True)
    return standardized_frame, schema, diagnosis_columns_with_matches


def aggregate_sih_hospitalizations(record_frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate record-level SIH hospitalizations to municipality-year."""
    if record_frame.empty:
        return pd.DataFrame(
            columns=[
                "municipality_code",
                "year",
                "motorcycle_hospitalizations",
                "total_hospitalization_cost",
                "total_length_of_stay_days",
                "mean_length_of_stay_days",
                "mean_hospitalization_cost",
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
        .agg(
            motorcycle_hospitalizations=("municipality_code", "size"),
            total_hospitalization_cost=("hospitalization_cost", "sum"),
            total_length_of_stay_days=("length_of_stay_days", "sum"),
        )
        .sort_values(["year", "municipality_code"])
        .reset_index(drop=True)
    )
    aggregated_frame["mean_length_of_stay_days"] = (
        aggregated_frame["total_length_of_stay_days"]
        / aggregated_frame["motorcycle_hospitalizations"]
    )
    aggregated_frame["mean_hospitalization_cost"] = (
        aggregated_frame["total_hospitalization_cost"]
        / aggregated_frame["motorcycle_hospitalizations"]
    )

    assert_no_duplicate_keys(
        aggregated_frame,
        subset=["municipality_code", "year"],
        error_cls=SihHospitalizationDataQualityError,
        message="SIH ETL produced duplicate municipality-year keys",
    )

    return aggregated_frame[
        [
            "municipality_code",
            "year",
            "motorcycle_hospitalizations",
            "total_hospitalization_cost",
            "total_length_of_stay_days",
            "mean_length_of_stay_days",
            "mean_hospitalization_cost",
            "municipality_scope",
            "source_municipality_column",
        ]
    ]


def build_default_output_path() -> Path:
    """Return the default parquet path for the SIH municipality-year dataset."""
    return SIH_INTERIM_DIR / DEFAULT_OUTPUT_FILE_NAME


def build_default_metadata_path() -> Path:
    """Return the default JSON metadata path for the SIH municipality-year dataset."""
    return SIH_INTERIM_DIR / DEFAULT_METADATA_FILE_NAME


def run_sih_hospitalization_etl(
    *,
    input_paths: list[Path] | None = None,
    output_path: Path | None = None,
    metadata_path: Path | None = None,
) -> SihHospitalizationEtlResult:
    """Execute the SIH ETL from raw files to an interim municipality-year parquet."""
    resolved_input_paths = resolve_sih_input_paths(input_paths)

    standardized_frames: list[pd.DataFrame] = []
    raw_row_count = 0
    diagnosis_columns_with_matches: set[str] = set()
    resolved_schema: SihSchemaResolution | None = None

    for input_path in resolved_input_paths:
        raw_frame = load_sih_raw_frame(input_path)
        raw_row_count += len(raw_frame)
        standardized_frame, schema, matched_columns = standardize_sih_hospitalization_frame(
            raw_frame,
            source_file_name=input_path.name,
        )
        standardized_frames.append(standardized_frame)
        diagnosis_columns_with_matches.update(matched_columns)

        if resolved_schema is None:
            resolved_schema = schema
            continue

        if schema != resolved_schema:
            msg = (
                "The SIH ETL found inconsistent schemas across input files. Ensure "
                "all files use the same municipality, time, cost, and diagnosis layout "
                "before aggregating them together."
            )
            raise SihHospitalizationSchemaError(msg)

    if resolved_schema is None:
        raise SihHospitalizationSchemaError("The SIH ETL did not load any raw frame.")

    combined_frame = pd.concat(standardized_frames, ignore_index=True)
    aggregated_frame = aggregate_sih_hospitalizations(combined_frame)

    resolved_output_path = resolve_output_path(
        output_path=output_path,
        default_path=build_default_output_path(),
    )
    saved_output_path = save_parquet_frame(
        aggregated_frame,
        output_path=resolved_output_path,
    )

    summary = SihQualitySummary(
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
        source_length_of_stay_column=resolved_schema.length_of_stay_column,
        source_cost_type=resolved_schema.cost.source_type,
        source_cost_columns=resolved_schema.cost.source_columns,
        diagnosis_columns_considered=resolved_schema.diagnosis_columns,
        diagnosis_columns_with_matches=tuple(sorted(diagnosis_columns_with_matches)),
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
                    "municipality_scope describes whether aggregation used residence, "
                    "occurrence, or hospital-location municipality according to the "
                    "raw columns available."
                ),
                (
                    "CID-10 filtering uses the diagnosis/external-cause columns listed "
                    "in diagnosis_columns_considered and keeps records with any V20-V29 "
                    "code found there."
                ),
                (
                    "This ETL does not assume a universal SIH raw layout. It only "
                    "accepts schemas that expose recognizable municipality, time, cost, "
                    "length-of-stay, and diagnosis/external-cause fields."
                ),
            ],
        },
        output_path=resolved_metadata_path,
    )

    LOGGER.info(
        (
            "SIH ETL summary: raw_files=%s raw_rows=%s filtered_rows=%s "
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
    LOGGER.info("Clean SIH municipality-year dataset saved to %s", saved_output_path)
    LOGGER.info("SIH ETL metadata saved to %s", saved_metadata_path)

    return SihHospitalizationEtlResult(
        input_paths=tuple(resolved_input_paths),
        output_path=saved_output_path,
        metadata_path=saved_metadata_path,
        summary=summary,
    )
