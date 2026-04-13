"""ETL helpers for municipality-level SENATRAN motorcycle fleet data."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
import zipfile
from xml.etree import ElementTree as ET

import pandas as pd

from motorcycle_growth.config import INTERIM_DATA_DIR, RAW_DATA_DIR
from motorcycle_growth.etl_utils import (
    assert_mask_empty,
    assert_no_duplicate_keys,
    build_normalized_column_map,
    normalize_co_ibge_like_code,
    normalize_label,
    normalize_lookup_text,
    resolve_output_path,
    save_json_payload,
    save_parquet_frame,
)
from motorcycle_growth.logging_utils import get_logger


LOGGER = get_logger(__name__)

SENATRAN_RAW_DIR = RAW_DATA_DIR / "senatran_motorcycle_fleet"
SENATRAN_INTERIM_DIR = INTERIM_DATA_DIR / "senatran"
POPULATION_INTERIM_DIR = INTERIM_DATA_DIR / "population"

DEFAULT_OUTPUT_FILE_NAME = "senatran_motorcycles_municipality_year.parquet"
DEFAULT_METADATA_FILE_NAME = "senatran_motorcycles_municipality_year_metadata.json"
YEARLY_AVERAGE_RULE_ID = "yearly_average_of_available_monthly_stocks"

REQUIRED_SOURCE_COLUMNS = (
    "UF",
    "MUNICIPIO",
    "TOTAL",
    "CICLOMOTOR",
    "MOTOCICLETA",
    "MOTONETA",
    "QUADRICICLO",
    "SIDE-CAR",
    "TRICICLO",
)
MOTORCYCLE_SOURCE_COLUMNS = (
    "CICLOMOTOR",
    "MOTOCICLETA",
    "MOTONETA",
    "QUADRICICLO",
    "SIDE-CAR",
    "TRICICLO",
)

UF_CODE_TO_ABBR = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}

PORTUGUESE_MONTH_TO_NUMBER = {
    "JANEIRO": 1,
    "FEVEREIRO": 2,
    "MARCO": 3,
    "MARÇO": 3,
    "ABRIL": 4,
    "MAIO": 5,
    "JUNHO": 6,
    "JULHO": 7,
    "AGOSTO": 8,
    "SETEMBRO": 9,
    "OUTUBRO": 10,
    "NOVEMBRO": 11,
    "DEZEMBRO": 12,
}

# The official SENATRAN municipality labels do not always match the current IBGE
# municipality nomenclature exactly. These aliases make the linkage explicit.
SENATRAN_MUNICIPALITY_NAME_ALIASES = {
    ("BA", "LAGEDO DO TABOCAL"): "LAJEDO DO TABOCAL",
    ("BA", "SANTA TERESINHA"): "SANTA TEREZINHA",
    ("GO", "BOM JESUS"): "BOM JESUS DE GOIAS",
    ("MG", "AMPARO DA SERRA"): "AMPARO DO SERRA",
    ("MG", "BARAO D0 MONTE ALTO"): "BARAO DO MONTE ALTO",
    ("MG", "QUELUZITA"): "QUELUZITO",
    ("MT", "SANTO ANTONIO DO LEVERGER"): "SANTO ANTONIO DE LEVERGER",
    ("MT", "VILA BELA DA SANTISSIMA TRINDA"): "VILA BELA DA SANTISSIMA TRINDADE",
    ("PA", "ELDORADO DOS CARAJAS"): "ELDORADO DO CARAJAS",
    ("PA", "SANTA ISABEL DO PARA"): "SANTA IZABEL DO PARA",
    ("PB", "SANTAREM"): "JOCA CLAUDINO",
    ("PB", "SAO DOMINGOS DE POMBAL"): "SAO DOMINGOS",
    ("PE", "BELEM DE SAO FRANCISCO"): "BELEM DO SAO FRANCISCO",
    ("PE", "IGUARACI"): "IGUARACY",
    ("PE", "LAGOA DO ITAENGA"): "LAGOA DE ITAENGA",
    ("PI", "SAO FRANCISCO DE ASSIS DO PIAU"): "SAO FRANCISCO DE ASSIS DO PIAUI",
    ("PR", "BELA VISTA DO CAROBA"): "BELA VISTA DA CAROBA",
    ("PR", "MUNHOZ DE MELLO"): "MUNHOZ DE MELO",
    ("PR", "PINHAL DO SAO BENTO"): "PINHAL DE SAO BENTO",
    ("PR", "SANTA CRUZ DO MONTE CASTELO"): "SANTA CRUZ DE MONTE CASTELO",
    ("RJ", "PARATI"): "PARATY",
    ("RJ", "TRAJANO DE MORAIS"): "TRAJANO DE MORAES",
    ("RN", "ASSU"): "ACU",
    ("RN", "BOA SAUDE"): "JANUARIO CICCO",
    ("RN", "LAGOA DANTA"): "LAGOA D ANTA",
    ("RS", "SANTANA DO LIVRAMENTO"): "SANT ANA DO LIVRAMENTO",
    ("SC", "BALNEARIO DE PICARRAS"): "BALNEARIO PICARRAS",
    ("SC", "LAGEADO GRANDE"): "LAJEADO GRANDE",
    ("SC", "PRESIDENTE CASTELO BRANCO"): "PRESIDENTE CASTELLO BRANCO",
    ("SC", "SAO LOURENCO D OESTE"): "SAO LOURENCO DO OESTE",
    ("SC", "SAO MIGUEL D OESTE"): "SAO MIGUEL DO OESTE",
    ("SE", "AMPARO DE SAO FRANCISCO"): "AMPARO DO SAO FRANCISCO",
    ("SE", "GRACCHO CARDOSO"): "GRACHO CARDOSO",
    ("TO", "COUTO DE MAGALHAES"): "COUTO MAGALHAES",
    ("TO", "FORTALEZA DO TABOCAO"): "TABOCAO",
    ("TO", "SAO VALERIO DA NATIVIDADE"): "SAO VALERIO",
}

XML_NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "pkg": "http://schemas.openxmlformats.org/package/2006/relationships",
}


class SenatranFleetEtlError(RuntimeError):
    """Base error for the SENATRAN fleet ETL flow."""


class SenatranFleetSchemaError(SenatranFleetEtlError):
    """Raised when the raw SENATRAN file does not match expectations."""


class SenatranFleetDataQualityError(SenatranFleetEtlError):
    """Raised when the cleaned SENATRAN dataset is not fit for downstream use."""


@dataclass(frozen=True)
class SenatranPeriod:
    """Metadata for one monthly SENATRAN raw file."""

    year: int
    month: int
    month_label: str


@dataclass(frozen=True)
class SenatranQualitySummary:
    """Small quality summary emitted after the ETL succeeds."""

    raw_file_count: int
    raw_row_count: int
    monthly_row_count: int
    annual_row_count: int
    unique_municipality_count: int
    unique_year_count: int
    partial_year_row_count: int


@dataclass(frozen=True)
class SenatranFleetEtlResult:
    """Result metadata for one SENATRAN ETL execution."""

    input_paths: tuple[Path, ...]
    output_path: Path
    metadata_path: Path
    summary: SenatranQualitySummary


def column_reference_to_index(cell_reference: str) -> int:
    """Convert an Excel cell reference such as A1 into a zero-based column index."""
    column_letters = "".join(character for character in cell_reference if character.isalpha())
    if not column_letters:
        msg = f"Invalid Excel cell reference without column letters: {cell_reference}"
        raise SenatranFleetSchemaError(msg)

    column_index = 0
    for character in column_letters:
        column_index = (column_index * 26) + (ord(character.upper()) - 64)

    return column_index - 1


def read_xlsx_shared_strings(workbook_file: zipfile.ZipFile) -> list[str]:
    """Read the shared strings table used by the workbook."""
    shared_strings_path = "xl/sharedStrings.xml"
    if shared_strings_path not in workbook_file.namelist():
        return []

    shared_strings_root = ET.fromstring(workbook_file.read(shared_strings_path))
    return [
        "".join(
            text_node.text or ""
            for text_node in string_node.iterfind(".//main:t", XML_NS)
        )
        for string_node in shared_strings_root.findall("main:si", XML_NS)
    ]


def read_xlsx_cell_value(cell_node: ET.Element, shared_strings: list[str]) -> str | None:
    """Return the textual representation of one workbook cell."""
    cell_type = cell_node.attrib.get("t")

    if cell_type == "inlineStr":
        text_nodes = cell_node.findall("main:is/main:t", XML_NS)
        return "".join(text_node.text or "" for text_node in text_nodes)

    value_node = cell_node.find("main:v", XML_NS)
    if value_node is None:
        return None

    cell_value = value_node.text
    if cell_value is None:
        return None

    if cell_type == "s":
        return shared_strings[int(cell_value)]

    if cell_type == "b":
        return "TRUE" if cell_value == "1" else "FALSE"

    return cell_value


def resolve_xlsx_first_sheet_path(workbook_file: zipfile.ZipFile) -> tuple[str, str]:
    """Resolve the first worksheet name and package path from the workbook."""
    workbook_root = ET.fromstring(workbook_file.read("xl/workbook.xml"))
    sheets = workbook_root.find("main:sheets", XML_NS)
    if sheets is None or len(sheets) == 0:
        raise SenatranFleetSchemaError("The SENATRAN workbook does not contain sheets.")

    first_sheet = sheets[0]
    sheet_name = first_sheet.attrib.get("name", "").strip()
    relation_id = first_sheet.attrib.get(f"{{{XML_NS['rel']}}}id")
    if not relation_id:
        raise SenatranFleetSchemaError("The SENATRAN workbook sheet is missing a relation id.")

    workbook_relationships_root = ET.fromstring(
        workbook_file.read("xl/_rels/workbook.xml.rels")
    )
    sheet_target = None
    for relationship in workbook_relationships_root.findall("pkg:Relationship", XML_NS):
        if relationship.attrib.get("Id") == relation_id:
            sheet_target = relationship.attrib.get("Target")
            break

    if sheet_target is None:
        raise SenatranFleetSchemaError(
            "Could not resolve the SENATRAN worksheet target from the workbook."
        )

    return sheet_name, f"xl/{sheet_target}"


def load_xlsx_sheet_rows(input_path: Path) -> tuple[list[list[str | None]], str]:
    """Load the first worksheet rows from an .xlsx file without optional engines."""
    with zipfile.ZipFile(input_path) as workbook_file:
        shared_strings = read_xlsx_shared_strings(workbook_file)
        sheet_name, sheet_path = resolve_xlsx_first_sheet_path(workbook_file)
        sheet_root = ET.fromstring(workbook_file.read(sheet_path))
        sheet_data = sheet_root.find("main:sheetData", XML_NS)
        if sheet_data is None:
            raise SenatranFleetSchemaError("The SENATRAN worksheet does not contain sheet data.")

        rows: list[list[str | None]] = []
        for row_node in sheet_data.findall("main:row", XML_NS):
            cells_by_index: dict[int, str | None] = {}
            highest_index = -1

            for cell_node in row_node.findall("main:c", XML_NS):
                cell_reference = cell_node.attrib.get("r")
                if cell_reference is None:
                    continue

                column_index = column_reference_to_index(cell_reference)
                cells_by_index[column_index] = read_xlsx_cell_value(cell_node, shared_strings)
                highest_index = max(highest_index, column_index)

            if highest_index < 0:
                rows.append([])
                continue

            row_values = [None] * (highest_index + 1)
            for column_index, cell_value in cells_by_index.items():
                row_values[column_index] = cell_value
            rows.append(row_values)

    return rows, sheet_name


def find_senatran_header_row_index(rows: list[list[str | None]]) -> int:
    """Find the row that contains the SENATRAN table header."""
    expected_labels = {normalize_label(column_name) for column_name in REQUIRED_SOURCE_COLUMNS}

    for row_index, row_values in enumerate(rows):
        normalized_values = {
            normalize_label(cell_value)
            for cell_value in row_values
            if cell_value is not None and str(cell_value).strip()
        }
        if expected_labels.issubset(normalized_values):
            return row_index

    raise SenatranFleetSchemaError(
        "Could not locate the SENATRAN table header in the raw workbook."
    )


def build_frame_from_sheet_rows(rows: list[list[str | None]]) -> pd.DataFrame:
    """Build a dataframe from worksheet rows after locating the header row."""
    header_row_index = find_senatran_header_row_index(rows)
    header_values = [str(value).strip() if value is not None else "" for value in rows[header_row_index]]
    header_width = len(header_values)

    data_rows: list[list[str | None]] = []
    for row_values in rows[header_row_index + 1 :]:
        padded_row = list(row_values[:header_width]) + [None] * max(0, header_width - len(row_values))
        if not any(value is not None and str(value).strip() for value in padded_row):
            continue
        data_rows.append(padded_row[:header_width])

    raw_frame = pd.DataFrame(data_rows, columns=header_values)
    repeated_header_mask = raw_frame.apply(
        lambda row: all(
            normalize_label(row[column_name]) == normalize_label(column_name)
            for column_name in REQUIRED_SOURCE_COLUMNS
        ),
        axis=1,
    )
    return raw_frame.loc[~repeated_header_mask].reset_index(drop=True)


def load_senatran_raw_frame(input_path: Path) -> tuple[pd.DataFrame, str]:
    """Load one raw SENATRAN file from disk."""
    LOGGER.info("Loading raw SENATRAN file from %s", input_path)

    if input_path.suffix.lower() != ".xlsx":
        msg = (
            "This ETL currently supports the official SENATRAN .xlsx layout only. "
            f"Received file: {input_path.name}"
        )
        raise SenatranFleetSchemaError(msg)

    rows, sheet_name = load_xlsx_sheet_rows(input_path)
    return build_frame_from_sheet_rows(rows), sheet_name


def resolve_source_columns(raw_frame: pd.DataFrame) -> dict[str, str]:
    """Resolve the raw columns required to build the standard output."""
    normalized_to_original = build_normalized_column_map(raw_frame.columns)

    missing = [
        column_name
        for column_name in REQUIRED_SOURCE_COLUMNS
        if normalize_label(column_name) not in normalized_to_original
    ]
    if missing:
        available_columns = ", ".join(str(column_name) for column_name in raw_frame.columns)
        msg = (
            "SENATRAN raw file is missing required columns for the ETL: "
            f"{', '.join(missing)}. Available columns: {available_columns}"
        )
        raise SenatranFleetSchemaError(msg)

    return {
        column_name: normalized_to_original[normalize_label(column_name)]
        for column_name in REQUIRED_SOURCE_COLUMNS
    }


def parse_period_from_text(text: str) -> SenatranPeriod | None:
    """Parse one SENATRAN month-year period from free text."""
    normalized_text = normalize_label(text)

    for month_label, month_number in PORTUGUESE_MONTH_TO_NUMBER.items():
        if month_label not in normalized_text:
            continue

        year_match = re.search(r"(?<!\d)(20\d{2})(?!\d)", normalized_text)
        if year_match is None:
            continue

        return SenatranPeriod(
            year=int(year_match.group(1)),
            month=month_number,
            month_label=month_label,
        )

    compact_match = re.search(
        r"(?P<month>[A-ZÇ]+?)(?P<year>20\d{2})",
        normalized_text.replace("_", "").replace("-", "").replace("/", ""),
    )
    if compact_match is None:
        return None

    month_label = compact_match.group("month")
    if month_label not in PORTUGUESE_MONTH_TO_NUMBER:
        return None

    return SenatranPeriod(
        year=int(compact_match.group("year")),
        month=PORTUGUESE_MONTH_TO_NUMBER[month_label],
        month_label=month_label,
    )


def infer_senatran_period(
    *,
    input_path: Path,
    sheet_name: str,
    raw_frame: pd.DataFrame,
) -> SenatranPeriod:
    """Infer the monthly period represented by one raw SENATRAN file."""
    candidate_texts = [
        sheet_name,
        input_path.stem,
    ]
    if not raw_frame.empty:
        first_cell_value = raw_frame.columns[0]
        candidate_texts.append(str(first_cell_value))

    for candidate_text in candidate_texts:
        parsed_period = parse_period_from_text(candidate_text)
        if parsed_period is not None:
            return parsed_period

    msg = (
        "Could not infer the SENATRAN month and year from the worksheet name or "
        f"file name: {input_path.name}"
    )
    raise SenatranFleetEtlError(msg)


def resolve_senatran_input_paths(
    input_paths: list[Path] | None = None,
) -> list[Path]:
    """Resolve one or more raw SENATRAN files from the expected input location."""
    if input_paths:
        resolved_paths = []
        for input_path in input_paths:
            resolved_path = input_path.expanduser().resolve()
            if not resolved_path.exists():
                msg = f"SENATRAN raw file not found: {resolved_path}"
                raise FileNotFoundError(msg)
            resolved_paths.append(resolved_path)
        return sorted(resolved_paths)

    candidates = sorted(
        path
        for pattern in ("*.xlsx", "*.xls")
        for path in SENATRAN_RAW_DIR.glob(pattern)
        if path.is_file() and not path.name.startswith(".~lock.")
    )
    if not candidates:
        msg = (
            "No raw SENATRAN file was found under "
            f"{SENATRAN_RAW_DIR}. Place the official monthly municipality workbook "
            "there or pass --input-path explicitly."
        )
        raise FileNotFoundError(msg)

    return candidates


def resolve_population_reference_path(population_path: Path | None = None) -> Path:
    """Resolve the population parquet used to recover CO_IBGE codes."""
    if population_path is not None:
        resolved_path = population_path.expanduser().resolve()
        if not resolved_path.exists():
            msg = f"Population reference parquet not found: {resolved_path}"
            raise FileNotFoundError(msg)
        return resolved_path

    candidates = sorted(
        path
        for path in POPULATION_INTERIM_DIR.glob("*.parquet")
        if path.is_file()
    )
    if not candidates:
        msg = (
            "The SENATRAN raw workbook does not include CO_IBGE. Run the population "
            "ETL first or pass --population-path with a cleaned municipality parquet "
            f"under {POPULATION_INTERIM_DIR}."
        )
        raise FileNotFoundError(msg)

    def candidate_priority(path: Path) -> tuple[int, str]:
        year_match = re.search(r"(?<!\d)(20\d{2})(?!\d)", path.stem)
        return (
            int(year_match.group(1)) if year_match is not None else -1,
            path.name,
        )

    return max(candidates, key=candidate_priority)


def build_population_crosswalk(population_path: Path) -> pd.DataFrame:
    """Load the municipality code crosswalk derived from the cleaned population parquet."""
    LOGGER.info("Loading municipality crosswalk from %s", population_path)
    population_frame = pd.read_parquet(population_path)

    required_columns = {"CO_IBGE", "municipality_name"}
    missing_columns = required_columns.difference(population_frame.columns)
    if missing_columns:
        msg = (
            "Population reference parquet is missing required columns for the SENATRAN "
            f"linkage: {', '.join(sorted(missing_columns))}"
        )
        raise SenatranFleetSchemaError(msg)

    crosswalk = population_frame[["CO_IBGE", "municipality_name"]].copy()
    crosswalk["CO_IBGE"] = crosswalk["CO_IBGE"].map(normalize_co_ibge_like_code)
    crosswalk["uf"] = crosswalk["CO_IBGE"].str[:2].map(UF_CODE_TO_ABBR)
    crosswalk["municipality_name"] = crosswalk["municipality_name"].astype("string").str.strip()
    crosswalk["municipality_lookup_key"] = crosswalk["municipality_name"].map(
        normalize_lookup_text
    )

    assert_mask_empty(
        ~crosswalk["CO_IBGE"].str.fullmatch(r"\d{7}"),
        error_cls=SenatranFleetDataQualityError,
        message="Population reference parquet contains invalid CO_IBGE values for the SENATRAN linkage",
    )
    assert_no_duplicate_keys(
        crosswalk,
        subset=["uf", "municipality_lookup_key"],
        error_cls=SenatranFleetDataQualityError,
        message="Population reference parquet contains duplicate municipality linkage keys",
    )

    return crosswalk[["CO_IBGE", "uf", "municipality_name", "municipality_lookup_key"]]


def apply_municipality_alias(uf: str, municipality_name: str) -> str:
    """Apply one explicit SENATRAN-to-IBGE municipality alias when needed."""
    normalized_name = normalize_lookup_text(municipality_name)
    return SENATRAN_MUNICIPALITY_NAME_ALIASES.get((uf, normalized_name), normalized_name)


def standardize_senatran_monthly_frame(
    raw_frame: pd.DataFrame,
    *,
    period: SenatranPeriod,
    source_file_name: str,
    population_crosswalk: pd.DataFrame,
) -> pd.DataFrame:
    """Validate and standardize one monthly municipality SENATRAN dataset."""
    resolved_columns = resolve_source_columns(raw_frame)
    rename_map = {
        resolved_columns["UF"]: "uf",
        resolved_columns["MUNICIPIO"]: "municipality_name_source",
        resolved_columns["TOTAL"]: "fleet_total",
        resolved_columns["CICLOMOTOR"]: "ciclomotor",
        resolved_columns["MOTOCICLETA"]: "motocicleta",
        resolved_columns["MOTONETA"]: "motoneta",
        resolved_columns["QUADRICICLO"]: "quadriciclo",
        resolved_columns["SIDE-CAR"]: "side_car",
        resolved_columns["TRICICLO"]: "triciclo",
    }
    working_frame = raw_frame.rename(columns=rename_map)[list(rename_map.values())].copy()

    working_frame["uf"] = working_frame["uf"].astype("string").str.strip().str.upper()
    working_frame["municipality_name_source"] = (
        working_frame["municipality_name_source"].astype("string").str.strip()
    )

    municipality_row_mask = (
        working_frame["uf"].notna()
        & working_frame["municipality_name_source"].notna()
        & working_frame["municipality_name_source"].ne("")
        & working_frame["municipality_name_source"].ne("MUNICIPIO")
        & working_frame["municipality_name_source"].ne("MUNICIPIO NAO INFORMADO")
    )
    working_frame = working_frame.loc[municipality_row_mask].copy()

    numeric_columns = [
        "fleet_total",
        "ciclomotor",
        "motocicleta",
        "motoneta",
        "quadriciclo",
        "side_car",
        "triciclo",
    ]
    for column_name in numeric_columns:
        working_frame[column_name] = pd.to_numeric(
            working_frame[column_name],
            errors="coerce",
        )

    assert_mask_empty(
        working_frame[numeric_columns].isna().any(axis=1),
        error_cls=SenatranFleetDataQualityError,
        message="SENATRAN ETL found rows with missing or invalid numeric vehicle counts",
    )

    working_frame["municipality_lookup_key"] = working_frame.apply(
        lambda row: apply_municipality_alias(
            str(row["uf"]),
            str(row["municipality_name_source"]),
        ),
        axis=1,
    )
    working_frame["year"] = period.year
    working_frame["month"] = period.month
    working_frame["month_label"] = period.month_label
    working_frame["source_file_name"] = source_file_name
    working_frame["motorcycles_total"] = working_frame[
        ["ciclomotor", "motocicleta", "motoneta", "quadriciclo", "side_car", "triciclo"]
    ].sum(axis=1)

    standardized_frame = working_frame.merge(
        population_crosswalk,
        left_on=["uf", "municipality_lookup_key"],
        right_on=["uf", "municipality_lookup_key"],
        how="left",
        validate="one_to_one",
    )

    unmatched_mask = standardized_frame["CO_IBGE"].isna()
    if bool(unmatched_mask.any()):
        unmatched_pairs = standardized_frame.loc[
            unmatched_mask,
            ["uf", "municipality_name_source"],
        ].drop_duplicates()
        unmatched_list = ", ".join(
            f"{row.uf}:{row.municipality_name_source}"
            for row in unmatched_pairs.itertuples(index=False)
        )
        msg = (
            "SENATRAN ETL could not recover CO_IBGE for some municipality labels. "
            f"Review the alias dictionary or the population reference parquet. "
            f"Unmatched pairs: {unmatched_list}"
        )
        raise SenatranFleetDataQualityError(msg)

    standardized_frame["CO_IBGE"] = standardized_frame["CO_IBGE"].map(
        normalize_co_ibge_like_code
    )

    assert_no_duplicate_keys(
        standardized_frame,
        subset=["CO_IBGE", "year", "month"],
        error_cls=SenatranFleetDataQualityError,
        message="SENATRAN ETL found duplicate municipality-month rows after linkage",
    )

    return (
        standardized_frame[
            [
                "CO_IBGE",
                "municipality_name",
                "uf",
                "year",
                "month",
                "month_label",
                "ciclomotor",
                "motocicleta",
                "motoneta",
                "quadriciclo",
                "side_car",
                "triciclo",
                "motorcycles_total",
                "source_file_name",
                "municipality_name_source",
            ]
        ]
        .sort_values(["year", "month", "CO_IBGE"])
        .reset_index(drop=True)
    )


def aggregate_monthly_to_year(monthly_frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate monthly municipality stock data into one municipality-year dataset."""
    measure_columns = [
        "ciclomotor",
        "motocicleta",
        "motoneta",
        "quadriciclo",
        "side_car",
        "triciclo",
        "motorcycles_total",
    ]

    annual_frame = (
        monthly_frame.groupby(["CO_IBGE", "municipality_name", "year"], as_index=False)
        .agg(
            {
                **{column_name: "mean" for column_name in measure_columns},
                "month": "nunique",
            }
        )
        .rename(columns={"month": "months_observed"})
    )

    source_months = (
        monthly_frame.groupby(["CO_IBGE", "year"])["month"]
        .apply(lambda values: ",".join(f"{int(value):02d}" for value in sorted(set(values))))
        .rename("source_months")
        .reset_index()
    )
    annual_frame = annual_frame.merge(
        source_months,
        on=["CO_IBGE", "year"],
        how="left",
        validate="one_to_one",
    )

    annual_frame["aggregation_rule"] = YEARLY_AVERAGE_RULE_ID
    annual_frame["is_partial_year"] = annual_frame["months_observed"] < 12
    for column_name in measure_columns:
        annual_frame[column_name] = annual_frame[column_name].round(2)

    assert_no_duplicate_keys(
        annual_frame,
        subset=["CO_IBGE", "year"],
        error_cls=SenatranFleetDataQualityError,
        message="SENATRAN ETL generated duplicate municipality-year rows in the annual dataset",
    )

    return (
        annual_frame[
            [
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
            ]
        ]
        .sort_values(["year", "CO_IBGE"])
        .reset_index(drop=True)
    )


def build_default_output_path() -> Path:
    """Return the default parquet path for the cleaned SENATRAN dataset."""
    return SENATRAN_INTERIM_DIR / DEFAULT_OUTPUT_FILE_NAME


def build_default_metadata_path(output_path: Path) -> Path:
    """Return the default metadata path alongside the parquet output."""
    return output_path.with_name(DEFAULT_METADATA_FILE_NAME)


def build_metadata_payload(
    *,
    resolved_input_paths: list[Path],
    periods_by_file: dict[str, SenatranPeriod],
    population_reference_path: Path,
    summary: SenatranQualitySummary,
) -> dict[str, object]:
    """Build one machine-readable metadata payload for the ETL run."""
    return {
        "dataset_id": "senatran_motorcycles_municipality_year",
        "aggregation_rule": {
            "rule_id": YEARLY_AVERAGE_RULE_ID,
            "description": (
                "For each municipality-year, compute the arithmetic mean of the "
                "monthly fleet stock counts observed in the raw SENATRAN files."
            ),
            "why_this_rule": (
                "The fleet variable is used as an annual exposure denominator. The "
                "yearly average stock is closer to exposure over the whole year than "
                "a single end-of-year snapshot."
            ),
            "partial_year_policy": (
                "When fewer than 12 monthly files are available, the ETL still "
                "computes the average over observed months only and flags coverage "
                "through months_observed and is_partial_year."
            ),
        },
        "columns_used": [
            {
                "source_column": "UF",
                "role": "State abbreviation used to recover CO_IBGE during municipality linkage.",
            },
            {
                "source_column": "MUNICIPIO",
                "role": "Municipality label from SENATRAN used in the crosswalk join.",
            },
            {
                "source_column": "MOTOCICLETA",
                "role": "Motorcycle stock component included in motorcycles_total.",
            },
            {
                "source_column": "MOTONETA",
                "role": "Scooter/moped-like stock component included in motorcycles_total.",
            },
            {
                "source_column": "CICLOMOTOR",
                "role": "Low-displacement motorcycle stock component included in motorcycles_total.",
            },
            {
                "source_column": "TRICICLO",
                "role": "Three-wheel motorcycle-type stock component included in motorcycles_total.",
            },
            {
                "source_column": "QUADRICICLO",
                "role": "Quadricycle stock component included in motorcycles_total.",
            },
            {
                "source_column": "SIDE-CAR",
                "role": "Side-car stock component included in motorcycles_total.",
            },
            {
                "source_column": "TOTAL",
                "role": "Used only as a raw schema validation column, not as the exposure measure.",
            },
        ],
        "municipality_code_linkage": {
            "population_reference_path": str(population_reference_path),
            "key_strategy": "UF plus municipality name after normalization and explicit alias correction.",
            "alias_count": len(SENATRAN_MUNICIPALITY_NAME_ALIASES),
        },
        "input_files": [
            {
                "path": str(input_path),
                "year": periods_by_file[input_path.name].year,
                "month": periods_by_file[input_path.name].month,
                "month_label": periods_by_file[input_path.name].month_label,
            }
            for input_path in resolved_input_paths
        ],
        "summary": asdict(summary),
    }


def run_senatran_fleet_etl(
    *,
    input_paths: list[Path] | None = None,
    output_path: Path | None = None,
    metadata_path: Path | None = None,
    population_path: Path | None = None,
) -> SenatranFleetEtlResult:
    """Execute the SENATRAN ETL from raw monthly files to annual parquet output."""
    resolved_input_paths = resolve_senatran_input_paths(input_paths)
    resolved_population_path = resolve_population_reference_path(population_path)
    population_crosswalk = build_population_crosswalk(resolved_population_path)

    monthly_frames: list[pd.DataFrame] = []
    periods_by_file: dict[str, SenatranPeriod] = {}
    raw_row_count = 0

    for input_path in resolved_input_paths:
        raw_frame, sheet_name = load_senatran_raw_frame(input_path)
        period = infer_senatran_period(
            input_path=input_path,
            sheet_name=sheet_name,
            raw_frame=raw_frame,
        )
        if input_path.name in periods_by_file:
            raise SenatranFleetDataQualityError(
                f"Duplicate SENATRAN input file name detected: {input_path.name}"
            )

        periods_by_file[input_path.name] = period
        raw_row_count += len(raw_frame)
        monthly_frames.append(
            standardize_senatran_monthly_frame(
                raw_frame,
                period=period,
                source_file_name=input_path.name,
                population_crosswalk=population_crosswalk,
            )
        )

    monthly_frame = pd.concat(monthly_frames, ignore_index=True)
    duplicate_period_mask = monthly_frame.duplicated(
        subset=["CO_IBGE", "year", "month"],
        keep=False,
    )
    if bool(duplicate_period_mask.any()):
        duplicate_count = int(duplicate_period_mask.sum())
        msg = (
            "SENATRAN ETL found duplicate municipality-month rows across the provided "
            f"raw files: {duplicate_count}"
        )
        raise SenatranFleetDataQualityError(msg)

    annual_frame = aggregate_monthly_to_year(monthly_frame)

    resolved_output_path = resolve_output_path(
        output_path=output_path,
        default_path=build_default_output_path(),
    )
    resolved_metadata_path = resolve_output_path(
        output_path=metadata_path,
        default_path=build_default_metadata_path(resolved_output_path),
    )

    saved_output_path = save_parquet_frame(
        annual_frame,
        output_path=resolved_output_path,
    )

    summary = SenatranQualitySummary(
        raw_file_count=len(resolved_input_paths),
        raw_row_count=raw_row_count,
        monthly_row_count=len(monthly_frame),
        annual_row_count=len(annual_frame),
        unique_municipality_count=int(annual_frame["CO_IBGE"].nunique()),
        unique_year_count=int(annual_frame["year"].nunique()),
        partial_year_row_count=int(annual_frame["is_partial_year"].sum()),
    )
    metadata_payload = build_metadata_payload(
        resolved_input_paths=resolved_input_paths,
        periods_by_file=periods_by_file,
        population_reference_path=resolved_population_path,
        summary=summary,
    )
    saved_metadata_path = save_json_payload(
        metadata_payload,
        output_path=resolved_metadata_path,
    )

    LOGGER.info(
        (
            "SENATRAN ETL summary: raw_files=%s raw_rows=%s monthly_rows=%s "
            "annual_rows=%s municipalities=%s years=%s partial_year_rows=%s"
        ),
        summary.raw_file_count,
        summary.raw_row_count,
        summary.monthly_row_count,
        summary.annual_row_count,
        summary.unique_municipality_count,
        summary.unique_year_count,
        summary.partial_year_row_count,
    )
    LOGGER.info("Clean SENATRAN dataset saved to %s", saved_output_path)
    LOGGER.info("SENATRAN metadata saved to %s", saved_metadata_path)

    return SenatranFleetEtlResult(
        input_paths=tuple(resolved_input_paths),
        output_path=saved_output_path,
        metadata_path=saved_metadata_path,
        summary=summary,
    )
