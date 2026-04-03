"""Data-source catalog for ingestion planning."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from motorcycle_growth.config import PROJECT_ROOT, RAW_DATA_DIR


class AutomationLevel(StrEnum):
    """Expected level of automation for a source."""

    LIKELY_FEASIBLE = "likely_feasible"
    UNCERTAIN = "uncertain"


@dataclass(frozen=True)
class DataSourceMetadata:
    """Metadata describing one planned project data source."""

    dataset_id: str
    name: str
    institution: str
    purpose: str
    unit_of_analysis: str
    expected_geographic_key: str
    expected_time_key: str
    automation_level: AutomationLevel
    automation_notes: str
    raw_directory: Path
    official_pages: tuple[str, ...]
    manual_files_needed: tuple[str, ...]
    manual_source_instructions: str
    notes: str

    @property
    def raw_directory_relative(self) -> Path:
        """Return the raw-data directory relative to the project root."""
        return self.raw_directory.relative_to(PROJECT_ROOT)


DATA_SOURCES: tuple[DataSourceMetadata, ...] = (
    DataSourceMetadata(
        dataset_id="senatran_motorcycle_fleet",
        name="SENATRAN motorcycle fleet",
        institution="Secretaria Nacional de Trânsito (SENATRAN), Ministério dos Transportes",
        purpose=(
            "Measure motorcycle fleet growth over time and provide exposure "
            "denominators for health outcomes."
        ),
        unit_of_analysis="Municipality by vehicle type by reference period.",
        expected_geographic_key=(
            "Municipality code or municipality name plus UF; code harmonization "
            "must be validated during ETL."
        ),
        expected_time_key="Month and year, or year depending on the official asset used.",
        automation_level=AutomationLevel.LIKELY_FEASIBLE,
        automation_notes=(
            "Official year pages and RENAVAM open-data resources exist, but the "
            "stable scriptable endpoint still needs validation."
        ),
        raw_directory=RAW_DATA_DIR / "senatran_motorcycle_fleet",
        official_pages=(
            "https://www.gov.br/transportes/pt-br/assuntos/transito/"
            "conteudo-Senatran/estatisticas-frota-de-veiculos-senatran",
            "https://dados.transportes.gov.br/dataset/"
            "registro-nacional-de-veiculos-automotores-renavam",
        ),
        manual_files_needed=(
            "Municipality-level fleet tables containing motorcycles by period.",
            "If available separately, total fleet tables by municipality and type "
            "for the same periods.",
        ),
        manual_source_instructions=(
            "Download from the official SENATRAN fleet-statistics pages for each "
            "year or from the RENAVAM open-data resources and place the originals "
            "under data/raw/senatran_motorcycle_fleet/."
        ),
        notes=(
            "The final list of vehicle categories to count as motorcycle fleet "
            "must be documented later in transformation code."
        ),
    ),
    DataSourceMetadata(
        dataset_id="ibge_population",
        name="IBGE population data",
        institution="Instituto Brasileiro de Geografia e Estatística (IBGE)",
        purpose=(
            "Provide municipality population denominators for rates and "
            "standardization."
        ),
        unit_of_analysis="Municipality by year.",
        expected_geographic_key="IBGE municipality code.",
        expected_time_key="Year.",
        automation_level=AutomationLevel.LIKELY_FEASIBLE,
        automation_notes=(
            "IBGE publishes official municipal population outputs and downloadable "
            "tables, but the exact source asset must be aligned with the study design."
        ),
        raw_directory=RAW_DATA_DIR / "ibge_population",
        official_pages=(
            "https://www.ibge.gov.br/estatisticas/sociais/populacao/"
            "9103-estimativas-de-populacao.html",
            "https://www.ibge.gov.br/cidades-e-estados/",
        ),
        manual_files_needed=(
            "Annual municipal population estimate tables for the full study period.",
            "If needed, official municipality-level census population tables as a "
            "separate anchor input.",
        ),
        manual_source_instructions=(
            "Download the official population estimate tables from IBGE and place "
            "the originals under data/raw/ibge_population/."
        ),
        notes=(
            "The choice between census counts, annual estimates, or a combined "
            "denominator strategy remains an analytical decision."
        ),
    ),
    DataSourceMetadata(
        dataset_id="sih_sus",
        name="SIH/SUS hospitalization data",
        institution="DATASUS, Ministério da Saúde",
        purpose=(
            "Measure hospitalizations associated with motorcycle-related trauma "
            "and other selected injury outcomes."
        ),
        unit_of_analysis=(
            "Hospitalization record or monthly aggregated export, depending on the "
            "official access method adopted later."
        ),
        expected_geographic_key=(
            "Municipality code for residence and/or occurrence; the exact field "
            "must be defined during ETL."
        ),
        expected_time_key="Competence month or hospitalization date.",
        automation_level=AutomationLevel.LIKELY_FEASIBLE,
        automation_notes=(
            "Official SIH/SUS public transfer access exists through the DATASUS "
            "transfer flow, but the downloader should stay parameterized and conservative."
        ),
        raw_directory=RAW_DATA_DIR / "sih_sus",
        official_pages=(
            "https://datasus.saude.gov.br/acesso-a-informacao/"
            "producao-hospitalar-sih-sus/",
            "https://datasus.saude.gov.br/informacoes-de-saude-tabnet/",
        ),
        manual_files_needed=(
            "Official SIH/SUS exports covering all months and UFs in the study period.",
            "Official dictionary or layout material for diagnosis, external cause, "
            "residence, occurrence, and competence fields.",
        ),
        manual_source_instructions=(
            "Export the required SIH/SUS files manually from official DATASUS access "
            "pages and place the originals under data/raw/sih_sus/. Do not assume a "
            "stable downloader until it is tested."
        ),
        notes=(
            "The motorcycle-trauma case definition belongs to downstream "
            "transformation logic, not to this intake plan."
        ),
    ),
    DataSourceMetadata(
        dataset_id="sim_mortality",
        name="SIM mortality data",
        institution="Ministério da Saúde / DATASUS / OpenDataSUS",
        purpose=(
            "Measure mortality associated with motorcycle-related causes and "
            "support mortality-rate calculations."
        ),
        unit_of_analysis="Death record.",
        expected_geographic_key=(
            "Municipality code for residence and/or occurrence; the analytical "
            "choice must be made in ETL."
        ),
        expected_time_key="Year of death, and possibly month in later analyses.",
        automation_level=AutomationLevel.LIKELY_FEASIBLE,
        automation_notes=(
            "OpenDataSUS lists SIM resources in machine-readable formats, but the "
            "exact downloader still needs validation against the current resources."
        ),
        raw_directory=RAW_DATA_DIR / "sim_mortality",
        official_pages=(
            "https://dadosabertos.saude.gov.br/dataset",
            "https://opendatasus.saude.gov.br/ne/dataset/groups/sim",
            "https://datasus.saude.gov.br/estatisticas-vitais/",
        ),
        manual_files_needed=(
            "Annual SIM files for all years in the study period.",
            "Official dictionary or metadata for cause-of-death and municipality fields.",
        ),
        manual_source_instructions=(
            "Download the official SIM resources from OpenDataSUS or other official "
            "DATASUS mortality pages and place them under data/raw/sim_mortality/."
        ),
        notes=(
            "Motorcycle-related death definitions must be coded explicitly later "
            "from the official cause fields."
        ),
    ),
    DataSourceMetadata(
        dataset_id="cnes_establishments",
        name="CNES establishments data",
        institution="Ministério da Saúde / OpenDataSUS / CNES",
        purpose=(
            "Identify emergency-care infrastructure, including establishment "
            "presence and potential SAMU-related service coverage."
        ),
        unit_of_analysis="Health establishment record.",
        expected_geographic_key="Municipality code, CNES establishment identifier, and UF.",
        expected_time_key="Update date or source reference period.",
        automation_level=AutomationLevel.LIKELY_FEASIBLE,
        automation_notes=(
            "The official CNES page exposes API and flat-file resources and appears "
            "to be regularly updated."
        ),
        raw_directory=RAW_DATA_DIR / "cnes_establishments",
        official_pages=(
            "https://dadosabertos.saude.gov.br/dataset/"
            "cnes-cadastro-nacional-de-estabelecimentos-de-saude",
            "https://opendatasus.saude.gov.br/ne/dataset/"
            "cnes-cadastro-nacional-de-estabelecimentos-de-saude",
        ),
        manual_files_needed=(
            "CNES establishment files for the study period.",
            "Official metadata needed to identify emergency establishments, mobile "
            "units, and service types.",
        ),
        manual_source_instructions=(
            "Download the official CNES establishment resources and place them under "
            "data/raw/cnes_establishments/."
        ),
        notes=(
            "SAMU should be modeled as a filtered CNES subset unless a separate "
            "official source is later confirmed."
        ),
    ),
    DataSourceMetadata(
        dataset_id="cnes_hospital_beds",
        name="CNES hospital beds data",
        institution="Ministério da Saúde / OpenDataSUS / CNES",
        purpose=(
            "Measure emergency and critical-care capacity, especially ICU-related "
            "bed availability."
        ),
        unit_of_analysis="Hospital-bed record or hospital-bed aggregate.",
        expected_geographic_key="Municipality code, establishment identifier, and UF.",
        expected_time_key="Reference year or update period, depending on the resource.",
        automation_level=AutomationLevel.LIKELY_FEASIBLE,
        automation_notes=(
            "The official Hospitais e Leitos dataset exposes downloadable resources "
            "and appears suitable for scripted intake after validation."
        ),
        raw_directory=RAW_DATA_DIR / "cnes_hospital_beds",
        official_pages=(
            "https://dadosabertos.saude.gov.br/dataset/hospitais-e-leitos",
            "https://opendatasus.saude.gov.br/ne/dataset/hospitais-e-leitos",
        ),
        manual_files_needed=(
            "Hospital and bed files covering the study period.",
            "Official metadata needed to identify ICU beds and other emergency-relevant "
            "bed categories.",
        ),
        manual_source_instructions=(
            "Download the official Hospitais e Leitos resources and place them under "
            "data/raw/cnes_hospital_beds/."
        ),
        notes=(
            "ICU definitions and bed-category filters should be documented later in "
            "the transformation layer."
        ),
    ),
)


def get_data_sources() -> tuple[DataSourceMetadata, ...]:
    """Return the project data-source catalog."""
    return DATA_SOURCES


def get_data_source(dataset_id: str) -> DataSourceMetadata:
    """Return one data source by identifier."""
    for data_source in DATA_SOURCES:
        if data_source.dataset_id == dataset_id:
            return data_source

    msg = f"Unknown dataset_id: {dataset_id}"
    raise KeyError(msg)
