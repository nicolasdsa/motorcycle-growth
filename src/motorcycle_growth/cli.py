"""Command-line utilities for the motorcycle growth project."""

from __future__ import annotations

import argparse
from pathlib import Path

from motorcycle_growth.cnes_infrastructure_etl import run_cnes_infrastructure_etl
from motorcycle_growth.config import get_directory_statuses
from motorcycle_growth.data_catalog import get_data_sources
from motorcycle_growth.logging_utils import configure_logging, get_logger
from motorcycle_growth.population_etl import run_population_etl
from motorcycle_growth.raw_data import (
    AcquisitionOptions,
    AcquisitionStatus,
    build_summary,
    run_raw_data_acquisition,
)
from motorcycle_growth.senatran_fleet_etl import run_senatran_fleet_etl
from motorcycle_growth.sim_mortality_etl import run_sim_mortality_etl
from motorcycle_growth.sih_hospitalization_etl import run_sih_hospitalization_etl

LOGGER = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the project CLI parser."""
    parser = argparse.ArgumentParser(
        description="Utility commands for the motorcycle growth project."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "show-paths",
        help="Display the main project directories.",
    )
    subparsers.add_parser(
        "check-project",
        help="Display project directories and verify that the expected folders exist.",
    )
    subparsers.add_parser(
        "show-data-catalog",
        help="Display the planned raw data sources and their ingestion status.",
    )
    acquire_raw_data_parser = subparsers.add_parser(
        "acquire-raw-data",
        help="Download verified raw files and validate manual raw inputs.",
    )
    acquire_raw_data_parser.add_argument(
        "--check-only",
        action="store_true",
        help="Only inspect raw-data requirements without attempting downloads.",
    )
    acquire_raw_data_parser.add_argument(
        "--senatran-year",
        type=int,
        help="Prefer one SENATRAN year page when discovering the fleet spreadsheet.",
    )
    acquire_raw_data_parser.add_argument(
        "--ibge-year",
        type=int,
        help="Prefer one IBGE population year when discovering the spreadsheet.",
    )
    acquire_raw_data_parser.add_argument(
        "--sim-year",
        type=int,
        help="Prefer one SIM resource year when discovering the mortality file.",
    )
    acquire_raw_data_parser.add_argument(
        "--sim-format",
        choices=("csv", "xml"),
        default="csv",
        help="Preferred SIM resource format for automated discovery. Default: csv.",
    )
    acquire_raw_data_parser.add_argument(
        "--sih-year",
        type=int,
        help="SIH/SUS year for one official competence file.",
    )
    acquire_raw_data_parser.add_argument(
        "--sih-month",
        type=int,
        help="SIH/SUS month for one official competence file.",
    )
    acquire_raw_data_parser.add_argument(
        "--sih-uf",
        help="SIH/SUS UF code for one official competence file, for example SP.",
    )
    acquire_raw_data_parser.add_argument(
        "--cache-ttl-hours",
        type=int,
        default=24,
        help="Discovery cache TTL in hours. Default: 24.",
    )
    etl_population_parser = subparsers.add_parser(
        "etl-population",
        help="Load, validate, standardize, and save the municipality population dataset.",
    )
    etl_population_parser.add_argument(
        "--input-path",
        type=Path,
        help="Optional raw population file path. Defaults to the expected raw directory.",
    )
    etl_population_parser.add_argument(
        "--output-path",
        type=Path,
        help="Optional parquet output path. Defaults to data/interim/population/.",
    )
    etl_population_parser.add_argument(
        "--year",
        type=int,
        help="Reference year override. Defaults to inference from the raw filename.",
    )
    etl_senatran_parser = subparsers.add_parser(
        "etl-senatran-fleet",
        help=(
            "Load, validate, annualize, and save the municipality-year SENATRAN "
            "motorcycle fleet dataset."
        ),
    )
    etl_senatran_parser.add_argument(
        "--input-path",
        action="append",
        type=Path,
        help=(
            "Optional raw SENATRAN file path. Repeat the flag to provide more than "
            "one monthly file. Defaults to all files under the expected raw directory."
        ),
    )
    etl_senatran_parser.add_argument(
        "--output-path",
        type=Path,
        help="Optional parquet output path. Defaults to data/interim/senatran/.",
    )
    etl_senatran_parser.add_argument(
        "--metadata-path",
        type=Path,
        help="Optional metadata json path. Defaults to the output parquet directory.",
    )
    etl_senatran_parser.add_argument(
        "--population-path",
        type=Path,
        help=(
            "Optional cleaned population parquet used to recover CO_IBGE from UF and "
            "municipality names."
        ),
    )
    etl_sih_parser = subparsers.add_parser(
        "etl-sih-hospitalizations",
        help=(
            "Load, validate, filter, aggregate, and save the municipality-year SIH "
            "motorcycle hospitalization dataset."
        ),
    )
    etl_sih_parser.add_argument(
        "--input-path",
        action="append",
        type=Path,
        help=(
            "Optional raw SIH file path. Repeat the flag to provide more than one "
            "file. Defaults to all supported files under the expected raw directory."
        ),
    )
    etl_sih_parser.add_argument(
        "--output-path",
        type=Path,
        help="Optional parquet output path. Defaults to data/interim/sih/.",
    )
    etl_sih_parser.add_argument(
        "--metadata-path",
        type=Path,
        help="Optional metadata json path. Defaults to the output parquet directory.",
    )
    etl_sim_parser = subparsers.add_parser(
        "etl-sim-mortality",
        help=(
            "Load, validate, filter, aggregate, and save the municipality-year SIM "
            "motorcycle mortality dataset."
        ),
    )
    etl_sim_parser.add_argument(
        "--input-path",
        action="append",
        type=Path,
        help=(
            "Optional raw SIM file path. Repeat the flag to provide more than one "
            "file. Defaults to all supported files under the expected raw directory."
        ),
    )
    etl_sim_parser.add_argument(
        "--output-path",
        type=Path,
        help="Optional parquet output path. Defaults to data/interim/sim/.",
    )
    etl_sim_parser.add_argument(
        "--metadata-path",
        type=Path,
        help="Optional metadata json path. Defaults to the output parquet directory.",
    )
    etl_cnes_parser = subparsers.add_parser(
        "etl-cnes-infrastructure",
        help=(
            "Load, standardize, aggregate, and save municipality-year CNES "
            "infrastructure indicators for emergency trauma care."
        ),
    )
    etl_cnes_parser.add_argument(
        "--establishments-input-path",
        action="append",
        type=Path,
        help=(
            "Optional CNES establishment input file path. Repeat the flag to "
            "provide more than one file. Defaults to supported files under the "
            "expected raw directory."
        ),
    )
    etl_cnes_parser.add_argument(
        "--beds-input-path",
        action="append",
        type=Path,
        help=(
            "Optional CNES bed input file path. Repeat the flag to provide more "
            "than one file. Defaults to supported files under the expected raw "
            "directory."
        ),
    )
    etl_cnes_parser.add_argument(
        "--output-path",
        type=Path,
        help="Optional parquet output path. Defaults to data/interim/cnes/.",
    )
    etl_cnes_parser.add_argument(
        "--metadata-path",
        type=Path,
        help="Optional metadata json path. Defaults to the output parquet directory.",
    )
    etl_cnes_parser.add_argument(
        "--establishments-year",
        type=int,
        help=(
            "Optional reference year for CNES establishment files when the raw "
            "layout does not expose a year and the file path does not include one."
        ),
    )

    return parser


def show_paths() -> None:
    """Log the current project directory map."""
    for directory_status in get_directory_statuses():
        LOGGER.info("%s=%s", directory_status.name, directory_status.path)


def check_project() -> int:
    """Check whether the expected project directories exist."""
    missing_directories = []

    for directory_status in get_directory_statuses():
        status_label = "OK" if directory_status.exists else "MISSING"
        LOGGER.info(
            "[%s] %s=%s",
            status_label,
            directory_status.name,
            directory_status.path,
        )
        if not directory_status.exists:
            missing_directories.append(directory_status)

    if missing_directories:
        LOGGER.error(
            "Project structure check failed. Missing directories: %s",
            ", ".join(item.name for item in missing_directories),
        )
        return 1

    LOGGER.info("Project structure check passed.")
    return 0


def show_data_catalog() -> None:
    """Log the planned data sources for the project."""
    for data_source in get_data_sources():
        LOGGER.info(
            "[%s] %s | institution=%s | raw_dir=%s",
            data_source.automation_level,
            data_source.name,
            data_source.institution,
            data_source.raw_directory_relative,
        )
        LOGGER.info("purpose=%s", data_source.purpose)
        LOGGER.info("unit=%s", data_source.unit_of_analysis)
        LOGGER.info("geo_key=%s", data_source.expected_geographic_key)
        LOGGER.info("time_key=%s", data_source.expected_time_key)
        LOGGER.info("automation_notes=%s", data_source.automation_notes)


def acquire_raw_data(
    *,
    check_only: bool,
    options: AcquisitionOptions | None = None,
) -> int:
    """Run raw data acquisition checks and verified downloads."""
    effective_options = options or AcquisitionOptions()
    records = run_raw_data_acquisition(
        download_enabled=not check_only,
        options=effective_options,
    )

    for record in records:
        log_level = (
            LOGGER.error
            if record.status in {AcquisitionStatus.MISSING, AcquisitionStatus.FAILED}
            else LOGGER.warning
            if record.status
            in {
                AcquisitionStatus.DOWNLOAD_AVAILABLE,
                AcquisitionStatus.REQUIRES_CONFIGURATION,
            }
            else LOGGER.info
        )
        log_level(
            "[%s] %s | %s",
            record.status,
            record.dataset_id,
            record.message,
        )

    summary = build_summary(records)
    LOGGER.info(
        (
            "Raw acquisition summary: total=%s present=%s downloaded=%s "
            "download_available=%s missing=%s failed=%s requires_configuration=%s"
        ),
        summary.total,
        summary.present,
        summary.downloaded,
        summary.download_available,
        summary.missing,
        summary.failed,
        summary.requires_configuration,
    )

    return 1 if summary.has_issues else 0


def etl_population(
    *,
    input_path: Path | None,
    output_path: Path | None,
    year: int | None,
) -> int:
    """Run the population ETL."""
    result = run_population_etl(
        input_path=input_path,
        output_path=output_path,
        year=year,
    )
    LOGGER.info(
        "Population ETL completed: input=%s output=%s",
        result.input_path,
        result.output_path,
    )
    return 0


def etl_senatran_fleet(
    *,
    input_paths: list[Path] | None,
    output_path: Path | None,
    metadata_path: Path | None,
    population_path: Path | None,
) -> int:
    """Run the SENATRAN fleet ETL."""
    result = run_senatran_fleet_etl(
        input_paths=input_paths,
        output_path=output_path,
        metadata_path=metadata_path,
        population_path=population_path,
    )
    LOGGER.info(
        "SENATRAN fleet ETL completed: outputs=%s metadata=%s",
        result.output_path,
        result.metadata_path,
    )
    return 0


def etl_sih_hospitalizations(
    *,
    input_paths: list[Path] | None,
    output_path: Path | None,
    metadata_path: Path | None,
) -> int:
    """Run the SIH hospitalization ETL."""
    result = run_sih_hospitalization_etl(
        input_paths=input_paths,
        output_path=output_path,
        metadata_path=metadata_path,
    )
    LOGGER.info(
        "SIH hospitalization ETL completed: output=%s metadata=%s",
        result.output_path,
        result.metadata_path,
    )
    return 0


def etl_sim_mortality(
    *,
    input_paths: list[Path] | None,
    output_path: Path | None,
    metadata_path: Path | None,
) -> int:
    """Run the SIM mortality ETL."""
    result = run_sim_mortality_etl(
        input_paths=input_paths,
        output_path=output_path,
        metadata_path=metadata_path,
    )
    LOGGER.info(
        "SIM mortality ETL completed: output=%s metadata=%s",
        result.output_path,
        result.metadata_path,
    )
    return 0


def etl_cnes_infrastructure(
    *,
    establishment_input_paths: list[Path] | None,
    bed_input_paths: list[Path] | None,
    output_path: Path | None,
    metadata_path: Path | None,
    establishments_year: int | None,
) -> int:
    """Run the CNES infrastructure ETL."""
    result = run_cnes_infrastructure_etl(
        establishment_input_paths=establishment_input_paths,
        bed_input_paths=bed_input_paths,
        output_path=output_path,
        metadata_path=metadata_path,
        establishments_year=establishments_year,
    )
    LOGGER.info(
        "CNES infrastructure ETL completed: output=%s metadata=%s",
        result.output_path,
        result.metadata_path,
    )
    return 0


def main() -> int:
    """Run the project CLI."""
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "show-paths":
        show_paths()
        return 0

    if args.command == "check-project":
        return check_project()

    if args.command == "show-data-catalog":
        show_data_catalog()
        return 0

    if args.command == "acquire-raw-data":
        options = AcquisitionOptions(
            senatran_year=args.senatran_year,
            ibge_year=args.ibge_year,
            sim_year=args.sim_year,
            sim_format=args.sim_format,
            sih_year=args.sih_year,
            sih_month=args.sih_month,
            sih_uf=args.sih_uf.upper() if args.sih_uf else None,
            cache_ttl_hours=args.cache_ttl_hours,
        )
        return acquire_raw_data(
            check_only=args.check_only,
            options=options,
        )

    if args.command == "etl-population":
        return etl_population(
            input_path=args.input_path,
            output_path=args.output_path,
            year=args.year,
        )

    if args.command == "etl-senatran-fleet":
        return etl_senatran_fleet(
            input_paths=args.input_path,
            output_path=args.output_path,
            metadata_path=args.metadata_path,
            population_path=args.population_path,
        )

    if args.command == "etl-sih-hospitalizations":
        return etl_sih_hospitalizations(
            input_paths=args.input_path,
            output_path=args.output_path,
            metadata_path=args.metadata_path,
        )

    if args.command == "etl-sim-mortality":
        return etl_sim_mortality(
            input_paths=args.input_path,
            output_path=args.output_path,
            metadata_path=args.metadata_path,
        )

    if args.command == "etl-cnes-infrastructure":
        return etl_cnes_infrastructure(
            establishment_input_paths=args.establishments_input_path,
            bed_input_paths=args.beds_input_path,
            output_path=args.output_path,
            metadata_path=args.metadata_path,
            establishments_year=args.establishments_year,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
