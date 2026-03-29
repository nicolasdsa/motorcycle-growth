"""Command-line utilities for the motorcycle growth project."""

from __future__ import annotations

import argparse

from motorcycle_growth.config import get_directory_statuses
from motorcycle_growth.logging_utils import configure_logging, get_logger

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

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
