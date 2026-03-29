"""Command-line utilities for the motorcycle growth project."""

from __future__ import annotations

import argparse
import logging

from motorcycle_growth.config.paths import PROJECT_DIRECTORIES


def configure_logging() -> None:
    """Configure a simple logging format for local commands."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def build_parser() -> argparse.ArgumentParser:
    """Build the project CLI parser."""
    parser = argparse.ArgumentParser(
        description="Utility commands for the motorcycle growth project."
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "show-paths",
        help="Display the main project directories.",
    )

    return parser


def show_paths() -> None:
    """Log the current project directory map."""
    logger = logging.getLogger(__name__)

    for name, path in PROJECT_DIRECTORIES.items():
        logger.info("%s=%s", name, path)


def main() -> None:
    """Run the project CLI."""
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "show-paths":
        show_paths()
        return

    parser.print_help()


if __name__ == "__main__":
    main()
