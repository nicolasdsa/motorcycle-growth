"""Placeholder entry point for future data pipelines."""

from __future__ import annotations

from motorcycle_growth.logging_utils import configure_logging, get_logger

LOGGER = get_logger(__name__)

def main() -> None:
    """Run the pipeline placeholder."""
    configure_logging()
    LOGGER.info("Pipeline entry point is not implemented yet.")


if __name__ == "__main__":
    main()
