"""Placeholder entry point for a future dashboard application."""

from __future__ import annotations

from motorcycle_growth.logging_utils import configure_logging, get_logger

LOGGER = get_logger(__name__)

def main() -> None:
    """Run the dashboard placeholder."""
    configure_logging()
    LOGGER.info("Dashboard entry point is not implemented yet.")


if __name__ == "__main__":
    main()
