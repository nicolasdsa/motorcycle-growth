"""Placeholder entry point for a future dashboard application."""

from __future__ import annotations

import logging


def main() -> None:
    """Run the dashboard placeholder."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger(__name__).info("Dashboard entry point is not implemented yet.")


if __name__ == "__main__":
    main()
