"""Placeholder entry point for future data pipelines."""

from __future__ import annotations

import logging


def main() -> None:
    """Run the pipeline placeholder."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logging.getLogger(__name__).info("Pipeline entry point is not implemented yet.")


if __name__ == "__main__":
    main()
