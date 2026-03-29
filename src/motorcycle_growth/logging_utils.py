"""Logging helpers for the motorcycle growth project."""

from __future__ import annotations

import logging

DEFAULT_LOG_FORMAT = "%(levelname)s: %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure application logging for local scripts and CLIs."""
    logging.basicConfig(level=level, format=DEFAULT_LOG_FORMAT)


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module name."""
    return logging.getLogger(name)
