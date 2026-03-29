"""Centralized project paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DirectoryStatus:
    """Status information for an expected project directory."""

    name: str
    path: Path
    exists: bool


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
TABLES_DIR = OUTPUTS_DIR / "tables"
TESTS_DIR = PROJECT_ROOT / "tests"

PROJECT_DIRECTORIES: dict[str, Path] = {
    "project_root": PROJECT_ROOT,
    "src_dir": SRC_DIR,
    "data_dir": DATA_DIR,
    "raw_data_dir": RAW_DATA_DIR,
    "interim_data_dir": INTERIM_DATA_DIR,
    "processed_data_dir": PROCESSED_DATA_DIR,
    "notebooks_dir": NOTEBOOKS_DIR,
    "outputs_dir": OUTPUTS_DIR,
    "figures_dir": FIGURES_DIR,
    "tables_dir": TABLES_DIR,
    "tests_dir": TESTS_DIR,
}


def ensure_project_directories() -> None:
    """Create the main project directories when they do not exist."""
    for path in PROJECT_DIRECTORIES.values():
        path.mkdir(parents=True, exist_ok=True)


def get_directory_statuses() -> list[DirectoryStatus]:
    """Return the existence status for the main project directories."""
    return [
        DirectoryStatus(name=name, path=path, exists=path.exists())
        for name, path in PROJECT_DIRECTORIES.items()
    ]
