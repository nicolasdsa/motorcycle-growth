"""Tests for the project paths module."""

from motorcycle_growth.config.paths import (
    PROJECT_ROOT,
    RAW_DATA_DIR,
    SRC_DIR,
    get_directory_statuses,
)


def test_project_root_contains_expected_directories() -> None:
    """The initial scaffold should expose the expected root directories."""
    assert SRC_DIR.parent == PROJECT_ROOT
    assert RAW_DATA_DIR.parent.name == "data"


def test_directory_statuses_include_existing_project_root() -> None:
    """The project root should always exist in the directory status list."""
    statuses = {item.name: item for item in get_directory_statuses()}
    assert statuses["project_root"].exists is True
