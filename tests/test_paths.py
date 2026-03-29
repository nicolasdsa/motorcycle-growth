"""Tests for the project paths module."""

from motorcycle_growth.config.paths import PROJECT_ROOT, RAW_DATA_DIR, SRC_DIR


def test_project_root_contains_expected_directories() -> None:
    """The initial scaffold should expose the expected root directories."""
    assert SRC_DIR.parent == PROJECT_ROOT
    assert RAW_DATA_DIR.parent.name == "data"
