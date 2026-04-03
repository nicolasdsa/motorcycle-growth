"""Tests for the data catalog module."""

from motorcycle_growth.config import RAW_DATA_DIR
from motorcycle_growth.data_catalog import (
    AutomationLevel,
    get_data_source,
    get_data_sources,
)


def test_data_catalog_contains_expected_sources() -> None:
    """The catalog should expose the planned first-wave sources."""
    dataset_ids = {item.dataset_id for item in get_data_sources()}

    assert dataset_ids == {
        "senatran_motorcycle_fleet",
        "ibge_population",
        "sih_sus",
        "sim_mortality",
        "cnes_establishments",
        "cnes_hospital_beds",
    }


def test_data_catalog_uses_raw_data_directories() -> None:
    """Every catalog entry should point to a directory inside data/raw."""
    for data_source in get_data_sources():
        assert data_source.raw_directory.parent == RAW_DATA_DIR


def test_sih_sus_is_marked_as_likely_feasible_for_automation() -> None:
    """SIH/SUS should be marked as likely feasible after conservative automation support."""
    assert get_data_source("sih_sus").automation_level == AutomationLevel.LIKELY_FEASIBLE
