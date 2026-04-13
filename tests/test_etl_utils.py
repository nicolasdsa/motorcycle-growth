"""Tests for shared ETL helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from motorcycle_growth.etl_utils import (
    assert_mask_empty,
    assert_no_duplicate_keys,
    build_normalized_column_map,
    clean_numeric_code,
    find_column_by_aliases,
    normalize_co_ibge_like_code,
    normalize_label,
    normalize_lookup_text,
    resolve_output_path,
)


def test_normalize_label_removes_accents_and_extra_spaces() -> None:
    """Normalized labels should be stable for schema matching."""
    assert normalize_label(" População\nEstimada ") == "POPULACAO ESTIMADA"


def test_normalize_lookup_text_builds_join_safe_key() -> None:
    """Lookup normalization should remove punctuation while preserving meaning."""
    assert normalize_lookup_text("São Miguel d'Oeste") == "SAO MIGUEL D OESTE"


def test_clean_numeric_code_zero_pads_codes() -> None:
    """Numeric code normalization should keep only digits and apply zero padding."""
    assert clean_numeric_code("50308.0", width=5) == "50308"
    assert clean_numeric_code(35, width=2) == "35"


def test_normalize_co_ibge_like_code_right_pads_short_values() -> None:
    """CO_IBGE-like codes should remain strings and reach seven digits."""
    assert normalize_co_ibge_like_code("3550308") == "3550308"
    assert normalize_co_ibge_like_code(110015) == "1100150"
    assert normalize_co_ibge_like_code("110015.0") == "1100150"


def test_build_normalized_column_map_and_find_column_by_aliases() -> None:
    """Schema helpers should support exact aliases and prefix matching."""
    column_map = build_normalized_column_map(["COD. UF", "POPULAÇÃO ESTIMADA 2025"])

    assert find_column_by_aliases(column_map, "COD UF") == "COD. UF"
    assert (
        find_column_by_aliases(column_map, "POPULACAO", allow_prefix="POPULA")
        == "POPULAÇÃO ESTIMADA 2025"
    )


def test_resolve_output_path_returns_default_or_explicit_path(tmp_path: Path) -> None:
    """Output-path resolution should preserve the default when no override is given."""
    default_path = tmp_path / "default.parquet"
    explicit_path = tmp_path / "custom.parquet"

    assert resolve_output_path(output_path=None, default_path=default_path) == default_path
    assert resolve_output_path(output_path=explicit_path, default_path=default_path) == explicit_path.resolve()


def test_assert_mask_empty_and_duplicate_keys_raise_counts() -> None:
    """Validation helpers should raise with the counted number of invalid rows."""
    with pytest.raises(ValueError, match="invalid rows: 2"):
        assert_mask_empty(
            pd.Series([True, False, True]),
            error_cls=ValueError,
            message="invalid rows",
        )

    with pytest.raises(ValueError, match="duplicate keys: 2"):
        assert_no_duplicate_keys(
            pd.DataFrame({"CO_IBGE": ["1", "1"], "year": [2025, 2025]}),
            subset=["CO_IBGE", "year"],
            error_cls=ValueError,
            message="duplicate keys",
        )
