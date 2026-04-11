"""Schema helpers shared across ETL modules."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from motorcycle_growth.etl_utils.text import normalize_label


def build_normalized_column_map(columns: Iterable[object]) -> dict[str, str]:
    """Map normalized column labels to their original names."""
    return {
        normalize_label(column_name): str(column_name)
        for column_name in columns
    }


def find_column_by_aliases(
    normalized_to_original: Mapping[str, str],
    *aliases: str,
    allow_prefix: str | None = None,
) -> str | None:
    """Find one source column by exact aliases or by a normalized prefix."""
    for alias in aliases:
        match = normalized_to_original.get(normalize_label(alias))
        if match is not None:
            return match

    if allow_prefix is not None:
        normalized_prefix = normalize_label(allow_prefix)
        for normalized_name, original_name in normalized_to_original.items():
            if normalized_name.startswith(normalized_prefix):
                return original_name

    return None
