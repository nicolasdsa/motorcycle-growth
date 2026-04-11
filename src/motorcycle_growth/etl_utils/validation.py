"""Validation helpers shared across ETL modules."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def assert_mask_empty(
    mask: pd.Series,
    *,
    error_cls: type[Exception],
    message: str,
) -> int:
    """Raise when a boolean mask contains one or more true values."""
    count = int(mask.fillna(False).sum())
    if count > 0:
        raise error_cls(f"{message}: {count}")
    return count


def assert_no_duplicate_keys(
    frame: pd.DataFrame,
    *,
    subset: Sequence[str],
    error_cls: type[Exception],
    message: str,
) -> int:
    """Raise when a dataframe contains duplicate rows for a key subset."""
    duplicate_mask = frame.duplicated(subset=list(subset), keep=False)
    duplicate_count = int(duplicate_mask.sum())
    if duplicate_count > 0:
        raise error_cls(f"{message}: {duplicate_count}")
    return duplicate_count
