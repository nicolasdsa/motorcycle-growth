"""Text and code normalization helpers shared across ETL modules."""

from __future__ import annotations

import re
import unicodedata

import pandas as pd


def normalize_label(label: object) -> str:
    """Return a normalized label for robust matching."""
    text = str(label).strip().replace("\n", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.upper()


def normalize_lookup_text(value: object) -> str:
    """Build a stable lookup key for text-based joins."""
    text = normalize_label(value)
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_numeric_code(value: object, *, width: int) -> str | None:
    """Normalize numeric codes stored as text or spreadsheet values."""
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    integer_like_match = re.fullmatch(r"(?P<digits>\d+)\.0+", text)
    if integer_like_match is not None:
        digits_only = integer_like_match.group("digits")
    else:
        digits_only = re.sub(r"\D", "", text)

    if not digits_only:
        return None

    if len(digits_only) > width:
        digits_only = digits_only[-width:]

    return digits_only.zfill(width)
