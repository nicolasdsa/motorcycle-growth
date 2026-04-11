"""IO helpers shared across ETL modules."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def resolve_output_path(*, output_path: Path | None, default_path: Path) -> Path:
    """Resolve one optional output path against a default location."""
    return output_path.expanduser().resolve() if output_path is not None else default_path


def save_parquet_frame(frame: pd.DataFrame, *, output_path: Path) -> Path:
    """Persist one dataframe as parquet."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output_path, index=False)
    return output_path


def save_json_payload(payload: dict[str, Any], *, output_path: Path) -> Path:
    """Persist one JSON payload with UTF-8 encoding."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return output_path
