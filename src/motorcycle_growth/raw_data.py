"""Raw data acquisition and validation helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import hashlib
import json
from enum import StrEnum
import html
from pathlib import Path
import re
from time import time
from typing import Any, Callable, Iterable
from urllib.parse import urlparse
from urllib.request import urlopen

import requests
import pandas as pd
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from motorcycle_growth.config import RAW_DATA_DIR
from motorcycle_growth.data_catalog import DataSourceMetadata, get_data_source
from motorcycle_growth.etl_utils import clean_numeric_code


DISCOVERY_CACHE_DIR = RAW_DATA_DIR / ".cache"
DATASUS_TRANSFER_URL = "https://datasus.saude.gov.br/wp-content/ftp.php"
IBGE_ESTIMATES_URL = (
    "https://www.ibge.gov.br/estatisticas/sociais/populacao/"
    "9103-estimativas-de-populacao.html"
)
SENATRAN_INDEX_URL = (
    "https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/"
    "estatisticas-frota-de-veiculos-senatran"
)
SIM_PANEL_PAGE_URL = (
    "https://svs.aids.gov.br/daent/centrais-de-conteudos/"
    "paineis-de-monitoramento/mortalidade/cid10/"
)
SIM_PANEL_API_URL = "https://svs.aids.gov.br/services2/v1/mortalidade/cid10/"
SIM_PANEL_MOTORCYCLE_INDICATOR_UID = 210188
SIM_PANEL_MONTH_COLUMNS = (
    ("jan", 1),
    ("fev", 2),
    ("mar", 3),
    ("abr", 4),
    ("mai", 5),
    ("jun", 6),
    ("jul", 7),
    ("ago", 8),
    ("set", 9),
    ("out", 10),
    ("nov", 11),
    ("dez", 12),
)


class AcquisitionStatus(StrEnum):
    """Statuses emitted by raw data acquisition checks."""

    PRESENT = "present"
    DOWNLOADED = "downloaded"
    DOWNLOAD_AVAILABLE = "download_available"
    MISSING = "missing"
    FAILED = "failed"
    REQUIRES_CONFIGURATION = "requires_configuration"


class DiscoveryError(RuntimeError):
    """Raised when a public discovery flow cannot find the expected asset."""


class ConfigurationError(RuntimeError):
    """Raised when a source requires explicit acquisition parameters."""


class ContentValidationError(RuntimeError):
    """Raised when a downloaded response does not look like the expected file."""


@dataclass(frozen=True)
class AcquisitionOptions:
    """Runtime options for raw data acquisition."""

    senatran_year: int | None = None
    ibge_year: int | None = None
    sim_year: int | None = None
    sim_format: str = "csv"
    sih_year: int | None = None
    sih_month: int | None = None
    sih_uf: str | None = None
    cnes_year: int | None = None
    cnes_month: int | None = None
    cnes_uf: str | None = None
    cache_ttl_hours: int = 24


@dataclass(frozen=True)
class RawFileRequirement:
    """One required raw file or file group for a source."""

    requirement_id: str
    display_name: str
    description: str
    filename_patterns: tuple[str, ...]
    source_reference: str
    local_directory: Path
    download_url: str | None = None
    notes: str | None = None


@dataclass(frozen=True)
class DiscoveredAsset:
    """A public asset discovered from an official page or endpoint."""

    download_url: str
    filename: str
    source_reference: str
    notes: str | None = None


@dataclass(frozen=True)
class PlannedAsset:
    """One raw asset that must be materialized locally from an official API."""

    filename: str
    source_reference: str
    notes: str | None = None


@dataclass(frozen=True)
class AcquisitionRecord:
    """Result of checking or acquiring one raw data requirement."""

    dataset_id: str
    dataset_name: str
    institution: str
    requirement_id: str
    display_name: str
    status: AcquisitionStatus
    message: str
    local_directory: Path
    local_path: Path | None = None


@dataclass(frozen=True)
class AcquisitionSummary:
    """Summary counts for a raw acquisition run."""

    total: int
    present: int
    downloaded: int
    download_available: int
    missing: int
    failed: int
    requires_configuration: int

    @property
    def has_issues(self) -> bool:
        """Return whether the run ended with unresolved inputs."""
        return any(
            (
                self.download_available > 0,
                self.missing > 0,
                self.failed > 0,
                self.requires_configuration > 0,
            )
        )


class HttpClient:
    """Minimal HTTP client with cache, retry, and content validation."""

    def __init__(
        self,
        *,
        cache_directory: Path,
        cache_ttl_hours: int,
        session: Session | None = None,
    ) -> None:
        self.cache_directory = cache_directory
        self.cache_ttl_seconds = cache_ttl_hours * 3600
        self.session = session or self._build_session()
        self.cache_directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _build_session() -> Session:
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD", "POST"}),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {
                "Accept-Encoding": "identity",
                "User-Agent": "motorcycle-growth/0.1.0 raw-data-ingestion",
            }
        )
        return session

    def get_text(
        self,
        url: str,
        *,
        cache_key: str | None = None,
        expected_substrings: tuple[str, ...] = (),
    ) -> str:
        cached = self._read_cache(cache_key or url)
        if cached is not None:
            self._validate_text_response(cached, expected_substrings)
            return cached

        response = self.session.get(url, timeout=(10, 120))
        response.raise_for_status()
        text = response.text
        self._validate_text_response(text, expected_substrings)
        self._write_cache(cache_key or url, text)
        return text

    def post_text(
        self,
        url: str,
        *,
        data: dict[str, str] | list[tuple[str, str]],
        cache_key: str | None = None,
        expected_substrings: tuple[str, ...] = (),
    ) -> str:
        effective_key = cache_key or self._request_cache_key(url, data)
        cached = self._read_cache(effective_key)
        if cached is not None:
            self._validate_text_response(cached, expected_substrings)
            return cached

        response = self.session.post(url, data=data, timeout=(10, 120))
        response.raise_for_status()
        text = response.text
        self._validate_text_response(text, expected_substrings)
        self._write_cache(effective_key, text)
        return text

    def download(
        self,
        *,
        download_url: str,
        output_directory: Path,
        filename: str | None = None,
    ) -> Path:
        output_directory.mkdir(parents=True, exist_ok=True)
        target_filename = filename or infer_filename_from_url(download_url)
        target_path = output_directory / target_filename
        temporary_path = target_path.with_suffix(f"{target_path.suffix}.part")

        if download_url.startswith("ftp://"):
            self._download_ftp_file(download_url, temporary_path)
        else:
            response = self.session.get(download_url, stream=True, timeout=(10, 300))
            response.raise_for_status()
            with temporary_path.open("wb") as output_file:
                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        output_file.write(chunk)

        validate_downloaded_file(temporary_path)
        temporary_path.replace(target_path)
        return target_path

    def _cache_path(self, cache_key: str) -> Path:
        digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
        return self.cache_directory / f"{digest}.txt"

    def _read_cache(self, cache_key: str) -> str | None:
        cache_path = self._cache_path(cache_key)
        if not cache_path.exists():
            return None

        age_seconds = time() - cache_path.stat().st_mtime
        if age_seconds > self.cache_ttl_seconds:
            return None

        return cache_path.read_text(encoding="utf-8")

    def _write_cache(self, cache_key: str, payload: str) -> None:
        self._cache_path(cache_key).write_text(payload, encoding="utf-8")

    @staticmethod
    def _request_cache_key(
        url: str,
        data: dict[str, str] | list[tuple[str, str]],
    ) -> str:
        """Build a stable cache key for POST discovery requests."""
        normalized_data = list(data.items()) if isinstance(data, dict) else list(data)
        return json.dumps(
            {"url": url, "data": normalized_data},
            sort_keys=True,
            ensure_ascii=True,
        )

    @staticmethod
    def _download_ftp_file(download_url: str, temporary_path: Path) -> None:
        """Download one official FTP file with a small retry loop."""
        last_error: Exception | None = None

        for attempt in range(1, 4):
            try:
                with urlopen(download_url, timeout=300) as response:
                    with temporary_path.open("wb") as output_file:
                        while True:
                            chunk = response.read(1024 * 64)
                            if not chunk:
                                break
                            output_file.write(chunk)
                return
            except OSError as exc:
                last_error = exc
                if temporary_path.exists():
                    temporary_path.unlink()
                if attempt == 3:
                    break

        raise ContentValidationError(
            f"Could not download FTP file after 3 attempts: {download_url}. "
            f"Last error: {last_error}"
        )

    @staticmethod
    def _validate_text_response(
        payload: str,
        expected_substrings: tuple[str, ...],
    ) -> None:
        if not payload.strip():
            raise DiscoveryError("Received an empty response from the official source.")

        if expected_substrings and not any(
            substring in payload for substring in expected_substrings
        ):
            raise DiscoveryError(
                "The official response did not contain the expected discovery markers."
            )


class RawSourceHandler(ABC):
    """Base class for one raw data source handler."""

    def __init__(
        self,
        metadata: DataSourceMetadata,
        requirements: tuple[RawFileRequirement, ...],
    ) -> None:
        self.metadata = metadata
        self.requirements = requirements

    @abstractmethod
    def run(
        self,
        *,
        download_enabled: bool,
        client: HttpClient,
        options: AcquisitionOptions,
    ) -> list[AcquisitionRecord]:
        """Run acquisition or validation for the source."""

    def _existing_matches(self, requirement: RawFileRequirement) -> list[Path]:
        matches: list[Path] = []

        for pattern in requirement.filename_patterns:
            matches.extend(requirement.local_directory.glob(pattern))

        return sorted({path for path in matches if path.is_file()})

    def _record(
        self,
        requirement: RawFileRequirement,
        status: AcquisitionStatus,
        message: str,
        *,
        local_path: Path | None = None,
    ) -> AcquisitionRecord:
        return AcquisitionRecord(
            dataset_id=self.metadata.dataset_id,
            dataset_name=self.metadata.name,
            institution=self.metadata.institution,
            requirement_id=requirement.requirement_id,
            display_name=requirement.display_name,
            status=status,
            message=message,
            local_directory=requirement.local_directory,
            local_path=local_path,
        )

    @staticmethod
    def _clean_text(value: str) -> str:
        """Normalize free-text fragments before composing log messages."""
        return value.strip().rstrip(".")

    @classmethod
    def _note_suffix(cls, notes: str | None) -> str:
        """Return an optional note fragment for log messages."""
        if not notes:
            return ""

        return f" Note: {cls._clean_text(notes)}."


class ManualSourceHandler(RawSourceHandler):
    """Handler for sources that currently rely on manual file placement."""

    def run(
        self,
        *,
        download_enabled: bool,
        client: HttpClient,
        options: AcquisitionOptions,
    ) -> list[AcquisitionRecord]:
        del download_enabled
        del client
        del options

        records: list[AcquisitionRecord] = []

        for requirement in self.requirements:
            existing_files = self._existing_matches(requirement)
            if existing_files:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.PRESENT,
                        (
                            "Manual raw input found. "
                            f"Matched {len(existing_files)} file(s) in "
                            f"{requirement.local_directory}."
                        ),
                        local_path=existing_files[0],
                    )
                )
                continue

            records.append(
                self._record(
                    requirement,
                    AcquisitionStatus.MISSING,
                    (
                        f"Missing file: {requirement.display_name}. "
                        f"Institution/source: {self.metadata.institution}. "
                        f"Expected content: {self._clean_text(requirement.description)}. "
                        f"Accepted filename patterns: {', '.join(requirement.filename_patterns)}. "
                        f"Official source: {requirement.source_reference}. "
                        f"Place the file under {requirement.local_directory}."
                        f"{self._note_suffix(requirement.notes)}"
                    ),
                )
            )

        return records


class StaticDownloadHandler(RawSourceHandler):
    """Handler for sources with a static official download URL."""

    def run(
        self,
        *,
        download_enabled: bool,
        client: HttpClient,
        options: AcquisitionOptions,
    ) -> list[AcquisitionRecord]:
        del options

        records: list[AcquisitionRecord] = []

        for requirement in self.requirements:
            existing_files = self._existing_matches(requirement)
            if existing_files:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.PRESENT,
                        (
                            "Downloadable raw input already present. "
                            f"Using {existing_files[0].name}."
                        ),
                        local_path=existing_files[0],
                    )
                )
                continue

            if requirement.download_url is None:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.FAILED,
                        (
                            f"Static download URL is not configured for "
                            f"{requirement.display_name}."
                        ),
                    )
                )
                continue

            if not download_enabled:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.DOWNLOAD_AVAILABLE,
                        (
                            f"Download available but not executed for {requirement.display_name}. "
                            f"Official URL: {requirement.download_url}. "
                            f"Target directory: {requirement.local_directory}. "
                            "Run the command without --check-only to download it."
                            f"{self._note_suffix(requirement.notes)}"
                        ),
                    )
                )
                continue

            try:
                downloaded_path = client.download(
                    download_url=requirement.download_url,
                    output_directory=requirement.local_directory,
                )
            except (requests.RequestException, ContentValidationError) as exc:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.FAILED,
                        (
                            f"Automatic download failed for {requirement.display_name}. "
                            f"Official URL: {requirement.download_url}. "
                            f"Target directory: {requirement.local_directory}. "
                            f"Error: {exc}."
                            f"{self._note_suffix(requirement.notes)}"
                        ),
                    )
                )
                continue

            records.append(
                self._record(
                    requirement,
                    AcquisitionStatus.DOWNLOADED,
                    (
                        f"Downloaded {downloaded_path.name} from {requirement.download_url} "
                        f"to {downloaded_path.parent}."
                    ),
                    local_path=downloaded_path,
                )
            )

        return records


DiscoveryFunction = Callable[
    [RawFileRequirement, AcquisitionOptions, HttpClient],
    DiscoveredAsset,
]
PlanFunction = Callable[
    [RawFileRequirement, AcquisitionOptions, HttpClient],
    PlannedAsset,
]
MaterializationFunction = Callable[
    [RawFileRequirement, PlannedAsset, AcquisitionOptions, HttpClient],
    Path,
]
OptionsPredicate = Callable[[AcquisitionOptions], bool]


class DiscoverableDownloadHandler(RawSourceHandler):
    """Handler for sources that require light discovery before download."""

    def __init__(
        self,
        metadata: DataSourceMetadata,
        requirements: tuple[RawFileRequirement, ...],
        discovery_function: DiscoveryFunction,
        parameter_options_present: OptionsPredicate | None = None,
    ) -> None:
        super().__init__(metadata, requirements)
        self.discovery_function = discovery_function
        self.parameter_options_present = parameter_options_present

    def run(
        self,
        *,
        download_enabled: bool,
        client: HttpClient,
        options: AcquisitionOptions,
    ) -> list[AcquisitionRecord]:
        records: list[AcquisitionRecord] = []

        for requirement in self.requirements:
            parameter_request = (
                self.parameter_options_present(options)
                if self.parameter_options_present is not None
                else False
            )
            if not parameter_request:
                existing_files = self._existing_matches(requirement)
                if existing_files:
                    records.append(
                        self._record(
                            requirement,
                            AcquisitionStatus.PRESENT,
                            (
                                "Raw input already present. "
                                f"Using {existing_files[0].name}."
                            ),
                            local_path=existing_files[0],
                        )
                    )
                    continue

            try:
                asset = self.discovery_function(requirement, options, client)
            except ConfigurationError as exc:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.REQUIRES_CONFIGURATION,
                        (
                            f"Automatic discovery for {requirement.display_name} "
                            f"requires additional input. {exc}. "
                            f"Official source: {requirement.source_reference}. "
                            f"You may also place the official file under "
                            f"{requirement.local_directory}."
                        ),
                    )
                )
                continue
            except (
                DiscoveryError,
                requests.RequestException,
                json.JSONDecodeError,
                ContentValidationError,
            ) as exc:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.FAILED,
                        (
                            f"Automatic discovery failed for {requirement.display_name}. "
                            f"Official source: {requirement.source_reference}. "
                            f"Target directory: {requirement.local_directory}. "
                            f"Error: {exc}. "
                            f"You may place the official file manually under "
                            f"{requirement.local_directory}."
                            f"{self._note_suffix(requirement.notes)}"
                        ),
                    )
                )
                continue

            exact_local_path = requirement.local_directory / asset.filename
            if exact_local_path.exists():
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.PRESENT,
                        (
                            "Raw input already present for the requested parameters. "
                            f"Using {exact_local_path.name}."
                        ),
                        local_path=exact_local_path,
                    )
                )
                continue

            if not download_enabled:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.DOWNLOAD_AVAILABLE,
                        (
                            f"Download available but not executed for {requirement.display_name}. "
                            f"Official URL: {asset.download_url}. "
                            f"Target filename: {asset.filename}. "
                            f"Target directory: {requirement.local_directory}. "
                            "Run the command without --check-only to download it."
                            f"{self._note_suffix(asset.notes or requirement.notes)}"
                        ),
                    )
                )
                continue

            try:
                downloaded_path = client.download(
                    download_url=asset.download_url,
                    output_directory=requirement.local_directory,
                    filename=asset.filename,
                )
            except (requests.RequestException, ContentValidationError) as exc:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.FAILED,
                        (
                            f"Automatic download failed for {requirement.display_name}. "
                            f"Official URL: {asset.download_url}. "
                            f"Target directory: {requirement.local_directory}. "
                            f"Error: {exc}."
                            f"{self._note_suffix(asset.notes or requirement.notes)}"
                        ),
                    )
                )
                continue

            records.append(
                self._record(
                    requirement,
                    AcquisitionStatus.DOWNLOADED,
                    (
                        f"Downloaded {downloaded_path.name} from {asset.download_url} "
                        f"to {downloaded_path.parent}."
                    ),
                    local_path=downloaded_path,
                )
            )

        return records


class ProgrammaticSourceHandler(RawSourceHandler):
    """Handler for raw sources that must be materialized from an official API."""

    def __init__(
        self,
        metadata: DataSourceMetadata,
        requirements: tuple[RawFileRequirement, ...],
        plan_function: PlanFunction,
        materialization_function: MaterializationFunction,
        parameter_options_present: OptionsPredicate | None = None,
    ) -> None:
        super().__init__(metadata, requirements)
        self.plan_function = plan_function
        self.materialization_function = materialization_function
        self.parameter_options_present = parameter_options_present

    def run(
        self,
        *,
        download_enabled: bool,
        client: HttpClient,
        options: AcquisitionOptions,
    ) -> list[AcquisitionRecord]:
        records: list[AcquisitionRecord] = []

        for requirement in self.requirements:
            parameter_request = (
                self.parameter_options_present(options)
                if self.parameter_options_present is not None
                else False
            )
            if not parameter_request:
                existing_files = self._existing_matches(requirement)
                if existing_files:
                    records.append(
                        self._record(
                            requirement,
                            AcquisitionStatus.PRESENT,
                            (
                                "Raw input already present. "
                                f"Using {existing_files[0].name}."
                            ),
                            local_path=existing_files[0],
                        )
                    )
                    continue

            try:
                asset = self.plan_function(requirement, options, client)
            except ConfigurationError as exc:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.REQUIRES_CONFIGURATION,
                        (
                            f"Automatic acquisition for {requirement.display_name} "
                            f"requires additional input. {exc}. "
                            f"Official source: {requirement.source_reference}. "
                            f"You may also place the official file under "
                            f"{requirement.local_directory}."
                        ),
                    )
                )
                continue
            except (
                DiscoveryError,
                requests.RequestException,
                json.JSONDecodeError,
                ContentValidationError,
            ) as exc:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.FAILED,
                        (
                            f"Automatic acquisition planning failed for {requirement.display_name}. "
                            f"Official source: {requirement.source_reference}. "
                            f"Target directory: {requirement.local_directory}. "
                            f"Error: {exc}. "
                            f"You may place the official file manually under "
                            f"{requirement.local_directory}."
                            f"{self._note_suffix(asset.notes if 'asset' in locals() else requirement.notes)}"
                        ),
                    )
                )
                continue

            exact_local_path = requirement.local_directory / asset.filename
            if exact_local_path.exists():
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.PRESENT,
                        (
                            "Raw input already present for the requested parameters. "
                            f"Using {exact_local_path.name}."
                        ),
                        local_path=exact_local_path,
                    )
                )
                continue

            if not download_enabled:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.DOWNLOAD_AVAILABLE,
                        (
                            f"Programmatic extraction is available for {requirement.display_name}. "
                            f"Official source: {asset.source_reference}. "
                            f"Target filename: {asset.filename}. "
                            f"Target directory: {requirement.local_directory}. "
                            "Run the command without --check-only to materialize it."
                            f"{self._note_suffix(asset.notes or requirement.notes)}"
                        ),
                    )
                )
                continue

            try:
                materialized_path = self.materialization_function(
                    requirement,
                    asset,
                    options,
                    client,
                )
            except (
                DiscoveryError,
                requests.RequestException,
                json.JSONDecodeError,
                ContentValidationError,
            ) as exc:
                records.append(
                    self._record(
                        requirement,
                        AcquisitionStatus.FAILED,
                        (
                            f"Programmatic extraction failed for {requirement.display_name}. "
                            f"Official source: {asset.source_reference}. "
                            f"Target directory: {requirement.local_directory}. "
                            f"Error: {exc}."
                            f"{self._note_suffix(asset.notes or requirement.notes)}"
                        ),
                    )
                )
                continue

            records.append(
                self._record(
                    requirement,
                    AcquisitionStatus.DOWNLOADED,
                    (
                        f"Materialized {materialized_path.name} from {asset.source_reference} "
                        f"to {materialized_path.parent}."
                    ),
                    local_path=materialized_path,
                )
            )

        return records


def infer_filename_from_url(download_url: str) -> str:
    """Infer a filename from a direct download URL."""
    filename = Path(urlparse(download_url).path).name
    if not filename:
        raise DiscoveryError(f"Could not infer a filename from URL: {download_url}")
    return filename


def validate_downloaded_file(file_path: Path) -> None:
    """Apply a lightweight validation to downloaded content."""
    if not file_path.exists():
        raise ContentValidationError(f"Downloaded file does not exist: {file_path}")

    header = file_path.read_bytes()[:512]
    lower_header = header.lower().lstrip()

    if not header:
        raise ContentValidationError(f"Downloaded file is empty: {file_path.name}")

    if lower_header.startswith(b"<!doctype html") or lower_header.startswith(b"<html"):
        raise ContentValidationError(
            f"Downloaded payload looks like HTML instead of {file_path.suffix}."
        )

    if file_path.suffix.lower() in {".zip", ".xlsx", ".ods"} and not header.startswith(b"PK"):
        raise ContentValidationError(
            f"Downloaded payload does not have the expected ZIP-based signature: {file_path.name}"
        )

    if file_path.suffix.lower() == ".xls" and header.startswith(b"<"):
        raise ContentValidationError(
            f"Downloaded payload looks like text/HTML instead of XLS: {file_path.name}"
        )


def request_json_payload(
    client: HttpClient,
    url: str,
    *,
    params: dict[str, str | int] | None = None,
) -> Any:
    """Request one JSON payload from an official HTTP endpoint."""
    response = client.session.get(url, params=params, timeout=(10, 120))
    response.raise_for_status()
    return response.json()


def select_sim_panel_year(
    year_payload: dict[str, Any],
    *,
    preferred_year: int | None,
) -> dict[str, Any]:
    """Select one year entry from the SIM panel year filter payload."""
    results = year_payload.get("resultados")
    if not isinstance(results, list):
        raise DiscoveryError("The SIM panel year endpoint returned an unexpected payload.")

    candidates = [
        item
        for item in results
        if isinstance(item, dict) and isinstance(item.get("uid"), int)
    ]
    if not candidates:
        raise DiscoveryError("The SIM panel year endpoint did not expose any year option.")

    if preferred_year is not None:
        for item in candidates:
            if item["uid"] == preferred_year:
                return item
        raise DiscoveryError(
            f"Could not find year {preferred_year} on the official SIM panel."
        )

    return max(candidates, key=lambda item: int(item["uid"]))


def build_sim_panel_extract_filename(year: int) -> str:
    """Return the standard raw filename for one SIM panel API extract."""
    return f"sim_panel_cid10_v20_v29_municipality_month_{year}.csv"


def build_sim_panel_export_params(
    *,
    year: int,
    uf_code: int,
    microrregiao_code: int,
) -> dict[str, str | int]:
    """Build the official SIM panel query for one microrregion export."""
    return {
        "ano": year,
        "local": 1,
        "indicador": SIM_PANEL_MOTORCYCLE_INDICATOR_UID,
        "categoria": 1,
        "estatistica": 1,
        "lococor": 0,
        "atestante": 0,
        "grupoetario": 2000,
        "racacor": 0,
        "sexo": 0,
        "uf": uf_code,
        "abrangencia": 5,
        "microrregiao": microrregiao_code,
        "espacial": "ibge",
        "parcial": "true",
    }


def build_sim_panel_monthly_rows(
    export_payload: dict[str, Any],
    *,
    year: int,
    uf_code: int,
    uf_name: str,
    microrregiao_code: int,
    microrregiao_name: str,
    year_is_preliminary: bool,
) -> list[dict[str, Any]]:
    """Convert one SIM panel municipality export payload into monthly raw rows."""
    results = export_payload.get("resultados")
    if not isinstance(results, list):
        raise DiscoveryError("The SIM municipality export payload has an unexpected format.")

    rows: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        abrangencia = item.get("abrangencia")
        if not isinstance(abrangencia, dict) or abrangencia.get("uid") != 8:
            continue

        municipality_code = clean_numeric_code(item.get("uid"), width=6)
        municipality_name = item.get("nome")
        if municipality_code is None or not isinstance(municipality_name, str):
            raise DiscoveryError(
                "The SIM municipality export did not expose a valid municipality identifier."
            )

        month_values = []
        for month_key, month_number in SIM_PANEL_MONTH_COLUMNS:
            month_value = pd.to_numeric(item.get(month_key), errors="coerce")
            if pd.isna(month_value):
                raise DiscoveryError(
                    f"The SIM municipality export is missing a valid value for month {month_key}."
                )
            month_count = int(month_value)
            month_values.append(month_count)
            rows.append(
                {
                    "year": year,
                    "month": month_number,
                    "municipality_code": municipality_code,
                    "municipality_name": municipality_name.strip(),
                    "uf_code": f"{uf_code:02d}",
                    "uf_name": uf_name.strip(),
                    "microrregiao_code": f"{microrregiao_code:05d}",
                    "microrregiao_name": microrregiao_name.strip(),
                    "motorcycle_deaths": month_count,
                    "year_is_preliminary": year_is_preliminary,
                    "source_data_label": str(export_payload.get("resumo", {}).get("data", "")),
                }
            )

        annual_value = pd.to_numeric(item.get("ano"), errors="coerce")
        if pd.isna(annual_value):
            raise DiscoveryError(
                "The SIM municipality export did not expose a valid annual total."
            )
        annual_total = int(annual_value)
        if annual_total != sum(month_values):
            raise DiscoveryError(
                "The SIM municipality export annual total does not match the monthly sum "
                f"for municipality {municipality_code}."
            )

    if not rows:
        raise DiscoveryError(
            "The SIM municipality export returned no municipality rows for the requested extract."
        )

    return rows


def discover_ibge_asset(
    requirement: RawFileRequirement,
    options: AcquisitionOptions,
    client: HttpClient,
) -> DiscoveredAsset:
    """Discover the latest official IBGE population spreadsheet."""
    html_text = client.get_text(
        IBGE_ESTIMATES_URL,
        expected_substrings=("ftp.ibge.gov.br/Estimativas_de_Populacao/",),
    )
    preferred_year = options.ibge_year
    discovered_url = select_ibge_population_url(html_text, preferred_year=preferred_year)

    return DiscoveredAsset(
        download_url=discovered_url,
        filename=infer_filename_from_url(discovered_url),
        source_reference=requirement.source_reference,
        notes=(
            f"Discovered from the official IBGE estimates page"
            f"{f' for year {preferred_year}' if preferred_year else ''}."
        ),
    )


def discover_senatran_asset(
    requirement: RawFileRequirement,
    options: AcquisitionOptions,
    client: HttpClient,
) -> DiscoveredAsset:
    """Discover the latest official SENATRAN municipality-type spreadsheet."""
    index_html = client.get_text(
        SENATRAN_INDEX_URL,
        expected_substrings=("frota-de-veiculos-",),
    )
    year_url = select_senatran_year_url(index_html, preferred_year=options.senatran_year)

    if year_url.lower().endswith(".zip"):
        discovered_url = year_url
    else:
        year_html = client.get_text(
            year_url,
            expected_substrings=("Frota por Município e Tipo", "frotapormunicipioetipo"),
        )
        discovered_url = select_senatran_municipality_url(year_html)

    return DiscoveredAsset(
        download_url=discovered_url,
        filename=infer_filename_from_url(discovered_url),
        source_reference=requirement.source_reference,
        notes=(
            "Discovered from the official SENATRAN fleet-statistics pages. "
            "The first municipality-type asset on the year page is used."
        ),
    )


def plan_sim_panel_asset(
    requirement: RawFileRequirement,
    options: AcquisitionOptions,
    client: HttpClient,
) -> PlannedAsset:
    """Plan one annual raw SIM extract built from the official panel API."""
    year_payload = request_json_payload(
        client,
        f"{SIM_PANEL_API_URL}filtro/ano",
    )
    year_option = select_sim_panel_year(
        year_payload,
        preferred_year=options.sim_year,
    )
    selected_year = int(year_option["uid"])
    label = str(year_option.get("nome", selected_year))

    note = (
        "Materialized from the official SVS/DAENT mortality panel API already "
        "filtered to CID-10 V20-V29."
    )
    if "*" in label:
        note += " The selected year is marked as preliminary on the official panel."

    return PlannedAsset(
        filename=build_sim_panel_extract_filename(selected_year),
        source_reference=requirement.source_reference,
        notes=note,
    )


def materialize_sim_panel_asset(
    requirement: RawFileRequirement,
    asset: PlannedAsset,
    options: AcquisitionOptions,
    client: HttpClient,
) -> Path:
    """Build one municipality-month SIM raw CSV from the official panel API."""
    year_payload = request_json_payload(client, f"{SIM_PANEL_API_URL}filtro/ano")
    year_option = select_sim_panel_year(
        year_payload,
        preferred_year=options.sim_year,
    )
    selected_year = int(year_option["uid"])
    year_is_preliminary = "*" in str(year_option.get("nome", ""))

    uf_payload = request_json_payload(client, f"{SIM_PANEL_API_URL}filtro/uf")
    uf_results = uf_payload.get("resultados")
    if not isinstance(uf_results, list) or not uf_results:
        raise DiscoveryError("The SIM panel API did not return any UF option.")

    rows: list[dict[str, Any]] = []
    for uf_item in uf_results:
        if not isinstance(uf_item, dict):
            continue

        uf_code = uf_item.get("uid")
        uf_name = uf_item.get("nome")
        if not isinstance(uf_code, int) or not isinstance(uf_name, str):
            raise DiscoveryError("The SIM panel UF payload has an unexpected format.")

        microrregiao_payload = request_json_payload(
            client,
            f"{SIM_PANEL_API_URL}filtro/microrregiao",
            params={"uf": uf_code},
        )
        microrregiao_results = microrregiao_payload.get("resultados")
        if not isinstance(microrregiao_results, list) or not microrregiao_results:
            raise DiscoveryError(
                f"The SIM panel did not expose microrregions for UF {uf_code}."
            )

        for microrregiao_item in microrregiao_results:
            if not isinstance(microrregiao_item, dict):
                continue

            microrregiao_code = microrregiao_item.get("uid")
            microrregiao_name = microrregiao_item.get("nome")
            if not isinstance(microrregiao_code, int) or not isinstance(
                microrregiao_name,
                str,
            ):
                raise DiscoveryError(
                    "The SIM panel microrregion payload has an unexpected format."
                )

            export_payload = request_json_payload(
                client,
                f"{SIM_PANEL_API_URL}exportar/localidade",
                params=build_sim_panel_export_params(
                    year=selected_year,
                    uf_code=uf_code,
                    microrregiao_code=microrregiao_code,
                ),
            )
            rows.extend(
                build_sim_panel_monthly_rows(
                    export_payload,
                    year=selected_year,
                    uf_code=uf_code,
                    uf_name=uf_name,
                    microrregiao_code=microrregiao_code,
                    microrregiao_name=microrregiao_name,
                    year_is_preliminary=year_is_preliminary,
                )
            )

    extract_frame = pd.DataFrame(rows).sort_values(
        ["year", "month", "municipality_code"],
        ignore_index=True,
    )
    requirement.local_directory.mkdir(parents=True, exist_ok=True)
    output_path = requirement.local_directory / asset.filename
    extract_frame.to_csv(output_path, index=False)
    return output_path


def discover_datasus_transfer_asset(
    *,
    requirement: RawFileRequirement,
    source: str,
    file_type: str,
    year: int | None,
    month: int | None,
    uf: str | None,
    client: HttpClient,
) -> DiscoveredAsset:
    """Discover one raw file through the official DATASUS transfer endpoint."""
    if year is None or month is None or uf is None:
        raise ConfigurationError(
            "Provide --year, --month, and --uf style parameters for this official transfer flow"
        )

    normalized_uf = uf.upper()
    payload = build_datasus_transfer_payload(
        file_type=file_type,
        modality="1",
        source=source,
        year=year,
        month=month,
        uf=normalized_uf,
    )
    response_text = client.post_text(
        DATASUS_TRANSFER_URL,
        data=payload,
        cache_key=json.dumps(payload, sort_keys=True),
        expected_substrings=("[", "{"),
    )
    files = json.loads(response_text)

    if not isinstance(files, list) or not files:
        raise DiscoveryError(
            "The DATASUS transfer endpoint did not return any file for the requested "
            f"{source} competence {year}-{month:02d} and UF {normalized_uf}"
        )

    first_file = files[0]
    if not isinstance(first_file, dict):
        raise DiscoveryError("The DATASUS transfer endpoint returned an unexpected payload.")

    download_url = first_file.get("endereco")
    filename = first_file.get("arquivo")

    if not isinstance(download_url, str) or not download_url:
        raise DiscoveryError("The DATASUS transfer payload did not expose a file URL.")
    if not isinstance(filename, str) or not filename:
        filename = infer_filename_from_url(download_url)

    return DiscoveredAsset(
        download_url=download_url,
        filename=filename,
        source_reference=requirement.source_reference,
        notes=(
            "Discovered from the official DATASUS transfer endpoint used by the public "
            "transfer page."
        ),
    )


def discover_sih_asset(
    requirement: RawFileRequirement,
    options: AcquisitionOptions,
    client: HttpClient,
) -> DiscoveredAsset:
    """Discover one official SIH/SUS raw file using the DATASUS transfer flow."""
    if options.sih_year is None or options.sih_month is None or options.sih_uf is None:
        raise ConfigurationError(
            "Provide --sih-year, --sih-month, and --sih-uf to request one official SIH/SUS file"
        )

    return discover_datasus_transfer_asset(
        requirement=requirement,
        source="SIHSUS",
        file_type="RD",
        year=options.sih_year,
        month=options.sih_month,
        uf=options.sih_uf,
        client=client,
    )


def discover_cnes_establishment_asset(
    requirement: RawFileRequirement,
    options: AcquisitionOptions,
    client: HttpClient,
) -> DiscoveredAsset:
    """Discover one official CNES establishment raw file via DATASUS transfer."""
    if options.cnes_year is None or options.cnes_month is None or options.cnes_uf is None:
        raise ConfigurationError(
            "Provide --cnes-year, --cnes-month, and --cnes-uf to request one official CNES establishment file"
        )

    return discover_datasus_transfer_asset(
        requirement=requirement,
        source="CNES",
        file_type="ST",
        year=options.cnes_year,
        month=options.cnes_month,
        uf=options.cnes_uf,
        client=client,
    )


def discover_cnes_bed_asset(
    requirement: RawFileRequirement,
    options: AcquisitionOptions,
    client: HttpClient,
) -> DiscoveredAsset:
    """Discover one official CNES bed raw file via DATASUS transfer."""
    if options.cnes_year is None or options.cnes_month is None or options.cnes_uf is None:
        raise ConfigurationError(
            "Provide --cnes-year, --cnes-month, and --cnes-uf to request one official CNES bed file"
        )

    return discover_datasus_transfer_asset(
        requirement=requirement,
        source="CNES",
        file_type="LT",
        year=options.cnes_year,
        month=options.cnes_month,
        uf=options.cnes_uf,
        client=client,
    )


def build_datasus_transfer_payload(
    *,
    file_type: str,
    modality: str,
    source: str,
    year: int,
    month: int,
    uf: str,
) -> list[tuple[str, str]]:
    """Build the array-style payload expected by DATASUS transfer selects."""
    return [
        ("tipo_arquivo[]", file_type),
        ("modalidade[]", modality),
        ("fonte[]", source),
        ("ano[]", str(year)),
        ("mes[]", f"{month:02d}"),
        ("uf[]", uf),
    ]


def select_ibge_population_url(html_text: str, *, preferred_year: int | None) -> str:
    """Select one official IBGE spreadsheet link from the public page HTML."""
    updated_matches = [
        (
            int(match.group("year")),
            match.group("url"),
            extension_priority(match.group("ext")),
        )
        for match in re.finditer(
            r'(?P<url>https://ftp\.ibge\.gov\.br/Estimativas_de_Populacao/'
            r'Estimativas_(?P<year>\d{4})/POP(?P=year)_\d+\.(?P<ext>xls|ods))',
            html_text,
            flags=re.IGNORECASE,
        )
    ]
    dou_matches = [
        (
            int(match.group("year")),
            match.group("url"),
            extension_priority(match.group("ext")),
        )
        for match in re.finditer(
            r'(?P<url>https://ftp\.ibge\.gov\.br/Estimativas_de_Populacao/'
            r'Estimativas_(?P<year>\d{4})/estimativa_dou_(?P=year)\.(?P<ext>xls|ods))',
            html_text,
            flags=re.IGNORECASE,
        )
    ]

    candidates = updated_matches or dou_matches
    if preferred_year is not None:
        candidates = [item for item in candidates if item[0] == preferred_year]
    if not candidates:
        raise DiscoveryError("Could not find an official IBGE XLS/ODS link on the page.")

    best_year = max(item[0] for item in candidates)
    best_candidates = [item for item in candidates if item[0] == best_year]
    best_candidates.sort(key=lambda item: item[2])
    return best_candidates[0][1]


def select_senatran_year_url(html_text: str, *, preferred_year: int | None) -> str:
    """Select the SENATRAN year page or direct file for the requested year."""
    normalized_matches: dict[int, str] = {}
    for match in re.finditer(
        r'href="(?P<url>https://www\.gov\.br/transportes/pt-br/assuntos/transito/'
        r'(?:conteudo-Senatran/frota-de-veiculos-(?P<year>\d{4})|arquivos-senatran/'
        r'estatisticas/renavam/(?P<legacy_year>\d{4})/[^"]+))"',
        html_text,
        flags=re.IGNORECASE,
    ):
        year_text = match.group("year") or match.group("legacy_year")
        if year_text is None:
            continue
        normalized_matches[int(year_text)] = html.unescape(match.group("url"))

    if preferred_year is not None:
        year_url = normalized_matches.get(preferred_year)
        if year_url is None:
            raise DiscoveryError(
                f"Could not find a SENATRAN entry for year {preferred_year}."
            )
        return year_url

    latest_year = max(normalized_matches)
    return normalized_matches[latest_year]


def select_senatran_municipality_url(html_text: str) -> str:
    """Select the first municipality-type spreadsheet from the year page."""
    patterns = (
        r'href="(?P<url>https://www\.gov\.br/transportes/[^"]*frotapormunicipioetipo[^"]+\.xlsx)"',
        r'href="(?P<url>https://www\.gov\.br/transportes/[^"]*frota-por-municipio-e-tipo[^"]+\.xlsx)"',
    )

    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE)
        if match is not None:
            return html.unescape(match.group("url"))

    raise DiscoveryError(
        "Could not find the 'Frota por Município e Tipo' spreadsheet on the SENATRAN year page."
    )


def extension_priority(extension: str) -> int:
    """Return a preference order for spreadsheet extensions."""
    return {"xls": 0, "ods": 1}.get(extension.lower(), 99)


def build_raw_source_handlers() -> tuple[RawSourceHandler, ...]:
    """Build the first-wave raw acquisition handlers."""
    return (
        DiscoverableDownloadHandler(
            get_data_source("senatran_motorcycle_fleet"),
            requirements=(
                RawFileRequirement(
                    requirement_id="motorcycle_fleet_tables",
                    display_name="SENATRAN motorcycle fleet tables",
                    description=(
                        "Municipality-level fleet tables containing motorcycle counts "
                        "for the study period."
                    ),
                    filename_patterns=(
                        "*municipio*tipo*.xlsx",
                        "frotapormunicipioetipo*.xlsx",
                        "frota-por-municipio-e-tipo*.xlsx",
                        "frota_20*.zip",
                    ),
                    source_reference=SENATRAN_INDEX_URL,
                    local_directory=get_data_source("senatran_motorcycle_fleet").raw_directory,
                    notes=(
                        "Discovery reads the public year index and then the selected "
                        "year page to locate the current municipality-type spreadsheet."
                    ),
                ),
            ),
            discovery_function=discover_senatran_asset,
            parameter_options_present=lambda options: options.senatran_year is not None,
        ),
        DiscoverableDownloadHandler(
            get_data_source("ibge_population"),
            requirements=(
                RawFileRequirement(
                    requirement_id="population_estimates",
                    display_name="IBGE municipal population tables",
                    description=(
                        "Annual municipality population tables for the study period."
                    ),
                    filename_patterns=(
                        "POP*.xls",
                        "POP*.ods",
                        "estimativa_dou_*.xls",
                        "estimativa_dou_*.ods",
                    ),
                    source_reference=IBGE_ESTIMATES_URL,
                    local_directory=get_data_source("ibge_population").raw_directory,
                    notes=(
                        "Discovery prefers the updated XLS/ODS table on the official "
                        "IBGE estimates page and falls back to the DOU table when needed."
                    ),
                ),
            ),
            discovery_function=discover_ibge_asset,
            parameter_options_present=lambda options: options.ibge_year is not None,
        ),
        DiscoverableDownloadHandler(
            get_data_source("sih_sus"),
            requirements=(
                RawFileRequirement(
                    requirement_id="hospitalization_extracts",
                    display_name="SIH/SUS hospitalization extracts",
                    description=(
                        "Official SIH/SUS RD files for one UF and competence."
                    ),
                    filename_patterns=("RD*.dbc", "RD*.dbf", "RD*.zip"),
                    source_reference="https://datasus.saude.gov.br/transferencia-de-arquivos",
                    local_directory=get_data_source("sih_sus").raw_directory,
                    notes=(
                        "This first automated version uses the same public DATASUS "
                        "transfer flow exposed on the official transfer page. Provide "
                        "--sih-year, --sih-month, and --sih-uf."
                    ),
                ),
            ),
            discovery_function=discover_sih_asset,
            parameter_options_present=lambda options: all(
                value is not None
                for value in (options.sih_year, options.sih_month, options.sih_uf)
            ),
        ),
        ProgrammaticSourceHandler(
            get_data_source("sim_mortality"),
            requirements=(
                RawFileRequirement(
                    requirement_id="mortality_panel_extracts",
                    display_name="SIM panel mortality extracts",
                    description=(
                        "Official municipality-month SIM extract generated from the "
                        "SVS/DAENT CID-10 mortality panel API already filtered to V20-V29."
                    ),
                    filename_patterns=(
                        "sim_panel_cid10_v20_v29_municipality_month_*.csv",
                    ),
                    source_reference=SIM_PANEL_PAGE_URL,
                    local_directory=get_data_source("sim_mortality").raw_directory,
                    notes=(
                        "The panel marks years with * as subject to change. The page "
                        "notes currently state that 2025 data were extracted in February 2026."
                    ),
                ),
            ),
            plan_function=plan_sim_panel_asset,
            materialization_function=materialize_sim_panel_asset,
            parameter_options_present=lambda options: options.sim_year is not None,
        ),
        DiscoverableDownloadHandler(
            get_data_source("cnes_establishments"),
            requirements=(
                RawFileRequirement(
                    requirement_id="cnes_establishment_extracts",
                    display_name="CNES establishment extracts",
                    description="Official CNES ST files for one UF and competence.",
                    filename_patterns=("ST*.dbc", "ST*.dbf", "ST*.zip"),
                    source_reference="https://datasus.saude.gov.br/transferencia-de-arquivos",
                    local_directory=get_data_source("cnes_establishments").raw_directory,
                    notes=(
                        "Uses the official DATASUS transfer flow for CNES/ST. Provide "
                        "--cnes-year, --cnes-month, and --cnes-uf."
                    ),
                ),
            ),
            discovery_function=discover_cnes_establishment_asset,
            parameter_options_present=lambda options: all(
                value is not None
                for value in (options.cnes_year, options.cnes_month, options.cnes_uf)
            ),
        ),
        DiscoverableDownloadHandler(
            get_data_source("cnes_hospital_beds"),
            requirements=(
                RawFileRequirement(
                    requirement_id="hospital_bed_extracts",
                    display_name="CNES hospital bed extracts",
                    description=(
                        "Official CNES LT files for one UF and competence."
                    ),
                    filename_patterns=("LT*.dbc", "LT*.dbf", "LT*.zip"),
                    source_reference="https://datasus.saude.gov.br/transferencia-de-arquivos",
                    local_directory=get_data_source("cnes_hospital_beds").raw_directory,
                    notes=(
                        "Uses the official DATASUS transfer flow for CNES/LT. Provide "
                        "--cnes-year, --cnes-month, and --cnes-uf."
                    ),
                ),
            ),
            discovery_function=discover_cnes_bed_asset,
            parameter_options_present=lambda options: all(
                value is not None
                for value in (options.cnes_year, options.cnes_month, options.cnes_uf)
            ),
        ),
    )


def run_raw_data_acquisition(
    *,
    download_enabled: bool,
    options: AcquisitionOptions,
) -> list[AcquisitionRecord]:
    """Run the raw data acquisition process."""
    records: list[AcquisitionRecord] = []
    client = HttpClient(
        cache_directory=DISCOVERY_CACHE_DIR,
        cache_ttl_hours=options.cache_ttl_hours,
    )

    for handler in build_raw_source_handlers():
        records.extend(
            handler.run(
                download_enabled=download_enabled,
                client=client,
                options=options,
            )
        )

    return records


def build_summary(records: Iterable[AcquisitionRecord]) -> AcquisitionSummary:
    """Build summary counts for a set of acquisition records."""
    record_list = list(records)

    return AcquisitionSummary(
        total=len(record_list),
        present=sum(item.status == AcquisitionStatus.PRESENT for item in record_list),
        downloaded=sum(item.status == AcquisitionStatus.DOWNLOADED for item in record_list),
        download_available=sum(
            item.status == AcquisitionStatus.DOWNLOAD_AVAILABLE for item in record_list
        ),
        missing=sum(item.status == AcquisitionStatus.MISSING for item in record_list),
        failed=sum(item.status == AcquisitionStatus.FAILED for item in record_list),
        requires_configuration=sum(
            item.status == AcquisitionStatus.REQUIRES_CONFIGURATION
            for item in record_list
        ),
    )
