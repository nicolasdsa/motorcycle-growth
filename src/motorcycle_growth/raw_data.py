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
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from motorcycle_growth.config import RAW_DATA_DIR
from motorcycle_growth.data_catalog import DataSourceMetadata, get_data_source


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
SIM_DATASET_URL = "https://dadosabertos.saude.gov.br/dataset/sim"


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


class DiscoverableDownloadHandler(RawSourceHandler):
    """Handler for sources that require light discovery before download."""

    def __init__(
        self,
        metadata: DataSourceMetadata,
        requirements: tuple[RawFileRequirement, ...],
        discovery_function: DiscoveryFunction,
    ) -> None:
        super().__init__(metadata, requirements)
        self.discovery_function = discovery_function

    def run(
        self,
        *,
        download_enabled: bool,
        client: HttpClient,
        options: AcquisitionOptions,
    ) -> list[AcquisitionRecord]:
        records: list[AcquisitionRecord] = []

        for requirement in self.requirements:
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


def extract_next_data_json(html_text: str) -> dict[str, Any]:
    """Extract the Next.js payload from an official HTML page."""
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html_text,
        flags=re.DOTALL,
    )
    if match is None:
        raise DiscoveryError("Could not locate __NEXT_DATA__ in the official page.")

    return json.loads(html.unescape(match.group(1)))


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


def discover_sim_asset(
    requirement: RawFileRequirement,
    options: AcquisitionOptions,
    client: HttpClient,
) -> DiscoveredAsset:
    """Discover one official SIM resource from the OpenDataSUS dataset page."""
    html_text = client.get_text(
        SIM_DATASET_URL,
        expected_substrings=("Mortalidade Geral", "__NEXT_DATA__"),
    )
    resource = select_sim_resource(
        html_text,
        preferred_year=options.sim_year,
        preferred_format=options.sim_format,
    )

    note = (
        f"Discovered from the official SIM dataset page in format "
        f"{resource['format']}."
    )
    if "prévio" in resource["name"].lower() or "previo" in resource["name"].lower():
        note += " The selected official resource is marked as preliminary."

    return DiscoveredAsset(
        download_url=resource["url"],
        filename=infer_filename_from_url(resource["url"]),
        source_reference=requirement.source_reference,
        notes=note,
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

    uf = options.sih_uf.upper()
    payload = build_datasus_transfer_payload(
        file_type="RD",
        modality="1",
        source="SIHSUS",
        year=options.sih_year,
        month=options.sih_month,
        uf=uf,
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
            f"SIH/SUS competence {options.sih_year}-{options.sih_month:02d} and UF {uf}"
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


def select_sim_resource(
    html_text: str,
    *,
    preferred_year: int | None,
    preferred_format: str,
) -> dict[str, str]:
    """Select one SIM resource from the OpenDataSUS dataset page."""
    next_data = extract_next_data_json(html_text)
    resources = extract_sim_resources_from_next_data(next_data)
    format_upper = preferred_format.upper()

    candidates: list[dict[str, str]] = []
    for resource in resources:
        name = str(resource.get("name", ""))
        url = str(resource.get("url", ""))
        resource_format = str(resource.get("format", "")).upper()
        year = extract_year_from_text(name) or extract_year_from_text(url)
        if not url or resource_format != format_upper or year is None:
            continue
        candidates.append(
            {
                "name": name,
                "url": url,
                "format": resource_format,
                "year": str(year),
            }
        )

    if preferred_year is not None:
        exact_matches = [
            resource
            for resource in candidates
            if int(resource["year"]) == preferred_year
        ]
        if not exact_matches:
            raise DiscoveryError(
                f"Could not find a SIM resource for year {preferred_year} in format {format_upper}."
            )
        exact_matches.sort(key=lambda item: is_preview_resource(item["name"]))
        return exact_matches[0]

    non_preview = [
        resource
        for resource in candidates
        if not is_preview_resource(resource["name"])
    ]
    target_pool = non_preview or candidates
    if not target_pool:
        raise DiscoveryError(
            f"Could not find any SIM resource in format {format_upper} on the dataset page."
        )

    target_pool.sort(key=lambda item: int(item["year"]), reverse=True)
    return target_pool[0]


def extract_sim_resources_from_next_data(next_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract SIM resources from supported OpenDataSUS page payload shapes."""
    page_props = next_data.get("props", {}).get("pageProps", {})
    if not isinstance(page_props, dict):
        raise DiscoveryError("OpenDataSUS pageProps payload has an unexpected format.")

    direct_resources = page_props.get("resources")
    if isinstance(direct_resources, list):
        return [
            resource
            for resource in direct_resources
            if isinstance(resource, dict)
        ]

    package_payload = page_props.get("package")
    if isinstance(package_payload, dict) and isinstance(
        package_payload.get("resources"),
        list,
    ):
        return [
            resource
            for resource in package_payload["resources"]
            if isinstance(resource, dict)
        ]

    raise DiscoveryError(
        "Could not find a SIM resources list in the OpenDataSUS page payload."
    )


def extension_priority(extension: str) -> int:
    """Return a preference order for spreadsheet extensions."""
    return {"xls": 0, "ods": 1}.get(extension.lower(), 99)


def extract_year_from_text(value: str) -> int | None:
    """Extract the first four-digit year from a string."""
    match = re.search(r"(19|20)\d{2}", value)
    if match is None:
        return None
    return int(match.group(0))


def is_preview_resource(name: str) -> bool:
    """Return whether a dataset resource appears to be preliminary."""
    lowered = name.lower()
    return "prévio" in lowered or "previo" in lowered or "prev" in lowered


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
        ),
        DiscoverableDownloadHandler(
            get_data_source("sim_mortality"),
            requirements=(
                RawFileRequirement(
                    requirement_id="mortality_extracts",
                    display_name="SIM mortality extracts",
                    description=(
                        "Official SIM mortality files from the public OpenDataSUS dataset."
                    ),
                    filename_patterns=(
                        "DO*OPEN_csv.zip",
                        "DO*OPEN_xml.zip",
                        "Mortalidade_Geral_*_csv.zip",
                        "Mortalidade_Geral_*_xml.zip",
                    ),
                    source_reference=SIM_DATASET_URL,
                    local_directory=get_data_source("sim_mortality").raw_directory,
                    notes=(
                        "Discovery parses the public dataset page and selects the "
                        "requested CSV/XML annual resource."
                    ),
                ),
            ),
            discovery_function=discover_sim_asset,
        ),
        StaticDownloadHandler(
            get_data_source("cnes_establishments"),
            requirements=(
                RawFileRequirement(
                    requirement_id="cnes_establishments_csv_snapshot",
                    display_name="CNES establishments CSV snapshot",
                    description="Official CNES establishments CSV snapshot.",
                    filename_patterns=("cnes_estabelecimentos_csv.zip",),
                    source_reference=(
                        "https://dadosabertos.saude.gov.br/dataset/"
                        "cnes-cadastro-nacional-de-estabelecimentos-de-saude"
                    ),
                    local_directory=get_data_source("cnes_establishments").raw_directory,
                    download_url=(
                        "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/CNES/"
                        "cnes_estabelecimentos_csv.zip"
                    ),
                    notes=(
                        "Verified from the official resource page on March 29, 2026."
                    ),
                ),
            ),
        ),
        StaticDownloadHandler(
            get_data_source("cnes_hospital_beds"),
            requirements=(
                RawFileRequirement(
                    requirement_id="hospital_beds_csv_2026",
                    display_name="CNES hospital beds 2026 CSV snapshot",
                    description=(
                        "Official 2026 hospital beds CSV snapshot from the Hospitais e "
                        "Leitos dataset."
                    ),
                    filename_patterns=("Leitos_csv_2026.zip",),
                    source_reference="https://dadosabertos.saude.gov.br/dataset/hospitais-e-leitos",
                    local_directory=get_data_source("cnes_hospital_beds").raw_directory,
                    download_url=(
                        "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/Leitos_SUS/"
                        "Leitos_csv_2026.zip"
                    ),
                    notes=(
                        "This is an explicit yearly asset, so the config should be "
                        "revisited when the reference year changes."
                    ),
                ),
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
