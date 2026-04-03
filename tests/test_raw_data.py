"""Tests for the raw data ingestion module."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from urllib.error import URLError

from motorcycle_growth.data_catalog import get_data_source
from motorcycle_growth.raw_data import (
    AcquisitionOptions,
    AcquisitionStatus,
    ConfigurationError,
    DiscoverableDownloadHandler,
    DiscoveredAsset,
    HttpClient,
    ManualSourceHandler,
    RawFileRequirement,
    build_summary,
    build_datasus_transfer_payload,
    discover_sih_asset,
    extract_next_data_json,
    extract_sim_resources_from_next_data,
    select_ibge_population_url,
    select_senatran_municipality_url,
    select_senatran_year_url,
    select_sim_resource,
)


class FakeClient:
    """Small fake client for handler tests."""

    def __init__(self, downloaded_path: Path | None = None) -> None:
        self.downloaded_path = downloaded_path
        self.download_calls: list[tuple[str, Path, str | None]] = []

    def download(
        self,
        *,
        download_url: str,
        output_directory: Path,
        filename: str | None = None,
    ) -> Path:
        self.download_calls.append((download_url, output_directory, filename))
        if self.downloaded_path is None:
            msg = "downloaded_path must be configured in FakeClient"
            raise AssertionError(msg)

        output_directory.mkdir(parents=True, exist_ok=True)
        target_path = output_directory / (filename or self.downloaded_path.name)
        target_path.write_bytes(b"PK\x03\x04fake")
        return target_path

    def post_text(self, *args, **kwargs) -> str:
        del args
        del kwargs
        msg = "post_text was not expected in this fake client"
        raise AssertionError(msg)


class FakeFtpResponse:
    """Minimal file-like response for FTP download tests."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.cursor = 0

    def __enter__(self) -> "FakeFtpResponse":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        del exc_type
        del exc_value
        del traceback

    def read(self, chunk_size: int) -> bytes:
        chunk = self.payload[self.cursor : self.cursor + chunk_size]
        self.cursor += len(chunk)
        return chunk


def test_manual_source_handler_reports_missing_file_details(tmp_path: Path) -> None:
    """Manual sources should report where missing files must be placed."""
    metadata = replace(
        get_data_source("ibge_population"),
        raw_directory=tmp_path / "ibge_population",
    )
    requirement = RawFileRequirement(
        requirement_id="population_estimates",
        display_name="IBGE municipal population tables",
        description="Annual municipality population tables for the study period.",
        filename_patterns=("*.csv", "*.xlsx"),
        source_reference="https://www.ibge.gov.br/",
        local_directory=metadata.raw_directory,
    )
    handler = ManualSourceHandler(metadata, requirements=(requirement,))

    records = handler.run(
        download_enabled=False,
        client=FakeClient(),
        options=AcquisitionOptions(),
    )

    assert len(records) == 1
    assert records[0].status == AcquisitionStatus.MISSING
    assert "Missing file: IBGE municipal population tables." in records[0].message
    assert str(metadata.raw_directory) in records[0].message


def test_select_ibge_population_url_prefers_updated_xls() -> None:
    """IBGE discovery should prefer the updated XLS asset over the DOU fallback."""
    html = """
    <a href="https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2024/estimativa_dou_2024.xls">XLS</a>
    <a href="https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2025/POP2025_20260113.ods">ODS</a>
    <a href="https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2025/POP2025_20260113.xls">XLS</a>
    """

    selected = select_ibge_population_url(html, preferred_year=None)

    assert selected.endswith("POP2025_20260113.xls")


def test_select_senatran_urls_choose_requested_year_and_municipality_file() -> None:
    """SENATRAN discovery should select the requested year and first municipality sheet."""
    index_html = """
    <a href="https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-2025">2025</a>
    <a href="https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-2026">2026</a>
    """
    year_html = """
    <a href="https://www.gov.br/transportes/file/frotapormunicipioetipofevereiro2026.xlsx">Frota por Município e Tipo</a>
    <a href="https://www.gov.br/transportes/file/frota-por-municipio-e-tipo-janeiro-2026.xlsx">Frota por Município e Tipo</a>
    """

    year_url = select_senatran_year_url(index_html, preferred_year=2026)
    municipality_url = select_senatran_municipality_url(year_html)

    assert year_url.endswith("frota-de-veiculos-2026")
    assert municipality_url.endswith("frotapormunicipioetipofevereiro2026.xlsx")


def test_extract_next_data_json_and_select_sim_resource() -> None:
    """SIM discovery should parse __NEXT_DATA__ and choose the latest non-preview CSV."""
    html = """
    <script id="__NEXT_DATA__" type="application/json">
    {
      "props": {
        "pageProps": {
          "package": {
            "resources": [
              {"name": "Mortalidade Geral 2023", "format": "CSV", "url": "https://example.org/Mortalidade_Geral_2023_csv.zip"},
              {"name": "Mortalidade Geral 2024", "format": "CSV", "url": "https://example.org/Mortalidade_Geral_2024_csv.zip"},
              {"name": "Mortalidade Geral 2025 - prévios", "format": "CSV", "url": "https://example.org/Mortalidade_Geral_2025_csv.zip"}
            ]
          }
        }
      }
    }
    </script>
    """

    next_data = extract_next_data_json(html)
    selected = select_sim_resource(
        html,
        preferred_year=None,
        preferred_format="csv",
    )

    assert "resources" in next_data["props"]["pageProps"]["package"]
    assert selected["url"].endswith("Mortalidade_Geral_2024_csv.zip")


def test_extract_sim_resources_from_direct_page_props_payload() -> None:
    """SIM parser should support the current payload shape where pageProps is the package."""
    next_data = {
        "props": {
            "pageProps": {
                "name": "sim",
                "resources": [
                    {
                        "name": "Mortalidade Geral 2024",
                        "format": "CSV",
                        "url": "https://example.org/Mortalidade_Geral_2024_csv.zip",
                    }
                ],
            }
        }
    }

    resources = extract_sim_resources_from_next_data(next_data)

    assert resources[0]["url"].endswith("Mortalidade_Geral_2024_csv.zip")


def test_discover_sih_asset_requires_parameters() -> None:
    """SIH discovery should require year, month, and UF to avoid broad downloads."""
    requirement = RawFileRequirement(
        requirement_id="hospitalization_extracts",
        display_name="SIH/SUS hospitalization extracts",
        description="Official SIH/SUS RD files for one UF and competence.",
        filename_patterns=("RD*.dbc",),
        source_reference="https://datasus.saude.gov.br/transferencia-de-arquivos",
        local_directory=Path("/tmp/sih_sus"),
    )

    try:
        discover_sih_asset(requirement, AcquisitionOptions(), FakeClient())
    except ConfigurationError as exc:
        assert "--sih-year" in str(exc)
    else:
        raise AssertionError("ConfigurationError was expected")


def test_build_datasus_transfer_payload_uses_array_style_field_names() -> None:
    """DATASUS payload should match the multiple-select format used by the public page."""
    payload = build_datasus_transfer_payload(
        file_type="RD",
        modality="1",
        source="SIHSUS",
        year=2025,
        month=1,
        uf="SP",
    )

    assert payload == [
        ("tipo_arquivo[]", "RD"),
        ("modalidade[]", "1"),
        ("fonte[]", "SIHSUS"),
        ("ano[]", "2025"),
        ("mes[]", "01"),
        ("uf[]", "SP"),
    ]


def test_discoverable_handler_downloads_discovered_file(tmp_path: Path) -> None:
    """Discoverable sources should download the asset discovered from the official page."""
    raw_directory = tmp_path / "sim_mortality"
    metadata = replace(get_data_source("sim_mortality"), raw_directory=raw_directory)
    requirement = RawFileRequirement(
        requirement_id="mortality_extracts",
        display_name="SIM mortality extracts",
        description="Official SIM mortality files.",
        filename_patterns=("Mortalidade_Geral_*_csv.zip",),
        source_reference="https://dadosabertos.saude.gov.br/dataset/sim",
        local_directory=raw_directory,
    )

    def fake_discovery(
        requirement: RawFileRequirement,
        options: AcquisitionOptions,
        client: FakeClient,
    ) -> DiscoveredAsset:
        del requirement
        del options
        del client
        return DiscoveredAsset(
            download_url="https://example.org/Mortalidade_Geral_2024_csv.zip",
            filename="Mortalidade_Geral_2024_csv.zip",
            source_reference="https://dadosabertos.saude.gov.br/dataset/sim",
        )

    handler = DiscoverableDownloadHandler(
        metadata,
        requirements=(requirement,),
        discovery_function=fake_discovery,
    )
    client = FakeClient(downloaded_path=raw_directory / "Mortalidade_Geral_2024_csv.zip")

    records = handler.run(
        download_enabled=True,
        client=client,
        options=AcquisitionOptions(),
    )

    assert records[0].status == AcquisitionStatus.DOWNLOADED
    assert records[0].local_path == raw_directory / "Mortalidade_Geral_2024_csv.zip"
    assert client.download_calls[0][0] == "https://example.org/Mortalidade_Geral_2024_csv.zip"


def test_sim_requirement_accepts_do_open_filename(tmp_path: Path) -> None:
    """SIM presence checks should recognize the current DOxxOPEN filename pattern."""
    raw_directory = tmp_path / "sim_mortality"
    raw_directory.mkdir(parents=True)
    sim_file = raw_directory / "DO24OPEN_csv.zip"
    sim_file.write_bytes(b"PK\x03\x04fake")

    metadata = replace(get_data_source("sim_mortality"), raw_directory=raw_directory)
    requirement = RawFileRequirement(
        requirement_id="mortality_extracts",
        display_name="SIM mortality extracts",
        description="Official SIM mortality files.",
        filename_patterns=(
            "DO*OPEN_csv.zip",
            "DO*OPEN_xml.zip",
            "Mortalidade_Geral_*_csv.zip",
            "Mortalidade_Geral_*_xml.zip",
        ),
        source_reference="https://dadosabertos.saude.gov.br/dataset/sim",
        local_directory=raw_directory,
    )

    handler = ManualSourceHandler(metadata, requirements=(requirement,))
    records = handler.run(
        download_enabled=False,
        client=FakeClient(),
        options=AcquisitionOptions(),
    )

    assert records[0].status == AcquisitionStatus.PRESENT
    assert records[0].local_path == sim_file


def test_http_client_download_supports_ftp_urls(monkeypatch, tmp_path: Path) -> None:
    """FTP downloads should be handled without calling requests.get."""

    def fake_urlopen(download_url: str, timeout: int) -> FakeFtpResponse:
        assert download_url == (
            "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RDSP2501.dbc"
        )
        assert timeout == 300
        return FakeFtpResponse(b"dbc-bytes")

    def fake_get(*args, **kwargs):
        del args
        del kwargs
        raise AssertionError("requests.get should not be used for ftp:// URLs")

    monkeypatch.setattr("motorcycle_growth.raw_data.urlopen", fake_urlopen)

    client = HttpClient(cache_directory=tmp_path / ".cache", cache_ttl_hours=24)
    monkeypatch.setattr(client.session, "get", fake_get)

    downloaded_path = client.download(
        download_url=(
            "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RDSP2501.dbc"
        ),
        output_directory=tmp_path / "sih_sus",
        filename="RDSP2501.dbc",
    )

    assert downloaded_path.read_bytes() == b"dbc-bytes"


def test_build_summary_counts_statuses() -> None:
    """Summary should count each acquisition status correctly."""
    records = [
        replace(_build_record(AcquisitionStatus.PRESENT), requirement_id="a"),
        replace(_build_record(AcquisitionStatus.DOWNLOADED), requirement_id="b"),
        replace(_build_record(AcquisitionStatus.DOWNLOAD_AVAILABLE), requirement_id="c"),
        replace(_build_record(AcquisitionStatus.FAILED), requirement_id="d"),
        replace(
            _build_record(AcquisitionStatus.REQUIRES_CONFIGURATION),
            requirement_id="e",
        ),
    ]

    summary = build_summary(records)

    assert summary.total == 5
    assert summary.present == 1
    assert summary.downloaded == 1
    assert summary.download_available == 1
    assert summary.missing == 0
    assert summary.failed == 1
    assert summary.requires_configuration == 1
    assert summary.has_issues is True


def _build_record(status: AcquisitionStatus):
    from motorcycle_growth.raw_data import AcquisitionRecord

    return AcquisitionRecord(
        dataset_id="dataset",
        dataset_name="Dataset",
        institution="Institution",
        requirement_id="requirement",
        display_name="Display Name",
        status=status,
        message="message",
        local_directory=Path("/tmp"),
        local_path=None,
    )
