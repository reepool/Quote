"""A failed re-download is not a newer report body at a knowledge cutoff."""
from dataclasses import replace

from research.announcement_assets.access import AnnouncementAssetAccess
from research.announcement_assets.models import IntegrityStatus
from research.announcement_assets.repository import AnnouncementAssetRepository
from research.announcement_assets.service import AnnouncementAssetService
from research.announcement_assets.storage import ContentAddressedBlobStore
from tests.unit.test_research.test_announcement_assets_access import _config, _record, _Retriever


def _source(tmp_path):
    config = _config(tmp_path)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(repository=repository, config=config,
        blob_store=store, attachment_retriever=_Retriever())
    attachment = service.register_discovered_record(_record(), instrument_id="600000.SH")[0]
    asset = service.acquire_attachment(attachment.attachment_id)
    version = repository.get_attachment_version(asset.version_id)
    access = AnnouncementAssetAccess(repository=repository, config=config, service=service)
    return repository, access, asset, version


def _failed(repository, version, *, content_hash=None):
    repository.add_attachment_version(replace(version, version_id="later-failed-download",
        observation_key="later-failed-download", content_hash=content_hash,
        retrieval_status="failed", integrity_status=IntegrityStatus.UNREADABLE,
        error_code="transport_failure", observed_at="2026-03-21T02:00:00+00:00",
        version_available_at="2026-03-21T02:00:00+00:00"))


def test_cutoff_keeps_prior_verified_body_after_failed_empty_redownload(tmp_path):
    repository, access, asset, version = _source(tmp_path)
    _failed(repository, version)
    current = access.get_effective_asset("600000.SH", fiscal_year=2025)
    historical = access.get_effective_asset("600000.SH", fiscal_year=2025, knowledge_cutoff="2026-03-22")
    assert current["content_hash"] == asset.content_hash
    assert historical is not None
    assert historical["observation_version"] == version.version_id
    assert historical["content_hash"] == asset.content_hash
    # Daily acquisition diagnostics must still expose the latest failed attempt.
    latest = repository.list_candidate_rows(instrument_id="600000.SH", fiscal_year=2025)
    assert latest[0]["version_id"] == "later-failed-download"
    assert latest[0]["retrieval_status"] == "failed"


def test_cutoff_does_not_use_verified_observation_from_the_future(tmp_path):
    repository, access, _, version = _source(tmp_path)
    _failed(repository, version)
    historical = access.get_effective_asset("600000.SH", fiscal_year=2025,
        knowledge_cutoff="2026-03-19")
    assert historical is None


def test_known_invalid_replacement_bytes_are_not_hidden_by_older_valid_body(tmp_path):
    repository, access, _, version = _source(tmp_path)
    old_blob = repository.get_blob(version.content_hash)
    repository.register_blob(replace(old_blob, content_hash="f" * 64,
        canonical_path=str(tmp_path / "invalid.pdf"), integrity_status=IntegrityStatus.UNREADABLE))
    _failed(repository, version, content_hash="f" * 64)
    historical = access.get_effective_asset("600000.SH", fiscal_year=2025,
        knowledge_cutoff="2026-03-22")
    assert historical is None or historical["availability"] != "local_valid"
    if historical is not None:
        assert historical["observation_version"] != version.version_id
