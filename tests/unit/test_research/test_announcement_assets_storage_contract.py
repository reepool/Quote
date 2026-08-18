from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pytest

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetService,
    ContentAddressedBlobStore,
)
from research.announcement_assets.models import (
    OfficialAnnouncement,
    OfficialAnnouncementAttachment,
)
from research.announcement_assets.path_segments import validate_path_segment
from research.announcement_assets.storage import MountIdentity, probe_mount_identity
from research.announcements import AnnouncementRetrievalResult

PDF_BYTES = b"%PDF-1.4\nstorage contract\n%%EOF\n"


def test_mount_probe_selects_latest_duplicate_mount_entry(tmp_path, monkeypatch):
    mount_point = str(tmp_path.resolve())
    mountinfo = (
        f"100 1 0:1 / {mount_point} ro - nfs4 old-nfs:/archive ro\n"
        f"101 1 0:1 / {mount_point} rw - nfs4 current-nfs:/archive rw\n"
    )
    original_open = Path.open

    def fake_open(path, *args, **kwargs):
        if path == Path("/proc/self/mountinfo"):
            return io.StringIO(mountinfo)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fake_open)

    identity = probe_mount_identity(tmp_path / "missing-child")

    assert identity.source == "current-nfs:/archive"
    assert identity.read_write is True


def _config(tmp_path: Path) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "storage": {
                "warning_utilization": 0.98,
                "hard_stop_utilization": 0.999,
                "free_space_reserve_bytes": 1,
                "max_attachment_bytes": 1024 * 1024,
                "unknown_length_reservation_bytes": 4096,
            },
        },
        project_root=tmp_path,
    )


def _identities(config: AnnouncementAssetConfig) -> tuple[MountIdentity, MountIdentity]:
    return (
        MountIdentity(
            requested_path=config.filings_root,
            mount_point=config.filings_root,
            source="nfs.example:/filings",
            fs_type="nfs4",
            device_id=10,
        ),
        MountIdentity(
            requested_path=config.filings_root,
            mount_point=config.filings_root,
            source="local-fallback",
            fs_type="ext4",
            device_id=11,
        ),
    )


def test_quarantine_move_revalidates_mount_at_mutation_boundary(tmp_path, monkeypatch):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    published = store.publish_bytes(PDF_BYTES)
    stable, changed = _identities(config)
    identities = iter((stable, changed))
    monkeypatch.setattr(
        "research.announcement_assets.storage.probe_mount_identity",
        lambda _: next(identities),
    )

    with pytest.raises(RuntimeError, match="identity changed"):
        store.quarantine_blob(published.content_hash, reason="hash_mismatch")

    assert published.path.is_file()
    assert list(config.quarantine_root.rglob("*.pdf")) == []


def test_unlink_revalidates_mount_at_mutation_boundary(tmp_path, monkeypatch):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    published = store.publish_bytes(PDF_BYTES)
    stable, changed = _identities(config)
    identities = iter((stable, changed))
    monkeypatch.setattr(
        "research.announcement_assets.storage.probe_mount_identity",
        lambda _: next(identities),
    )

    with pytest.raises(RuntimeError, match="identity changed"):
        store.unlink_blob(published.content_hash)

    assert published.path.is_file()


def test_quarantine_rejects_path_like_dynamic_reason(tmp_path):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    published = store.publish_bytes(PDF_BYTES)

    with pytest.raises(ValueError, match="unsafe path segment"):
        store.quarantine_blob(published.content_hash, reason="../escape")

    assert published.path.is_file()


@pytest.mark.parametrize(
    "content_hash",
    [
        "A" * 64,
        "a" * 63,
        "a" * 63 + "/",
        "%2e%2e" + "a" * 58,
    ],
)
def test_blob_path_rejects_noncanonical_hash_segment(tmp_path, content_hash):
    store = ContentAddressedBlobStore(_config(tmp_path))

    with pytest.raises(
        ValueError,
        match="lowercase SHA-256|path separator|encoded traversal",
    ):
        store.blob_path(content_hash)


@pytest.mark.parametrize(
    "segment",
    ["", ".", "..", "../escape", "a/b", "a\\b", "%2e%2e", "bad\x00id", " has-space"],
)
def test_typed_identifier_segment_rejects_unsafe_values(segment):
    with pytest.raises(ValueError, match="path segment|path separator|encoded traversal"):
        validate_path_segment(
            segment,
            kind="identifier",
            field_name="operation_id",
        )


def test_quarantine_cleanup_keeps_evidence_when_mount_changes_before_unlink(
    tmp_path, monkeypatch
):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    published = store.publish_bytes(PDF_BYTES)
    quarantine = store.quarantine_blob(published.content_hash, reason="corrupt")
    stable, changed = _identities(config)
    # The first probe is operation start; the second is immediately before unlink.
    identities = iter((stable, changed))
    monkeypatch.setattr(
        "research.announcement_assets.storage.probe_mount_identity",
        lambda _: next(identities),
    )
    events: list[dict[str, object]] = []

    cleaned = store.cleanup_quarantine(
        authorized=True,
        actor="operator",
        audit=lambda event: events.append(dict(event)),
        older_than_seconds=0,
    )

    assert cleaned == 0
    assert quarantine.is_file()
    assert [event["status"] for event in events] == ["planned", "failed"]


def test_quarantine_cleanup_recovers_crash_after_unlink(tmp_path):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    published = store.publish_bytes(PDF_BYTES)
    quarantine = store.quarantine_blob(published.content_hash, reason="corrupt")
    sidecar = Path(f"{quarantine}.json")
    events: list[dict[str, object]] = []

    with pytest.raises(SystemExit, match="crash after quarantine unlink"):
        store.cleanup_quarantine(
            authorized=True,
            actor="operator",
            audit=lambda event: events.append(dict(event)),
            older_than_seconds=0,
            after_unlink=lambda _: (_ for _ in ()).throw(
                SystemExit("crash after quarantine unlink")
            ),
        )

    assert not quarantine.exists()
    assert sidecar.exists()
    recovered = store.cleanup_quarantine(
        authorized=True,
        actor="operator",
        audit=lambda event: events.append(dict(event)),
        older_than_seconds=0,
    )
    assert recovered == 1
    assert not sidecar.exists()
    assert [event["status"] for event in events] == ["planned", "deleted"]
    assert events[-1]["recovered_after_crash"] is True


def test_post_rename_reopen_hash_check_rejects_tampered_target(tmp_path, monkeypatch):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    original_replace = __import__("os").replace

    def replace_then_tamper(source, target):
        original_replace(source, target)
        Path(target).write_bytes(b"%PDF-1.4\ntampered\n%%EOF\n")

    monkeypatch.setattr("research.announcement_assets.storage.os.replace", replace_then_tamper)
    digest = hashlib.sha256(PDF_BYTES).hexdigest()

    with pytest.raises(RuntimeError, match="published blob validation failed"):
        store.publish_bytes(PDF_BYTES, expected_hash=digest)

    validation = store.validate_blob(
        store.blob_path(digest), expected_hash=digest, expected_length=len(PDF_BYTES)
    )
    assert validation.status.value in {"hash_mismatch", "size_mismatch"}


def test_service_uses_streamed_length_for_reservation_and_rejects_mismatch(tmp_path):
    config = _config(tmp_path)
    store = ContentAddressedBlobStore(config)
    store.prepare()
    content_hash = hashlib.sha256(PDF_BYTES).hexdigest()

    class MismatchRetriever:
        def retrieve(self, source, attachment, *, require_pdf=False):
            return AnnouncementRetrievalResult(
                source=source,
                attachment=attachment,
                status="success",
                content=PDF_BYTES,
                content_hash=content_hash,
                content_length=1,
                final_url=attachment.resolved_url or attachment.source_url,
                response_media_type="application/pdf",
            )

    service = AnnouncementAssetService(
        repository=object(),
        config=config,
        blob_store=store,
        attachment_retriever=MismatchRetriever(),
    )
    attachment = OfficialAnnouncementAttachment(
        attachment_id="attachment-1",
        announcement_id="announcement-1",
        attachment_identity="cninfo:attachment-1",
        source_attachment_id="attachment-1",
        source_url="https://static.example/report.pdf",
        normalized_source_url="https://static.example/report.pdf",
        name="report.pdf",
        media_type="application/pdf",
        content_length_hint=None,
        first_observed_at="2026-08-10T00:00:00+00:00",
        last_observed_at="2026-08-10T00:00:00+00:00",
        metadata={},
    )
    announcement = OfficialAnnouncement(
        announcement_id="announcement-1",
        source="cninfo",
        source_announcement_id="announcement-1",
        title="2025 annual report",
        instrument_id=None,
        exchange="SSE",
        published_at="2026-04-01T00:00:00+00:00",
        published_at_raw="2026-04-01",
        raw_payload_hash="payload-hash",
        first_observed_at="2026-08-10T00:00:00+00:00",
        last_observed_at="2026-08-10T00:00:00+00:00",
    )

    with pytest.raises(ValueError, match="content length"):
        service._retrieve_publish_and_recompute(
            attachment=attachment,
            announcement=announcement,
            attempt=1,
            reservation={"reservation_id": "reservation-1"},
            lease_owner="worker",
            lease_generation=1,
        )

    assert list(config.blob_root.rglob("*.pdf")) == []
