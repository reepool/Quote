from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from research.announcement_assets.repository import AnnouncementAssetRepository


def register_shared_annual_report(
    db_path: Path,
    pdf_path: Path,
    *,
    asset_id: str,
    instrument_id: str,
    report_period: str,
    variant: str = "original",
    source: str = "cninfo",
    source_announcement_id: str | None = None,
    published_at: str = "2026-03-31T00:00:00+08:00",
    availability: str = "local_valid",
    integrity_status: str = "valid",
) -> str:
    """Register the minimum authoritative shared annual-report projection."""

    repository = AnnouncementAssetRepository(db_path)
    with sqlite3.connect(db_path) as existing_conn:
        schema_ready = existing_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='effective_annual_reports'"
        ).fetchone()
    if schema_ready is None:
        repository.initialize_schema()
    content = pdf_path.read_bytes()
    content_hash = hashlib.sha256(content).hexdigest()
    filing_id = source_announcement_id or asset_id
    fiscal_year = int(report_period[:4])
    now = published_at
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO official_document_blobs (
                content_hash, schema_version, content_length, canonical_path,
                signature_status, integrity_status, first_available_at,
                last_verified_at, created_at, updated_at
            ) VALUES (?, 'official_document_blob.v1', ?, ?, 'pdf', ?, ?, ?, ?, ?)
            """,
            (
                content_hash,
                len(content),
                str(pdf_path),
                integrity_status,
                now,
                now,
                now,
                now,
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO effective_annual_reports (
                asset_id, schema_version, instrument_id, fiscal_year,
                report_period, announcement_id, attachment_id, version_id,
                content_hash, source, source_announcement_id, published_at,
                document_family, variant, is_full_report, classifier_version,
                decision_state, availability, predecessor_asset_id,
                pending_candidate_id, activated_at, last_checked_at,
                decision_reasons_json, decision_evidence_json,
                equivalent_source_filings_json,
                canonical_projection_policy_version, evidence_set_hash,
                visibility_state, created_at, updated_at
            ) VALUES (
                ?, 'effective_annual_report.v2', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                'annual_report', ?, 1, 'test.formal_annual_report.v1',
                'effective', ?, NULL, NULL, ?, ?, '[]', '{}', '[]',
                'canonical_source_filing.v1', NULL, 'production', ?, ?
            )
            """,
            (
                asset_id,
                instrument_id,
                fiscal_year,
                report_period,
                f"announcement:{filing_id}",
                f"attachment:{filing_id}",
                f"version:{filing_id}",
                content_hash,
                source,
                filing_id,
                published_at,
                variant,
                availability,
                now,
                now,
                now,
                now,
            ),
        )
        conn.commit()
    return f"shared-asset:{asset_id}"
