from __future__ import annotations

from contextlib import nullcontext
import json
import sqlite3

import pytest

from scripts.dev_validation.migrate_broker_shared_annual_reports import (
    _stale_superseded_default_processing,
    migrate_broker_shared_annual_reports,
)


def _asset(instrument_id: str = "600030.SH", fiscal_year: int = 2025):
    return {
        "asset_id": f"asset-{instrument_id}-{fiscal_year}",
        "instrument_id": instrument_id,
        "fiscal_year": fiscal_year,
        "report_period": f"{fiscal_year}-12-31",
        "source": "cninfo",
        "source_announcement_id": f"filing-{fiscal_year}",
        "attachment_id": f"attachment-{fiscal_year}",
        "observation_version": f"version-{fiscal_year}",
        "content_hash": "a" * 64,
        "document_family": "annual_report",
        "availability": "local_valid",
        "effective_state": "current",
    }


class _Repository:
    def __init__(self):
        self.rows = []

    def upsert_consumer_processing(self, **kwargs):
        self.rows.append(kwargs)


class _SqliteRepository:
    def __init__(self, path):
        self.path = path

    def transaction(self):
        class _Transaction:
            def __enter__(inner):
                inner.conn = sqlite3.connect(self.path)
                return inner.conn

            def __exit__(inner, exc_type, _exc, _traceback):
                if exc_type is None:
                    inner.conn.commit()
                else:
                    inner.conn.rollback()
                inner.conn.close()

        return _Transaction()


class _Access:
    def __init__(self, assets):
        self.assets = assets
        self.repository = _Repository()

    def list_assets(self, *, instrument_id, limit):
        return {
            "items": [
                asset for asset in self.assets if asset["instrument_id"] == instrument_id
            ]
        }

    def get_effective_asset(self, instrument_id, *, fiscal_year):
        return next(
            (
                asset
                for asset in self.assets
                if asset["instrument_id"] == instrument_id
                and asset["fiscal_year"] == fiscal_year
            ),
            None,
        )


class _Storage:
    research_config = type("Config", (), {"modules": {}})()

    def financial_database_scope(self):
        return nullcontext()

    def get_financial_source_file_manifests(self, **_kwargs):
        return []

    def get_financial_numeric_facts(self, *_args, **_kwargs):
        return []


class _Service:
    def __init__(self, **kwargs):
        self.provider = kwargs["announcement_service"]
        self.downloader = kwargs["payload_fetcher"]

    def process_shared_asset_event(self, event, *, instrument, dry_run, **kwargs):
        assert event["asset_id"].startswith("asset-")
        assert instrument["instrument_id"] == event["instrument_id"]
        assert dry_run is True
        return {
            "status": "success",
            "reports_parsed": 1,
            "facts_parsed": 1,
            "report_summaries": [{"missing_required_facts": []}],
        }


class _PartialFactService(_Service):
    def process_shared_asset_event(self, event, *, instrument, dry_run, **kwargs):
        assert event["asset_id"].startswith("asset-")
        assert instrument["instrument_id"] == event["instrument_id"]
        return {
            "status": "success",
            "reports_parsed": 1,
            "facts_parsed": 4,
            "parse_failures": 0,
            "retryable_pending_reports": 0,
            "report_summaries": [{"missing_required_facts": ["net_capital"]}],
            "dry_run": dry_run,
        }


def test_migration_dry_run_is_local_only_and_writes_no_processing_rows():
    access = _Access([_asset()])
    result = migrate_broker_shared_annual_reports(
        access=access,
        storage=_Storage(),
        instruments={
            "600030.SH": {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
            }
        },
        write=False,
        expected_asset_count=1,
        service_factory=_Service,
    )

    assert result["selected_asset_count"] == 1
    assert result["provider_requests"] == 0
    assert result["attachment_downloads"] == 0
    assert result["archive_copies_or_writes"] == 0
    assert access.repository.rows == []


def test_write_requires_exact_preapproved_asset_count_before_service_creation():
    access = _Access([_asset()])

    with pytest.raises(ValueError, match="expected=2 actual=1"):
        migrate_broker_shared_annual_reports(
            access=access,
            storage=_Storage(),
            instruments={
                "600030.SH": {
                    "instrument_id": "600030.SH",
                    "symbol": "600030",
                    "exchange": "SSE",
                }
            },
            write=True,
            expected_asset_count=2,
            service_factory=lambda **_kwargs: pytest.fail(
                "service must not be created before bound approval passes"
            ),
        )


def test_write_marks_lineage_valid_partial_fact_output_current(monkeypatch):
    access = _Access([_asset("002670.SZ", 2024)])
    validations = iter(
        (
            {"ready": False, "reason_code": "shared_broker_manifest_not_unique"},
            {
                "ready": True,
                "reason_code": None,
                "fact_count": 4,
                "missing_required_facts": ["net_capital"],
                "business_fact_complete": False,
            },
        )
    )
    monkeypatch.setattr(
        "scripts.dev_validation.migrate_broker_shared_annual_reports.validate_broker_shared_asset_processing",
        lambda *_args, **_kwargs: next(validations),
    )
    monkeypatch.setattr(
        "scripts.dev_validation.migrate_broker_shared_annual_reports._stale_superseded_default_processing",
        lambda *_args, **_kwargs: 0,
    )

    result = migrate_broker_shared_annual_reports(
        access=access,
        storage=_Storage(),
        instruments={
            "002670.SZ": {
                "instrument_id": "002670.SZ",
                "symbol": "002670",
                "exchange": "SZSE",
            }
        },
        write=True,
        expected_asset_count=1,
        service_factory=_PartialFactService,
    )

    assert result["status"] == "completed"
    assert result["current_count"] == 1
    assert result["failed_count"] == 0
    assert result["business_incomplete_count"] == 1
    assert result["business_incomplete_assets"][0]["missing_required_facts"] == [
        "net_capital"
    ]
    assert result["provider_requests"] == 0
    assert result["attachment_downloads"] == 0
    assert access.repository.rows[0]["status"].value == "current"


def test_stale_superseded_default_processing_preserves_exact_selector(tmp_path):
    database = tmp_path / "catalog.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """CREATE TABLE official_asset_consumer_processing(
                   asset_id TEXT, consumer TEXT, parser_version TEXT,
                   parameter_hash TEXT, status TEXT, error_code TEXT,
                   metadata_json TEXT, updated_at TEXT
               )"""
        )
        rows = (
            ("old-default", "current", {"selector_mode": "default_effective"}),
            ("old-failed", "failed", {"selector_mode": "default_effective"}),
            ("old-exact", "current", {"selector_mode": "exact_filing"}),
            ("old-observation", "current", {"selector_kind": "exact_observation"}),
            ("new-default", "current", {"selector_mode": "default_effective"}),
        )
        for parameter_hash, status, metadata in rows:
            conn.execute(
                "INSERT INTO official_asset_consumer_processing VALUES(?,?,?,?,?,?,?,?)",
                (
                    "asset-1",
                    "broker_risk_control",
                    "broker_annual_report_embedded_risk_control_pdf.v1",
                    parameter_hash,
                    status,
                    None,
                    json.dumps(metadata),
                    "2026-08-13T00:00:00Z",
                ),
            )

    changed = _stale_superseded_default_processing(
        _SqliteRepository(database),
        asset_id="asset-1",
        parameter_hash="new-default",
    )

    with sqlite3.connect(database) as conn:
        statuses = dict(
            conn.execute(
                "SELECT parameter_hash, status FROM official_asset_consumer_processing"
            ).fetchall()
        )
    assert changed == 2
    assert statuses == {
        "old-default": "stale",
        "old-failed": "stale",
        "old-exact": "current",
        "old-observation": "current",
        "new-default": "current",
    }
