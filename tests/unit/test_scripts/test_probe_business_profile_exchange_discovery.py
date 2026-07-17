import pytest

from research.business_profile_discovery import (
    BusinessProfileDiscoveryResult,
    BusinessProfileDocumentCandidate,
)
from research.business_profile_documents import classify_business_profile_document
from research.business_profile_exchange_discovery import (
    BusinessProfileDiscoveryResolution,
    BusinessProfileSourceAttempt,
)
from scripts.dev_validation.probe_business_profile_exchange_discovery import (
    parse_instrument_id,
    run_live_discovery_probe,
)


def _candidate(source):
    return BusinessProfileDocumentCandidate(
        announcement_id=f"{source}:1",
        title="测试公司2025年年度报告",
        announcement_time="2026-04-30",
        symbols=["600001"],
        adjunct_url="https://example/report.pdf",
        adjunct_type="PDF",
        classification=classify_business_profile_document(
            "测试公司2025年年度报告",
            adjunct_type="PDF",
        ),
        source=source,
        source_tier=("official_primary" if source == "cninfo" else "official_backup"),
    )


class _BackupAdapter:
    def __init__(self):
        self.calls = []

    def discover_instrument(self, instrument, **kwargs):
        self.calls.append((instrument, kwargs))
        return BusinessProfileDiscoveryResult(
            status="success",
            purpose_key="probe",
            instrument_id=instrument["instrument_id"],
            symbol=instrument["symbol"],
            exchange=instrument["exchange"],
            pages_scanned=1,
            announcements_seen=2,
            candidates=[_candidate("sse")],
            max_announcement_time="2026-04-30",
            stopped_at_watermark=False,
            source="sse",
            source_tier="official_backup",
        )


class _Coordinator:
    def __init__(self, *, backups=None):
        self.backup_adapters = dict(backups or {})
        self.calls = []

    def discover_instrument(self, instrument, **kwargs):
        self.calls.append((instrument, kwargs))
        candidate = _candidate("cninfo")
        return BusinessProfileDiscoveryResolution(
            status="success",
            selected_source="cninfo",
            selected_source_tier="official_primary",
            fallback_used=False,
            fallback_reason=None,
            candidates=[candidate],
            attempts=[
                BusinessProfileSourceAttempt(
                    source="cninfo",
                    source_tier="official_primary",
                    status="success",
                    candidate_count=1,
                    pages_scanned=1,
                    announcements_seen=2,
                )
            ],
        )


def test_parse_instrument_id_normalizes_supported_a_share_suffixes():
    assert parse_instrument_id("600001.sh") == {
        "instrument_id": "600001.SH",
        "symbol": "600001",
        "exchange": "SSE",
    }
    assert parse_instrument_id("000001.SZ")["exchange"] == "SZSE"
    assert parse_instrument_id("920001.BJ")["exchange"] == "BSE"


def test_parse_instrument_id_rejects_noncanonical_input():
    with pytest.raises(ValueError, match="canonical"):
        parse_instrument_id("600001")


def test_chain_probe_is_read_only_and_reports_primary_attempt():
    coordinator = _Coordinator()

    result = run_live_discovery_probe(
        ["600001.SH"],
        start_date="2026-01-01",
        end_date="2026-07-17",
        coordinator=coordinator,
    )

    assert result["status"] == "success"
    assert result["results"][0]["selected_source"] == "cninfo"
    assert result["bounds"]["download_documents"] is False
    assert result["bounds"]["write_production_state"] is False
    _, kwargs = coordinator.calls[0]
    assert kwargs["dry_run"] is True
    assert kwargs["max_pages"] == 1


def test_backup_probe_calls_only_the_matching_exchange_adapter():
    backup = _BackupAdapter()
    coordinator = _Coordinator(backups={"SSE": backup})

    result = run_live_discovery_probe(
        ["600001.SH"],
        start_date="2026-01-01",
        end_date="2026-07-17",
        mode="backup",
        coordinator=coordinator,
    )

    assert result["status"] == "success"
    assert result["results"][0]["selected_source"] == "sse"
    assert coordinator.calls == []
    assert backup.calls[0][1]["dry_run"] is True


def test_disabled_backup_is_blocked_not_reported_as_empty_disclosure():
    result = run_live_discovery_probe(
        ["920001.BJ"],
        start_date="2026-01-01",
        end_date="2026-07-17",
        mode="backup",
        coordinator=_Coordinator(),
    )

    item = result["results"][0]
    assert result["status"] == "degraded"
    assert item["status"] == "blocked"
    assert item["fallback_reason"] == ("exchange_backup_disabled_or_unconfigured")


def test_probe_enforces_instrument_and_page_bounds():
    with pytest.raises(ValueError, match="exceeds"):
        run_live_discovery_probe(
            ["600001.SH", "600002.SH"],
            start_date="2026-01-01",
            end_date="2026-07-17",
            max_instruments=1,
            coordinator=_Coordinator(),
        )
    with pytest.raises(ValueError, match="between 1 and 5"):
        run_live_discovery_probe(
            ["600001.SH"],
            start_date="2026-01-01",
            end_date="2026-07-17",
            max_pages=6,
            coordinator=_Coordinator(),
        )
