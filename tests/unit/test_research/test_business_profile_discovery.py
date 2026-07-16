from research.business_profile_discovery import (
    CninfoBusinessProfileDiscoveryAdapter,
)
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanResult,
)


class _Scanner:
    def __init__(self, records, *, identity=True):
        self.records = records
        self.identity = identity
        self.config = None

    def resolve_stock_identity(self, symbol):
        if not self.identity:
            return None
        return {"symbol": symbol, "org_id": "org-1", "stock": f"{symbol},org-1"}

    def scan(self, config, *, filters):
        self.config = config
        selected = []
        for record in self.records:
            reasons = []
            for predicate in filters:
                reasons.extend(predicate(record))
            if reasons:
                selected.append(
                    CninfoAnnouncementRecord(
                        **{
                            **record.__dict__,
                            "selection_reasons": reasons,
                        }
                    )
                )
        return CninfoAnnouncementScanResult(
            config=config,
            records=self.records,
            selected_records=selected,
            pages_scanned=1,
            announcements_seen=len(self.records),
            max_announcement_time="2026-04-21T00:00:00+00:00",
            stopped_at_watermark=False,
            errors=[],
        )


class _Storage:
    def __init__(self):
        self.state = {
            "last_watermark": "2026-01-01T00:00:00+00:00",
        }
        self.state_writes = []
        self.audits = []

    def get_cninfo_announcement_scan_state(self, **kwargs):
        return self.state

    def upsert_cninfo_announcement_scan_state(self, **kwargs):
        self.state_writes.append(kwargs)

    def store_cninfo_announcement_audit(self, **kwargs):
        self.audits.append(kwargs)


def _record(announcement_id, title):
    return CninfoAnnouncementRecord(
        announcement_id=announcement_id,
        title=title,
        announcement_time="2026-04-21T00:00:00+00:00",
        market="SSE",
        column="sse",
        symbols=["600309"],
        sec_names=["万华化学"],
        org_ids=["org-1"],
        adjunct_url="finalpage/report.PDF",
        adjunct_type="PDF",
        raw_payload={"source": "fixture"},
    )


def test_instrument_discovery_selects_full_report_and_uses_watermark():
    scanner = _Scanner(
        [
            _record("full", "万华化学2025年年度报告"),
            _record("summary", "万华化学2025年年度报告摘要"),
        ]
    )
    storage = _Storage()
    adapter = CninfoBusinessProfileDiscoveryAdapter(
        storage=storage,
        scanner=scanner,
    )

    result = adapter.discover_instrument(
        {"instrument_id": "600309.SH", "symbol": "600309", "exchange": "SSE"},
        start_date="2026-01-01",
        end_date="2026-07-16",
        dry_run=False,
        ingestion_run_id=7,
    )

    assert [item.announcement_id for item in result.candidates] == ["full"]
    assert result.candidates[0].classification.document_type == "annual_report"
    assert scanner.config.stock == "600309,org-1"
    assert scanner.config.stop_at_watermark == "2026-01-01T00:00:00+00:00"
    assert storage.state_writes[0]["selected_announcements"] == 1
    assert storage.audits[0]["ingestion_run_id"] == 7
    assert storage.audits[0]["raw_payload"][
        "business_profile_classification"
    ]["document_type"] == "annual_report"


def test_discovery_keeps_restructuring_as_candidate_hint():
    scanner = _Scanner(
        [_record("event", "重大资产置换及发行股份购买资产公告")]
    )
    adapter = CninfoBusinessProfileDiscoveryAdapter(scanner=scanner)

    result = adapter.discover_instrument(
        {"instrument_id": "600001.SH", "symbol": "600001", "exchange": "SSE"}
    )

    candidate = result.candidates[0]
    assert candidate.classification.document_type == "profile_change_event"
    assert candidate.classification.profile_event_hints == [
        "reverse_merger",
        "major_asset_restructuring",
    ]


def test_discovery_reports_missing_cninfo_identity_without_scan():
    scanner = _Scanner([], identity=False)
    adapter = CninfoBusinessProfileDiscoveryAdapter(scanner=scanner)

    result = adapter.discover_instrument(
        {"instrument_id": "920001.BJ", "symbol": "920001", "exchange": "BSE"}
    )

    assert result.status == "not_found"
    assert result.errors == ["cninfo_stock_identity_not_found"]
    assert scanner.config is None
