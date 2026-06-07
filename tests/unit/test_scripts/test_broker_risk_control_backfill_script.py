from dataclasses import dataclass

from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanResult,
)
from scripts.dev_validation.backfill_broker_risk_control_reports import (
    build_candidate_report_periods,
    build_default_announcement_window,
    run_broker_risk_control_backfill,
    select_broker_instruments,
)


class _FakeDbOps:
    def __init__(self, rows_by_exchange):
        self.rows_by_exchange = rows_by_exchange

    def get_research_target_instruments_by_exchange_sync(self, exchange):
        return list(self.rows_by_exchange.get(exchange, []))


class _FakeScanner:
    def __init__(self, records):
        self.records = records
        self.configs = []

    def scan(self, config, *, filters=None):
        self.configs.append(config)
        selected = []
        for record in self.records:
            reasons = []
            for predicate in filters or []:
                reasons.extend(predicate(record) or [])
            if reasons:
                selected.append(
                    CninfoAnnouncementRecord(
                        announcement_id=record.announcement_id,
                        title=record.title,
                        announcement_time=record.announcement_time,
                        market=record.market,
                        column=record.column,
                        symbols=record.symbols,
                        sec_names=record.sec_names,
                        org_ids=record.org_ids,
                        adjunct_url=record.adjunct_url,
                        adjunct_type=record.adjunct_type,
                        raw_payload=record.raw_payload,
                        selection_reasons=reasons,
                    )
                )
        return CninfoAnnouncementScanResult(
            config=config,
            records=list(self.records),
            selected_records=selected,
            pages_scanned=1,
            announcements_seen=len(self.records),
            max_announcement_time="2026-03-30",
        )


@dataclass
class _FakeStorage:
    manifests_written: int = 0
    facts_written: int = 0

    def get_financial_source_file_manifests(self, **kwargs):
        return []

    def upsert_financial_source_file_manifest(self, manifest, *, ingestion_run_id=None):
        self.manifests_written += 1
        return manifest.source_file_id or f"source-file-{self.manifests_written}"

    def upsert_financial_numeric_facts(self, facts, *, ingestion_run_id=None, tier="hot"):
        self.facts_written += len(facts)
        return len(facts)


def _risk_control_text():
    return """
    年度风险控制指标相关情况报告
    口径：母公司
    单位：人民币万元
    净资本 2,800.50
    风险覆盖率 311.17%
    """


def test_default_window_uses_past_12_quarters():
    window = build_default_announcement_window(as_of_date="2026-06-06", quarters=12)

    assert window == {"start_date": "2023-07-01", "end_date": "2026-06-06"}
    assert build_candidate_report_periods(as_of_date="2026-06-06", quarters=12) == [
        "2023-06-30",
        "2023-09-30",
        "2023-12-31",
        "2024-03-31",
        "2024-06-30",
        "2024-09-30",
        "2024-12-31",
        "2025-03-31",
        "2025-06-30",
        "2025-09-30",
        "2025-12-31",
        "2026-03-31",
    ]


def test_select_broker_instruments_defaults_to_five():
    rows = [
        {"instrument_id": f"6000{i:02d}.SH", "symbol": f"6000{i:02d}", "exchange": "SSE", "industry": "证券"}
        for i in range(8)
    ]
    rows.append({"instrument_id": "600999.SH", "symbol": "600999", "exchange": "SSE", "industry": "银行"})

    selected = select_broker_instruments(
        _FakeDbOps({"SSE": rows}),
        exchanges=["SSE"],
        limit=5,
    )

    assert len(selected) == 5
    assert all(item["industry"] == "证券" for item in selected)


def test_backfill_script_dry_run_parses_without_writes():
    storage = _FakeStorage()
    scanner = _FakeScanner(
        [
            CninfoAnnouncementRecord(
                announcement_id="risk-2025",
                title="2025年度风险控制指标相关情况报告",
                announcement_time="2026-03-30",
                market="SSE",
                column="sse",
                symbols=["600030"],
                adjunct_url="/risk.pdf",
                adjunct_type="PDF",
            ),
            CninfoAnnouncementRecord(
                announcement_id="annual-2025",
                title="2025年年度报告",
                announcement_time="2026-03-30",
                market="SSE",
                column="sse",
                symbols=["600030"],
            ),
        ]
    )

    result = run_broker_risk_control_backfill(
        db_ops=_FakeDbOps(
            {
                "SSE": [
                    {
                        "instrument_id": "600030.SH",
                        "symbol": "600030",
                        "exchange": "SSE",
                        "name": "中信证券",
                        "industry": "证券",
                    }
                ]
            }
        ),
        storage=storage,
        exchanges=["SSE"],
        as_of_date="2026-06-06",
        limit_instruments=5,
        scanner=scanner,
        payload_fetcher=lambda record: _risk_control_text(),
        write=False,
    )

    assert result["dry_run"] is True
    assert result["announcement_scan"]["selected_announcements"] == 1
    assert result["backfill"]["reports_parsed"] == 1
    assert result["backfill"]["facts_parsed"] >= 2
    assert result["backfill"]["facts_written"] == 0
    assert storage.manifests_written == 0
    assert storage.facts_written == 0


def test_backfill_script_scan_only_skips_payload_fetch():
    result = run_broker_risk_control_backfill(
        db_ops=_FakeDbOps(
            {
                "SSE": [
                    {
                        "instrument_id": "600030.SH",
                        "symbol": "600030",
                        "exchange": "SSE",
                        "industry": "证券",
                    }
                ]
            }
        ),
        storage=_FakeStorage(),
        exchanges=["SSE"],
        as_of_date="2026-06-06",
        scanner=_FakeScanner(
            [
                CninfoAnnouncementRecord(
                    announcement_id="risk-2025",
                    title="2025年度风险控制指标相关情况报告",
                    announcement_time="2026-03-30",
                    market="SSE",
                    column="sse",
                    symbols=["600030"],
                )
            ]
        ),
        payload_fetcher=lambda record: (_ for _ in ()).throw(AssertionError("should not fetch")),
        scan_only=True,
    )

    assert result["status"] == "scan_only"
    assert result["backfill"]["reports_discovered"] == 1
    assert result["backfill"]["reports_parsed"] == 0
