from dataclasses import dataclass

from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanResult,
)
from scripts.dev_validation.backfill_broker_risk_control_reports import (
    build_candidate_report_periods,
    build_default_announcement_window,
    filter_standalone_supplement_records_for_primary_gaps,
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
        records = list(self.records)
        if getattr(config, "stock", None):
            stock_code = str(config.stock).split(",", 1)[0]
            records = [record for record in records if stock_code in record.symbols]
        else:
            records = [
                record
                for record in records
                if record.raw_payload.get("market_scan", True)
            ]
        selected = []
        for record in records:
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
            records=records,
            selected_records=selected,
            pages_scanned=1,
            announcements_seen=len(records),
            max_announcement_time="2026-03-30",
        )


@dataclass
class _FakeStorage:
    manifests_written: int = 0
    facts_written: int = 0
    numeric_facts: list | None = None

    def get_financial_source_file_manifests(self, **kwargs):
        return []

    def upsert_financial_source_file_manifest(self, manifest, *, ingestion_run_id=None):
        self.manifests_written += 1
        return manifest.source_file_id or f"source-file-{self.manifests_written}"

    def upsert_financial_numeric_facts(self, facts, *, ingestion_run_id=None, tier="hot"):
        self.facts_written += len(facts)
        return len(facts)

    def get_financial_numeric_facts(
        self,
        instrument_id,
        *,
        include_history=False,
        report_period=None,
        fact_name=None,
        canonical_fact_name=None,
        limit=None,
    ):
        rows = [
            row
            for row in self.numeric_facts or []
            if row.get("instrument_id") == instrument_id
        ]
        if report_period:
            rows = [row for row in rows if row.get("report_period") == report_period]
        if fact_name:
            rows = [row for row in rows if row.get("fact_name") == fact_name]
        if canonical_fact_name:
            rows = [
                row
                for row in rows
                if row.get("canonical_fact_name") == canonical_fact_name
            ]
        return rows[:limit] if limit is not None else rows


def _risk_control_text():
    return """
    年度风险控制指标相关情况报告
    口径：母公司
    单位：人民币万元
    净资本 280,050.00
    风险覆盖率 311.17%
    """


def _annual_without_net_capital_text():
    return """
    2025年年度报告
    母公司的净资本及风险控制指标
    单位：人民币万元
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
        {"instrument_id": instrument_id, "symbol": instrument_id[:6], "exchange": "SSE", "industry": "证券"}
        for instrument_id in (
            "600030.SH",
            "600109.SH",
            "600369.SH",
            "600906.SH",
            "600909.SH",
            "600918.SH",
            "600958.SH",
            "600999.SH",
        )
    ]
    rows.append({"instrument_id": "600061.SH", "symbol": "600061", "exchange": "SSE", "industry": "证券"})

    selected = select_broker_instruments(
        _FakeDbOps({"SSE": rows}),
        exchanges=["SSE"],
        limit=5,
    )

    assert len(selected) == 5
    assert all(item["industry"] == "证券" for item in selected)
    assert "600061.SH" not in {item["instrument_id"] for item in selected}


def test_backfill_script_dry_run_parses_without_writes():
    storage = _FakeStorage()
    scanner = _FakeScanner(
        [
            CninfoAnnouncementRecord(
                announcement_id="annual-2025",
                title="2025年年度报告",
                announcement_time="2026-03-30",
                market="SSE",
                column="sse",
                symbols=["600030"],
                adjunct_url="/annual.pdf",
                adjunct_type="PDF",
            ),
            CninfoAnnouncementRecord(
                announcement_id="risk-2025",
                title="2025年度风险控制指标相关情况报告",
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


def test_backfill_script_per_instrument_scan_keeps_full_broker_universe():
    storage = _FakeStorage()
    scanner = _FakeScanner(
        [
            CninfoAnnouncementRecord(
                announcement_id="annual-600030-2025",
                title="2025年年度报告",
                announcement_time="2026-03-30",
                market="SSE",
                column="sse",
                symbols=["600030"],
                adjunct_url="/annual-600030.pdf",
                adjunct_type="PDF",
            ),
            CninfoAnnouncementRecord(
                announcement_id="annual-600109-2025",
                title="2025年年度报告",
                announcement_time="2026-03-30",
                market="SSE",
                column="sse",
                symbols=["600109"],
                adjunct_url="/annual-600109.pdf",
                adjunct_type="PDF",
                raw_payload={"market_scan": False},
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
                    },
                    {
                        "instrument_id": "600109.SH",
                        "symbol": "600109",
                        "exchange": "SSE",
                        "name": "国金证券",
                        "industry": "证券",
                    },
                    {
                        "instrument_id": "600061.SH",
                        "symbol": "600061",
                        "exchange": "SSE",
                        "name": "国投资本",
                        "industry": "证券",
                    },
                ]
            }
        ),
        storage=storage,
        exchanges=["SSE"],
        as_of_date="2026-06-06",
        limit_instruments=0,
        scanner=scanner,
        payload_fetcher=lambda record: _risk_control_text(),
        write=False,
    )

    assert len(result["target_instruments"]) == 2
    assert result["announcement_scan"]["selected_announcements"] == 2
    per_instrument = result["announcement_scan"]["per_instrument_scan"]
    assert per_instrument["enabled"] is True
    assert per_instrument["attempted_instruments"] == 2
    assert per_instrument["instruments_with_matches"] == 2
    assert per_instrument["selected_announcements_added"] == 2
    assert result["backfill"]["reports_parsed"] == 2


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
                    title="2025年年度报告",
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


def test_standalone_supplement_only_parses_primary_net_capital_gaps():
    storage = _FakeStorage()
    records = [
        CninfoAnnouncementRecord(
            announcement_id="annual-2025",
            title="2025年年度报告",
            announcement_time="2026-03-30",
            market="SSE",
            column="sse",
            symbols=["600030"],
            adjunct_url="/annual.pdf",
            adjunct_type="PDF",
        ),
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
    ]

    def _payload(record):
        if record.announcement_id == "annual-2025":
            return _annual_without_net_capital_text()
        return _risk_control_text()

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
        scanner=_FakeScanner(records),
        payload_fetcher=_payload,
        write=False,
        include_standalone_supplement=True,
    )

    supplement = result["backfill"]["supplementary_standalone"]
    assert result["announcement_scan"]["standalone_supplement"]["selected_announcements"] == 1
    assert result["announcement_scan"]["standalone_supplement"]["gap_fill_announcements"] == 1
    assert supplement["reports_parsed"] == 1
    assert supplement["primary_gap_filter"]["selected_records_count"] == 1
    assert supplement["primary_gap_filter"]["missing_primary_pairs"] == [
        {"instrument_id": "600030.SH", "report_period": "2025-12-31"}
    ]


def test_standalone_supplement_skips_primary_net_capital_covered_periods():
    storage = _FakeStorage()
    records = [
        CninfoAnnouncementRecord(
            announcement_id="annual-2025",
            title="2025年年度报告",
            announcement_time="2026-03-30",
            market="SSE",
            column="sse",
            symbols=["600030"],
            adjunct_url="/annual.pdf",
            adjunct_type="PDF",
        ),
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
    ]

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
        scanner=_FakeScanner(records),
        payload_fetcher=lambda record: _risk_control_text(),
        write=False,
        include_standalone_supplement=True,
    )

    supplement = result["backfill"]["supplementary_standalone"]
    assert result["announcement_scan"]["standalone_supplement"]["selected_announcements"] == 1
    assert result["announcement_scan"]["standalone_supplement"]["gap_fill_announcements"] == 0
    assert supplement["reports_parsed"] == 0
    assert supplement["primary_gap_filter"]["selected_records_count"] == 0


def test_standalone_gap_filter_uses_existing_facts_for_unchanged_primary_reports():
    instruments = [
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
        }
    ]
    primary_records = [
        CninfoAnnouncementRecord(
            announcement_id="annual-2025",
            title="2025年年度报告",
            announcement_time="2026-03-30",
            market="SSE",
            column="sse",
            symbols=["600030"],
        ),
        CninfoAnnouncementRecord(
            announcement_id="semi-2025",
            title="2025年半年度报告",
            announcement_time="2025-08-30",
            market="SSE",
            column="sse",
            symbols=["600030"],
        ),
    ]
    standalone_records = [
        CninfoAnnouncementRecord(
            announcement_id="risk-annual-2025",
            title="2025年度风险控制指标相关情况报告",
            announcement_time="2026-03-30",
            market="SSE",
            column="sse",
            symbols=["600030"],
        ),
        CninfoAnnouncementRecord(
            announcement_id="risk-semi-2025",
            title="2025年半年度风险控制指标相关情况报告",
            announcement_time="2025-08-30",
            market="SSE",
            column="sse",
            symbols=["600030"],
        ),
    ]
    storage = _FakeStorage(
        numeric_facts=[
            {
                "instrument_id": "600030.SH",
                "report_period": "2025-12-31",
                "canonical_fact_name": "net_capital",
                "fact_value": 2800500000.0,
            }
        ]
    )

    result = filter_standalone_supplement_records_for_primary_gaps(
        standalone_records,
        instruments=instruments,
        report_periods=["2025-06-30", "2025-12-31"],
        primary_result={"report_summaries": [], "unchanged_reports": 2},
        primary_records=primary_records,
        storage=storage,
    )

    assert result["expected_pairs_source"] == "primary_announcement_records"
    assert result["missing_primary_pairs"] == [
        {"instrument_id": "600030.SH", "report_period": "2025-06-30"}
    ]
    assert [record.announcement_id for record in result["selected_records"]] == [
        "risk-semi-2025"
    ]
