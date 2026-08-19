from dataclasses import dataclass

from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    build_announcement_key,
)
from scripts.dev_validation.backfill_broker_risk_control_reports import (
    build_candidate_report_periods,
    build_default_announcement_window,
    filter_standalone_supplement_records_for_primary_gaps,
    load_shared_broker_annual_report_records,
    run_broker_risk_control_backfill,
    select_broker_instruments,
)


class _FakeDbOps:
    def __init__(self, rows_by_exchange):
        self.rows_by_exchange = rows_by_exchange

    def get_research_target_instruments_by_exchange_sync(self, exchange):
        return list(self.rows_by_exchange.get(exchange, []))


def _record(
    announcement_id,
    *,
    title,
    announcement_time,
    market,
    symbols,
    adjunct_url=None,
    adjunct_type=None,
    raw_payload=None,
):
    return AnnouncementRecord(
        source="cninfo",
        source_announcement_id=announcement_id,
        announcement_key=build_announcement_key("cninfo", announcement_id),
        title=title,
        published_at=announcement_time,
        market=market,
        exchange=market,
        symbols=tuple(symbols),
        attachments=(
            (
                AnnouncementAttachment(
                    source_url=adjunct_url,
                    file_extension=adjunct_type,
                ),
            )
            if adjunct_url
            else ()
        ),
        raw_payload=dict(raw_payload or {}),
    )


class _FakeAnnouncementService:
    def __init__(self, records):
        self.records = records
        self.configs = []

    def acquire(self, query, *, selectors=None):
        self.configs.append(query)
        records = list(self.records)
        if query.scope.symbol:
            stock_code = query.scope.symbol
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
            for predicate in selectors or []:
                reasons.extend(predicate(record) or [])
            if reasons:
                selected.append(record.with_selection_reasons(reasons))
        scan_result = AnnouncementScanResult(
            source="cninfo",
            query=query.for_source("cninfo"),
            status="success",
            records=tuple(records),
            selected_records=tuple(selected),
            pages_scanned=1,
            announcements_seen=len(records),
            max_published_at="2026-03-30",
            is_complete=True,
        )
        return AnnouncementRouteResult(
            query=query,
            status="success",
            selected_source="cninfo",
            scan_result=scan_result,
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

    def upsert_financial_numeric_facts(
        self, facts, *, ingestion_run_id=None, tier="hot"
    ):
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
        {
            "instrument_id": instrument_id,
            "symbol": instrument_id[:6],
            "exchange": "SSE",
            "industry": "证券",
        }
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
    rows.append(
        {
            "instrument_id": "600061.SH",
            "symbol": "600061",
            "exchange": "SSE",
            "industry": "证券",
        }
    )

    selected = select_broker_instruments(
        _FakeDbOps({"SSE": rows}),
        exchanges=["SSE"],
        limit=5,
    )

    assert len(selected) == 5
    assert all(item["industry"] == "证券" for item in selected)
    assert "600061.SH" not in {item["instrument_id"] for item in selected}


def test_shared_only_backfill_lists_local_assets_without_provider_scan():
    class _SharedAccess:
        def __init__(self):
            self.calls = []

        def list_effective_assets(
            self, *, instrument_id, document_family, availability, limit
        ):
            self.calls.append((instrument_id, document_family, availability, limit))
            if document_family != "annual_report":
                return {"items": []}
            return {
                "items": [
                    {
                        "asset_id": "asset-2025",
                        "instrument_id": instrument_id,
                        "report_period": "2025-12-31",
                        "document_family": "annual_report",
                        "availability": "local_valid",
                        "source": "cninfo",
                        "source_announcement_id": "annual-2025",
                        "attachment_id": "attachment-2025",
                        "observation_version": "observation-2025",
                        "content_hash": "a" * 64,
                        "published_at": "2026-03-30T00:00:00+08:00",
                        "is_correction": False,
                    }
                ]
            }

    shared = _SharedAccess()
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
        storage=_FakeStorage(),
        exchanges=["SSE"],
        as_of_date="2026-06-06",
        announcement_service=None,
        payload_fetcher=lambda record: (_ for _ in ()).throw(
            AssertionError("shared-only scan must not fetch a broker payload")
        ),
        scan_only=True,
        shared_asset_access=shared,
    )

    assert shared.calls == [
        ("600030.SH", "annual_report", "local_valid", 1000),
        ("600030.SH", "semiannual_report", "local_valid", 1000),
    ]
    assert result["announcement_scan"]["source_mode"] == ("shared_announcement_asset")
    assert result["announcement_scan"]["selected_announcements"] == 1
    assert result["backfill"]["reports_discovered"] == 1


def test_shared_asset_projection_preserves_exact_filing_identity():
    class _SharedAccess:
        def list_effective_assets(
            self, *, instrument_id, document_family, availability, limit
        ):
            if document_family != "annual_report":
                return {"items": []}
            return {
                "items": [
                    {
                        "asset_id": "asset-correction",
                        "report_period": "2025-12-31",
                        "document_family": "annual_report",
                        "availability": "local_valid",
                        "source": "cninfo",
                        "source_announcement_id": "annual-correction",
                        "attachment_id": "attachment-correction",
                        "observation_version": "observation-correction",
                        "content_hash": "b" * 64,
                        "published_at": "2026-04-02T00:00:00+08:00",
                        "is_correction": True,
                    }
                ]
            }

    projection = load_shared_broker_annual_report_records(
        _SharedAccess(),
        instruments=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
            }
        ],
        report_periods=["2025-12-31"],
    )

    record = projection["selected_records"][0]
    assert record.source_announcement_id == "annual-correction"
    assert record.attachments[0].attachment_id == "attachment-correction"
    assert record.raw_payload["shared_asset_id"] == "asset-correction"
    assert record.raw_payload["shared_asset_binding_mode"] == "exact_observation"
    assert "修订版" in record.title


def test_shared_asset_projection_accepts_semiannual_family():
    class _SharedAccess:
        def list_effective_assets(
            self, *, instrument_id, document_family, availability, limit
        ):
            if document_family != "semiannual_report":
                return {"items": []}
            return {
                "items": [
                    {
                        "asset_id": "asset-semiannual",
                        "instrument_id": instrument_id,
                        "fiscal_year": 2025,
                        "report_period": "2025-06-30",
                        "document_family": "semiannual_report",
                        "availability": "local_valid",
                        "source": "cninfo",
                        "source_announcement_id": "semiannual-2025",
                        "attachment_id": "attachment-semiannual",
                        "observation_version": "observation-semiannual",
                        "content_hash": "c" * 64,
                        "published_at": "2025-08-30T00:00:00+08:00",
                        "is_correction": False,
                    }
                ]
            }

    projection = load_shared_broker_annual_report_records(
        _SharedAccess(),
        instruments=[
            {
                "instrument_id": "600030.SH",
                "symbol": "600030",
                "exchange": "SSE",
            }
        ],
        report_periods=["2025-06-30"],
    )

    record = projection["selected_records"][0]
    assert record.title == "2025年半年度报告"
    assert record.raw_payload["shared_asset_binding_mode"] == "exact_observation"


def test_standalone_gap_filter_uses_existing_facts_for_unchanged_primary_reports():
    instruments = [
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
        }
    ]
    primary_records = [
        _record(
            announcement_id="annual-2025",
            title="2025年年度报告",
            announcement_time="2026-03-30",
            market="SSE",
            symbols=["600030"],
        ),
        _record(
            announcement_id="semi-2025",
            title="2025年半年度报告",
            announcement_time="2025-08-30",
            market="SSE",
            symbols=["600030"],
        ),
    ]
    standalone_records = [
        _record(
            announcement_id="risk-annual-2025",
            title="2025年度风险控制指标相关情况报告",
            announcement_time="2026-03-30",
            market="SSE",
            symbols=["600030"],
        ),
        _record(
            announcement_id="risk-semi-2025",
            title="2025年半年度风险控制指标相关情况报告",
            announcement_time="2025-08-30",
            market="SSE",
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
    assert [record.source_announcement_id for record in result["selected_records"]] == [
        "risk-semi-2025"
    ]
