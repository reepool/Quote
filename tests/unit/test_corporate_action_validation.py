from datetime import date

from data_sources.corporate_action_validation import (
    compare_cumulative_factor_paths,
    match_official_announcement_evidence,
    normalize_cninfo_implementation_announcements,
    normalize_eastmoney_events,
    normalize_tdx_events,
    reconcile_event_fields,
)
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanner,
)


class _Response:
    def raise_for_status(self):
        return None

    def json(self):
        return [{"code": "600000", "orgId": "gssh0600000", "category": "A股"}]


class _Session:
    def post(self, url, data=None, headers=None, timeout=None):
        return _Response()


def test_eastmoney_normalization_keeps_implemented_rows_and_source_identity():
    rows = [
        {
            "代码": "600000",
            "名称": "浦发银行",
            "现金分红-现金分红比例": 1.5,
            "送转股份-送转总比例": 2.0,
            "除权除息日": "2025-07-10",
            "方案进度": "实施分配",
            "最新公告日期": "2025-07-04",
            "_report_period": "20241231",
        },
        {
            "代码": "600000",
            "现金分红-现金分红比例": 2.0,
            "除权除息日": "2026-07-10",
            "方案进度": "董事会预案",
        },
        {
            "代码": "600000",
            "现金分红-现金分红比例": 2.0,
            "除权除息日": "2026-07-11",
            "方案进度": "",
        },
    ]

    result = normalize_eastmoney_events(
        rows,
        symbol_to_instrument={"600000": "600000.SH"},
        start_date=date(2025, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert len(result) == 1
    assert result[0]["cash_per_10"] == 1.5
    assert result[0]["bonus_per_10"] == 2.0
    assert result[0]["source"] == "eastmoney_stock_fhps"
    assert result[0]["adapter"] == "akshare.stock_fhps_em"


def test_event_reconciliation_separates_conflicts_shifts_and_rights_only():
    tdx = normalize_tdx_events(
        [
            {
                "instrument_id": "600000.SH",
                "ex_date": "2025-07-10",
                "fenhong": 1.5,
                "songzhuangu": 0,
            },
            {
                "instrument_id": "000001.SZ",
                "ex_date": "2025-07-10",
                "fenhong": 2.0,
                "songzhuangu": 1.0,
            },
            {
                "instrument_id": "600001.SH",
                "ex_date": "2025-07-10",
                "peigu": 3.0,
                "peigujia": 5.0,
            },
        ],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
    )
    reference = [
        {
            "instrument_id": "600000.SH",
            "ex_date": date(2025, 7, 11),
            "cash_per_10": 1.5,
            "bonus_per_10": 0.0,
            "source": "eastmoney_stock_fhps",
            "adapter": "akshare.stock_fhps_em",
        },
        {
            "instrument_id": "000001.SZ",
            "ex_date": date(2025, 7, 10),
            "cash_per_10": 2.5,
            "bonus_per_10": 1.0,
            "source": "eastmoney_stock_fhps",
            "adapter": "akshare.stock_fhps_em",
        },
    ]

    result = reconcile_event_fields(
        tdx,
        reference,
        trading_sessions_by_exchange={
            "SSE": [date(2025, 7, 10), date(2025, 7, 11)],
            "SZSE": [date(2025, 7, 10), date(2025, 7, 11)],
        },
    )

    assert result["status"] == "partial"
    assert result["totals"]["shifted_event_field_matches"] == 1
    assert result["totals"]["event_field_conflicts"] == 1
    assert result["totals"]["unsupported_rights_only_tdx_events"] == 1
    assert result["totals"]["tdx_event_only"] == 0


def test_cumulative_comparison_keeps_historical_conflict_when_latest_converges():
    result = compare_cumulative_factor_paths(
        [
            {
                "instrument_id": "600000.SH",
                "ex_date": date(2020, 6, 1),
                "factor": 2.0,
                "validation_result": "computed_unvalidated",
            },
            {
                "instrument_id": "600000.SH",
                "ex_date": date(2021, 6, 1),
                "factor": 0.5,
                "validation_result": "computed_unvalidated",
            },
        ],
        [
            {
                "instrument_id": "600000.SH",
                "ex_date": date(2019, 1, 1),
                "source": "baostock",
                "factor": 1.0,
                "cumulative_factor": 1.0,
            }
        ],
        start_date=date(2020, 1, 1),
        end_date=date(2021, 12, 31),
        reference_sources=["baostock"],
        instrument_ids=["600000.SH"],
    )

    assert result["status"] == "partial"
    assert result["totals"]["latest_acceptable"] == 1
    assert result["totals"]["latest_conflict"] == 0
    assert result["totals"]["historical_conflict_anchors"] == 1


def test_cumulative_comparison_is_partial_when_one_requested_source_is_missing():
    result = compare_cumulative_factor_paths(
        [
            {
                "instrument_id": "600000.SH",
                "ex_date": date(2025, 6, 1),
                "factor": 1.01,
                "validation_result": "computed_unvalidated",
            }
        ],
        [
            {
                "instrument_id": "600000.SH",
                "ex_date": date(2025, 6, 1),
                "source": "baostock",
                "factor": 1.01,
                "cumulative_factor": 1.01,
            }
        ],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        reference_sources=["baostock", "akshare"],
    )

    assert result["status"] == "partial"
    assert result["totals"]["latest_acceptable"] == 1
    assert result["totals"]["reference_paths_unavailable"] == 1
    assert result["unavailable_samples"][0]["source"] == "akshare"


def test_cninfo_metadata_is_existence_evidence_not_amount_validation():
    announcements = normalize_cninfo_implementation_announcements(
        [
            CninfoAnnouncementRecord(
                announcement_id="a1",
                title="浦发银行2024年年度普通股<em>权益</em><em>分派</em><em>实施</em><em>公告</em>",
                announcement_time="2025-07-04T16:00:00+00:00",
                market="SSE",
                column="sse",
                symbols=["600000"],
                adjunct_url="finalpage/a1.pdf",
            ),
            CninfoAnnouncementRecord(
                announcement_id="a2",
                title="关于实施权益分派时可转债停止转股的提示性公告",
                announcement_time="2025-07-03T16:00:00+00:00",
                market="SSE",
                column="sse",
                symbols=["600000"],
            ),
        ],
        symbol_to_instrument={"600000": "600000.SH"},
    )
    result = match_official_announcement_evidence(
        [{"instrument_id": "600000.SH", "ex_date": date(2025, 7, 10)}],
        announcements,
    )

    assert len(announcements) == 1
    assert result["status"] == "success"
    assert result["evidence_scope"] == "announcement_existence_only"
    assert result["totals"]["official_announcement_evidence_found"] == 1


def test_cninfo_scanner_resolves_stock_identity_for_targeted_scans():
    scanner = CninfoAnnouncementScanner(session=_Session(), retry_attempts=0)

    result = scanner.resolve_stock_identity("600000")

    assert result == {
        "symbol": "600000",
        "org_id": "gssh0600000",
        "stock": "600000,gssh0600000",
    }
