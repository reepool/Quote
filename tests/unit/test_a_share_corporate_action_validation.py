import json
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from data_manager import DataManager
from research.providers.cninfo_announcements import CninfoAnnouncementRecord
from scheduler.tasks import (
    ScheduledTasks,
    _format_a_share_corporate_action_validation_report,
    data_manager,
)


def test_scheduler_config_registers_validation_as_manual_only():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    job = config["scheduler_config"]["jobs"][
        "a_share_corporate_action_validation"
    ]

    assert job["enabled"] is True
    assert job["manual_only"] is True
    assert "trigger" not in job
    assert "cron" not in job


def test_eastmoney_report_periods_exclude_future_period_end_dates():
    periods = DataManager._corporate_action_report_periods(
        date(2020, 1, 1),
        date(2026, 7, 15),
    )

    assert "20191231" in periods
    assert "20260630" in periods
    assert "20261231" not in periods


class _ValidationDbOps:
    async def get_research_target_instruments_by_exchange(
        self,
        exchange,
        is_active=None,
    ):
        return [{
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": exchange,
            "type": "stock",
        }]

    async def execute_read_query(self, query, params):
        if "adjustment_factors_tdx" in query:
            return [{
                "instrument_id": "600000.SH",
                "ex_date": "2025-07-10",
                "factor": 1.01,
                "cumulative_factor": 1.01,
                "validation_result": "computed_unvalidated",
                "fenhong": 1.5,
                "songzhuangu": 0.0,
                "peigu": 0.0,
                "peigujia": 0.0,
            }]
        return [{
            "instrument_id": "600000.SH",
            "ex_date": "2025-07-10",
            "source": "baostock",
            "factor": 1.01,
            "cumulative_factor": 1.01,
        }]

    async def get_trading_calendar_records(self, exchange, start_date, end_date):
        return [{"date": date(2025, 7, 10), "is_trading_day": True}]


@pytest.mark.asyncio
async def test_data_manager_combines_event_official_and_cumulative_evidence(monkeypatch):
    manager = object.__new__(DataManager)
    manager.db_ops = _ValidationDbOps()
    manager._fetch_eastmoney_corporate_action_rows = AsyncMock(return_value={
        "status": "success",
        "source": "eastmoney_stock_fhps",
        "adapter": "akshare.stock_fhps_em",
        "rows": [{
            "代码": "600000",
            "名称": "浦发银行",
            "现金分红-现金分红比例": 1.5,
            "送转股份-送转总比例": 0.0,
            "除权除息日": "2025-07-10",
            "方案进度": "实施分配",
            "最新公告日期": "2025-07-05",
            "_report_period": "20241231",
        }],
        "periods_requested": ["20241231"],
        "periods_succeeded": 1,
        "empty_periods": [],
        "failed_periods": [],
    })
    manager._scan_cninfo_corporate_action_announcements = AsyncMock(return_value={
        "status": "success",
        "source": "cninfo_announcement_metadata",
        "records": [CninfoAnnouncementRecord(
            announcement_id="a1",
            title="浦发银行2024年年度普通股权益分派实施公告",
            announcement_time="2025-07-04T16:00:00+00:00",
            market="SSE",
            column="sse",
            symbols=["600000"],
        )],
        "instruments_requested": 1,
        "instruments_scanned": 1,
        "errors": [],
    })

    result = await manager.validate_a_share_corporate_actions(
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        exchanges=["SSE"],
        instrument_ids=["600000.SH"],
        reference_sources=["baostock"],
        official_sample_limit=1,
    )

    assert result["status"] == "success"
    assert result["read_only"] is True
    assert result["event_validation"]["totals"]["exact_event_field_matches"] == 1
    assert result["official_validation"]["totals"][
        "official_announcement_evidence_found"
    ] == 1
    assert result["cumulative_validation"]["totals"]["latest_acceptable"] == 1


@pytest.mark.asyncio
async def test_scheduler_manual_validation_passes_normalized_parameters(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    validation_mock = AsyncMock(return_value={
        "status": "partial",
        "operation": "a_share_corporate_action_validation",
        "read_only": True,
        "parameters": {
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
            "exchanges": ["SSE"],
        },
        "universe": {"instrument_count": 1},
        "source_coverage": {},
        "event_validation": {"totals": {}},
        "official_validation": {"totals": {}},
        "cumulative_validation": {
            "totals": {"reference_paths_unavailable": 1},
            "unavailable_samples": [{
                "instrument_id": "600000.SH",
                "source": "akshare",
                "reason": "reference_factor_path_unavailable",
            }],
        },
        "reasons": ["event_field_evidence_unresolved"],
    })
    monkeypatch.setattr(
        data_manager,
        "validate_a_share_corporate_actions",
        validation_mock,
    )

    result = await task.a_share_corporate_action_validation(
        start_date="2025-01-01",
        end_date="2025-12-31",
        exchanges="sse",
        instrument_ids="600000.sh",
        reference_sources="baostock",
        scan_official_announcements="false",
    )

    assert result["status"] == "partial"
    assert validation_mock.await_args.kwargs["exchanges"] == ["SSE"]
    assert validation_mock.await_args.kwargs["instrument_ids"] == ["600000.SH"]
    assert validation_mock.await_args.kwargs["scan_official_announcements"] is False
    report = _format_a_share_corporate_action_validation_report(result)
    assert "event_field_evidence_unresolved" in report
    assert "reference_paths_unavailable=1" in report
    assert "reference_factor_path_unavailable" in report
