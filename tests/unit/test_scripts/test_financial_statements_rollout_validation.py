import asyncio

from scripts.research_financial_statements_rollout_validation import (
    exit_code_for_result,
    parse_report_periods,
    run_rollout_validation,
)
from utils.config_manager import ResearchConfig


class _FakeFinancialStatementsRepo:
    def validate_readiness(self, **kwargs):
        return {
            "status": "ready",
            "ready_for_rollout": True,
            "blockers": [],
            "kwargs": kwargs,
        }


class _FakeStorage:
    financial_statements = _FakeFinancialStatementsRepo()


class _FakeDbOps:
    async def get_instruments_by_exchange(self, exchange):
        return [
            {
                "instrument_id": "600000.SH",
                "exchange": exchange,
                "type": "stock",
                "is_active": True,
            },
            {
                "instrument_id": "000000.SH",
                "exchange": exchange,
                "type": "index",
                "is_active": True,
            },
        ]


class _FakeManager:
    def __init__(self):
        self.research_config = ResearchConfig(
            enabled=True,
            markets=["SSE"],
            modules={
                "financial_statements": {
                    "enabled": True,
                    "history": {"baseline_report_period": "2024Q1"},
                    "readiness": {
                        "required_core_facts": ["revenue", "net_income"],
                    },
                    "fallback_policy": {"fallback_source_priority": ["akshare"]},
                }
            },
        )
        self.db_ops = _FakeDbOps()
        self.research_storage = _FakeStorage()
        self.sync_called = False

    async def run_financial_statements_shadow_sync(self, **kwargs):
        self.sync_called = True
        return {"status": "success", "kwargs": kwargs}


def test_parse_report_periods():
    assert parse_report_periods("2024Q1, 2024-06-30") == [
        "2024Q1",
        "2024-06-30",
    ]
    assert parse_report_periods(None) is None


def test_financial_statements_rollout_validation_skip_sync():
    manager = _FakeManager()

    result = asyncio.run(
        run_rollout_validation(
            manager,
            exchanges=["SSE"],
            report_periods=["2024-03-31"],
            skip_sync=True,
        )
    )

    assert result["status"] == "ready"
    assert result["sync"]["status"] == "skipped"
    assert result["summary"]["target_instrument_count"] == 1
    assert manager.sync_called is False


def test_financial_statements_rollout_validation_exit_code():
    assert exit_code_for_result(
        {"summary": {"ready_for_rollout": False}},
        fail_on_not_ready=True,
    ) == 2
    assert exit_code_for_result(
        {"summary": {"ready_for_rollout": False}},
        fail_on_not_ready=False,
    ) == 0
