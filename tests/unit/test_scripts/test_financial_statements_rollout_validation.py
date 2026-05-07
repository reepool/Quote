import asyncio

from scripts.research_financial_statements_rollout_validation import (
    enable_official_source_config,
    exit_code_for_result,
    normalize_report_periods,
    parse_official_sources,
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


def test_parse_official_sources():
    assert parse_official_sources(["sse, cninfo", "SSE"]) == ["sse", "cninfo"]
    assert parse_official_sources(None) == []


def test_normalize_report_periods():
    assert normalize_report_periods(["2024Q1", "2024-06-30"]) == [
        "2024-03-31",
        "2024-06-30",
    ]


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


def test_enable_official_source_config_enables_source_candidates():
    research_config = ResearchConfig(
        modules={
            "financial_statements": {
                "official_structured_sources": {
                    "enabled": False,
                    "candidates": [
                        {"source": "sse", "enabled": False},
                        {"source": "cninfo", "enabled": False},
                    ],
                }
            }
        },
        sources={
            "sse": {
                "enabled": False,
                "financial_statements": {
                    "enabled": False,
                    "endpoint_candidates": [
                        {"key": "income", "kind": "structured_json", "enabled": False},
                        {"key": "balance", "kind": "structured_json"},
                    ],
                },
            },
            "cninfo": {"enabled": False, "financial_statements": {}},
        },
    )

    overrides = enable_official_source_config(research_config, "sse")

    assert research_config.sources["sse"]["enabled"] is True
    assert research_config.sources["sse"]["financial_statements"]["enabled"] is True
    assert (
        research_config.modules["financial_statements"]["official_structured_sources"][
            "enabled"
        ]
        is True
    )
    official_candidates = research_config.modules["financial_statements"][
        "official_structured_sources"
    ]["candidates"]
    assert official_candidates[0]["enabled"] is True
    assert official_candidates[1]["enabled"] is False
    assert all(
        candidate["enabled"] is True
        for candidate in research_config.sources["sse"]["financial_statements"][
            "endpoint_candidates"
        ]
    )
    assert overrides["source_enabled_before"] is False
    assert len(overrides["endpoint_candidate_states"]) == 2


def test_financial_statements_rollout_validation_records_official_overrides():
    manager = _FakeManager()
    manager.research_config.modules["financial_statements"][
        "official_structured_sources"
    ] = {
        "enabled": False,
        "candidates": [{"source": "sse", "enabled": False}],
    }
    manager.research_config.sources["sse"] = {
        "enabled": False,
        "financial_statements": {
            "enabled": False,
            "endpoint_candidates": [{"key": "income", "enabled": False}],
        },
    }

    result = asyncio.run(
        run_rollout_validation(
            manager,
            exchanges=["SSE"],
            report_periods=["2024-03-31"],
            skip_sync=True,
            enable_official_sources=["sse"],
        )
    )

    overrides = result["runtime_overrides"]["official_source_overrides"]["sse"]
    assert result["requested"]["enable_official_sources"] == ["sse"]
    assert overrides["source_enabled_before"] is False
    assert overrides["source_enabled_after"] is True
    assert manager.research_config.sources["sse"]["financial_statements"][
        "endpoint_candidates"
    ][0]["enabled"] is True


def test_financial_statements_rollout_validation_exit_code():
    assert exit_code_for_result(
        {"summary": {"ready_for_rollout": False}},
        fail_on_not_ready=True,
    ) == 2
    assert exit_code_for_result(
        {"summary": {"ready_for_rollout": False}},
        fail_on_not_ready=False,
    ) == 0
