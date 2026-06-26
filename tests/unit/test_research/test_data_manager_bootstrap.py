import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from data_manager import DataManager
from research.financial_source_field_mapping import MAPPING_VERSION
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _build_mock_config(tmp_path, *, research_enabled: bool = True):
    research_config = ResearchConfig(
        enabled=research_enabled,
        modules={
            "industry": {"enabled": True},
        },
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(),
    )

    config = Mock()
    config.get_research_config.return_value = research_config

    def _get_nested(path, default=None):
        mapping = {
            "telegram_config.enabled": False,
            "data_config": {
                "data_dir": str(tmp_path),
                "instrument_master_governance": {"enabled": False},
            },
        }
        return mapping.get(path, default)

    config.get_nested.side_effect = _get_nested
    return config


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def _sync_to_thread(func, /, *args, **kwargs):
    return func(*args, **kwargs)


class _SplitFinancialReadStorage:
    def __init__(self, bundle=None, summary=None):
        self.in_financial_scope = False
        self.bundle = bundle or {"instrument_id": "600000.SH", "latest_facts": {}}
        self.summary = summary or {"instrument_id": "600000.SH"}

    def financial_database_scope(self):
        storage = self

        class Scope:
            def __enter__(self):
                storage.in_financial_scope = True

            def __exit__(self, exc_type, exc, tb):
                storage.in_financial_scope = False

        return Scope()

    def _require_scope(self):
        if not self.in_financial_scope:
            raise RuntimeError("no such table: financial_facts")

    def get_financial_summary(self, instrument_id, **kwargs):
        self._require_scope()
        return dict(self.summary, instrument_id=instrument_id)

    def get_financial_statement_bundle(self, instrument_id, **kwargs):
        self._require_scope()
        return dict(self.bundle, instrument_id=instrument_id)

    def validate_financial_statement_readiness(self, **kwargs):
        self._require_scope()
        return {"ready_for_rollout": True, "blockers": []}

    def get_financial_numeric_facts(self, *args, **kwargs):
        self._require_scope()
        return []

    def get_financial_source_file_manifests(self, **kwargs):
        self._require_scope()
        return []


def test_research_industry_standard_sync_forces_master_governance_refresh(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.data_config = {
        "instrument_master_sync": {"enabled": True},
        "instrument_master_governance": {
            "enabled": True,
            "force_refresh_job_names": ["industry_standard_sync"],
        },
    }
    manager.ensure_instrument_master_fresh = AsyncMock(
        return_value={"status": "success"}
    )

    result = _run(
        manager._ensure_research_job_instrument_master_governance(
            exchanges=["BSE"],
            job_name="industry_standard_sync",
        )
    )

    assert result["status"] == "success"
    manager.ensure_instrument_master_fresh.assert_awaited_once_with(
        ["BSE"],
        job_name="industry_standard_sync",
        job_type="current",
        force_refresh=True,
    )


def test_data_manager_refresh_runtime_config_rebinds_research_config(tmp_path):
    initial_research_config = ResearchConfig(
        enabled=True,
        modules={"shareholders": {"delivery_mode": "free_best_effort"}},
    )
    updated_research_config = ResearchConfig(
        enabled=True,
        modules={"shareholders": {"delivery_mode": "paid_high_availability"}},
    )
    mock_config = Mock()
    mock_config.get_research_config.side_effect = [
        initial_research_config,
        updated_research_config,
    ]
    mock_config.get_nested.side_effect = lambda path, default=None: {
        "telegram_config.enabled": True,
        "data_config": {"data_dir": str(tmp_path), "download_chunk_days": 13},
    }.get(path, default)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    assert manager.research_config.modules["shareholders"]["delivery_mode"] == "free_best_effort"

    manager.refresh_runtime_config()

    assert manager.research_config.modules["shareholders"]["delivery_mode"] == "paid_high_availability"
    assert manager.telegram_enabled is True
    assert manager.download_chunk_days == 13
    assert manager.progress_file == str(tmp_path / "download_progress.json")


def test_data_manager_bootstraps_research_storage_when_enabled(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    with patch("research.storage.ResearchStorageManager") as storage_cls:
        storage_instance = Mock()
        storage_cls.return_value = storage_instance

        manager._initialize_research_storage()

    storage_cls.assert_called_once_with(manager.research_config)
    storage_instance.initialize.assert_called_once()
    assert manager.research_storage is storage_instance


def test_data_manager_research_storage_init_failure_does_not_raise(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    with patch("research.storage.ResearchStorageManager") as storage_cls:
        storage_instance = Mock()
        storage_instance.initialize.side_effect = RuntimeError("boom")
        storage_cls.return_value = storage_instance

        manager._initialize_research_storage()

    assert manager.research_storage is None


def test_data_manager_run_company_profile_shadow_sync_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_company_profile_shadow_sync(exchanges=["SSE"], limit_per_exchange=10)
    )

    assert result["status"] == "unavailable"


def test_data_manager_list_industry_component_sets_resolves_sw_index_alias(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.list_industry_taxonomy_records.return_value = [
        {
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "industry_code": "340301",
            "industry_name": "白酒",
            "industry_level": 3,
            "sw_index_code": "850111",
        }
    ]
    storage.list_industry_component_set_records.return_value = [
        {
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "industry_code": "340301",
            "component_count": 2,
            "source": "swsresearch",
            "source_mode": "direct",
            "built_at": "2026-04-25T13:00:00",
            "ingestion_run_id": None,
            "created_at": "2026-04-25T13:00:00",
            "updated_at": "2026-04-25T13:00:00",
            "symbols": ["600519", "000568"],
        }
    ]
    storage.count_industry_component_sets.return_value = 1
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(
            manager.list_research_industry_component_sets(
                sw_index_code="850111",
                limit=10,
                offset=0,
            )
        )

    assert result["sw_index_code"] == "850111"
    assert result["resolved_industry_code"] == "340301"
    assert result["total"] == 1
    storage.list_industry_component_set_records.assert_called_once_with(
        taxonomy_system="sw",
        taxonomy_version=None,
        industry_code="340301",
        max_age_days=None,
        limit=10,
        offset=0,
        include_symbols=True,
    )


def test_data_manager_list_industry_component_sets_derives_from_memberships_when_cache_empty(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.list_industry_component_set_records.return_value = []
    storage.count_industry_component_sets.return_value = 0
    storage.list_industry_component_set_records_from_memberships.return_value = (
        [
            {
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "industry_code": "340301",
                "component_count": 2,
                "source": "swsresearch",
                "source_mode": "direct",
                "built_at": "2026-04-25T13:00:00",
                "ingestion_run_id": 123,
                "created_at": "2026-04-25T13:00:00",
                "updated_at": "2026-04-25T13:00:00",
                "symbols": ["000568", "600519"],
            }
        ],
        1,
    )
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(
            manager.list_research_industry_component_sets(
                industry_code="340301",
                limit=10,
                offset=0,
            )
        )

    assert result["resolved_industry_code"] == "340301"
    assert result["total"] == 1
    assert result["items"][0]["symbols"] == ["000568", "600519"]
    storage.list_industry_component_set_records_from_memberships.assert_called_once_with(
        taxonomy_system="sw",
        taxonomy_version=None,
        industry_code="340301",
        max_age_days=None,
        limit=10,
        offset=0,
        include_symbols=True,
    )


def test_data_manager_list_industry_component_sets_returns_empty_for_missing_alias(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.list_industry_taxonomy_records.return_value = []
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(
            manager.list_research_industry_component_sets(
                sw_index_code="850111",
                limit=10,
                offset=0,
            )
        )

    assert result["missing_reason"] == "taxonomy_alias_not_found"
    assert result["total"] == 0
    assert result["items"] == []
    storage.list_industry_component_set_records.assert_not_called()


def test_data_manager_run_company_profile_shadow_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.company_profile_sync.CompanyProfileShadowSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_company_profile_shadow_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_run_financial_summary_shadow_sync_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_financial_summary_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_financial_summary_shadow_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.financial_summary_sync.FinancialSummaryShadowSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_financial_summary_shadow_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_financial_summary_sync_attaches_master_governance(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()
    manager.ensure_instrument_master_fresh = AsyncMock(
        return_value={
            "status": "fresh",
            "action": "reused_fresh_master",
            "summary": {"active_count": 1},
            "warnings": [],
            "errors": [],
        }
    )

    with patch("research.financial_summary_sync.FinancialSummaryShadowSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_financial_summary_shadow_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    manager.ensure_instrument_master_fresh.assert_awaited_once_with(
        ["SSE"],
        job_name="financial_summary_shadow_sync",
        job_type="current",
    )
    assert result["instrument_master_governance"]["status"] == "fresh"


def test_data_manager_run_shareholder_shadow_sync_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_shareholder_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_shareholder_shadow_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.shareholder_sync.ShareholderShadowSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_shareholder_shadow_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_run_shareholder_incremental_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch(
        "research.shareholder_incremental_sync.ShareholderIncrementalSyncService"
    ) as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_shareholder_incremental_sync(
                exchanges=["SSE"],
                lookback_days=7,
                dry_run=True,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()
    service_instance.sync.assert_awaited_once_with(
        exchanges=["SSE"],
        lookback_days=7,
        overlap_days=None,
        page_size=None,
        max_pages_per_market=None,
        max_candidates=None,
        pending_recheck_days=None,
        budget_mode=None,
        allow_paid_proxy=None,
        dry_run=True,
    )


def test_data_manager_run_financial_statements_shadow_sync_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_financial_statements_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_financial_statements_shadow_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.FinancialStatementsShadowSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_financial_statements_shadow_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_run_financial_l1_full_import_delegates_to_python_orchestrator(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch(
        "scripts.research_financial_l1_full_import.run_full_import",
        new_callable=AsyncMock,
    ) as run_full_import:
        run_full_import.return_value = {"status": "success"}
        result = _run(
            manager.run_financial_l1_full_import(
                exchanges=["SSE"],
                period_window="latest",
                rolling_quarters=1,
                latest_report_period="2026Q1",
                db_path=str(tmp_path / "financials.db"),
                log_dir=str(tmp_path / "log"),
            )
        )

    assert result["status"] == "success"
    run_full_import.assert_awaited_once()
    kwargs = run_full_import.await_args.kwargs
    assert kwargs["exchanges"] == ["SSE"]
    assert kwargs["report_periods"][0] == "2024-03-31"
    assert kwargs["report_periods"][-1] == "2026-03-31"
    assert str(kwargs["db_path"]).endswith("financials.db")


def test_data_manager_run_financial_disclosure_incremental_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()
    manager._ensure_research_job_instrument_master_governance = AsyncMock(
        return_value={"status": "skipped"}
    )

    with patch(
        "research.financial_disclosure_incremental_sync.FinancialDisclosureIncrementalSyncService"
    ) as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance
        result = _run(
            manager.run_financial_disclosure_incremental_sync(
                exchanges=["SZSE"],
                dry_run=True,
            )
        )

    assert result["status"] == "success"
    assert result["instrument_master_governance"]["status"] == "skipped"
    service_instance.sync.assert_awaited_once_with(
        exchanges=["SZSE"],
        lookback_days=None,
        overlap_days=None,
        page_size=None,
        max_pages_per_market=None,
        max_candidates=None,
        pending_recheck_days=None,
        target_instrument_ids=None,
        target_symbols=None,
        announcement_search_key=None,
        report_periods=None,
        period_window="latest",
        rolling_quarters=10,
        baseline_report_period="2024Q1",
        latest_report_period=None,
        db_path=None,
        request_interval_seconds=0.2,
        request_timeout_seconds=20.0,
        dry_run=True,
        reconciliation=False,
    )


def test_data_manager_run_broker_risk_control_incremental_sync_uses_hot_tier(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "SZSE", "BSE"]
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {
            "enabled": True,
            "broker_risk_control_reports": {
                "enabled": True,
                "source_profile": "broker_annual_report_embedded_risk_control",
                "exchanges": ["SSE", "SZSE", "BSE"],
                "storage": {
                    "archive_root": "data/filings/financial_statements/broker_risk_control",
                    "incremental_tier": "hot",
                },
                "incremental": {
                    "lookback_days": 14,
                    "overlap_days": 3,
                    "quarters": 12,
                    "page_size": 30,
                    "max_pages": 10,
                    "per_instrument_page_size": 30,
                    "per_instrument_max_pages": 2,
                    "limit_instruments": 0,
                    "report_period_types": ["annual", "semiannual"],
                },
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()
    manager.db_ops = Mock()
    manager._ensure_research_job_instrument_master_governance = AsyncMock(
        return_value={"status": "skipped"}
    )

    expected = {
        "status": "success",
        "backfill": {"reports_parsed": 1, "facts_parsed": 10, "facts_written": 10},
    }
    with patch(
        "scripts.dev_validation.backfill_broker_risk_control_reports.run_broker_risk_control_backfill",
        return_value=expected,
    ) as run_backfill:
        with patch("data_manager.asyncio.to_thread", _sync_to_thread):
            result = _run(
                manager.run_broker_risk_control_incremental_sync(
                    exchanges=["SSE"],
                    dry_run=False,
                )
            )

    assert result["status"] == "success"
    assert result["mode"] == "incremental_update"
    assert result["instrument_master_governance"]["status"] == "skipped"
    kwargs = run_backfill.call_args.kwargs
    assert kwargs["exchanges"] == ["SSE"]
    assert kwargs["write"] is True
    assert kwargs["tier"] == "hot"
    assert kwargs["limit_instruments"] == 0
    assert kwargs["report_period_types"] == ["annual", "semiannual"]


def test_data_manager_run_industry_shadow_sync_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_industry_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_industry_shadow_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.industry_sync.IndustryShadowSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_industry_shadow_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_run_industry_standard_sync_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_industry_standard_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_industry_standard_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "standard": {"enabled": True},
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.industry_standard_sync.IndustryStandardSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_industry_standard_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
                instrument_ids_by_exchange={"SSE": ["600000.SH"]},
                force_component_refresh=True,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()
    service_instance.sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=10,
        instrument_ids_by_exchange={"SSE": ["600000.SH"]},
        budget_mode=None,
        allow_paid_proxy=None,
        force_component_refresh=True,
    )


def test_data_manager_list_research_target_instrument_ids_prefers_db_ops_reader(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops.get_research_target_instrument_ids_by_exchange = AsyncMock(
        return_value=["600000.SH", "600519.SH"]
    )
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=AssertionError("fallback reader should not be used")
    )

    result = _run(
        manager._list_research_target_instrument_ids_by_exchange("SSE")
    )

    assert result == ["600000.SH", "600519.SH"]
    manager.db_ops.get_research_target_instrument_ids_by_exchange.assert_awaited_once_with(
        "SSE",
        is_active=True,
    )


def test_data_manager_get_research_industry_standard_coverage_gaps_reports_missing_ids(
    tmp_path,
):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "SZSE"]
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
            },
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.list_industry_membership_instrument_ids.side_effect = [
        ["600519.SH"],
        ["000001.SZ"],
    ]
    manager.research_storage = storage

    async def _get_target_ids(exchange: str, *, is_active: bool = True):
        if exchange == "SSE":
            return ["600000.SH", "600519.SH"]
        if exchange == "SZSE":
            return ["000001.SZ"]
        return []

    manager.db_ops.get_research_target_instrument_ids_by_exchange = AsyncMock(
        side_effect=_get_target_ids
    )

    result = _run(
        manager.get_research_industry_standard_coverage_gaps(
            missing_limit_per_exchange=5,
        )
    )

    assert result["taxonomy_system"] == "sw"
    assert result["taxonomy_version"] == "sw_2021"
    assert result["target_instrument_count"] == 3
    assert result["authoritative_membership_total"] == 2
    assert result["missing_authoritative_membership_count"] == 1
    assert result["ready"] is False
    assert result["targeted_missing_instrument_ids_by_exchange"] == {
        "SSE": ["600000.SH"]
    }
    assert result["exchange_gaps"] == [
        {
            "exchange": "SSE",
            "target_instruments": 2,
            "authoritative_memberships": 1,
            "missing_instrument_count": 1,
            "coverage_ratio": 0.5,
            "ready": False,
            "optional_empty_exchange": False,
            "missing_instrument_ids": ["600000.SH"],
            "missing_ids_truncated": False,
        },
        {
            "exchange": "SZSE",
            "target_instruments": 1,
            "authoritative_memberships": 1,
            "missing_instrument_count": 0,
            "coverage_ratio": 1.0,
            "ready": True,
            "optional_empty_exchange": False,
            "missing_instrument_ids": [],
            "missing_ids_truncated": False,
        },
    ]


def test_data_manager_run_industry_standard_gap_fill_sync_targets_missing_ids(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "SZSE"]
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
            },
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.list_industry_membership_instrument_ids.side_effect = [
        ["600519.SH"],
        ["000001.SZ"],
        ["600519.SH", "600000.SH"],
        ["000001.SZ"],
    ]
    manager.research_storage = storage

    async def _get_target_ids(exchange: str, *, is_active: bool = True):
        if exchange == "SSE":
            return ["600000.SH", "600519.SH"]
        if exchange == "SZSE":
            return ["000001.SZ"]
        return []

    manager.db_ops.get_research_target_instrument_ids_by_exchange = AsyncMock(
        side_effect=_get_target_ids
    )
    manager.run_industry_standard_sync = AsyncMock(
        return_value={
            "status": "success",
            "successful_exchanges": 1,
            "total_memberships_written": 1,
        }
    )

    result = _run(
        manager.run_industry_standard_gap_fill_sync(
            exchanges=["SSE", "SZSE"],
            missing_limit_per_exchange=5,
            budget_mode="availability_first",
            allow_paid_proxy=True,
        )
    )

    assert result["status"] == "success"
    assert result["repaired_instrument_count"] == 1
    assert result["remaining_missing_instrument_count"] == 0
    assert result["targeted_exchanges"] == ["SSE"]
    assert result["targeted_instrument_count"] == 1
    assert result["targeted_missing_instrument_ids_by_exchange"] == {
        "SSE": ["600000.SH"]
    }
    manager.run_industry_standard_sync.assert_awaited_once_with(
        exchanges=["SSE"],
        instrument_ids_by_exchange={"SSE": ["600000.SH"]},
        budget_mode="availability_first",
        allow_paid_proxy=True,
    )


def test_data_manager_run_industry_official_mapping_refresh_returns_unavailable_without_storage(
    tmp_path,
):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "standard": {"enabled": True},
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_industry_official_mapping_refresh(
            exchanges=["SSE"],
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_industry_official_mapping_refresh_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "standard": {"enabled": True},
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.industry_standard_sync.IndustryStandardSyncService") as service_cls:
        service_instance = Mock()
        service_instance.refresh_official_mapping_cache = AsyncMock(
            return_value={"status": "success"}
        )
        service_cls.return_value = service_instance

        result = _run(
            manager.run_industry_official_mapping_refresh(
                exchanges=["SSE"],
                budget_mode="balanced",
                allow_paid_proxy=False,
            )
        )

    assert result["status"] == "success"
    service_instance.refresh_official_mapping_cache.assert_awaited_once_with(
        exchanges=["SSE"],
        budget_mode="balanced",
        allow_paid_proxy=False,
    )
    service_cls.assert_called_once()


def test_data_manager_run_valuation_history_rebuild_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_valuation_history_rebuild(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_valuation_history_rebuild_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.valuation_history_sync.ValuationHistoryRebuildService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_valuation_history_rebuild(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_run_valuation_history_rebuild_can_force_disabled_module_for_validation(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": False},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.valuation_history_sync.ValuationHistoryRebuildService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_valuation_history_rebuild(
                exchanges=["SSE"],
                limit_per_exchange=10,
                allow_disabled_module=True,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_run_valuation_input_sync_delegates_to_service_even_when_module_disabled(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": False},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.valuation_input_sync.ValuationInputSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_valuation_input_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
                sync_mode="incremental",
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def _beta_quotes(start: str, returns: list[float]) -> pd.DataFrame:
    values = [100.0]
    for item in returns:
        values.append(values[-1] * (1.0 + item))
    return pd.DataFrame(
        [
            {"time": day.to_pydatetime(), "close": value}
            for day, value in zip(pd.date_range(start, periods=len(values), freq="D"), values)
        ]
    )


def test_data_manager_get_research_beta_calculates_default_windows(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "beta": {
            "enabled": True,
            "windows": [3, 5],
            "min_observations_floor": 2,
            "min_observation_ratio": 0.5,
            "stock_adjustment": "none",
            "benchmark_adjustment": "none",
            "board_benchmark_rules": [
                {
                    "name": "sse_main_board",
                    "exchanges": ["SSE"],
                    "benchmark_instrument_id": "000001.SH",
                    "benchmark_name": "上证综合指数",
                }
            ],
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "type": "stock",
        }
    )
    benchmark_returns = [0.01, -0.02, 0.03, 0.01, -0.01]
    stock_returns = [item * 1.5 for item in benchmark_returns]
    manager.db_ops.get_daily_data = AsyncMock(
        side_effect=[
            _beta_quotes("2026-01-01", stock_returns),
            _beta_quotes("2026-01-01", benchmark_returns),
        ]
    )

    result = _run(manager.get_research_beta("600000.SH"))

    assert result["data_points"] == 2
    assert result["windows"] == [3, 5]
    assert {item["window_days"] for item in result["items"]} == {3, 5}
    assert all(item["status"] == "success" for item in result["items"])
    assert all(round(item["beta"], 6) == 1.5 for item in result["items"])
    assert result["items"][0]["diagnostics"]["benchmark_selection_rule"].startswith(
        "market_default_from_"
    )


def test_data_manager_get_research_beta_accepts_custom_window(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "beta": {
            "enabled": True,
            "windows": [60, 120, 252],
            "min_observations_floor": 2,
            "min_observation_ratio": 0.5,
            "stock_adjustment": "none",
            "benchmark_adjustment": "none",
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "type": "stock",
        }
    )
    benchmark_returns = [0.01, -0.02, 0.03, 0.01]
    stock_returns = [item * 0.8 for item in benchmark_returns]
    manager.db_ops.get_daily_data = AsyncMock(
        side_effect=[
            _beta_quotes("2026-01-01", stock_returns),
            _beta_quotes("2026-01-01", benchmark_returns),
        ]
    )

    result = _run(
        manager.get_research_beta(
            "600000.SH",
            benchmark_family="custom",
            benchmark_instrument_id="000300.SH",
            window_days=4,
            include_details=False,
        )
    )

    assert result["windows"] == [4]
    assert result["data_points"] == 1
    item = result["items"][0]
    assert item["benchmark_family"] == "custom"
    assert item["benchmark_instrument_id"] == "000300.SH"
    assert item["window_days"] == 4
    assert round(item["beta"], 6) == 0.8
    assert "diagnostics" not in item


def test_data_manager_get_research_beta_all_returns_deduped_benchmarks(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "beta": {
            "enabled": True,
            "windows": [3],
            "min_observations_floor": 2,
            "min_observation_ratio": 0.5,
            "stock_adjustment": "none",
            "benchmark_adjustment": "none",
            "benchmarks": {
                "market_broad": [
                    {"instrument_id": "000300.SH", "name": "沪深300"},
                    {"instrument_id": "000905.SH", "name": "中证500"},
                ]
            },
            "board_benchmark_rules": [
                {
                    "name": "sse_main_board",
                    "exchanges": ["SSE"],
                    "benchmark_instrument_id": "000001.SH",
                    "benchmark_name": "上证综合指数",
                }
            ],
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = None
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "type": "stock",
        }
    )
    returns = [0.01, -0.02, 0.03]
    manager.db_ops.get_daily_data = AsyncMock(
        side_effect=[
            _beta_quotes("2026-01-01", returns),
            _beta_quotes("2026-01-01", returns),
            _beta_quotes("2026-01-01", returns),
            _beta_quotes("2026-01-01", returns),
        ]
    )

    result = _run(
        manager.get_research_beta(
            "600000.SH",
            benchmark_family="all",
            window_days=3,
        )
    )

    assert result["benchmark_family"] == "all"
    assert result["data_points"] == 4
    assert {
        item["benchmark_instrument_id"]
        for item in result["items"]
        if item["benchmark_instrument_id"]
    } == {"000001.SH", "000300.SH", "000905.SH"}
    assert any(
        item["status"] == "unavailable"
        and item["diagnostics"]["benchmark_selection_rule"]
        == "research_storage_required_for_industry_beta"
        for item in result["items"]
    )


def test_data_manager_get_research_beta_reports_missing_industry_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "beta": {
            "enabled": True,
            "windows": [3],
            "min_observations_floor": 2,
            "min_observation_ratio": 0.5,
            "stock_adjustment": "none",
            "benchmark_adjustment": "none",
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = None
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "type": "stock",
        }
    )
    manager.db_ops.get_daily_data = AsyncMock(
        return_value=_beta_quotes("2026-01-01", [0.01, -0.02, 0.03])
    )

    result = _run(
        manager.get_research_beta(
            "600000.SH",
            benchmark_family="industry_sw_l2",
            window_days=3,
        )
    )

    assert result["data_points"] == 1
    assert result["items"][0]["status"] == "unavailable"
    assert result["items"][0]["missing_reason"] == "benchmark_quotes_not_available"
    assert (
        result["items"][0]["diagnostics"]["benchmark_selection_rule"]
        == "research_storage_required_for_industry_beta"
    )


def test_data_manager_run_analyst_forecast_shadow_sync_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "analyst_forecasts": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_analyst_forecast_shadow_sync(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_analyst_forecast_shadow_sync_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "analyst_forecasts": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.analyst_forecast_sync.AnalystForecastShadowSyncService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_analyst_forecast_shadow_sync(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_run_risk_snapshot_rebuild_returns_unavailable_without_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "risk": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.run_risk_snapshot_rebuild(
            exchanges=["SSE"],
            limit_per_exchange=10,
        )
    )

    assert result["status"] == "unavailable"


def test_data_manager_run_risk_snapshot_rebuild_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "risk": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.risk_snapshot_sync.RiskSnapshotRebuildService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(return_value={"status": "success"})
        service_cls.return_value = service_instance

        result = _run(
            manager.run_risk_snapshot_rebuild(
                exchanges=["SSE"],
                limit_per_exchange=10,
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()


def test_data_manager_get_research_company_profile_requires_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    with pytest.raises(RuntimeError, match="research storage is not initialized"):
        _run(manager.get_research_company_profile("600000.SH"))


def test_data_manager_get_research_company_profile_delegates_to_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_company_profile.return_value = {"instrument_id": "600000.SH"}
    manager.research_storage = storage

    result = _run(manager.get_research_company_profile("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    storage.get_company_profile.assert_called_once_with("600000.SH", include_snapshot=True)


def test_data_manager_get_research_company_profile_returns_optional_empty_bse_placeholder(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "company_profile": {
            "enabled": True,
            "optional_empty_exchanges": ["BSE"],
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_company_profile.return_value = None
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={
            "instrument_id": "430001.BJ",
            "symbol": "430001",
            "name": "北交样本",
            "exchange": "BSE",
            "type": "stock",
            "is_active": True,
        }
    )

    result = _run(manager.get_research_company_profile("430001.BJ"))

    assert result["instrument_id"] == "430001.BJ"
    assert result["source"] == "empty_placeholder"
    assert result["profile"]["missing_reason"] == "optional_empty_exchange"


def test_data_manager_get_research_financial_summary_delegates_to_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_summary.return_value = {"instrument_id": "600000.SH"}
    manager.research_storage = storage

    result = _run(manager.get_research_financial_summary("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    storage.get_financial_summary.assert_called_once_with("600000.SH", include_snapshot=True)


def test_data_manager_get_research_financial_summary_uses_financial_db_scope(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = _SplitFinancialReadStorage(
        summary={"source": "split_financial_db"}
    )

    result = _run(manager.get_research_financial_summary("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    assert result["source"] == "split_financial_db"


def test_data_manager_get_research_financial_summary_returns_optional_empty_bse_placeholder(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_summary": {
            "enabled": True,
            "optional_empty_exchanges": ["BSE"],
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_summary.return_value = None
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={
            "instrument_id": "430001.BJ",
            "symbol": "430001",
            "name": "北交样本",
            "exchange": "BSE",
            "type": "stock",
            "is_active": True,
        }
    )

    result = _run(manager.get_research_financial_summary("430001.BJ"))

    assert result["instrument_id"] == "430001.BJ"
    assert result["source"] == "empty_placeholder"
    assert result["summary"]["missing_reason"] == "optional_empty_exchange"


def test_data_manager_get_research_shareholders_requires_enabled_module(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {
            "enabled": False,
            "delivery_mode": "free_best_effort",
            "snapshot_api_requires_mode": "paid_high_availability",
            "allowed_scope": [
                "holder_count",
                "top10_holders",
                "reference_only_ownership_clues",
            ],
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = Mock()

    with pytest.raises(RuntimeError, match="research shareholders module is disabled"):
        _run(manager.get_research_shareholders("600000.SH"))


def test_data_manager_get_research_shareholders_requires_snapshot_gate(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {
            "enabled": True,
            "delivery_mode": "free_best_effort",
            "snapshot_api_requires_mode": "paid_high_availability",
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = Mock()

    with pytest.raises(RuntimeError, match="paid_high_availability"):
        _run(manager.get_research_shareholders("600000.SH"))


def test_data_manager_get_research_shareholders_delegates_to_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {
            "enabled": True,
            "delivery_mode": "paid_high_availability",
            "snapshot_api_requires_mode": "paid_high_availability",
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_shareholder_snapshot.return_value = {"instrument_id": "600000.SH"}
    manager.research_storage = storage

    result = _run(manager.get_research_shareholders("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    storage.get_shareholder_snapshot.assert_called_once_with(
        "600000.SH",
        include_snapshot=True,
    )


def test_data_manager_get_research_shareholders_returns_optional_empty_bse_placeholder(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {
            "enabled": True,
            "delivery_mode": "paid_high_availability",
            "snapshot_api_requires_mode": "paid_high_availability",
            "optional_empty_exchanges": ["BSE"],
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_shareholder_snapshot.return_value = None
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={
            "instrument_id": "430001.BJ",
            "symbol": "430001",
            "name": "北交样本",
            "exchange": "BSE",
            "type": "stock",
            "is_active": True,
        }
    )

    result = _run(manager.get_research_shareholders("430001.BJ"))

    assert result["instrument_id"] == "430001.BJ"
    assert result["coverage_status"] == "optional_empty_exchange"
    assert result["snapshot"]["missing_reason"] == "optional_empty_exchange"


def test_data_manager_get_research_shareholder_readiness_reports_blockers(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "SZSE"]
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {
            "enabled": False,
            "delivery_mode": "free_best_effort",
            "snapshot_api_requires_mode": "paid_high_availability",
            "allowed_scope": [
                "holder_count",
                "top10_holders",
                "reference_only_ownership_clues",
            ],
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_shareholder_snapshots.return_value = {
        "total": 2,
        "coverage_status_counts": {"reference_only": 2},
        "source_counts": {"akshare": 1, "cninfo": 1},
        "source_mode_counts": {"proxy_patch": 1, "direct": 1},
        "scope_counts": {
            "holder_count": 2,
            "reference_only_ownership_clues": 2,
            "top10_holders": 1,
        },
        "latest_updated_at": "2026-04-19T12:00:00+08:00",
        "latest_data_as_of": "2026-04-19T12:00:00+08:00",
    }
    storage.count_shareholder_snapshots_by_exchange.return_value = {
        "SSE": 1,
        "SZSE": 1,
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [
                {"instrument_id": "600519.SH", "type": "stock"},
                {"instrument_id": "600000.SH", "type": "stock"},
            ],
            [
                {"instrument_id": "000001.SZ", "type": "stock"},
            ],
        ]
    )

    result = _run(manager.get_research_shareholder_readiness())

    assert result["module_enabled"] is False
    assert result["delivery_mode"] == "free_best_effort"
    assert result["snapshot_api_enabled"] is False
    assert result["target_instrument_count"] == 3
    assert result["snapshot_total"] == 2
    assert result["missing_snapshot_count"] == 1
    assert result["required_scope"] == [
        "holder_count",
        "top10_holders",
        "reference_only_ownership_clues",
    ]
    assert result["source_counts"] == {"akshare": 1, "cninfo": 1}
    assert result["scope_counts"]["top10_holders"] == 1
    assert result["exchange_coverage"][0]["exchange"] == "SSE"
    assert result["exchange_coverage"][0]["coverage_ratio"] == 0.5
    assert result["exchange_coverage"][1]["exchange"] == "SZSE"
    assert result["exchange_coverage"][1]["coverage_ratio"] == 1.0
    assert result["scope_coverage"][1]["scope"] == "top10_holders"
    assert result["scope_coverage"][1]["coverage_ratio"] == pytest.approx(1 / 3)
    assert result["ready_for_paid_high_availability_rollout"] is False
    assert "shareholders_module_disabled" in result["blockers"]
    assert "shareholder_snapshot_coverage_incomplete" in result["blockers"]
    assert "required_scope_coverage_incomplete" in result["blockers"]
    assert "delivery_mode_gate_not_satisfied" in result["blockers"]
    storage.summarize_shareholder_snapshots.assert_called_once()
    storage.count_shareholder_snapshots_by_exchange.assert_called_once()


def test_data_manager_get_research_shareholder_readiness_excludes_optional_empty_bse(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "BSE"]
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {
            "enabled": True,
            "delivery_mode": "paid_high_availability",
            "snapshot_api_requires_mode": "paid_high_availability",
            "allowed_scope": [
                "holder_count",
                "top10_holders",
            ],
            "optional_empty_exchanges": ["BSE"],
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_shareholder_snapshots.return_value = {
        "total": 2,
        "coverage_status_counts": {"reference_only": 2},
        "source_counts": {"akshare": 2},
        "source_mode_counts": {"proxy_patch": 2},
        "scope_counts": {"holder_count": 2, "top10_holders": 2},
        "latest_updated_at": "2026-04-20T10:00:00+08:00",
        "latest_data_as_of": "2026-04-20T10:00:00+08:00",
    }
    storage.summarize_shareholder_snapshots_by_exchanges.return_value = {
        "total": 1,
        "coverage_status_counts": {"reference_only": 1},
        "source_counts": {"akshare": 1},
        "source_mode_counts": {"proxy_patch": 1},
        "scope_counts": {"holder_count": 1, "top10_holders": 1},
        "latest_updated_at": "2026-04-20T10:00:00+08:00",
        "latest_data_as_of": "2026-04-20T10:00:00+08:00",
    }
    storage.count_shareholder_snapshots_by_exchange.return_value = {
        "SSE": 1,
        "BSE": 0,
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [{"instrument_id": "600000.SH", "type": "stock", "exchange": "SSE"}],
            [{"instrument_id": "430001.BJ", "type": "stock", "exchange": "BSE"}],
        ]
    )

    result = _run(manager.get_research_shareholder_readiness())

    assert result["target_instrument_count"] == 1
    assert result["target_instruments_by_exchange"]["BSE"] == 0
    assert result["snapshot_total"] == 1
    assert result["ready_for_paid_high_availability_rollout"] is True
    assert result["blockers"] == []
    storage.summarize_shareholder_snapshots_by_exchanges.assert_called_once_with(["SSE"])


def test_data_manager_get_research_shareholder_readiness_optional_bse_does_not_mask_target_scope_gap(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "BSE"]
    mock_config.get_research_config.return_value.modules = {
        "shareholders": {
            "enabled": True,
            "delivery_mode": "paid_high_availability",
            "snapshot_api_requires_mode": "paid_high_availability",
            "allowed_scope": [
                "holder_count",
                "top10_holders",
            ],
            "optional_empty_exchanges": ["BSE"],
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_shareholder_snapshots.return_value = {
        "total": 2,
        "coverage_status_counts": {"reference_only": 2},
        "source_counts": {"akshare": 2},
        "source_mode_counts": {"proxy_patch": 2},
        "scope_counts": {"holder_count": 2, "top10_holders": 2},
        "latest_updated_at": "2026-04-20T10:00:00+08:00",
        "latest_data_as_of": "2026-04-20T10:00:00+08:00",
    }
    storage.summarize_shareholder_snapshots_by_exchanges.return_value = {
        "total": 1,
        "coverage_status_counts": {"reference_only": 1},
        "source_counts": {"akshare": 1},
        "source_mode_counts": {"proxy_patch": 1},
        "scope_counts": {"top10_holders": 1},
        "latest_updated_at": "2026-04-20T10:00:00+08:00",
        "latest_data_as_of": "2026-04-20T10:00:00+08:00",
    }
    storage.count_shareholder_snapshots_by_exchange.return_value = {
        "SSE": 1,
        "BSE": 1,
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [{"instrument_id": "600000.SH", "type": "stock", "exchange": "SSE"}],
            [{"instrument_id": "430001.BJ", "type": "stock", "exchange": "BSE"}],
        ]
    )

    result = _run(manager.get_research_shareholder_readiness())

    assert result["target_instrument_count"] == 1
    assert result["snapshot_total"] == 1
    assert result["scope_counts"] == {"top10_holders": 1}
    assert result["ready_for_paid_high_availability_rollout"] is False
    assert "required_scope_coverage_incomplete" in result["blockers"]


def test_data_manager_get_research_financial_statements_requires_enabled_module(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": False},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = Mock()

    with pytest.raises(RuntimeError, match="research financial_statements module is disabled"):
        _run(manager.get_research_financial_statements("600000.SH"))


def test_data_manager_get_research_financial_statements_delegates_to_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {"instrument_id": "600000.SH"}
    manager.research_storage = storage

    result = _run(manager.get_research_financial_statements("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    storage.get_financial_statement_bundle.assert_called_once_with(
        "600000.SH",
        include_statements=True,
        report_period=None,
    )


def test_data_manager_get_research_financial_statements_history_delegates_to_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundles.return_value = [
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "report_period": "2026-03-31",
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "report_period": "2025-12-31",
        },
    ]
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"}
    )

    result = _run(
        manager.get_research_financial_statements_history(
            "600000.SH",
            include_statements=False,
            rolling_quarters=10,
        )
    )

    assert result["period_count"] == 2
    assert result["report_periods"] == ["2026-03-31", "2025-12-31"]
    storage.get_financial_statement_bundles.assert_called_once_with(
        "600000.SH",
        include_statements=False,
        report_periods=None,
        limit=10,
    )


def test_data_manager_get_research_financial_statements_history_uses_financial_db_scope(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    class SplitFinancialStorage:
        def __init__(self):
            self.in_financial_scope = False

        def financial_database_scope(self):
            storage = self

            class Scope:
                def __enter__(self):
                    storage.in_financial_scope = True

                def __exit__(self, exc_type, exc, tb):
                    storage.in_financial_scope = False

            return Scope()

        def get_financial_statement_bundles(self, instrument_id, **kwargs):
            if not self.in_financial_scope:
                raise RuntimeError("no such table: financial_facts")
            return [
                {
                    "instrument_id": instrument_id,
                    "symbol": "600030",
                    "exchange": "SSE",
                    "report_period": "2025-12-31",
                }
            ]

    manager.research_storage = SplitFinancialStorage()
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={"instrument_id": "600030.SH", "symbol": "600030", "exchange": "SSE"}
    )

    result = _run(
        manager.get_research_financial_statements_history(
            "600030.SH",
            include_statements=False,
            rolling_quarters=12,
        )
    )

    assert result["period_count"] == 1
    assert result["report_periods"] == ["2025-12-31"]


def test_data_manager_get_research_financial_statements_readiness_uses_financial_db_scope(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE"]
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {
            "enabled": True,
            "history": {"baseline_report_period": "2024Q1", "rolling_min_quarters": 1},
            "storage": {"hot_anchor_policy": {"include_ttm_anchor_period": False}},
            "readiness": {"required_core_facts": []},
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = _SplitFinancialReadStorage()
    manager._count_research_target_instruments_by_exchange = AsyncMock(return_value=({"SSE": 1}, 1))
    manager._list_research_target_instrument_ids_by_exchange = AsyncMock(return_value=["600000.SH"])

    result = _run(manager.get_research_financial_statements_readiness())

    assert result["ready_for_rollout"] is True
    assert result["readiness"]["ready_for_rollout"] is True


def test_data_manager_get_research_financial_statements_history_includes_local_core_per_period(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }
    mock_config.get_research_config.return_value.sources = {
        "akshare": {
            "financial_statements": {
                "service_layers": {
                    "local_core": {
                        "enabled": True,
                        "mapping_version": "sina_ths_core_financial_facts.v1",
                    }
                }
            }
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundles.return_value = [
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "report_period": "2026-03-31",
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "report_period": "2025-12-31",
        },
    ]
    storage.get_financial_local_core_facts.side_effect = [
        {"ready": True, "facts": {"revenue": {"fact_value": 1}}, "missing_fields": []},
        {"ready": True, "facts": {"revenue": {"fact_value": 2}}, "missing_fields": []},
    ]
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"}
    )

    result = _run(
        manager.get_research_financial_statements_history(
            "600000.SH",
            report_periods=["2026-03-31", "2025-12-31"],
            requested_canonical_facts=["revenue"],
            profile="nonbank",
            include_local_core=True,
        )
    )

    assert result["items"][0]["service_layers"]["local_core"]["status"] == "passed"
    assert storage.get_financial_local_core_facts.call_count == 2
    storage.get_financial_statement_bundles.assert_called_once_with(
        "600000.SH",
        include_statements=False,
        report_periods=["2026-03-31", "2025-12-31"],
        limit=12,
    )


def test_data_manager_get_research_financial_statements_includes_local_core_layer(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }
    mock_config.get_research_config.return_value.sources = {
        "akshare": {
            "financial_statements": {
                "service_layers": {
                    "local_core": {
                        "enabled": True,
                        "mapping_version": "sina_ths_core_financial_facts.v1",
                    }
                }
            }
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "report_period": "2025-12-31",
    }
    storage.get_financial_local_core_facts.return_value = {
        "instrument_id": "600000.SH",
        "report_period": "2025-12-31",
        "profile": "nonbank",
        "mapping_version": "sina_ths_core_financial_facts.v1",
        "requested_canonical_facts": ["revenue"],
        "approved_canonical_facts": ["revenue"],
        "facts": {"revenue": {"fact_value": 100.0}},
        "missing_fields": [],
        "ready": True,
    }
    manager.research_storage = storage

    result = _run(
        manager.get_research_financial_statements(
            "600000.SH",
            report_period="2025-12-31",
            requested_canonical_facts=["revenue"],
            profile="nonbank",
            include_local_core=True,
        )
    )

    assert result["service_layers"]["local_core"]["status"] == "passed"
    assert result["service_layers"]["local_core"]["facts"]["revenue"]["fact_value"] == 100.0
    storage.get_financial_statement_bundle.assert_called_once_with(
        "600000.SH",
        include_statements=True,
        report_period="2025-12-31",
    )
    storage.get_financial_local_core_facts.assert_called_once_with(
        "600000.SH",
        report_period="2025-12-31",
        requested_canonical_facts=["revenue"],
        profile="nonbank",
        mapping_version="sina_ths_core_financial_facts.v1",
        include_history=True,
    )


def test_data_manager_get_research_financial_statements_auto_resolves_local_core_profile(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }
    mock_config.get_research_config.return_value.sources = {
        "akshare": {
            "financial_statements": {
                "service_layers": {
                    "local_core": {
                        "enabled": True,
                        "mapping_version": MAPPING_VERSION,
                    }
                }
            }
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600030.SH",
        "symbol": "600030",
        "exchange": "SSE",
        "report_period": "2025-12-31",
    }
    storage.get_industry_membership.return_value = {
        "taxonomy_system": "sw",
        "taxonomy_version": "sw_2021",
        "industry_code": "490101",
        "industry_name": "证券Ⅲ",
        "sw_l1_name": "非银金融",
        "sw_l2_name": "证券Ⅱ",
        "sw_l3_name": "证券Ⅲ",
    }
    storage.get_company_profile.return_value = None
    storage.get_financial_local_core_facts.return_value = {
        "instrument_id": "600030.SH",
        "report_period": "2025-12-31",
        "profile": "securities",
        "mapping_version": MAPPING_VERSION,
        "requested_canonical_facts": ["equity_parent"],
        "approved_canonical_facts": ["equity_parent"],
        "facts": {"equity_parent": {"fact_value": 293108725612.16}},
        "missing_fields": [],
        "ready": True,
    }
    manager.research_storage = storage

    result = _run(
        manager.get_research_financial_statements(
            "600030.SH",
            report_period="2025-12-31",
            requested_canonical_facts=["equity_parent"],
            include_local_core=True,
        )
    )

    local_core = result["service_layers"]["local_core"]
    assert local_core["status"] == "passed"
    assert local_core["profile_resolution"]["profile"] == "securities"
    assert local_core["profile_resolution"]["source"] == "industry_membership"
    storage.get_financial_local_core_facts.assert_called_once_with(
        "600030.SH",
        report_period="2025-12-31",
        requested_canonical_facts=["equity_parent"],
        profile="securities",
        mapping_version=MAPPING_VERSION,
        include_history=True,
    )


def test_data_manager_get_research_financial_statements_exposes_bank_industry_pack(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }
    mock_config.get_research_config.return_value.sources = {
        "akshare": {
            "financial_statements": {
                "service_layers": {
                    "local_core": {
                        "enabled": False,
                        "mapping_version": MAPPING_VERSION,
                    },
                    "industry_pack": {
                        "enabled": True,
                        "pack_version": "sina_ths_industry_financial_facts.v1",
                    },
                }
            }
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "report_period": "2026-03-31",
        "facts": {"revenue": 1.0},
    }
    storage.get_financial_local_core_facts.return_value = {
        "instrument_id": "600000.SH",
        "report_period": "2026-03-31",
        "profile": "bank",
        "mapping_version": MAPPING_VERSION,
        "facts": {
            "balance_sheet.loans_payments_behalf": {
                "fact_value": 200.0,
                "canonical_fact_name": "balance_sheet.loans_payments_behalf",
            },
        },
        "missing_fields": [],
        "ready": False,
    }
    storage.get_financial_numeric_facts.return_value = [
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "report_period": "2026-03-31",
            "fact_name": "吸收存款",
            "source": "cninfo",
            "source_mode": "direct",
            "fact_value": 10.0,
            "raw_fact": {},
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "report_period": "2026-03-31",
            "fact_name": "同业存放及其他金融机构存放款项",
            "source": "cninfo",
            "source_mode": "direct",
            "fact_value": 3.0,
            "raw_fact": {},
        },
    ]
    manager.research_storage = storage

    result = _run(
        manager.get_research_financial_statements(
            "600000.SH",
            report_period="2026-03-31",
            profile="bank",
            include_industry_facts=True,
        )
    )

    assert result["facts"] == {"revenue": 1.0}
    industry_pack = result["service_layers"]["industry_pack"]
    assert industry_pack["is_optional"] is True
    assert industry_pack["profile"] == "bank"
    assert (
        industry_pack["facts"]["balance_sheet.loans_payments_behalf"]["fact_value"]
        == 200.0
    )
    assert (
        industry_pack["facts"]["balance_sheet.deposits_and_deposits"]["fact_value"]
        == 13.0
    )
    _, kwargs = storage.get_financial_local_core_facts.call_args
    assert kwargs["profile"] == "bank"
    assert "balance_sheet.loans_payments_behalf" in kwargs["requested_canonical_facts"]
    assert "balance_sheet.deposits_and_deposits" not in kwargs["requested_canonical_facts"]
    storage.get_financial_numeric_facts.assert_called_once_with(
        "600000.SH",
        report_period="2026-03-31",
        include_history=True,
    )


def test_data_manager_get_research_financial_statements_exposes_securities_industry_pack(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }
    mock_config.get_research_config.return_value.sources = {
        "akshare": {
            "financial_statements": {
                "service_layers": {
                    "local_core": {"enabled": False, "mapping_version": MAPPING_VERSION},
                    "industry_pack": {
                        "enabled": True,
                        "pack_version": "sina_ths_industry_financial_facts.v1",
                    },
                }
            }
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600030.SH",
        "symbol": "600030",
        "exchange": "SSE",
        "report_period": "2026-03-31",
    }
    storage.get_financial_local_core_facts.return_value = {
        "instrument_id": "600030.SH",
        "report_period": "2026-03-31",
        "profile": "securities",
        "mapping_version": MAPPING_VERSION,
        "facts": {
            "balance_sheet.trade_financial_assets": {
                "fact_value": 20.0,
                "canonical_fact_name": "balance_sheet.trade_financial_assets",
            },
        },
        "missing_fields": [],
        "ready": False,
    }
    storage.get_financial_numeric_facts.return_value = [
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "report_period": "2026-03-31",
            "fact_name": "代理买卖证券款",
            "source": "cninfo",
            "source_mode": "direct",
            "fact_value": 100.0,
            "raw_fact": {},
        },
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "report_period": "2026-03-31",
            "fact_name": "净资本",
            "canonical_fact_name": "net_capital",
            "source": "cninfo",
            "source_mode": "direct",
            "fact_value": 157145566468.97,
            "raw_fact": {},
        },
        {
            "instrument_id": "600030.SH",
            "symbol": "600030",
            "exchange": "SSE",
            "report_period": "2026-03-31",
            "fact_name": "风险覆盖率",
            "canonical_fact_name": "risk_coverage_ratio",
            "source": "cninfo",
            "source_mode": "direct",
            "fact_value": 2.42,
            "raw_fact": {},
        }
    ]
    manager.research_storage = storage

    result = _run(
        manager.get_research_financial_statements(
            "600030.SH",
            report_period="2026-03-31",
            profile="securities",
            include_industry_facts=True,
        )
    )

    industry_pack = result["service_layers"]["industry_pack"]
    assert industry_pack["profile_pack_status"]["status"] == "approved"
    assert industry_pack["facts"]["balance_sheet.trade_financial_assets"]["fact_value"] == 20.0
    assert industry_pack["facts"]["balance_sheet.agent_trading_security"]["fact_value"] == 100.0
    assert industry_pack["facts"]["net_capital"]["fact_value"] == 157145566468.97
    assert industry_pack["facts"]["risk_coverage_ratio"]["fact_value"] == 2.42
    _, kwargs = storage.get_financial_local_core_facts.call_args
    assert kwargs["profile"] == "securities"
    assert "balance_sheet.trade_financial_assets" in kwargs["requested_canonical_facts"]
    assert "balance_sheet.agent_trading_security" not in kwargs["requested_canonical_facts"]
    assert "net_capital" not in kwargs["requested_canonical_facts"]


def test_data_manager_get_research_financial_statements_remote_extension_disabled_by_config(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {"enabled": True},
    }
    mock_config.get_research_config.return_value.sources = {
        "akshare": {
            "financial_statements": {
                "service_layers": {
                    "remote_extension": {
                        "enabled": False,
                        "source": "akshare",
                        "statement_interface": "eastmoney_report",
                    }
                }
            }
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "report_period": "2025-12-31",
    }
    manager.research_storage = storage

    result = _run(
        manager.get_research_financial_statements(
            "600000.SH",
            requested_canonical_facts=["eastmoney_only_metric"],
            allow_remote_extension=True,
        )
    )

    remote = result["service_layers"]["remote_extension"]
    assert remote["status"] == "disabled_by_config"
    assert remote["missing_fields"][0]["reason"] == "remote_extension_disabled_by_config"


def test_data_manager_get_research_financial_statements_returns_optional_empty_bse_placeholder(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {
            "enabled": True,
            "optional_empty_exchanges": ["BSE"],
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_financial_statement_bundle.return_value = None
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={
            "instrument_id": "430001.BJ",
            "symbol": "430001",
            "name": "北交样本",
            "exchange": "BSE",
            "type": "stock",
            "is_active": True,
        }
    )

    result = _run(manager.get_research_financial_statements("430001.BJ"))

    assert result["instrument_id"] == "430001.BJ"
    assert result["source"] == "empty_placeholder"
    assert result["facts"]["missing_reason"] == "optional_empty_exchange"


def test_data_manager_get_research_valuation_history_requires_enabled_module(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": False},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = Mock()

    with pytest.raises(RuntimeError, match="research valuation module is disabled"):
        _run(manager.get_research_valuation_history("600000.SH"))


def test_data_manager_get_research_valuation_history_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_valuation_history_rows.return_value = [
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "as_of_date": "2026-04-18",
            "calc_method": "valuation_history_builtin",
            "calc_version": "valuation_history.v1",
            "parameter_hash": "hash",
            "details": {},
        }
    ]
    manager.research_storage = storage

    result = _run(manager.get_research_valuation_history("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    storage.get_valuation_history_rows.assert_called_once()


def test_data_manager_get_research_relative_valuation_requires_enabled_module(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": False},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = Mock()

    with pytest.raises(RuntimeError, match="research valuation module is disabled"):
        _run(manager.get_research_relative_valuation("600000.SH"))


def test_data_manager_get_research_dcf_valuation_requires_enabled_module(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": False},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = Mock()

    with pytest.raises(RuntimeError, match="research valuation module is disabled"):
        _run(manager.get_research_dcf_valuation("600000.SH"))


def test_data_manager_get_research_dcf_assumptions_returns_lineage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True, "dcf": {"professional": {"enabled": True}}},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(manager.get_research_dcf_assumptions(market="SSE", currency="CNY"))

    assumptions = {item["assumption_key"]: item for item in result["assumptions"]}
    assert result["market"] == "SSE"
    assert assumptions["risk_free_rate_rmb_10y"]["tenor"] == "10Y"
    assert assumptions["risk_free_rate_rmb_10y"]["currency"] == "CNY"
    assert assumptions["risk_free_rate_rmb_10y"]["source"] == "manual_config"
    assert assumptions["risk_free_rate_rmb_10y"]["quality_flag"] == "configured_fallback"
    assert assumptions["risk_free_rate_rmb_10y"]["fallback_used"] is True
    assert assumptions["risk_free_rate_rmb_10y"]["lineage_hash"]
    assert result["source_registry"][0]["source_profile"]


def test_data_manager_refresh_research_dcf_assumptions_is_explicit_and_local_first(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True, "dcf": {"professional": {"enabled": True}}},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(
        manager.refresh_research_dcf_assumptions(
            source_profile="china_bond_10y",
            timeout_seconds=5,
            dry_run=False,
        )
    )

    assert result["status"] == "unsupported"
    assert result["refreshed"] is False
    assert result["source_results"][0]["timeout_seconds"] == 5
    assert result["diagnostics"]["remote_fetch_performed"] is False
    assert result["diagnostics"]["hidden_refresh_inside_dcf"] is False


def test_data_manager_get_research_dcf_model_profiles_returns_registry(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True, "dcf": {"professional": {"enabled": True}}},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    result = _run(manager.get_research_dcf_model_profiles())

    profiles = {item["model_profile"]: item for item in result["model_profiles"]}
    assert profiles["nonfinancial_fcff.v1"]["implementation_status"] == "implemented"
    assert "capital_expenditure" in profiles["nonfinancial_fcff.v1"]["required_fields"]
    assert profiles["bank_residual_income.v1"]["implementation_status"] == "implemented"
    assert "shares_outstanding" in profiles["bank_residual_income.v1"]["required_fields"]
    assert profiles["broker_excess_capital.v1"]["implementation_status"] == "implemented"
    assert "net_capital" in profiles["broker_excess_capital.v1"]["required_fields"]
    assert "shares_outstanding" in profiles["broker_excess_capital.v1"]["required_fields"]
    assert profiles["cyclical_fcff_midcycle.v1"]["implementation_status"] == "implemented"
    assert "capital_expenditure" in profiles["cyclical_fcff_midcycle.v1"]["required_fields"]


def test_data_manager_get_research_dcf_input_gaps_reports_missing_required_fields(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True, "dcf": {"professional": {"enabled": True}}},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "industry": "制造业",
        }
    )
    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600000.SH",
        "latest_facts": {
            "revenue": 1000.0,
            "operating_profit": 120.0,
            "data_available_date": "2026-03-31",
        },
    }
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(manager.get_research_dcf_input_gaps("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    assert result["ready"] is False
    missing = {item["field"]: item for item in result["missing_fields"]}
    assert "capital_expenditure" in missing
    assert missing["capital_expenditure"]["candidate_primary_source"] == "official_cash_flow_statement"
    assert missing["capital_expenditure"]["refresh_eligible"] is False


def test_data_manager_get_research_dcf_input_gaps_uses_financial_db_scope(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True, "dcf": {"professional": {"enabled": True}}},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "industry": "制造业",
        }
    )
    manager.research_storage = _SplitFinancialReadStorage(
        bundle={
            "latest_facts": {
                "revenue": 1000.0,
                "operating_profit": 120.0,
                "data_available_date": "2026-03-31",
            }
        }
    )

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(manager.get_research_dcf_input_gaps("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    assert result["ready"] is False


def test_data_manager_get_research_dcf_readiness_reports_profile_status(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True, "dcf": {"professional": {"enabled": True}}},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "industry": "制造业",
        }
    )
    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600000.SH",
        "latest_facts": {
            "revenue": 1000.0,
            "operating_profit": 120.0,
            "capital_expenditure": 30.0,
            "data_available_date": "2026-03-31",
        },
    }
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(manager.get_research_dcf_readiness("600000.SH"))

    profiles = {item["model_profile"]: item for item in result["profiles"]}
    assert result["ready"] is True
    assert profiles["nonfinancial_fcff.v1"]["ready"] is True
    assert profiles["bank_residual_income.v1"]["ready"] is False
    assert "missing_equity" in profiles["bank_residual_income.v1"]["blockers"]
    assert "missing_net_income" in profiles["bank_residual_income.v1"]["blockers"]
    assert "missing_shares_outstanding" in profiles["bank_residual_income.v1"]["blockers"]
    assert profiles["broker_excess_capital.v1"]["ready"] is False
    assert "missing_equity" in profiles["broker_excess_capital.v1"]["blockers"]
    assert "missing_net_income" in profiles["broker_excess_capital.v1"]["blockers"]
    assert "missing_net_capital" in profiles["broker_excess_capital.v1"]["blockers"]
    assert "missing_shares_outstanding" in profiles["broker_excess_capital.v1"]["blockers"]
    assert profiles["cyclical_fcff_midcycle.v1"]["ready"] is True
    assert result["coverage_diagnostics"]["ready_profile_count"] == 2


def test_data_manager_get_research_dcf_readiness_uses_financial_db_scope(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {"enabled": True, "dcf": {"professional": {"enabled": True}}},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "industry": "制造业",
        }
    )
    manager.research_storage = _SplitFinancialReadStorage(
        bundle={
            "latest_facts": {
                "revenue": 1000.0,
                "operating_profit": 120.0,
                "capital_expenditure": 30.0,
                "data_available_date": "2026-03-31",
            }
        }
    )

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(manager.get_research_dcf_readiness("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    profiles = {item["model_profile"]: item for item in result["profiles"]}
    assert profiles["cyclical_fcff_midcycle.v1"]["ready"] is True
    assert result["coverage_diagnostics"]["ready_profile_count"] == 2


def test_data_manager_get_research_dcf_valuation_uses_financial_db_scope(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {
            "enabled": True,
            "dcf": {
                "beta": {"enabled": False},
                "professional": {"enabled": True},
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "industry_name": "食品饮料",
        }
    )
    manager.db_ops.get_daily_data = AsyncMock(return_value=pd.DataFrame([{"close": 12.0}]))
    manager.research_storage = _SplitFinancialReadStorage(
        bundle={
            "latest_facts": {
                "report_period": "2025-12-31",
                "data_available_date": "2026-03-30",
                "revenue": 1000.0,
                "operating_profit": 180.0,
                "capital_expenditure": 60.0,
                "shares_outstanding": 10.0,
            }
        }
    )

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(
            manager.get_research_dcf_valuation(
                "600519.SH",
                valuation_date="2026-04-18",
                include_sensitivity=False,
                include_model_comparison=False,
            )
        )

    assert result["instrument_id"] == "600519.SH"
    assert result["status"] in {"success", "partial"}


def test_data_manager_dcf_futures_context_falls_back_to_industry_mapping(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {
            "enabled": True,
            "dcf": {"beta": {"enabled": False}, "professional": {"enabled": True}},
        },
        "commodity_market_data": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    research_storage = Mock()
    research_storage.get_industry_membership.return_value = {
        "instrument_id": "601088.SH",
        "industry_code": "煤炭",
        "industry_name": "煤炭",
        "sw_l1_name": "煤炭",
    }
    futures_storage = Mock()

    def _get_exposure_mappings(*, scope_type, scope_id):
        if scope_type == "industry" and scope_id == "煤炭":
            return [
                {
                    "mapping_id": "industry-coal-j",
                    "scope_type": "industry",
                    "scope_id": "煤炭",
                    "product_name": "焦煤",
                    "revenue_series_id": "CNF.JM.DCE.main",
                    "cost_series_ids": [],
                    "spread_ids": [],
                    "direction": "positive",
                    "transmission_strength": "medium",
                    "lag_days": 0,
                    "confidence": "medium",
                }
            ]
        return []

    futures_storage.get_exposure_mappings.side_effect = _get_exposure_mappings
    futures_storage.get_cycle_diagnostics.return_value = [
        {
            "series_id": "CNF.JM.DCE.main",
            "as_of_date": "2026-06-25",
            "lookback_years": 10,
            "latest_price": 1238.5,
            "mean_price": 1484.98,
            "median_price": 1318.0,
            "percentile": 0.3379,
            "mean_deviation_pct": -0.166,
            "cycle_state": "normal",
            "history_coverage_ratio": 1.0,
            "observation_count": 2427,
        }
    ]
    manager.research_storage = research_storage
    manager.futures_storage = futures_storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        context = _run(manager._get_dcf_futures_cycle_context("601088.SH"))

    assert context["mapping_scope"] == "industry"
    assert context["mapping_scope_id"] == "煤炭"
    assert context["selected_series_id"] == "CNF.JM.DCE.main"
    assert context["commodity_price_assumption"] == 1238.5
    assert context["midcycle_price_candidate"] == 1484.98
    assert context["diagnostics_summary"]["10"]["percentile"] == 0.3379
    futures_storage.get_exposure_mappings.assert_any_call(
        scope_type="instrument",
        scope_id="601088.SH",
    )
    futures_storage.get_exposure_mappings.assert_any_call(
        scope_type="industry",
        scope_id="煤炭",
    )


def test_data_manager_dcf_bounded_cache_hits_and_invalidates_on_price_change(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {
            "enabled": True,
            "dcf": {
                "beta": {"enabled": False},
                "professional": {
                    "enabled": True,
                    "bounded_cache": {"enabled": True, "ttl_hours": 1, "max_entries": 8},
                    "workbook": {"artifact_dir": str(tmp_path / "workbooks")},
                },
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "industry_name": "食品饮料",
        }
    )
    manager.db_ops.get_daily_data = AsyncMock(
        side_effect=[
            pd.DataFrame([{"close": 12.0}]),
            pd.DataFrame([{"close": 12.0}]),
            pd.DataFrame([{"close": 13.0}]),
        ]
    )
    storage = Mock()
    storage.get_financial_statement_bundle.return_value = {
        "instrument_id": "600519.SH",
        "latest_facts": {
            "report_period": "2025-12-31",
            "data_available_date": "2026-03-30",
            "revenue": 1000.0,
            "operating_profit": 180.0,
            "capital_expenditure": 60.0,
            "shares_outstanding": 10.0,
        },
    }
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        first = _run(
            manager.get_research_dcf_valuation(
                "600519.SH",
                valuation_date="2026-04-18",
                include_workbook=True,
            )
        )
        second = _run(
            manager.get_research_dcf_valuation(
                "600519.SH",
                valuation_date="2026-04-18",
                include_workbook=True,
            )
        )
        third = _run(
            manager.get_research_dcf_valuation(
                "600519.SH",
                valuation_date="2026-04-18",
                include_workbook=True,
            )
        )

    assert first["cache_info"]["cache_hit"] is False
    assert second["cache_info"]["cache_hit"] is True
    assert second["cache_info"]["cache_key"] == first["cache_info"]["cache_key"]
    assert first["cache_info"]["input_hash"] == first["input_hash"]
    assert first["cache_info"]["parameter_hash"] == first["parameter_hash"]
    assert first["cache_info"]["cached_at"]
    assert first["cache_info"]["expires_at"]
    assert first["cache_info"]["entry_count"] >= 1
    assert second["cache_info"]["entry_count"] >= 1
    assert third["cache_info"]["cache_hit"] is False
    assert third["cache_info"]["cache_key"] != first["cache_info"]["cache_key"]
    assert first["workbook"]["workbook_available"] is True
    cache_entry = manager._dcf_run_cache[first["cache_info"]["cache_key"]]
    assert cache_entry["summary"]["input_hash"] == first["input_hash"]
    assert cache_entry["summary"]["parameter_hash"] == first["parameter_hash"]
    assert cache_entry["summary"]["assumption_snapshot"]
    assert cache_entry["summary"]["forecast_rows"]
    assert cache_entry["summary"]["sensitivity"]
    assert cache_entry["summary"]["workbook"]["workbook_artifact_id"]
    storage.get_valuation_history_rows.assert_not_called()
    storage.get_latest_valuation_history_row.assert_not_called()


def test_data_manager_relative_valuation_skips_peers_for_reference_only_membership(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {
            "enabled": True,
            "relative": {
                "require_authoritative": True,
                "benchmark_field": "sw_l2_code",
                "min_peer_count": 1,
                "max_peer_rows": 20,
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
        }
    )
    storage = Mock()
    storage.get_latest_valuation_history_row.return_value = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "as_of_date": "2026-04-18",
        "close_price": 10.0,
        "market_cap": 2000.0,
        "pe_ratio": 6.0,
        "pb_ratio": 0.6,
        "ps_ratio": 1.2,
        "data_as_of": "2026-04-18T18:30:00",
    }
    storage.get_industry_membership.return_value = {
        "taxonomy_system": "sw",
        "taxonomy_version": "sw_2021",
        "mapping_status": "reference_only",
        "sw_l2_code": "801780.SI",
        "sw_l2_name": "股份制银行",
    }
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(manager.get_research_relative_valuation("600000.SH"))

    assert result["status"] == "benchmark_unavailable"
    assert result["missing_reason"] == "authoritative_sw_l2_membership_required"
    assert result["benchmark_field"] == "sw_l2_code"
    assert result["benchmark_code"] == "801780.SI"
    storage.get_latest_peer_valuation_rows.assert_not_called()


def test_data_manager_relative_valuation_attaches_industry_index_benchmark(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "valuation": {
            "enabled": True,
            "relative": {
                "require_authoritative": True,
                "benchmark_field": "sw_l2_code",
                "min_peer_count": 1,
                "max_peer_rows": 20,
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops = Mock()
    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
        }
    )
    storage = Mock()
    storage.get_latest_valuation_history_row.return_value = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "as_of_date": "2026-04-18",
        "close_price": 10.0,
        "market_cap": 2000.0,
        "pe_ratio": 6.0,
        "pb_ratio": 0.6,
        "ps_ratio": 1.2,
        "data_as_of": "2026-04-18T18:30:00",
    }
    storage.get_industry_membership.return_value = {
        "taxonomy_system": "sw",
        "taxonomy_version": "sw_2021",
        "mapping_status": "authoritative",
        "sw_l2_code": "340300",
        "sw_l2_name": "饮料乳品",
        "sw_l2_index_code": "801124",
    }
    storage.get_latest_peer_valuation_rows.return_value = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "as_of_date": "2026-04-18",
            "pe_ratio": 8.0,
            "pb_ratio": 0.8,
            "ps_ratio": 1.8,
        }
    ]
    storage.get_latest_industry_index_analysis.return_value = {
        "sw_index_code": "801124",
        "sw_index_name": "饮料乳品",
        "trade_date": "2026-04-24",
        "pe": 18.14,
        "pb": 1.29,
    }
    manager.research_storage = storage

    with patch("data_manager.asyncio.to_thread", side_effect=_sync_to_thread):
        result = _run(manager.get_research_relative_valuation("600000.SH"))

    assert result["status"] == "success"
    assert result["industry_index_benchmark"]["sw_index_code"] == "801124"
    assert result["industry_index_benchmark"]["index_analysis"]["pe"] == 18.14
    storage.get_latest_industry_index_analysis.assert_called_once_with(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        sw_index_code="801124",
        include_payload=False,
    )


def test_data_manager_get_research_valuation_readiness_reports_blockers(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "SZSE"]
    mock_config.get_research_config.return_value.modules = {
        "valuation": {
            "enabled": False,
            "relative": {
                "require_authoritative": True,
                "benchmark_level": 2,
                "benchmark_field": "sw_l2_code",
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_valuation_history.return_value = {
        "total": 2,
        "source_counts": {"local_quotes_financial_facts": 2},
        "source_mode_counts": {"derived": 2},
        "calc_method_counts": {"valuation_history_builtin": 2},
        "calc_version_counts": {"valuation_history.v1": 2},
        "latest_as_of_date": "2026-04-18",
        "latest_updated_at": "2026-04-18T18:30:00+08:00",
        "latest_data_as_of": "2026-04-18T18:30:00+08:00",
    }
    storage.count_valuation_history_by_exchange.return_value = {
        "SSE": 1,
        "SZSE": 1,
    }
    storage.summarize_valuation_input_coverage.return_value = {
        "instrument_count": 2,
        "market_cap_count": 2,
        "shares_outstanding_count": 0,
        "usable_input_count": 2,
        "source_counts": {"manual": 2},
        "source_mode_counts": {"local": 2},
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [
                {"instrument_id": "600519.SH", "type": "stock"},
                {"instrument_id": "600000.SH", "type": "stock"},
            ],
            [
                {"instrument_id": "000001.SZ", "type": "stock"},
            ],
        ]
    )
    manager.get_research_industry_standard_readiness = AsyncMock(
        return_value={
            "relative_valuation": {
                "ready": False,
                "blockers": ["authoritative_membership_coverage_incomplete"],
            }
        }
    )

    result = _run(manager.get_research_valuation_readiness())

    assert result["module_enabled"] is False
    assert result["target_instrument_count"] == 3
    assert result["valuation_history_total"] == 2
    assert result["missing_valuation_history_count"] == 1
    assert result["valuation_input_total"] == 2
    assert result["missing_valuation_input_count"] == 1
    assert result["source_counts"] == {"local_quotes_financial_facts": 2}
    assert result["exchange_coverage"][0]["exchange"] == "SSE"
    assert result["exchange_coverage"][0]["coverage_ratio"] == 0.5
    assert result["relative_valuation"]["benchmark_field"] == "sw_l2_code"
    assert result["relative_valuation"]["ready"] is False
    assert result["ready_for_rollout"] is False
    assert "valuation_module_disabled" in result["blockers"]
    assert "valuation_history_coverage_incomplete" in result["blockers"]
    assert "valuation_input_coverage_incomplete" in result["blockers"]
    assert "authoritative_membership_coverage_incomplete" in result["blockers"]
    storage.summarize_valuation_history.assert_called_once()
    storage.count_valuation_history_by_exchange.assert_called_once()
    manager.get_research_industry_standard_readiness.assert_awaited_once_with()


def test_data_manager_get_research_financial_statements_readiness_reports_blockers(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE"]
    mock_config.get_research_config.return_value.modules = {
        "financial_statements": {
            "enabled": True,
            "history": {"baseline_report_period": "2026Q1", "rolling_min_quarters": 1},
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.validate_financial_statement_readiness.return_value = {
        "status": "not_ready",
        "ready_for_rollout": False,
        "blockers": ["missing_core_facts"],
        "gaps": {
            "period_coverage": {"coverage_ratio": 0.0},
            "core_facts": {"coverage_ratio": 0.0},
            "source_files": {"parser_version_distribution": {}},
            "tier_coverage": {},
        },
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        return_value=[{"instrument_id": "600519.SH", "type": "stock"}]
    )

    result = _run(manager.get_research_financial_statements_readiness())

    assert result["module_enabled"] is True
    assert result["target_instrument_count"] == 1
    assert result["ready_for_rollout"] is False
    assert result["blockers"] == ["missing_core_facts"]
    storage.validate_financial_statement_readiness.assert_called_once()


def test_data_manager_valuation_readiness_requires_financial_readiness(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE"]
    mock_config.get_research_config.return_value.modules = {
        "valuation": {
            "enabled": True,
            "relative": {
                "require_authoritative": True,
                "benchmark_level": 2,
                "benchmark_field": "sw_l2_code",
                "metric_variants": ["pe_ttm", "pb_mrq", "ps_ttm"],
            },
        },
        "financial_statements": {
            "enabled": True,
            "history": {"baseline_report_period": "2026Q1", "rolling_min_quarters": 1},
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_valuation_history.return_value = {
        "total": 1,
        "source_counts": {"local_quotes_financial_facts": 1},
        "source_mode_counts": {"derived": 1},
        "calc_method_counts": {"valuation_history_builtin": 1},
        "calc_version_counts": {"valuation.v1": 1},
        "latest_as_of_date": "2026-04-18",
    }
    storage.count_valuation_history_by_exchange.return_value = {"SSE": 1}
    storage.summarize_valuation_metric_coverage.return_value = {
        "instrument_count": 1,
        "metrics": {"pe_ttm": {"covered_instruments": 1, "coverage_ratio": 1.0}},
    }
    storage.summarize_valuation_input_coverage.return_value = {
        "instrument_count": 1,
        "market_cap_count": 1,
        "shares_outstanding_count": 0,
        "usable_input_count": 1,
    }
    storage.validate_financial_statement_readiness.return_value = {
        "status": "not_ready",
        "ready_for_rollout": False,
        "blockers": ["missing_core_facts"],
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        return_value=[{"instrument_id": "600519.SH", "type": "stock"}]
    )
    manager.get_research_industry_standard_readiness = AsyncMock(
        return_value={"relative_valuation": {"ready": True, "blockers": []}}
    )

    result = _run(manager.get_research_valuation_readiness())

    assert result["ready_for_rollout"] is False
    assert "financial_statement_readiness_incomplete" in result["blockers"]
    assert result["financial_statements"]["ready_for_rollout"] is False
    assert result["metric_coverage"]["metrics"]["pe_ttm"]["coverage_ratio"] == 1.0


def test_data_manager_get_research_metadata_readiness_reports_domain_blockers(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "SZSE"]
    mock_config.get_research_config.return_value.modules = {
        "analyst_forecasts": {"enabled": False},
        "research_reports": {"enabled": False},
        "sentiment_events": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_analyst_forecasts.return_value = {
        "row_total": 2,
        "instrument_total": 2,
        "source_counts": {"akshare": 2},
        "source_mode_counts": {"proxy_patch": 2},
        "latest_item_date": "2026-04-18",
        "latest_updated_at": "2026-04-18T18:30:00+08:00",
        "latest_data_as_of": "2026-04-18T18:30:00+08:00",
    }
    storage.count_analyst_forecasts_by_exchange.return_value = {
        "SSE": 1,
        "SZSE": 1,
    }
    storage.summarize_research_reports.return_value = {
        "row_total": 1,
        "instrument_total": 1,
        "source_counts": {"akshare": 1},
        "source_mode_counts": {"proxy_patch": 1},
        "institution_name_counts": {"示例证券": 1},
        "rating_counts": {"买入": 1},
        "latest_item_date": "2026-04-18",
        "latest_updated_at": "2026-04-18T18:30:00+08:00",
        "latest_data_as_of": "2026-04-18T18:30:00+08:00",
    }
    storage.count_research_reports_by_exchange.return_value = {
        "SSE": 1,
        "SZSE": 0,
    }
    storage.summarize_sentiment_events.return_value = {
        "row_total": 0,
        "instrument_total": 0,
        "source_counts": {},
        "source_mode_counts": {},
        "event_type_counts": {},
        "severity_counts": {},
        "latest_item_date": None,
        "latest_updated_at": None,
        "latest_data_as_of": None,
    }
    storage.count_sentiment_events_by_exchange.return_value = {}
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [
                {"instrument_id": "600519.SH", "type": "stock"},
                {"instrument_id": "600000.SH", "type": "stock"},
            ],
            [{"instrument_id": "000001.SZ", "type": "stock"}],
            [
                {"instrument_id": "600519.SH", "type": "stock"},
                {"instrument_id": "600000.SH", "type": "stock"},
            ],
            [{"instrument_id": "000001.SZ", "type": "stock"}],
            [
                {"instrument_id": "600519.SH", "type": "stock"},
                {"instrument_id": "600000.SH", "type": "stock"},
            ],
            [{"instrument_id": "000001.SZ", "type": "stock"}],
        ]
    )

    result = _run(manager.get_research_metadata_readiness())

    assert result["domain_count"] == 3
    assert result["ready_domain_count"] == 0
    assert result["ready_for_rollout"] is False
    analyst = result["domains"][0]
    reports = result["domains"][1]
    sentiment = result["domains"][2]
    assert analyst["domain"] == "analyst_forecasts"
    assert analyst["instrument_total"] == 2
    assert "analyst_forecasts_module_disabled" in analyst["blockers"]
    assert "analyst_forecast_coverage_incomplete" in analyst["blockers"]
    assert reports["extra_counts"]["institution_name_counts"] == {"示例证券": 1}
    assert "research_reports_module_disabled" in reports["blockers"]
    assert "research_report_coverage_incomplete" in reports["blockers"]
    assert sentiment["module_enabled"] is True
    assert "no_sentiment_events" in sentiment["blockers"]
    assert "sentiment_event_coverage_incomplete" in sentiment["blockers"]
    assert "analyst_forecasts:analyst_forecasts_module_disabled" in result["blockers"]
    storage.summarize_analyst_forecasts.assert_called_once()
    storage.count_sentiment_events_by_exchange.assert_called_once()


def test_data_manager_get_research_industry_delegates_to_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "industry": {"enabled": True},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_industry_membership.return_value = {"instrument_id": "600000.SH"}
    manager.research_storage = storage

    result = _run(manager.get_research_industry("600000.SH"))

    assert result["instrument_id"] == "600000.SH"
    storage.get_industry_membership.assert_called_once_with(
        "600000.SH",
        include_snapshot=True,
    )


def test_data_manager_get_research_industry_returns_optional_empty_bse_placeholder(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "industry": {"enabled": True, "optional_empty_exchanges": ["BSE"]},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.get_industry_membership.return_value = None
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={
            "instrument_id": "430001.BJ",
            "symbol": "430001",
            "name": "北交样本",
            "exchange": "BSE",
            "type": "stock",
            "is_active": True,
        }
    )

    result = _run(manager.get_research_industry("430001.BJ"))

    assert result["instrument_id"] == "430001.BJ"
    assert result["mapping_status"] == "optional_empty_exchange"
    assert result["membership"]["missing_reason"] == "optional_empty_exchange"


def test_data_manager_get_research_industry_standard_readiness_reports_blockers(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "SZSE"]
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "official_mapping": {
                    "cache_max_age_days": 7,
                    "minimum_mapping_rows": 2,
                    "minimum_mapped_rows": 1,
                },
            },
        },
        "valuation": {
            "enabled": False,
            "relative": {
                "require_authoritative": True,
                "benchmark_level": 2,
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_official_industry_code_mappings.return_value = {
        "mapped": 2,
        "unmapped": 1,
    }
    recent_ts = datetime.now(timezone(timedelta(hours=8))).isoformat()
    storage.get_latest_official_industry_code_mapping_cache_info.return_value = {
        "source": "akshare",
        "source_mode": "proxy_patch",
        "built_at": recent_ts,
        "updated_at": recent_ts,
    }
    storage.summarize_official_industry_classifications.return_value = {
        "total": 2,
        "counts": {"mapped": 2, "unmapped": 0},
        "latest_updated_at": "2026-04-19T12:00:00+08:00",
        "latest_official_update_time": "2026-04-19T11:59:00+08:00",
    }
    storage.summarize_industry_memberships.return_value = {
        "total": 2,
        "counts": {"authoritative": 2, "reference_only": 0},
        "latest_updated_at": "2026-04-19T12:00:00+08:00",
        "latest_data_as_of": "2026-04-19T12:00:00+08:00",
    }
    storage.count_industry_memberships_by_exchange.return_value = {
        "SSE": 1,
        "SZSE": 1,
    }
    storage.summarize_unmapped_official_industry_code_backlog.return_value = {
        "official_code_total": 1,
        "current_classification_total": 1,
    }
    storage.list_unmapped_official_industry_code_backlog.return_value = [
        {
            "official_industry_code": "480301",
            "best_taxonomy_industry_code": "857831.SI",
            "current_classification_count": 1,
            "impacted_exchange_counts": {"SZSE": 1},
            "sample_instruments": ["000001.SZ"],
        }
    ]
    storage.summarize_industry_index_analysis_daily.return_value = {
        "total": 2,
        "distinct_index_codes": 2,
        "latest_trade_date": "2026-04-24",
        "latest_updated_at": "2026-04-25T12:00:00+08:00",
        "index_type_counts": {"一级行业": {"rows": 2, "codes": 2}},
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [
                {"instrument_id": "600519.SH", "type": "stock"},
                {"instrument_id": "600000.SH", "type": "stock"},
            ],
            [
                {"instrument_id": "000001.SZ", "type": "stock"},
            ],
        ]
    )
    manager.get_research_official_mapping_override_review = AsyncMock(
        return_value={
            "configured_override_total": 1,
            "ready_candidate_total": 1,
            "applied_override_total": 0,
            "pending_manual_override_total": 1,
            "status_counts": {"ready_candidate_pending_config": 1},
            "items": [
                {
                    "official_industry_code": "480301",
                    "review_status": "ready_candidate_pending_config",
                    "status_reason": "ready_candidate_not_yet_configured",
                }
            ],
        }
    )

    result = _run(manager.get_research_industry_standard_readiness())

    assert result["taxonomy_system"] == "sw"
    assert result["target_instrument_count"] == 3
    assert result["official_mapping_cache"]["fresh"] is True
    assert result["official_mapping_cache"]["meets_minimum_rows"] is True
    assert result["official_mapping_cache"]["meets_minimum_mapped_rows"] is True
    assert result["industry_standard_ready"] is False
    assert "authoritative_membership_coverage_incomplete" in result["blockers"]
    assert "official_classification_coverage_incomplete" not in result["blockers"]
    assert (
        "unmapped_official_code_backlog_impacts_current_classifications"
        not in result["blockers"]
    )
    assert "official_override_review_requires_attention" not in result["blockers"]
    assert result["unmapped_backlog"]["official_code_total"] == 1
    assert result["unmapped_backlog"]["current_classification_total"] == 1
    assert result["unmapped_backlog"]["top_items"][0]["official_industry_code"] == "480301"
    assert result["override_review"]["requires_attention"] is True
    assert result["override_review"]["pending_manual_override_total"] == 1
    assert result["override_review"]["top_items"][0]["official_industry_code"] == "480301"
    assert result["index_analysis"]["total"] == 2
    assert result["index_analysis"]["latest_trade_date"] == "2026-04-24"


def test_data_manager_get_research_industry_standard_readiness_excludes_optional_empty_bse(
    tmp_path,
):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.markets = ["SSE", "BSE"]
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "optional_empty_exchanges": ["BSE"],
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "official_mapping": {
                    "cache_max_age_days": 7,
                    "minimum_mapping_rows": 1,
                    "minimum_mapped_rows": 1,
                },
            },
        },
        "valuation": {
            "enabled": False,
            "relative": {
                "require_authoritative": True,
                "benchmark_level": 2,
            },
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_official_industry_code_mappings.return_value = {
        "mapped": 1,
        "unmapped": 0,
    }
    storage.get_latest_official_industry_code_mapping_cache_info.return_value = {
        "source": "akshare",
        "source_mode": "proxy_patch",
        "built_at": "2026-04-20T12:00:00+08:00",
        "updated_at": "2026-04-20T12:00:00+08:00",
    }
    storage.summarize_official_industry_classifications.return_value = {
        "total": 1,
        "counts": {"mapped": 1},
        "latest_updated_at": "2026-04-20T12:00:00+08:00",
        "latest_official_update_time": "2026-04-20",
    }
    storage.summarize_industry_memberships.return_value = {
        "total": 1,
        "counts": {"authoritative": 1},
        "latest_updated_at": "2026-04-20T12:00:00+08:00",
        "latest_data_as_of": "2026-04-20",
    }
    storage.count_industry_memberships_by_exchange.return_value = {
        "SSE": 1,
    }
    storage.summarize_unmapped_official_industry_code_backlog.return_value = {
        "official_code_total": 0,
        "current_classification_total": 0,
    }
    storage.list_unmapped_official_industry_code_backlog.return_value = []
    storage.summarize_industry_index_analysis_daily.return_value = {
        "total": 0,
        "distinct_index_codes": 0,
        "latest_trade_date": None,
        "latest_updated_at": None,
        "index_type_counts": {},
    }
    manager.research_storage = storage
    manager.db_ops = Mock()
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ],
            [
                {
                    "instrument_id": "430001.BJ",
                    "symbol": "430001",
                    "exchange": "BSE",
                    "type": "stock",
                    "is_active": True,
                }
            ],
        ]
    )
    manager.get_research_official_mapping_override_review = AsyncMock(
        return_value={
            "configured_override_total": 1,
            "ready_candidate_total": 1,
            "applied_override_total": 1,
            "pending_manual_override_total": 0,
            "status_counts": {"configured_and_applied": 1},
            "items": [
                {
                    "official_industry_code": "480301",
                    "review_status": "configured_and_applied",
                    "status_reason": "configured_override_reflected_in_mapping_cache",
                }
            ],
        }
    )

    result = _run(manager.get_research_industry_standard_readiness())

    assert result["target_instrument_count"] == 1
    assert result["target_instruments_by_exchange"]["SSE"] == 1
    assert result["exchange_coverage"][1]["exchange"] == "BSE"
    assert result["exchange_coverage"][1]["target_instruments"] == 0
    assert result["industry_standard_ready"] is True
    assert result["relative_valuation"]["ready"] is True
    assert result["unmapped_backlog"]["official_code_total"] == 0
    assert result["unmapped_backlog"]["top_items"] == []
    assert result["override_review"]["requires_attention"] is False
    assert result["override_review"]["status_counts"] == {"configured_and_applied": 1}
    assert result["override_review"]["top_items"] == []
    assert result["exchange_coverage"][0]["exchange"] == "SSE"
    assert result["exchange_coverage"][0]["coverage_ratio"] == 1.0
    storage.summarize_official_industry_code_mappings.assert_called_once()
    storage.get_latest_official_industry_code_mapping_cache_info.assert_called_once()
    storage.summarize_official_industry_classifications.assert_called_once()
    storage.summarize_industry_memberships.assert_called_once()
    storage.count_industry_memberships_by_exchange.assert_called_once_with(
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        mapping_status="authoritative",
    )


def test_data_manager_list_research_unmapped_official_industry_code_backlog(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "industry": {
            "enabled": True,
            "standard": {
                "enabled": True,
                "taxonomy_system": "sw",
                "taxonomy_version": "sw_2021",
                "official_mapping": {
                    "backlog_review": {
                        "minimum_current_classification_count": 1,
                        "minimum_overlap_count": 2,
                        "minimum_precision": 0.2,
                        "minimum_recall": 0.5,
                        "minimum_top_candidate_overlap_gap": 1,
                    }
                },
            },
        }
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    class FakeStorage:
        def __init__(self):
            self.list_calls = []
            self.summary_calls = []

        def list_unmapped_official_industry_code_backlog(self, **kwargs):
            self.list_calls.append(kwargs)
            return [
                {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "official_industry_code": "480301",
                    "best_taxonomy_industry_code": "857831.SI",
                    "mapped_industry_code": None,
                    "mapping_status": "unmapped",
                    "mapping_confidence": "unmapped",
                    "overlap_count": 2,
                    "official_symbol_count": 4,
                    "taxonomy_symbol_count": 9,
                    "precision": 0.22,
                    "recall": 0.5,
                    "source": "akshare",
                    "source_mode": "proxy_patch",
                    "built_at": "2026-04-20T12:00:00+08:00",
                    "ingestion_run_id": 12,
                    "created_at": "2026-04-20T12:00:01+08:00",
                    "updated_at": "2026-04-20T12:00:02+08:00",
                    "current_classification_count": 2,
                    "impacted_exchange_counts": {"SSE": 1, "SZSE": 1},
                    "sample_instruments": ["600000.SH", "000001.SZ"],
                    "mapping": {
                        "candidate_rankings": [
                            {
                                "taxonomy_industry_code": "857831.SI",
                                "overlap_count": 2,
                                "taxonomy_symbol_count": 9,
                                "precision": 0.22,
                                "recall": 0.5,
                            }
                        ]
                    },
                },
                {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "official_industry_code": "999999",
                    "best_taxonomy_industry_code": None,
                    "mapped_industry_code": None,
                    "mapping_status": "unmapped",
                    "mapping_confidence": "unmapped",
                    "overlap_count": 0,
                    "official_symbol_count": 1,
                    "taxonomy_symbol_count": 0,
                    "precision": 0.0,
                    "recall": 0.0,
                    "source": "akshare",
                    "source_mode": "proxy_patch",
                    "built_at": "2026-04-20T12:00:00+08:00",
                    "ingestion_run_id": 12,
                    "created_at": "2026-04-20T12:00:01+08:00",
                    "updated_at": "2026-04-20T12:00:02+08:00",
                    "current_classification_count": 0,
                    "impacted_exchange_counts": {},
                    "sample_instruments": [],
                    "mapping": {"candidate_rankings": []},
                },
            ]

        def summarize_unmapped_official_industry_code_backlog(self, **kwargs):
            self.summary_calls.append(kwargs)
            return {
                "official_code_total": 2,
                "current_classification_total": 2,
            }

    storage = FakeStorage()
    manager.research_storage = storage

    async def _fake_to_thread(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    with patch("data_manager.asyncio.to_thread", AsyncMock(side_effect=_fake_to_thread)):
        result = _run(
            manager.list_research_unmapped_official_industry_code_backlog(
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                limit=50,
                offset=0,
                include_mapping=True,
            )
        )

    assert result["taxonomy_system"] == "sw"
    assert result["taxonomy_version"] == "sw_2021"
    assert result["total"] == 2
    assert result["current_classification_total"] == 2
    assert result["items"][0]["official_industry_code"] == "480301"
    assert result["items"][0]["review_priority"] == "high"
    assert result["items"][0]["override_candidate_ready"] is True
    assert (
        result["items"][0]["override_candidate_reason"]
        == "single_strong_candidate_with_current_impact"
    )
    assert result["items"][0]["candidate_count"] == 1
    assert result["items"][0]["top_candidate_overlap_gap"] is None
    assert result["items"][0]["manual_override_suggestion"]["official_industry_code"] == "480301"
    assert result["items"][0]["manual_override_suggestion"]["taxonomy_industry_code"] == "857831.SI"
    assert result["override_candidate_total"] == 1
    assert result["review_priority_counts"] == {"high": 1, "low": 1}
    assert result["items"][1]["official_industry_code"] == "999999"
    assert result["items"][1]["override_candidate_ready"] is False
    assert result["items"][1]["manual_override_suggestion"] is None
    assert storage.list_calls == [
        {
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "source": "akshare",
            "source_mode": "proxy_patch",
            "max_age_days": 7,
            "limit": 50,
            "offset": 0,
            "include_mapping": True,
        }
    ]
    assert storage.summary_calls == [
        {
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "source": "akshare",
            "source_mode": "proxy_patch",
            "max_age_days": 7,
        }
    ]

    with patch("data_manager.asyncio.to_thread", AsyncMock(side_effect=_fake_to_thread)):
        ready_only_result = _run(
            manager.list_research_unmapped_official_industry_code_backlog(
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                limit=50,
                offset=0,
                include_mapping=True,
                override_candidate_ready_only=True,
            )
        )

    assert ready_only_result["total"] == 1
    assert ready_only_result["current_classification_total"] == 2
    assert ready_only_result["override_candidate_total"] == 1
    assert ready_only_result["review_priority_counts"] == {"high": 1}
    assert [item["official_industry_code"] for item in ready_only_result["items"]] == ["480301"]


def test_data_manager_lists_research_official_mapping_override_candidates(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    class FakeStorage:
        def list_unmapped_official_industry_code_backlog(self, **kwargs):
            return [
                {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "official_industry_code": "480301",
                    "best_taxonomy_industry_code": "857831.SI",
                    "mapped_industry_code": None,
                    "mapping_status": "unmapped",
                    "mapping_confidence": "unmapped",
                    "overlap_count": 2,
                    "official_symbol_count": 4,
                    "taxonomy_symbol_count": 9,
                    "precision": 0.22,
                    "recall": 0.5,
                    "source": "akshare",
                    "source_mode": "proxy_patch",
                    "built_at": "2026-04-20T11:00:00",
                    "ingestion_run_id": 12,
                    "created_at": "2026-04-20T11:00:01",
                    "updated_at": "2026-04-20T11:00:02",
                    "current_classification_count": 2,
                    "impacted_exchange_counts": {"SSE": 1, "SZSE": 1},
                    "sample_instruments": ["600000.SH", "000001.SZ"],
                    "mapping": {
                        "candidate_rankings": [
                            {
                                "taxonomy_industry_code": "857831.SI",
                                "overlap_count": 2,
                                "taxonomy_symbol_count": 9,
                                "precision": 0.22,
                                "recall": 0.5,
                            }
                        ]
                    },
                },
                {
                    "taxonomy_system": "sw",
                    "taxonomy_version": "sw_2021",
                    "official_industry_code": "999999",
                    "best_taxonomy_industry_code": None,
                    "mapped_industry_code": None,
                    "mapping_status": "unmapped",
                    "mapping_confidence": "unmapped",
                    "overlap_count": 1,
                    "official_symbol_count": 1,
                    "taxonomy_symbol_count": 3,
                    "precision": 0.1,
                    "recall": 0.1,
                    "source": "akshare",
                    "source_mode": "proxy_patch",
                    "built_at": "2026-04-20T11:00:00",
                    "ingestion_run_id": 12,
                    "created_at": "2026-04-20T11:00:01",
                    "updated_at": "2026-04-20T11:00:02",
                    "current_classification_count": 0,
                    "impacted_exchange_counts": {},
                    "sample_instruments": [],
                    "mapping": {"candidate_rankings": []},
                },
            ]

        def summarize_unmapped_official_industry_code_backlog(self, **kwargs):
            return {
                "official_code_total": 2,
                "current_classification_total": 2,
            }

    manager.research_storage = FakeStorage()

    async def _fake_to_thread(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    with patch("data_manager.asyncio.to_thread", AsyncMock(side_effect=_fake_to_thread)):
        result = _run(
            manager.list_research_official_mapping_override_candidates(
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                limit=50,
                offset=0,
                include_mapping=True,
            )
        )

    assert result["total"] == 1
    assert result["override_candidate_total"] == 1
    assert result["review_priority_counts"] == {"high": 1}
    assert list(result["manual_overrides"].keys()) == ["480301"]
    assert result["manual_overrides"]["480301"] == {
        "taxonomy_industry_code": "857831.SI",
        "confidence": "review_candidate",
        "reason": (
            "Suggested from official mapping backlog: "
            "single_strong_candidate_with_current_impact "
            "(current_classification_count=2, overlap=2, precision=0.2200, recall=0.5000)"
        ),
    }
    assert [item["official_industry_code"] for item in result["items"]] == ["480301"]


def test_data_manager_reviews_research_official_mapping_overrides(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_config.modules["industry"]["standard"] = {
        "enabled": True,
        "taxonomy_system": "sw",
        "taxonomy_version": "sw_2021",
        "official_mapping": {
            "manual_overrides": {
                "480301": {
                    "taxonomy_industry_code": "857831.SI",
                    "confidence": "high",
                    "reason": "Validated",
                },
                "111111": {
                    "taxonomy_industry_code": "801001.SI",
                    "confidence": "high",
                    "reason": "Configured but not yet applied",
                },
            }
        },
    }

    class FakeStorage:
        def get_official_industry_code_mappings(self, **kwargs):
            return [
                {
                    "official_industry_code": "480301",
                    "mapped_industry_code": "857831.SI",
                    "best_taxonomy_industry_code": "857831.SI",
                    "source": "akshare",
                    "source_mode": "proxy_patch",
                    "built_at": "2026-04-20T11:00:00",
                    "mapping": {
                        "mapping_source": "manual_override",
                        "override_reason": "Validated",
                    },
                },
                {
                    "official_industry_code": "222222",
                    "mapped_industry_code": "801777.SI",
                    "best_taxonomy_industry_code": "801777.SI",
                    "source": "akshare",
                    "source_mode": "proxy_patch",
                    "built_at": "2026-04-20T11:00:00",
                    "mapping": {
                        "mapping_source": "manual_override",
                        "override_reason": "Stale cache row",
                    },
                },
            ]

    manager.research_storage = FakeStorage()
    manager.list_research_official_mapping_override_candidates = AsyncMock(
        return_value={
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "source": "akshare",
            "source_mode": "proxy_patch",
            "max_age_days": 7,
            "limit": 500,
            "offset": 0,
            "total": 3,
            "current_classification_total": 5,
            "override_candidate_total": 3,
            "review_priority_counts": {"high": 3},
            "manual_overrides": {
                "480301": {
                    "taxonomy_industry_code": "857831.SI",
                    "confidence": "review_candidate",
                    "reason": "same as configured",
                },
                "222222": {
                    "taxonomy_industry_code": "801777.SI",
                    "confidence": "review_candidate",
                    "reason": "cache row still exists",
                },
                "333333": {
                    "taxonomy_industry_code": "801888.SI",
                    "confidence": "review_candidate",
                    "reason": "new ready candidate",
                },
            },
            "items": [],
        }
    )

    async def _fake_to_thread(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    with patch("data_manager.asyncio.to_thread", AsyncMock(side_effect=_fake_to_thread)):
        result = _run(
            manager.get_research_official_mapping_override_review(
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                include_mapping=True,
            )
        )

    assert result["attention_only"] is False
    assert result["review_status"] == []
    assert result["configured_override_total"] == 2
    assert result["ready_candidate_total"] == 3
    assert result["applied_override_total"] == 2
    assert result["pending_manual_override_total"] == 1
    assert result["status_counts"] == {
        "applied_not_configured": 1,
        "configured_and_applied": 1,
        "configured_not_applied": 1,
        "ready_candidate_pending_config": 1,
    }
    assert result["pending_manual_overrides"] == {
        "333333": {
            "taxonomy_industry_code": "801888.SI",
            "confidence": "review_candidate",
            "reason": "new ready candidate",
        }
    }
    items = {item["official_industry_code"]: item for item in result["items"]}
    assert items["480301"]["review_status"] == "configured_and_applied"
    assert items["111111"]["review_status"] == "configured_not_applied"
    assert items["222222"]["review_status"] == "applied_not_configured"
    assert items["333333"]["review_status"] == "ready_candidate_pending_config"

    with patch("data_manager.asyncio.to_thread", AsyncMock(side_effect=_fake_to_thread)):
        attention_only_result = _run(
            manager.get_research_official_mapping_override_review(
                source="akshare",
                source_mode="proxy_patch",
                max_age_days=7,
                include_mapping=True,
                attention_only=True,
                review_status=["configured_not_applied", "ready_candidate_pending_config"],
            )
        )

    assert attention_only_result["attention_only"] is True
    assert attention_only_result["review_status"] == [
        "configured_not_applied",
        "ready_candidate_pending_config",
    ]
    assert attention_only_result["status_counts"] == {
        "configured_not_applied": 1,
        "ready_candidate_pending_config": 1,
    }
    assert attention_only_result["pending_manual_override_total"] == 1
    filtered_items = {
        item["official_industry_code"]: item
        for item in attention_only_result["items"]
    }
    assert set(filtered_items) == {"111111", "333333"}


def test_data_manager_get_research_company_overview_requires_storage(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    with pytest.raises(RuntimeError, match="research storage is not initialized"):
        _run(manager.get_research_company_overview("600000.SH"))


def test_data_manager_get_research_company_overview_delegates_to_query_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()

    with patch("research.query_service.ResearchQueryService") as service_cls:
        service_instance = Mock()
        service_instance.get_company_overview.return_value = {
            "instrument_id": "600000.SH",
            "data_as_of": "2026-04-17T19:00:00",
            "source_summary": {
                "company_profile": {"available": True},
                "industry": {"available": True},
                "financial_summary": {"available": False},
            },
            "missing_sections": ["financial_summary"],
        }
        service_cls.return_value = service_instance

        result = _run(
            manager.get_research_company_overview(
                "600000.SH",
                include_profile_snapshot=True,
                include_industry_snapshot=True,
                include_financial_snapshot=False,
            )
        )

    assert result["instrument_id"] == "600000.SH"
    service_cls.assert_called_once_with(manager.research_storage)
    service_instance.get_company_overview.assert_called_once_with(
        "600000.SH",
        include_profile_snapshot=True,
        include_industry_snapshot=True,
        include_financial_snapshot=False,
    )


def test_data_manager_returns_optional_empty_bse_collection_payloads_and_overview(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "company_profile": {"enabled": True, "optional_empty_exchanges": ["BSE"]},
        "industry": {"enabled": True, "optional_empty_exchanges": ["BSE"]},
        "financial_summary": {"enabled": True, "optional_empty_exchanges": ["BSE"]},
        "analyst_forecasts": {"enabled": True, "optional_empty_exchanges": ["BSE"]},
        "research_reports": {"enabled": True, "optional_empty_exchanges": ["BSE"]},
        "sentiment_events": {"enabled": True, "optional_empty_exchanges": ["BSE"]},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = object()
    manager.db_ops = Mock()
    manager.db_ops.get_instrument_info = AsyncMock(
        return_value={
            "instrument_id": "430001.BJ",
            "symbol": "430001",
            "name": "北交样本",
            "exchange": "BSE",
            "type": "stock",
            "is_active": True,
        }
    )

    with patch("research.query_service.ResearchQueryService") as service_cls:
        service_instance = Mock()
        service_instance.get_latest_analyst_forecast.return_value = None
        service_instance.list_research_reports.return_value = []
        service_instance.list_sentiment_events.return_value = []
        service_instance.get_company_overview.return_value = None
        service_cls.return_value = service_instance

        analyst = _run(manager.get_research_analyst_coverage("430001.BJ"))
        reports = _run(manager.get_research_reports("430001.BJ"))
        events = _run(manager.get_research_sentiment_events("430001.BJ"))
        overview = _run(
            manager.get_research_company_overview(
                "430001.BJ",
                include_industry_snapshot=True,
            )
        )

    assert analyst["status"] == "empty"
    assert analyst["missing_reason"] == "optional_empty_exchange"
    assert reports["data_points"] == 0
    assert reports["exchange"] == "BSE"
    assert events["data_points"] == 0
    assert events["exchange"] == "BSE"
    assert overview["instrument_id"] == "430001.BJ"
    assert "company_profile" in overview["missing_sections"]
    assert "industry" in overview["missing_sections"]
    assert "financial_summary" in overview["missing_sections"]
    assert overview["industry"]["mapping_status"] == "optional_empty_exchange"


def test_data_manager_get_research_technical_summary_requires_enabled_module(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "technical": {"enabled": False},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    with pytest.raises(RuntimeError, match="research technical module is disabled"):
        _run(manager.get_research_technical_summary("600000.SH"))


def test_data_manager_get_research_technical_summary_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "technical": {
            "enabled": True,
            "default_adjustment": "qfq",
            "summary": {"lookback_bars": 120},
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "type": "stock",
        }
    )
    manager.db_ops.get_daily_data = AsyncMock(
        return_value=pd.DataFrame(
            [
                {
                    "time": "2026-04-16T00:00:00",
                    "instrument_id": "600000.SH",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.9,
                    "close": 10.2,
                    "volume": 1000,
                    "amount": 10200.0,
                    "quality_score": 1.0,
                }
            ]
        )
    )
    manager.get_cached_adjustment_factors = AsyncMock(return_value=[])

    with patch("research.technical_service.ResearchTechnicalAnalysisService") as service_cls:
        service_instance = Mock()
        service_instance.build_summary.return_value = {
            "instrument_id": "600000.SH",
            "status": "insufficient_data",
            "data_as_of": "2026-04-16T00:00:00",
            "calc_method": "ta_builtin",
            "calc_version": "technical_summary.v1",
            "parameter_hash": "hash",
            "signal": "insufficient_data",
            "quote_summary": {
                "quote_source": "quotes_db",
                "data_points": 1,
                "window_start": "2026-04-16T00:00:00",
                "window_end": "2026-04-16T00:00:00",
                "requested_adjustment": "qfq",
                "applied_adjustment": "none",
                "latest_quality_score": 1.0,
            },
        }
        service_cls.return_value = service_instance

        result = _run(manager.get_research_technical_summary("600000.SH", adjust="qfq"))

    assert result["instrument_id"] == "600000.SH"
    manager.db_ops.get_instrument_by_id.assert_awaited_once_with("600000.SH")
    manager.db_ops.get_daily_data.assert_awaited_once_with(
        instrument_id="600000.SH",
        limit=120,
        return_format="pandas",
    )
    service_cls.assert_called_once_with({"lookback_bars": 120})
    assert service_instance.build_summary.call_args.kwargs["requested_adjustment"] == "qfq"
    assert service_instance.build_summary.call_args.kwargs["applied_adjustment"] == "none"


def test_data_manager_get_research_technical_cache_readiness_reports_blockers(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    research_config = mock_config.get_research_config.return_value
    research_config.markets = ["SSE", "SZSE"]
    research_config.modules = {
        "technical": {
            "enabled": True,
            "default_adjustment": "qfq",
            "latest_cache": {"period": "1d", "adjustment": "qfq"},
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    storage = Mock()
    storage.summarize_technical_indicator_latest.return_value = {
        "instrument_total": 1,
        "row_total": 1,
        "source_counts": {"local_quotes": 1},
        "source_mode_counts": {"derived": 1},
        "calc_method_counts": {"ta_builtin": 1},
        "calc_version_counts": {"technical_summary.v1": 1},
        "status_counts": {"complete": 1},
        "signal_counts": {"bullish": 1},
        "latest_as_of_date": "2026-04-17",
        "latest_updated_at": "2026-04-17T18:00:00+08:00",
        "latest_data_as_of": "2026-04-17T15:00:00+08:00",
    }
    storage.count_technical_indicator_latest_by_exchange.return_value = {"SSE": 1}
    manager.research_storage = storage
    manager.db_ops.get_research_target_instrument_ids_by_exchange = AsyncMock(
        side_effect=[
            ["600000.SH"],
            ["000001.SZ"],
        ]
    )
    manager.db_ops.get_instruments_by_exchange = AsyncMock(
        side_effect=[
            [
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ],
            [
                {
                    "instrument_id": "000001.SZ",
                    "symbol": "000001",
                    "exchange": "SZSE",
                    "type": "stock",
                    "is_active": True,
                }
            ],
        ]
    )

    result = _run(manager.get_research_technical_cache_readiness())

    assert result["period"] == "1d"
    assert result["adjustment"] == "qfq"
    assert result["cache_enabled"] is True
    assert result["target_instrument_count"] == 2
    assert result["snapshot_total"] == 1
    assert result["missing_snapshot_count"] == 1
    assert result["ready_for_rollout"] is False
    assert "technical_indicator_latest_coverage_incomplete" in result["blockers"]
    assert result["exchange_coverage"][0]["snapshot_count"] == 1
    assert result["exchange_coverage"][1]["snapshot_count"] == 0
    storage.summarize_technical_indicator_latest.assert_called_once_with(
        period="1d",
        adjustment="qfq",
    )


def test_data_manager_run_technical_snapshot_refresh_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "technical": {"enabled": True, "default_adjustment": "qfq"},
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.research_storage = Mock()

    with patch("research.technical_snapshot_sync.TechnicalIndicatorLatestRefreshService") as service_cls:
        service_instance = Mock()
        service_instance.sync = AsyncMock(
            return_value={"status": "success", "total_rows_written": 1}
        )
        service_cls.return_value = service_instance

        result = _run(
            manager.run_technical_snapshot_refresh(
                exchanges=["SSE"],
                limit_per_exchange=1,
                adjustment="qfq",
                period="1d",
            )
        )

    assert result["status"] == "success"
    service_cls.assert_called_once()
    assert service_cls.call_args.kwargs["db_ops"] is manager.db_ops
    assert service_cls.call_args.kwargs["storage"] is manager.research_storage
    assert service_cls.call_args.kwargs["adjust_quotes"] == manager._apply_research_adjustment
    service_instance.sync.assert_awaited_once_with(
        exchanges=["SSE"],
        limit_per_exchange=1,
        adjustment="qfq",
        period="1d",
    )


def test_data_manager_get_research_technical_indicators_delegates_to_service(tmp_path):
    mock_config = _build_mock_config(tmp_path, research_enabled=True)
    mock_config.get_research_config.return_value.modules = {
        "technical": {
            "enabled": True,
            "default_adjustment": "qfq",
            "summary": {"lookback_bars": 180},
        },
    }

    with patch("data_manager.config_manager", mock_config):
        manager = DataManager()

    manager.db_ops.get_instrument_by_id = AsyncMock(
        return_value={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "type": "stock",
        }
    )
    manager.db_ops.get_daily_data = AsyncMock(
        return_value=pd.DataFrame(
            [
                {
                    "time": "2026-04-15T00:00:00",
                    "instrument_id": "600000.SH",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.1,
                    "volume": 1000,
                    "amount": 10100.0,
                    "quality_score": 1.0,
                },
                {
                    "time": "2026-04-16T00:00:00",
                    "instrument_id": "600000.SH",
                    "open": 10.1,
                    "high": 10.6,
                    "low": 9.9,
                    "close": 10.3,
                    "volume": 1200,
                    "amount": 12360.0,
                    "quality_score": 1.0,
                },
            ]
        )
    )
    manager.get_cached_adjustment_factors = AsyncMock(return_value=[])

    with patch("research.technical_service.ResearchTechnicalAnalysisService") as service_cls:
        service_instance = Mock()
        service_instance.build_indicator_series.return_value = {
            "instrument_id": "600000.SH",
            "calc_method": "ta_builtin",
            "calc_version": "technical_summary.v1",
            "parameter_hash": "hash",
            "requested_adjustment": "qfq",
            "applied_adjustment": "none",
            "data_points": 2,
            "window_start": "2026-04-15T00:00:00",
            "window_end": "2026-04-16T00:00:00",
            "items": [],
        }
        service_cls.return_value = service_instance

        result = _run(
            manager.get_research_technical_indicators(
                "600000.SH",
                adjust="qfq",
                limit=2,
            )
        )

    assert result["instrument_id"] == "600000.SH"
    manager.db_ops.get_daily_data.assert_awaited_once_with(
        instrument_id="600000.SH",
        start_date=None,
        end_date=None,
        limit=180,
        return_format="pandas",
    )
    service_instance.build_indicator_series.assert_called_once()
    assert service_instance.build_indicator_series.call_args.kwargs["limit"] == 2
