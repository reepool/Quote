from datetime import date, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from data_manager import DataManager
from instrument_master_governance import (
    AShareIndexPolicy,
    AShareStockPolicy,
    HKEXInstrumentPolicy,
    MasterGovernanceOrchestrator,
    MasterGovernanceRequirement,
    PolicyRegistry,
)


class _Policy:
    def __init__(self, scope, result):
        self.scope = scope
        self.result = result
        self.calls = []

    async def execute(self, requirement):
        self.calls.append(requirement)
        return self.result


def _build_config_manager(data_config=None):
    config = Mock()
    config.get_research_config.return_value = {}
    base_data_config = {
        "data_dir": "data",
        "instrument_types": ["stock", "index"],
        "instrument_master_sync": {
            "enabled": True,
            "run_before_daily_update": True,
            "skip_for_backfill": True,
            "continue_on_failure": True,
            "timeout_sec": 30,
            "freshness_threshold_hours": 48,
            "pytdx_validation_enabled": False,
            "exchanges": ["SSE", "SZSE", "BSE"],
        },
        "instrument_master_governance": {
            "enabled": True,
            "reuse_fresh_master": True,
            "skip_for_backfill": True,
            "continue_on_failure": True,
            "timeout_sec": 30,
            "freshness_threshold_hours": 48,
            "pytdx_validation_enabled": False,
            "supported_exchanges": ["SSE", "SZSE", "BSE", "HKEX"],
            "force_refresh_job_names": ["daily_data_update"],
        },
        "index_master_governance": {
            "enabled": True,
            "run_before_daily_update": True,
            "timeout_sec": 120,
            "freshness_threshold_hours": 48,
            "continue_on_failure": True,
            "exchanges": ["SSE", "SZSE"],
        },
        "hkex_instrument_master_sync": {
            "enabled": True,
            "mode": "audit_only",
            "timeout_sec": 60,
        },
    }
    if data_config:
        base_data_config.update(data_config)
    config.get_nested.side_effect = lambda key, default=None: {
        "telegram_config.enabled": False,
        "data_config": base_data_config,
    }.get(key, default)
    return config


def _manager(data_config=None):
    with patch("data_manager.config_manager", _build_config_manager(data_config)):
        return DataManager()


def test_requirement_validation_rejects_invalid_scope_and_exchange():
    with pytest.raises(ValueError, match="unsupported master governance scope"):
        MasterGovernanceRequirement(scope="unknown").validate()

    with pytest.raises(ValueError, match="unsupported exchanges"):
        MasterGovernanceRequirement(
            scope="a_share_index",
            exchanges=["BSE"],
            instrument_types=["index"],
        ).validate()


def test_requirement_from_config_rejects_unknown_keys():
    with pytest.raises(ValueError, match="unknown master governance requirement keys"):
        MasterGovernanceRequirement.from_config(
            {"scope": "a_share_stock", "unexpected": True},
            job_name="daily_data_update",
        )


@pytest.mark.asyncio
async def test_orchestrator_dispatches_and_merges_policy_results():
    stock = _Policy(
        "a_share_stock",
        {
            "status": "success",
            "action": "synced",
            "summary": {"exchanges": ["SSE"], "active_count": 2, "added_instruments": 1},
            "exchanges": {"SSE": {"status": "success"}},
            "warnings": [],
            "errors": [],
        },
    )
    index = _Policy(
        "a_share_index",
        {
            "status": "warning",
            "action": "index_master_governance",
            "summary": {"exchanges": ["SZSE"], "active_count": 3, "lifecycle_skip_count": 1},
            "exchanges": {"SZSE": {"status": "success"}},
            "warnings": ["CSIndex full-list endpoint is not enabled"],
            "errors": [],
        },
    )
    orchestrator = MasterGovernanceOrchestrator(
        registry=PolicyRegistry([stock, index]),
        policy_config={"a_share_stock": {"enabled": True}, "a_share_index": {"enabled": True}},
    )

    result = await orchestrator.run([
        MasterGovernanceRequirement(scope="a_share_stock", exchanges=["SSE"], instrument_types=["stock"]),
        MasterGovernanceRequirement(scope="a_share_index", exchanges=["SZSE"], instrument_types=["index"]),
    ])

    assert result["status"] == "warning"
    assert result["summary"]["active_count"] == 5
    assert result["summary"]["added_instruments"] == 1
    assert result["summary"]["lifecycle_skip_count"] == 1
    assert len(result["children"]) == 2
    assert result["index_master_governance"]["scope"] == "a_share_index"


@pytest.mark.asyncio
async def test_orchestrator_handles_unsupported_policy_as_visible_warning():
    orchestrator = MasterGovernanceOrchestrator(registry=PolicyRegistry())
    result = await orchestrator.run([
        MasterGovernanceRequirement(scope="a_share_stock", exchanges=["SSE"], instrument_types=["stock"]),
    ])

    assert result["status"] == "warning"
    assert result["children"][0]["reason"] == "unsupported_master_governance_policy"
    assert "unsupported master governance policy" in result["errors"][0]


@pytest.mark.asyncio
async def test_a_share_stock_adapter_preserves_bse_and_fallback_warnings():
    manager = Mock()
    manager.sync_instrument_master = AsyncMock(return_value={
        "status": "warning",
        "summary": {"exchanges": ["SSE", "SZSE", "BSE"], "active_count": 5527},
        "exchanges": {"BSE": {"status": "warning"}},
        "warnings": ["BSE: BaoStock primary did not contribute rows"],
        "errors": [],
    })
    policy = AShareStockPolicy(manager, {
        "reuse_fresh_master": False,
        "pytdx_validation_enabled": False,
        "timeout_sec": 30,
        "freshness_threshold_hours": 48,
    })

    result = await policy.execute(MasterGovernanceRequirement(
        scope="a_share_stock",
        exchanges=["SSE", "SZSE", "BSE"],
        instrument_types=["stock"],
        mode="force_refresh",
    ))

    assert result["status"] == "warning"
    assert "BSE: BaoStock primary did not contribute rows" in result["warnings"]
    manager.sync_instrument_master.assert_awaited_once_with(
        ["SSE", "SZSE", "BSE"],
        include_pytdx_validation=False,
        timeout_sec=30,
        freshness_threshold_hours=48,
    )


@pytest.mark.asyncio
async def test_a_share_stock_adapter_syncs_when_freshness_check_fails():
    manager = Mock()
    manager._build_fresh_master_governance_result = AsyncMock(
        side_effect=RuntimeError("freshness read failed")
    )
    manager.sync_instrument_master = AsyncMock(return_value={
        "status": "success",
        "summary": {"exchanges": ["SSE"], "active_count": 2314},
        "exchanges": {"SSE": {"status": "success"}},
        "warnings": [],
        "errors": [],
    })
    policy = AShareStockPolicy(manager, {
        "reuse_fresh_master": True,
        "pytdx_validation_enabled": False,
        "timeout_sec": 30,
        "freshness_threshold_hours": 48,
    })

    result = await policy.execute(MasterGovernanceRequirement(
        scope="a_share_stock",
        exchanges=["SSE"],
        instrument_types=["stock"],
        mode="freshness_gated",
        job_name="financial_summary_shadow_sync",
    ))

    assert result["status"] == "success"
    assert result["action"] == "synced"
    manager.sync_instrument_master.assert_awaited_once_with(
        ["SSE"],
        include_pytdx_validation=False,
        timeout_sec=30,
        freshness_threshold_hours=48,
    )


@pytest.mark.asyncio
async def test_index_adapter_preserves_lifecycle_summary_and_samples():
    manager = Mock()
    manager.sync_index_master = AsyncMock(return_value={
        "status": "warning",
        "summary": {
            "active_count": 1694,
            "lifecycle_skip_count": 6,
            "ambiguous_master_duplicate_groups_skipped": 2,
            "samples": [{"instrument_id": "480055.SZ"}],
        },
        "exchanges": {"SZSE": {"status": "success"}},
        "warnings": ["duplicate skipped"],
        "errors": [],
    })
    policy = AShareIndexPolicy(manager, {"timeout_sec": 120})

    result = await policy.execute(MasterGovernanceRequirement(
        scope="a_share_index",
        exchanges=["SZSE"],
        instrument_types=["index"],
        target_date=date(2026, 6, 13),
    ))

    assert result["summary"]["lifecycle_skip_count"] == 6
    assert result["summary"]["ambiguous_master_duplicate_groups_skipped"] == 2
    assert result["summary"]["samples"][0]["instrument_id"] == "480055.SZ"
    manager.sync_index_master.assert_awaited_once_with(
        exchanges=["SZSE"],
        target_date=date(2026, 6, 13),
        timeout_sec=120,
    )


@pytest.mark.asyncio
async def test_hkex_adapter_maps_modes_without_changing_official_sync():
    manager = Mock()
    manager.sync_hkex_instrument_master = AsyncMock(return_value={
        "status": "success",
        "mode": "audit_only",
        "summary": {"active_count": 3000},
        "exchanges": {"HKEX": {"status": "success"}},
        "warnings": [],
        "errors": [],
    })
    policy = HKEXInstrumentPolicy(manager, {"mode": "lifecycle_write", "timeout_sec": 60})

    await policy.execute(MasterGovernanceRequirement(
        scope="hkex_instrument",
        exchanges=["HKEX"],
        instrument_types=["stock"],
        mode="audit_only",
    ))

    manager.sync_hkex_instrument_master.assert_awaited_once_with(
        mode="audit_only",
        timeout_sec=60,
    )


@pytest.mark.asyncio
async def test_hkex_adapter_rejects_unsupported_mode_without_writes():
    manager = Mock()
    manager.sync_hkex_instrument_master = AsyncMock()
    policy = HKEXInstrumentPolicy(manager, {"mode": "unexpected_write_mode", "timeout_sec": 60})

    result = await policy.execute(MasterGovernanceRequirement(
        scope="hkex_instrument",
        exchanges=["HKEX"],
        instrument_types=["stock"],
        mode="force_refresh",
    ))

    assert result["status"] == "error"
    assert result["reason"] == "unsupported_hkex_governance_mode"
    manager.sync_hkex_instrument_master.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_update_builds_stock_and_index_requirements_and_skips_historical():
    manager = _manager({
        "master_governance": {
            "enabled": True,
            "policies": {
                "a_share_stock": {"enabled": True},
                "a_share_index": {"enabled": True},
            },
            "job_requirements": {
                "daily_data_update": [
                    {
                        "scope": "a_share_stock",
                        "mode": "force_refresh",
                        "exchanges": ["SSE", "SZSE", "BSE"],
                        "instrument_types": ["stock"],
                    },
                    {
                        "scope": "a_share_index",
                        "mode": "freshness_gated",
                        "exchanges": ["SSE", "SZSE"],
                        "instrument_types": ["index"],
                    },
                ]
            },
        }
    })
    manager.run_master_governance = AsyncMock(return_value={
        "status": "skipped",
        "reason": "historical_current_master_governance_skipped",
        "summary": {},
        "children": [],
        "warnings": [],
        "errors": [],
    })

    target = date.today() - timedelta(days=3)
    result = await manager._maybe_sync_instrument_master_before_daily_update(
        ["SSE", "SZSE", "BSE"],
        target,
        instrument_types=["stock", "index"],
    )

    requirements = manager.run_master_governance.await_args.args[0]
    assert {requirement.scope for requirement in requirements} == {"a_share_stock", "a_share_index"}
    assert {requirement.mode for requirement in requirements} == {"skip_for_backfill"}
    assert result["reason"] == "historical_backfill_current_master_sync_skipped"
