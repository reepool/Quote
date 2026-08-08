import asyncio
import json
from copy import deepcopy
from unittest.mock import Mock

import pytest

from data_manager import DataManager
from research.business_profile_production_rollout import (
    RUNTIME_IDENTITY_KEYS,
    build_business_profile_rollout_status,
    derive_business_profile_runtime_identities,
    evaluate_business_profile_rollout_readiness,
    parse_business_profile_rollout_config,
    resolve_business_profile_runtime_identities,
)
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from utils.config_manager import UnifiedConfigManager


def _payload():
    return deepcopy(
        UnifiedConfigManager("config").get_nested("business_profile_rollout", {})
    )


def test_production_rollout_starts_in_structured_shadow_with_bounded_budgets():
    rollout = parse_business_profile_rollout_config(_payload())

    phase = rollout.phase()
    assert phase.name == "structured_shadow"
    assert phase.field_families == (
        "structured_segments",
        "tabular_operating_facts",
    )
    assert phase.promotion_enabled is False
    assert set(phase.stage_budgets) == {"acquire", "parse", "semantic", "publish"}
    assert rollout.phases["daily_incremental"].enabled is False
    assert rollout.bootstrap["selection_policy"] == "latest_annual_only"
    assert rollout.bootstrap["start_date"] is None


def test_expanded_rollout_bootstrap_still_requires_start_date():
    payload = _payload()
    payload["bootstrap"]["selection_policy"] = "expanded"

    with pytest.raises(ValueError, match="bootstrap start_date is required"):
        parse_business_profile_rollout_config(payload)


def test_disabled_phase_and_unpassed_promotion_manifest_fail_closed():
    payload = _payload()
    rollout = parse_business_profile_rollout_config(payload)
    with pytest.raises(ValueError, match="phase is disabled"):
        rollout.phase("semantic_shadow")

    payload["active_phase"] = "structured_promotion"
    payload["phases"]["structured_promotion"]["enabled"] = True
    rollout = parse_business_profile_rollout_config(payload)
    phase = rollout.phase()
    with pytest.raises(ValueError, match="manifests are missing"):
        rollout.manifests_for(phase)


def test_runtime_identity_is_derived_and_explicit_values_must_match():
    llm_config = UnifiedConfigManager("config").get_llm_config()
    first = derive_business_profile_runtime_identities(llm_config)
    second = derive_business_profile_runtime_identities(llm_config)

    assert first == second
    assert set(first) == RUNTIME_IDENTITY_KEYS
    assert "business_profile_semantic_schemas.v2" in first["schema"]
    assert "business_profile_atomic_extraction.v3" in first["schema"]
    assert "business_profile_structured_extraction.v3" in first["schema"]
    assert "business_profile_structured_extraction.v3" in first["policy"]
    assert "logical_profile=semantic_extraction" in first["model"]
    assert llm_config.route_fingerprint("semantic_extraction") in first["model"]
    assert (
        resolve_business_profile_runtime_identities(
            llm_config=llm_config,
            mode="explicit",
            explicit=first,
        )
        == first
    )
    with pytest.raises(ValueError, match="incomplete"):
        resolve_business_profile_runtime_identities(
            llm_config=llm_config,
            mode="derived",
            explicit={"model": "stale"},
        )
    with pytest.raises(ValueError, match="do not match"):
        resolve_business_profile_runtime_identities(
            llm_config=llm_config,
            mode="explicit",
            explicit={**first, "model": "stale"},
        )


def test_rollout_status_reports_field_completion_and_exception_backlog(tmp_path):
    storage = _storage(tmp_path)
    phase = parse_business_profile_rollout_config(_payload()).phase()
    now = "2026-08-04T08:00:00+08:00"
    identities = {key: f"{key}.v1" for key in RUNTIME_IDENTITY_KEYS}
    with storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO business_profile_semantic_runs ("
            "run_id, instrument_id, source_document_id, field_family, status, "
            "bundle_hash, started_at, completed_at, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?)",
            (
                "run-1",
                "600000.SH",
                "annual-2025",
                "structured_segments",
                "bundle-1",
                now,
                now,
                now,
                now,
            ),
        )
        conn.execute(
            "INSERT INTO business_profile_announcement_frontier ("
            "frontier_id, instrument_id, symbol, exchange, source, announcement_id, "
            "title, published_at, report_period, document_type, index_payload_hash, "
            "status, first_seen_at, last_seen_at, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processed', ?, ?, ?, ?)",
            (
                "frontier-1",
                "600000.SH",
                "600000",
                "SSE",
                "cninfo",
                "annual-2025",
                "某公司2025年年度报告",
                now,
                "2025-12-31",
                "annual_report",
                "frontier-hash",
                now,
                now,
                now,
                now,
            ),
        )
        conn.execute(
            "INSERT INTO business_profile_work_items ("
            "work_id, frontier_id, instrument_id, source, announcement_id, "
            "report_period, document_type, policy, processing_identity_hash, "
            "stage, status, checkpoint_path, metadata_json, completed_at, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'publish', 'completed', ?, ?, ?, ?, ?)",
            (
                "work-1",
                "frontier-1",
                "600000.SH",
                "cninfo",
                "annual-2025",
                "2025-12-31",
                "annual_report",
                "latest_annual_only",
                "identity-hash",
                str(tmp_path / "work-1.json"),
                json.dumps(
                    {
                        "processing_identity": {
                            "field_families": list(phase.field_families),
                            "runtime_identities": identities,
                            "rollout_phase": phase.name,
                        }
                    }
                ),
                now,
                now,
                now,
            ),
        )
        conn.execute(
            "INSERT INTO business_profile_exceptions ("
            "exception_id, target_type, target_id, instrument_id, field_family, "
            "tier, reason_codes_json, gate_signature, gate_manifest_hash, "
            "created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, 'deep_review', '[]', ?, ?, ?, ?)",
            (
                "exception-1",
                "segments",
                "segment-1",
                "600000.SH",
                "structured_segments",
                "gate-1",
                "manifest-1",
                now,
                now,
            ),
        )
        conn.commit()

    status = build_business_profile_rollout_status(
        storage,
        phase=phase,
        active_universe_count=2,
        manifests={},
        runtime_identities=identities,
    )

    assert status["field_families"]["structured_segments"]["completion_ratio"] == 0.5
    assert status["field_families"]["tabular_operating_facts"][
        "completion_ratio"
    ] == 0.5
    assert status["open_deep_review"] == 1


def test_daily_readiness_requires_complete_discovery_queue_and_field_families():
    payload = _payload()
    payload["active_phase"] = "daily_incremental"
    for phase in payload["phases"].values():
        phase["enabled"] = True
    phase = parse_business_profile_rollout_config(payload).phase()
    family_status = {
        family: {"completion_ratio": 1.0, "manifest_ready": True}
        for family in phase.field_families
    }

    ready = evaluate_business_profile_rollout_readiness(
        phase=phase,
        queue_health={"claimable": 0, "terminal": 0},
        discovery={"status": "success", "discovery_window_backlog": 0},
        reconciliation={
            "active_universe_count": 100,
            "current_annual_instrument_count": 100,
            "stalled_frontier_count": 0,
        },
        rollout_status={
            "field_families": family_status,
            "open_machine_rework": 0,
            "open_quick_review": 0,
            "open_deep_review": 0,
        },
        readiness=payload["readiness"],
        scheduler_enabled=False,
    )
    assert ready["phase_ready"] is True
    assert ready["phase_reason_codes"] == []
    assert ready["daily_ready"] is True

    blocked = evaluate_business_profile_rollout_readiness(
        phase=phase,
        queue_health={"claimable": 1, "terminal": 0},
        discovery={"status": "degraded", "discovery_window_backlog": 2},
        reconciliation={
            "active_universe_count": 100,
            "current_annual_instrument_count": 100,
            "stalled_frontier_count": 0,
        },
        rollout_status={
            "field_families": {
                **family_status,
                "atomic_activities": {
                    "completion_ratio": 0.5,
                    "manifest_ready": False,
                },
            },
            "open_machine_rework": 1,
            "open_quick_review": 0,
            "open_deep_review": 0,
        },
        readiness=payload["readiness"],
        scheduler_enabled=False,
    )
    assert blocked["phase_ready"] is False
    assert blocked["daily_ready"] is False
    assert set(blocked["reason_codes"]) >= {
        "discovery_frontier_incomplete",
        "claimable_work_remaining",
        "field_family_coverage_incomplete",
        "promotion_manifests_not_ready",
        "machine_rework_backlog_present",
    }


def test_structured_phase_can_be_ready_without_daily_phase_activation():
    payload = _payload()
    phase = parse_business_profile_rollout_config(payload).phase("structured_shadow")
    family_status = {
        family: {"completion_ratio": 1.0, "manifest_ready": False}
        for family in phase.field_families
    }

    ready = evaluate_business_profile_rollout_readiness(
        phase=phase,
        queue_health={"claimable": 0, "terminal": 0},
        discovery={"status": "success", "discovery_window_backlog": 0},
        reconciliation={
            "active_universe_count": 100,
            "current_annual_instrument_count": 100,
            "stalled_frontier_count": 0,
        },
        rollout_status={
            "field_families": family_status,
            "open_machine_rework": 0,
            "open_quick_review": 0,
            "open_deep_review": 0,
        },
        readiness=payload["readiness"],
        scheduler_enabled=False,
    )

    assert ready["phase_ready"] is True
    assert ready["phase_reason_codes"] == []
    assert ready["daily_ready"] is False
    assert "daily_phase_not_active" in ready["reason_codes"]


def test_phase_readiness_waits_for_in_flight_queue_leases():
    payload = _payload()
    phase = parse_business_profile_rollout_config(payload).phase("structured_shadow")
    family_status = {
        family: {"completion_ratio": 1.0, "manifest_ready": False}
        for family in phase.field_families
    }

    readiness = evaluate_business_profile_rollout_readiness(
        phase=phase,
        queue_health={"claimable": 0, "running": 1, "terminal": 0},
        discovery={"status": "success", "discovery_window_backlog": 0},
        reconciliation={
            "active_universe_count": 100,
            "current_annual_instrument_count": 100,
            "stalled_frontier_count": 0,
        },
        rollout_status={
            "field_families": family_status,
            "open_machine_rework": 0,
            "open_quick_review": 0,
            "open_deep_review": 0,
        },
        readiness=payload["readiness"],
        scheduler_enabled=False,
    )

    assert readiness["phase_ready"] is False
    assert readiness["running_work_items"] == 1
    assert "running_work_remaining" in readiness["phase_reason_codes"]


def test_daily_and_disabled_backfill_phase_stop_before_storage_initialization():
    storage = Mock()
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.research_config = Mock(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": True,
                "semantic_production": {"enabled": True},
                "production_operations": {
                    "async_production_enabled": True,
                    "use_rollout_config": True,
                    "runtime_identity_mode": "derived",
                },
            }
        },
    )

    daily = asyncio.run(manager.run_business_profile_daily_incremental())
    backfill = asyncio.run(
        manager.run_business_profile_backfill(rollout_phase="semantic_shadow")
    )

    assert daily["status"] == "not_ready"
    assert daily["active_phase"] == "structured_shadow"
    assert backfill["status"] == "not_ready"
    storage.initialize.assert_not_called()


def test_backfill_rejects_field_families_outside_active_phase_before_storage_init():
    storage = Mock()
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.research_config = Mock(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": True,
                "semantic_production": {"enabled": True},
                "production_operations": {
                    "async_production_enabled": True,
                    "use_rollout_config": True,
                    "runtime_identity_mode": "derived",
                },
            }
        },
    )

    result = asyncio.run(
        manager.run_business_profile_backfill(
            field_families=["atomic_activities"],
        )
    )

    assert result["status"] == "not_ready"
    assert "outside rollout phase" in result["reason"]
    storage.initialize.assert_not_called()


def test_expanded_backfill_does_not_require_the_disabled_active_rollout_phase(
    monkeypatch,
):
    from data_manager import config_manager

    payload = _payload()
    payload["phases"]["structured_shadow"]["enabled"] = False
    original_get_nested = config_manager.get_nested
    monkeypatch.setattr(
        config_manager,
        "get_nested",
        lambda path, default=None: payload
        if path == "business_profile_rollout"
        else original_get_nested(path, default),
    )
    manager = DataManager.__new__(DataManager)
    manager.research_storage = Mock()
    manager.research_config = Mock(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": True,
                "semantic_production": {"enabled": True},
                "production_operations": {
                    "async_production_enabled": True,
                    "use_rollout_config": True,
                    "runtime_identity_mode": "derived",
                },
            }
        },
    )

    with pytest.raises(ValueError, match="requires instrument_ids or start_date"):
        asyncio.run(
            manager.run_business_profile_backfill(
                selection_policy="expanded",
                document_types=["resource_report"],
                field_families=["commodity_exposure_facts"],
            )
        )


def test_expanded_backfill_requires_explicit_document_and_field_families():
    manager = DataManager.__new__(DataManager)
    manager.research_storage = Mock()
    manager.research_config = Mock(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": True,
                "semantic_production": {"enabled": True},
                "production_operations": {
                    "async_production_enabled": True,
                    "use_rollout_config": True,
                    "runtime_identity_mode": "derived",
                },
            }
        },
    )

    with pytest.raises(ValueError, match="requires document_types and field_families"):
        asyncio.run(
            manager.run_business_profile_backfill(
                selection_policy="expanded",
                instrument_ids=["601088.SH"],
            )
        )


def test_rollout_rejects_fractional_integer_stage_budgets():
    payload = _payload()
    payload["phases"]["structured_shadow"]["stage_budgets"]["parse"][
        "max_concurrency"
    ] = 1.5

    with pytest.raises(ValueError, match="invalid rollout stage budget"):
        parse_business_profile_rollout_config(payload)
