import asyncio
import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, StreamObject

import research.business_profile_semantic_runtime as runtime_module
from research.business_profile_activity_production import GovernedCounterpartyResolver
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_promotion import FieldFamilyPromotionManifest
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_section_selection import (
    SelectedSection,
    SelectedSectionArtifact,
)
from research.business_profile_semantic_extraction import (
    DETERMINISTIC_VERIFICATION_PROOF_VERSION,
    SEMANTIC_EXTRACTION_SCHEMA_VERSION,
    SEMANTIC_VERIFIER_PROMPT_VERSION,
    STRUCTURED_EXTRACTION_SCHEMA_VERSION,
    _verification_claim,
)
from research.business_profile_semantic_pipeline import (
    BusinessProfileSemanticPipeline,
    SemanticProductionBudgets,
    SemanticProductionCheckpointStore,
    SemanticProductionConfig,
    SemanticProductionScope,
    SemanticProductionThresholds,
)
from research.business_profile_semantic_runtime import (
    BusinessProfileSemanticRuntime,
    _atomic_activity_fact_type,
    _atomic_activity_operating_fact,
    _ambiguous_operating_row_groups,
    _bind_promotion_validation,
    _bind_semantic_transformation_lineage,
    _catalog_version_scope,
    _catalogs_current,
    _document_period_basis,
    _expanded_action_verification_target,
    _normalized_value,
    _select_current_semantic_activities,
    _semantic_failure_reason,
    _semantic_operating_record,
    _semantic_relationship_assertion_ids,
    _verification_allows_promotion,
    _verification_result_is_current,
    compute_business_profile_semantic_source_revision,
    discover_business_profile_semantic_scope,
)
from research.providers.base import FinancialSourceFileManifest
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)
from utils.llm import LlmAuthenticationError, LlmResponse, LlmUsage


def test_runtime_async_bridge_accepts_concurrent_thread_submissions():
    bridges = [
        runtime_module._RuntimeAsyncBridge(),
        runtime_module._RuntimeAsyncBridge(),
    ]
    state = {"active": 0, "peak": 0, "loop_ids": set()}

    async def probe(value):
        state["loop_ids"].add(id(asyncio.get_running_loop()))
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        try:
            await asyncio.sleep(0.03)
            return value
        finally:
            state["active"] -= 1

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(bridge.run, probe(index))
                for index, bridge in enumerate(bridges)
            ]
            assert [future.result(timeout=2) for future in futures] == [0, 1]
        assert state["peak"] == 2
        assert len(state["loop_ids"]) == 1
    finally:
        for bridge in bridges:
            bridge.close()


def test_annual_document_basis_and_transformation_lineage_are_deterministic():
    assert _document_period_basis({"document_type": "annual_report"}) == (
        "period_total",
        "annual_document_type",
    )
    assert _document_period_basis({"document_type": "annual_report_correction"}) == (
        "period_total",
        "annual_document_type",
    )
    assert _document_period_basis({"document_type": "quarterly_report"}) is None

    process = {
        "activity_id": "process-1",
        "action": "processes",
        "object_raw": "废旧电池金属材料",
        "metadata": {
            "transformation_input_objects_raw": ["废旧电池金属材料"],
            "transformation_output_objects_raw": ["锂盐、前驱体及正极材料"],
        },
    }
    output = {
        "activity_id": "produce-1",
        "action": "produces",
        "object_raw": "锂盐、前驱体及正极材料等",
        "metadata": {},
    }
    records = [("activities", process), ("activities", output)]

    _bind_semantic_transformation_lineage(records)

    assert process["metadata"]["transformation_input_activity_ids"] == ["process-1"]
    assert process["metadata"]["transformation_output_activity_ids"] == ["produce-1"]
    assert process["metadata"]["transformation_lineage_status"] == "bound"


def test_transformation_lineage_rejects_missing_and_ambiguous_bindings():
    process = {
        "activity_id": "process-ambiguous",
        "action": "processes",
        "object_raw": "废旧电池",
        "metadata": {
            "transformation_input_objects_raw": ["不存在的输入"],
            "transformation_output_objects_raw": ["锂盐"],
        },
    }
    output_a = {
        "activity_id": "output-a",
        "action": "produces",
        "object_raw": "锂盐材料",
        "metadata": {},
    }
    output_b = {
        "activity_id": "output-b",
        "action": "sells",
        "object_raw": "锂盐产品",
        "metadata": {},
    }

    _bind_semantic_transformation_lineage(
        [("activities", process), ("activities", output_a), ("activities", output_b)]
    )

    assert process["metadata"]["transformation_input_activity_ids"] == []
    assert process["metadata"]["transformation_output_activity_ids"] == []
    assert process["metadata"]["transformation_lineage_status"] == "ambiguous"


def test_numeric_reconciliation_failure_is_not_classified_as_gateway_failure():
    assert (
        _semantic_failure_reason(
            ValueError("numeric_reconciliation_failed: reported margin mismatch")
        )
        == "numeric_reconciliation_failed"
    )


def test_operating_fact_row_identity_keeps_same_product_contracts_separate():
    item = {
        "instrument_id": "601012.SH",
        "document": {
            "identity": "annual-report-2025",
            "report_period": "2025-12-31",
            "published_at": "2026-03-30T00:00:00+08:00",
        },
    }
    evidence = {"source_document_id": "annual-report-2025"}
    first = _semantic_operating_record(
        item,
        {
            "segment_name_raw": "多晶硅料",
            "fact_type": "purchase_amount",
            "value": 4.18,
            "unit_raw": "亿元",
            "fact_scope": "多晶硅料:采购金额",
            "source_row_key": "row-contract-1",
            "evidence": evidence,
        },
        "span-contract-1",
    )
    second = _semantic_operating_record(
        item,
        {
            "segment_name_raw": "多晶硅料",
            "fact_type": "purchase_amount",
            "value": 0,
            "unit_raw": "亿元",
            "fact_scope": "多晶硅料:采购金额",
            "source_row_key": "row-contract-2",
            "evidence": evidence,
        },
        "span-contract-2",
    )

    assert first["record_id"] != second["record_id"]
    assert first["fact_scope"] != second["fact_scope"]
    assert first["metadata"]["source_row_key"] == "row-contract-1"
    assert second["metadata"]["source_row_key"] == "row-contract-2"
    assert _ambiguous_operating_row_groups([first, second]) == [[first, second]]
    assert (
        _semantic_failure_reason(ValueError("unsupported ratio unit: （%）"))
        == "unit_normalization_failed"
    )


def test_legacy_semantic_run_is_not_reused_without_occurrence_identity():
    repository = Mock()
    repository.get_record.side_effect = [
        {"fact_type": "purchase_amount", "metadata": {"semantic_synthesis": True}},
        {
            "fact_type": "purchase_amount",
            "metadata": {
                "semantic_synthesis": True,
                "source_row_key": "row-contract-1",
                "occurrence_identity_quality": "derived_from_evidence",
            }
        },
    ]
    runtime = object.__new__(BusinessProfileSemanticRuntime)
    runtime.repository = repository
    assert runtime._semantic_reuse_has_legacy_occurrence_identity(
        {"operating_facts": ["legacy-fact"]}
    ) is True
    assert runtime._semantic_reuse_has_legacy_occurrence_identity(
        {"operating_facts": ["current-fact"]}
    ) is False


def test_concentration_projection_is_not_blocked_by_contract_identity_gate():
    repository = Mock()
    repository.get_record.return_value = {
        "fact_type": "customer_concentration_share",
        "metadata": {"semantic_synthesis": True},
    }
    runtime = object.__new__(BusinessProfileSemanticRuntime)
    runtime.repository = repository
    assert runtime._semantic_reuse_has_legacy_occurrence_identity(
        {"operating_facts": ["concentration-fact"]}
    ) is False


def test_legacy_joint_response_is_rejected_before_durable_replay():
    assert BusinessProfileSemanticRuntime._semantic_response_has_legacy_occurrence_identity({
        "activities": [{"action": "produces", "object_raw": "农药", "value": 1, "unit": "吨"}]
    }) is True
    assert BusinessProfileSemanticRuntime._semantic_response_has_legacy_occurrence_identity({
        "activities": [{
            "action": "produces", "object_raw": "农药", "value": 1, "unit": "吨",
            "source_row_key": "row-1",
        }]
    }) is False


def test_local_value_error_is_not_classified_as_gateway_congestion():
    assert _semantic_failure_reason(ValueError("business profile temporal conflict")) == (
        "business_rule_validation_failed"
    )


def test_scoped_exception_backlog_ignores_historical_reports_and_identities(tmp_path):
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path)),
        artifact_root=tmp_path / "artifacts",
    )
    scope = _scope("atomic_activities")
    identities = dict(scope.identities)
    runtime.repository.list_exceptions = Mock(
        return_value=[
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "tier": "machine_rework",
                "metadata": {
                    "source_document_id": "current-report",
                    "runtime_identities": identities,
                },
            },
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "tier": "machine_rework",
                "metadata": {
                    "source_document_id": "historical-report",
                    "runtime_identities": identities,
                },
            },
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "tier": "machine_rework",
                "metadata": {
                    "source_document_id": "current-report",
                    "runtime_identities": {**identities, "parser": "parser.v0"},
                },
            },
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "tier": "deep_review",
                "metadata": {"source_document_id": "current-report"},
            },
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "tier": "machine_rework",
                "metadata": {"source_document_id": "current-report"},
            },
        ]
    )

    backlog = runtime._scoped_exception_backlog(
        scope,
        plans=[
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "included": [{"identity": "current-report"}],
            }
        ],
    )

    assert backlog == 2


def test_network_budget_ignores_elapsed_time_from_prior_checkpoint(tmp_path):
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path)),
        artifact_root=tmp_path / "artifacts",
        clock=lambda: 101.0,
    )

    assert runtime._network_budget_stop_reason(
        config=SemanticProductionConfig(
            enabled=True,
            budgets=SemanticProductionBudgets(max_elapsed_seconds=10),
        ),
        checkpoint_metrics={"elapsed_seconds": 3_600},
        stage_metrics={},
        stage_started_at=100.0,
    ) is None


def test_plan_returns_quality_stop_for_current_scoped_exception_backlog(
    tmp_path, monkeypatch
):
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path)),
        artifact_root=tmp_path / "artifacts",
    )
    monkeypatch.setattr(
        runtime, "_scoped_exception_backlog", lambda *_args, **_kwargs: 3
    )

    result = runtime.plan(
        scope=_scope("atomic_activities"),
        config=SemanticProductionConfig(
            enabled=True,
            thresholds=SemanticProductionThresholds(max_exception_backlog=2),
        ),
        checkpoint={"metrics": {}},
    )

    assert result["quality"] == {
        "stage_ready": False,
        "blocking_machine_rework": 3,
    }


def test_only_current_self_consistent_verification_can_be_reused_or_promoted():
    checks = {
        "subject": True,
        "action": True,
        "object": True,
        "scope": True,
        "period": True,
        "evidence": True,
    }
    current = {
        "decision": "confirmed",
        "checks": checks,
        "prompt_version": SEMANTIC_VERIFIER_PROMPT_VERSION,
    }
    old = {**current, "prompt_version": "business_profile_atomic_verifier.v5"}
    contradictory = {
        **current,
        "checks": {**checks, "object": False},
    }
    stale_local_proof = {
        "decision": "confirmed",
        "proof": {
            "skip_semantic_verifier": True,
            "canonical_promotion_allowed": True,
        },
    }
    current_local_proof = {
        "decision": "confirmed",
        "proof": {
            "proof_version": DETERMINISTIC_VERIFICATION_PROOF_VERSION,
            "skip_semantic_verifier": True,
            "canonical_promotion_allowed": True,
            "promotion_block_reasons": [],
        },
    }

    assert _verification_result_is_current(current) is True
    assert _verification_allows_promotion(current) is True
    assert _verification_result_is_current(old) is False
    assert _verification_allows_promotion(old) is False
    assert _verification_result_is_current(contradictory) is False
    assert _verification_allows_promotion(contradictory) is False
    assert _verification_result_is_current(stale_local_proof) is False
    assert _verification_allows_promotion(stale_local_proof) is False
    assert _verification_result_is_current(current_local_proof) is False
    assert _verification_allows_promotion(current_local_proof) is True
    assert _verification_allows_promotion(None) is False


def test_concentration_verification_never_uses_opaque_scope_as_semantics():
    target = {
        "record_id": "bp-operating-opaque",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "fact_type": "supplier_concentration_share",
        "value_raw": 0.144,
        "unit_raw": "fraction",
        "value_normalized": 0.144,
        "unit_normalized": "fraction",
        "fact_scope": "anonymous-concentration-scope:" + "a" * 32,
        "metadata": {"object_raw": "采购额"},
    }

    claim = _verification_claim("concentration", target)

    assert claim["object_raw"] == "采购额"
    assert "fact_scope" not in claim
    assert "scope_label_raw" not in claim


def test_derived_validation_uses_current_catalog_without_mutating_evidence():
    evidence = {
        "metadata": {
            "promotion_validation": {
                "official_identity_verified": True,
                "artifact_quality_verified": True,
                "exact_evidence_verified": True,
                "catalog_versions": {
                    "fact": "old-facts",
                    "product": "old-products",
                    "unit": "old-units",
                },
            }
        }
    }
    record = {
        "data_available_date": "2026-03-30",
        "knowledge_from": "2026-03-30",
        "metadata": {},
    }

    _bind_promotion_validation(record, evidence)

    current = runtime_module._current_catalog_versions()
    assert record["metadata"]["promotion_validation"]["catalog_versions"] == current
    assert evidence["metadata"]["promotion_validation"]["catalog_versions"] == {
        "fact": "old-facts",
        "product": "old-products",
        "unit": "old-units",
    }


def test_unchanged_approved_record_resolves_its_stale_open_exception(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    scope_without_manifest = _scope("named_relationships")
    manifest = FieldFamilyPromotionManifest(
        field_family="named_relationships",
        enabled=True,
        benchmark_passed=True,
        identities=scope_without_manifest.identities,
    )
    scope = _scope("named_relationships", manifest)
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
    )
    target_id = "relationship-approved-with-stale-exception"
    now = "2026-08-23T12:00:00+08:00"
    with repository.storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO business_profile_exceptions ("
            "exception_id, target_type, target_id, instrument_id, field_family, "
            "tier, reason_codes_json, gate_signature, gate_manifest_hash, "
            "created_at, updated_at"
            ") VALUES (?, 'relationships', ?, '601088.SH', "
            "'named_relationships', 'deep_review', ?, ?, ?, ?, ?)",
            (
                "stale-approved-exception",
                target_id,
                json.dumps(["failed_gate:semantic_proof"]),
                "stale-approved-gate",
                manifest.manifest_hash,
                now,
                now,
            ),
        )
        conn.commit()

    result = runtime._promote_record(
        "relationships",
        {"relationship_id": target_id, "review_status": "approved"},
        family="named_relationships",
        manifest=manifest,
        scope=scope,
        semantic_proof=True,
    )

    assert result["decision"]["classification"] == "unchanged"
    assert result["promoted"] is True
    assert repository.list_exceptions(status="open") == []
    assert {
        item["exception_id"] for item in repository.list_exceptions(status="resolved")
    } == {"stale-approved-exception"}


def test_operating_fact_catalog_gate_ignores_unrelated_product_release():
    recorded = {
        "fact": "business_profile_facts.2026.2",
        "product": "business_profile_products.2026.3",
        "unit": "business_profile_units.2026.7",
    }
    current = {
        **recorded,
        "product": "business_profile_products.2026.4",
    }

    assert _catalog_version_scope("operating_facts", "tabular_operating_facts") == {
        "fact",
        "unit",
    }
    assert (
        _catalogs_current(
            recorded,
            current,
            required_keys=_catalog_version_scope(
                "operating_facts", "tabular_operating_facts"
            ),
        )
        is True
    )


def test_exposure_catalog_gate_requires_product_release():
    recorded = {
        "fact": "business_profile_facts.2026.2",
        "product": "business_profile_products.2026.3",
        "unit": "business_profile_units.2026.7",
    }
    current = {
        **recorded,
        "product": "business_profile_products.2026.4",
    }

    assert (
        _catalogs_current(
            recorded,
            current,
            required_keys=_catalog_version_scope(
                "exposure_facts", "commodity_exposure_facts"
            ),
        )
        is False
    )


def test_current_activity_selection_prefers_canonical_mapping():
    base = {
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "evidence_id": "evidence-1",
        "subject_scope": "consolidated_group",
        "action": "sells",
        "object_type": "product",
        "object_raw": "聚乙烯",
        "segment_id": None,
        "geography": "中国",
        "value": 373.9,
        "unit": "千吨",
        "share": None,
        "business_regime_id": "regime-2025",
        "knowledge_from": "2026-03-30",
        "version": 1,
    }
    old = {
        **base,
        "activity_id": "activity-old",
        "object_id": None,
        "updated_at": "2026-08-20T00:00:00+08:00",
    }
    mapped = {
        **base,
        "activity_id": "activity-mapped",
        "object_id": "polymer.polyethylene",
        "updated_at": "2026-08-19T00:00:00+08:00",
    }
    subsidiary = {
        **mapped,
        "activity_id": "activity-subsidiary",
        "subject_scope": "named_subsidiary",
    }
    overseas = {
        **mapped,
        "activity_id": "activity-overseas",
        "geography": "海外",
    }
    prior_regime = {
        **mapped,
        "activity_id": "activity-prior-regime",
        "business_regime_id": "regime-2024",
    }

    selected = _select_current_semantic_activities(
        [old, mapped, subsidiary, overseas, prior_regime]
    )

    assert [item["activity_id"] for item in selected] == [
        "activity-mapped",
        "activity-subsidiary",
        "activity-overseas",
        "activity-prior-regime",
    ]


def test_atomic_sales_measurements_group_into_one_activity_and_two_facts(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
    )
    text = "LED芯片及外延片 销售量23,634,408片 销售收入1,392,187,620.56元"
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    section = SelectedSection(
        section_id="section-led-sales",
        page_number=38,
        section_key="operating_analysis",
        text=text,
        normalized_text=text,
        normalized_start=0,
        normalized_end=len(text),
        page_hash=text_hash,
        section_hash=text_hash,
        selector_reasons=("test",),
        quality="native",
    )
    selected = SelectedSectionArtifact(
        artifact_version="business_profile_selected_sections.v1",
        bundle={"bundle_id": "bundle-led-sales"},
        sections=(section,),
        previous_bundle_id=None,
        expansion_reason=None,
        artifact_hash="selected-led-sales",
    )
    exact_evidence = {
        "section_id": section.section_id,
        "page_number": section.page_number,
        "section_hash": section.section_hash,
        "quote": text,
        "quote_hash": text_hash,
    }

    def assertion(assertion_id, label, summary, value, unit):
        return {
            "activity_id": assertion_id,
            "instrument_id": "300708.SZ",
            "report_period": "2025-12-31",
            "subject_scope": "issuer",
            "action": "sells",
            "object_raw": "LED芯片及外延片",
            "source_label_raw": label,
            "semantic_summary_zh": summary,
            "source_value": value,
            "source_unit_raw": unit,
            "value": value,
            "unit": unit,
            "model_derived_hints": {},
            "evidence": exact_evidence,
        }

    item = {
        "instrument_id": "300708.SZ",
        "field_family": "atomic_activities",
        "selected_artifact_hash": selected.artifact_hash,
        "document": {
            "identity": "shared-asset:300708-2025",
            "report_period": "2025-12-31",
            "published_at": "2026-04-20T18:00:00+08:00",
            "content_hash": "a" * 64,
            "source": "cninfo",
            "source_tier": "official_filing",
            "document_type": "annual_report",
            "title": "2025年年度报告",
            "metadata": {},
        },
    }
    envelope = SimpleNamespace(
        activities=(
            assertion(
                "semantic-led-sales-volume",
                "销售量",
                "2025年公司销售LED芯片及外延片23,634,408片。",
                23634408,
                "片",
            ),
            assertion(
                "semantic-led-sales-revenue",
                "销售收入",
                "2025年公司销售LED芯片及外延片实现销售收入1,392,187,620.56元。",
                1392187620.56,
                "元",
            ),
        ),
        relationships=(),
    )

    evidence_records, converted, exceptions = runtime._semantic_records(
        item,
        selected,
        envelope,
        record_types=("activities",),
    )
    activities = [record for kind, record in converted if kind == "activities"]
    facts = [record for kind, record in converted if kind == "operating_facts"]

    assert exceptions == []
    assert len(evidence_records["evidence"]) == 1
    assert len(activities) == 1
    assert len(facts) == 2
    assert {fact["fact_type"] for fact in facts} == {
        "sales_volume",
        "sales_revenue",
    }
    assert len({fact["record_id"] for fact in facts}) == 2
    assert {fact["metadata"]["source_activity_id"] for fact in facts} == {
        activities[0]["activity_id"]
    }
    assert activities[0]["value"] == 23634408
    assert activities[0]["unit"] == "片"
    assert activities[0]["metadata"]["measurement_projection_fact_type"] == (
        "sales_volume"
    )
    assert activities[0]["metadata"]["semantic_assertion_ids"] == [
        "semantic-led-sales-volume",
        "semantic-led-sales-revenue",
    ]
    activities[0]["run_id"] = "run-300708-grouped"
    persisted = repository.persist_document_field_family_bundle(
        run={
            "run_id": "run-300708-grouped",
            "instrument_id": "300708.SZ",
            "source_document_id": item["document"]["identity"],
            "field_family": "atomic_activities",
            "bundle_hash": selected.artifact_hash,
            "fact_catalog_version": "business_profile_facts.2026.3",
            "product_catalog_version": "business_profile_products.2026.4",
            "metadata": {"document_hash": item["document"]["content_hash"]},
        },
        records_by_type={
            "evidence": evidence_records["evidence"],
            "activities": activities,
            "operating_facts": facts,
        },
    )
    assert persisted["activity_count"] == 1
    assert persisted["fact_count"] == 2


@pytest.mark.parametrize(
    ("source_label", "action", "unit", "expected"),
    [
        ("生产量", "produces", "片", "production_volume"),
        ("库存量", "stores", "片", "inventory_volume"),
        ("采购量", "purchases", "万吨", "purchase_volume"),
        ("采购额", "purchases", "元", "purchase_amount"),
        ("销售收入", "sells", "元", "sales_revenue"),
        ("其他披露指标", "transports", "万吨", "other_measurement"),
    ],
)
def test_atomic_measurement_classification_is_program_owned(
    source_label, action, unit, expected
):
    assert (
        _atomic_activity_fact_type(
            {
                "source_label_raw": source_label,
                "action": action,
                "source_unit_raw": unit,
            }
        )
        == expected
    )


def test_atomic_measurement_classification_uses_approved_runtime_unit_rule():
    assert (
        _atomic_activity_fact_type(
            {
                "source_label_raw": "报告期指标",
                "action": "sells",
                "source_unit_raw": "自定义货币单位",
            },
            runtime_unit_rules=(
                {
                    "rule_id": "runtime:custom-currency",
                    "normalized_lexeme": "自定义货币单位",
                    "dimension": "currency",
                    "canonical_unit": "CNY",
                    "multiplier": "1",
                    "status": "auto_approved",
                },
            ),
        )
        == "sales_revenue"
    )


def test_atomic_unknown_unit_preserves_raw_fact_as_pending():
    item = {
        "instrument_id": "300708.SZ",
        "document": {
            "identity": "shared-asset:300708-2025",
            "report_period": "2025-12-31",
            "published_at": "2026-04-20T18:00:00+08:00",
        },
    }
    assertion = {
        "activity_id": "semantic-unknown-unit",
        "action": "sells",
        "object_raw": "测试产品",
        "source_label_raw": "销售量",
        "semantic_summary_zh": "报告期销售测试产品12未治理单位。",
        "source_value": 12,
        "source_unit_raw": "未治理单位",
        "model_derived_hints": {},
        "evidence": {"section_id": "section-1", "quote_hash": "b" * 64},
    }

    fact = _atomic_activity_operating_fact(
        item,
        assertion,
        activity_id="activity-unknown-unit",
        evidence_id="evidence-unknown-unit",
    )

    assert fact["fact_type"] == "sales_volume"
    assert fact["value_raw"] == 12
    assert fact["unit_raw"] == "未治理单位"
    assert fact["value_normalized"] is None
    assert fact["unit_normalized"] is None
    assert fact["metadata"]["unit_resolution_status"] == ("unit_resolution_pending")
    assert fact["metadata"]["publication_blocker"] == "unit_resolution_pending"


def test_legacy_relationship_reconstructs_exact_semantic_assertion_ids():
    evidence = {
        "quote": "本集团向示例供应商采购原料",
        "quote_hash": "quote-hash",
    }
    record = {
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "relationship_type": "buys_from",
        "counterparty_name_raw": "示例供应商有限公司",
        "anonymous": 0,
        "disclosed_share": 0.015,
        "object_raw": "采购",
        "metadata": {"exact_evidence": evidence},
    }
    expected = tuple(
        hashlib.sha256(
            json.dumps(
                {
                    "instrument_id": "601088.SH",
                    "report_period": "2025-12-31",
                    "subject_scope": subject_scope,
                    "relationship_type": "buys_from",
                    "counterparty_name_raw": "示例供应商有限公司",
                    "anonymous": False,
                    "disclosed_share": 0.015,
                    "object_raw": "采购",
                    "evidence": evidence,
                    "semantic_synthesis": True,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        for subject_scope in ("issuer", "consolidated_group")
    )

    assert _semantic_relationship_assertion_ids(record) == expected
    record["metadata"]["semantic_assertion_id"] = "persisted-assertion-id"
    assert _semantic_relationship_assertion_ids(record) == ("persisted-assertion-id",)


def test_latest_selected_artifact_query_returns_completed_document_match(tmp_path):
    storage = _storage(tmp_path)
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(storage),
        artifact_root=tmp_path / "artifacts",
    )
    text = "报告期内公司从事煤炭生产与销售。"
    section_hash = hashlib.sha256(text.encode()).hexdigest()
    selected = SelectedSectionArtifact(
        artifact_version="business_profile_selected_sections.v1",
        bundle={
            "source_document_id": "document-2025",
            "field_family": "atomic_activities",
        },
        sections=(
            SelectedSection(
                section_id="section-1",
                page_number=1,
                section_key="business_overview",
                text=text,
                normalized_text=text,
                normalized_start=0,
                normalized_end=len(text),
                page_hash=section_hash,
                section_hash=section_hash,
                selector_reasons=("test",),
                quality="native",
            ),
        ),
        previous_bundle_id=None,
        expansion_reason=None,
        artifact_hash="selected-artifact-hash",
    )
    selected_path, _ = runtime.section_store.write(selected)
    now = "2026-03-30T10:00:00+08:00"
    with storage.get_connection() as conn:
        storage._apply_pragmas(conn)
        conn.execute(
            "INSERT INTO business_profile_semantic_runs ("
            "run_id, instrument_id, source_document_id, field_family, status, "
            "bundle_hash, metadata_json, started_at, completed_at, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?)",
            (
                "run-selected-artifact",
                "601088.SH",
                "document-2025",
                "atomic_activities",
                "bundle-hash",
                json.dumps({"selected_artifact_path": str(selected_path)}),
                now,
                now,
                now,
                now,
            ),
        )
        conn.commit()

    recovered = runtime._latest_selected_artifact(
        instrument_id="601088.SH",
        field_family="atomic_activities",
        source_document_id="document-2025",
    )

    assert recovered is not None
    assert recovered.artifact_hash == "selected-artifact-hash"


def _reusable_context_fixture(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
    )
    text = "报告期内公司生产与销售测试产品。"
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    section = SelectedSection(
        section_id="section-reuse-source",
        page_number=7,
        section_key="principal_business",
        text=text,
        normalized_text=text,
        normalized_start=0,
        normalized_end=len(text),
        page_hash=text_hash,
        section_hash=text_hash,
        selector_reasons=("test",),
        quality="native",
    )
    bundle = {
        "schema_version": "business_profile_selected_section_bundle.v1",
        "source_document_id": "shared-asset:reuse-document",
        "document_hash": "d" * 64,
        "field_family": "atomic_activities",
        "section_ids": [section.section_id],
        "section_hash": runtime_module._stable_hash([section.section_hash]),
    }
    artifact_core = {
        "artifact_version": "business_profile_selected_sections.v1",
        "bundle": bundle,
        "sections": [section.to_dict()],
        "previous_bundle_id": None,
        "expansion_reason": None,
    }
    selected = SelectedSectionArtifact(
        artifact_version=artifact_core["artifact_version"],
        bundle=bundle,
        sections=(section,),
        previous_bundle_id=None,
        expansion_reason=None,
        artifact_hash=runtime_module._stable_hash(artifact_core),
    )
    selected_path, _ = runtime.section_store.write(selected)
    evidence = {
        "source_document_id": "shared-asset:reuse-document",
        "metadata": {
            "selected_artifact_hash": selected.artifact_hash,
            "evidence_spans": [
                {
                    "evidence_span_id": "span-reuse-source",
                    "section_id": section.section_id,
                    "page_number": section.page_number,
                    "section_hash": section.section_hash,
                    "normalized_start": 0,
                    "normalized_end": len(text),
                    "quote": text,
                    "quote_hash": text_hash,
                }
            ],
        },
    }
    return runtime, selected, selected_path, evidence


def test_semantic_reuse_uses_source_artifact_when_current_selection_differs(tmp_path):
    runtime, selected, selected_path, evidence = _reusable_context_fixture(tmp_path)
    runtime.repository.get_record = Mock(return_value=evidence)
    item = {
        "instrument_id": "601088.SH",
        "field_family": "atomic_activities",
        "document": {
            "identity": "shared-asset:reuse-document",
            "content_hash": "d" * 64,
            "report_period": "2025-12-31",
        },
    }
    validated, reason = runtime._validate_semantic_reuse_context(
        item=item,
        metadata={"evidence_ids": ["evidence-reuse"]},
        source_path=str(selected_path),
        source_hash=selected.artifact_hash,
    )
    assert validated is not None, reason
    assert validated.artifact_hash == selected.artifact_hash
    assert reason == ""


def test_semantic_reuse_rejects_evidence_from_different_section_context(tmp_path):
    runtime, selected, selected_path, evidence = _reusable_context_fixture(tmp_path)
    evidence["metadata"]["evidence_spans"][0]["section_id"] = "section-from-other-artifact"
    runtime.repository.get_record = Mock(return_value=evidence)
    item = {
        "instrument_id": "601088.SH",
        "field_family": "atomic_activities",
        "document": {
            "identity": "shared-asset:reuse-document",
            "content_hash": "d" * 64,
            "report_period": "2025-12-31",
        },
    }
    validated, reason = runtime._validate_semantic_reuse_context(
        item=item,
        metadata={"evidence_ids": ["evidence-reuse"]},
        source_path=str(selected_path),
        source_hash=selected.artifact_hash,
    )
    assert validated is None
    assert reason == "unknown_section:evidence-reuse"


def test_semantic_reuse_rejects_malformed_evidence_range_without_raising(tmp_path):
    runtime, selected, selected_path, evidence = _reusable_context_fixture(tmp_path)
    evidence["metadata"]["evidence_spans"][0]["normalized_start"] = "invalid"
    runtime.repository.get_record = Mock(return_value=evidence)
    item = {
        "instrument_id": "601088.SH",
        "field_family": "atomic_activities",
        "document": {
            "identity": "shared-asset:reuse-document",
            "content_hash": "d" * 64,
            "report_period": "2025-12-31",
        },
    }
    validated, reason = runtime._validate_semantic_reuse_context(
        item=item,
        metadata={"evidence_ids": ["evidence-reuse"]},
        source_path=str(selected_path),
        source_hash=selected.artifact_hash,
    )
    assert validated is None
    assert reason == "malformed_range:evidence-reuse"


def test_semantic_reuse_accepts_deterministic_evidence_without_span_manifest(tmp_path):
    runtime, selected, selected_path, evidence = _reusable_context_fixture(tmp_path)
    evidence.pop("section_path", None)
    evidence["section_path"] = selected.sections[0].section_id
    evidence["page_number"] = selected.sections[0].page_number
    evidence["extraction_method"] = "deterministic_table"
    evidence["metadata"].pop("evidence_spans", None)
    evidence["metadata"]["section_hash"] = selected.sections[0].section_hash
    runtime.repository.get_record = Mock(return_value=evidence)
    item = {
        "instrument_id": "601088.SH",
        "field_family": "structured_segments",
        "document": {
            "identity": "shared-asset:reuse-document",
            "content_hash": "d" * 64,
            "report_period": "2025-12-31",
        },
    }
    validated, reason = runtime._validate_semantic_reuse_context(
        item=item,
        metadata={"evidence_ids": ["evidence-reuse"]},
        source_path=str(selected_path),
        source_hash=selected.artifact_hash,
    )
    assert validated is not None, reason
    assert reason == ""


def test_verify_missing_selected_artifact_becomes_rework_instead_of_crashing(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
    )
    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    checkpoint = pipeline.checkpoint_store.load()
    selected_payload = pipeline.handlers["select"].__self__.stage_store.read(
        checkpoint["artifacts"]["select"], expected_stage="select"
    )
    selected_path = selected_payload["selected"][0]["selected_artifact_path"]
    selected_file = runtime_module.Path(selected_path)
    selected_file.unlink()

    result = pipeline.run("verify", scope=scope)

    assert result["status"] == "stopped"
    assert result["reason"].startswith("quality_gate:verify:")
    verify_payload = pipeline.handlers["verify"].__self__.stage_store.read(
        pipeline.checkpoint_store.load()["artifacts"]["verify"],
        expected_stage="verify",
    )
    assert verify_payload["verifications"] == []
    assert verify_payload["machine_rework"]
    assert {
        item["reason_code"] for item in verify_payload["machine_rework"]
    } == {"evidence_provenance_failed"}


def test_structured_record_ids_rotate_without_changing_stable_segment_identity(
    monkeypatch,
):
    item = {
        "instrument_id": "600000.SH",
        "document": {
            "identity": "report-2025",
            "report_period": "2025-12-31",
            "published_at": "2026-03-31T08:00:00+08:00",
        },
    }
    evidence = {
        "evidence_span_ids": ["span-1"],
        "evidence_spans": [{"evidence_span_id": "span-1"}],
    }
    segment_row = {
        "revenue": 100,
        "segment_cost": 60,
        "gross_margin": 40,
        "gross_margin_unit": "%",
        "currency_unit": "万元",
        "revenue_unit_raw": "万元",
        "cost_unit_raw": "万元",
        "segment_name_raw": "主营产品",
        "segment_type": "product",
        "source_label_raw": "主营产品",
        "semantic_summary_zh": "主营产品收入和成本情况。",
        "model_derived_hints": {},
        "evidence": evidence,
    }
    operating_row = {
        "value": 20,
        "unit_raw": "万吨",
        "segment_name_raw": "主营产品",
        "fact_type": "production_volume",
        "fact_scope": "报告期产量",
        "source_label_raw": "产量",
        "semantic_summary_zh": "报告期产量为20万吨。",
        "model_derived_hints": {},
        "evidence": evidence,
    }

    first_segment = runtime_module._semantic_segment_record(
        item,
        segment_row,
        "evidence-1",
    )
    first_operating = runtime_module._semantic_operating_record(
        item,
        operating_row,
        "evidence-1",
    )
    monkeypatch.setattr(
        runtime_module,
        "RUNTIME_SCHEMA_VERSION",
        "business_profile_semantic_runtime.next",
    )
    next_segment = runtime_module._semantic_segment_record(
        item,
        segment_row,
        "evidence-1",
    )
    next_operating = runtime_module._semantic_operating_record(
        item,
        operating_row,
        "evidence-1",
    )

    assert first_segment["record_id"] != next_segment["record_id"]
    assert first_operating["record_id"] != next_operating["record_id"]
    assert first_segment["segment_id"] == next_segment["segment_id"]
    assert first_operating["segment_id"] == next_operating["segment_id"]


def _storage(tmp_path):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage


def _pdf_bytes(text):
    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    page[NameObject("/Resources")] = DictionaryObject(
        {
            NameObject("/Font"): DictionaryObject(
                {NameObject("/F1"): writer._add_object(font)}
            )
        }
    )
    escaped = str(text).replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    stream = StreamObject()
    stream.set_data(f"BT /F1 12 Tf 72 720 Td ({escaped}) Tj ET".encode("latin-1"))
    page[NameObject("/Contents")] = writer._add_object(stream)
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _manifest(path, content, *, instrument_id="601088.SH"):
    return {
        "source_file_id": "source-2025",
        "instrument_id": instrument_id,
        "source": "cninfo",
        "report_period": "2025-12-31",
        "report_type": "annual_report",
        "filing_id": "announcement-2025",
        "archive_path": str(path),
        "content_hash": hashlib.sha256(content).hexdigest(),
        "published_at": "2026-03-30T10:00:00+08:00",
        "status": "verified",
        "source_tier": "official_primary",
        "schema_version": "business_profile_source_asset.v1",
        "metadata": {"announcement_title": "2025 Annual Report"},
    }


def _scope(family, manifest=None):
    identities = {
        "document": "document.v1",
        "section": "section.v1",
        "selector": "selector.v1",
        "parser": "parser.v1",
        "schema": "schema.v1",
        "catalog": "catalog.v1",
        "model": "model.v1",
        "verifier": "verifier.v1",
        "rules": "rules.v1",
        "policy": "policy.v1",
    }
    promotion_hashes = {} if manifest is None else {family: manifest.manifest_hash}
    return SemanticProductionScope(
        instruments=("601088.SH",),
        field_families=(family,),
        knowledge_cutoff="2026-08-01",
        identities=identities,
        promotion_manifest_hashes=promotion_hashes,
    )


def _response(data, request):
    raw = json.dumps(data, ensure_ascii=False)
    return LlmResponse(
        status="success",
        data=data,
        raw_content=raw,
        provider="fake",
        model="model.v1",
        finish_reason="stop",
        usage=LlmUsage(input_tokens=30, output_tokens=10, total_tokens=40),
        request_id="request",
        provider_request_id="provider-request",
        request_hash=hashlib.sha256(repr(request).encode()).hexdigest(),
        response_hash=hashlib.sha256(raw.encode()).hexdigest(),
        schema_name=request.schema_name,
        schema_version=request.schema_version,
        structured_output_mode="json_object",
        latency_ms=5,
        attempt_count=1,
        warnings=(),
        lineage={},
    )


def _request_span_ids(payload, *required_texts):
    spans = payload["evidence_spans"]
    ids = [
        item["evidence_span_id"]
        for item in spans
        if any(text in item["text"] for text in required_texts)
    ]
    assert ids
    return ids


def _batch_verification_decision(target_id, *, decision="supported", failed=()):
    failed_aspects = tuple(failed)
    return {
        "target_id": target_id,
        "decision": decision,
        "checks": {
            key: key not in failed_aspects
            for key in ("subject", "action", "object", "scope", "period", "evidence")
        },
        "failed_aspects": list(failed_aspects),
        "reason_zh": (
            "公告证据完整支持该业务断言"
            if decision == "supported"
            else "公告证据不足以支持该业务断言"
        ),
    }


class _FakeGateway:
    def __init__(self):
        self.requests = []
        self.loop = None
        self.closed = False

    async def complete(self, request):
        running_loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = running_loop
        assert running_loop is self.loop
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            payload = json.loads(request.messages[-1].content)
            if payload.get("records"):
                decisions = [
                    _batch_verification_decision(item["target_id"])
                    for item in payload["records"]
                ]
                return _response(
                    {
                        "schema_version": "business_profile_semantic_batch_verifier.v1",
                        "decisions": decisions,
                    },
                    request,
                )
            data = {
                "decision": "confirmed",
                "checks": {
                    "subject": True,
                    "action": True,
                    "object": True,
                    "scope": True,
                    "period": True,
                    "evidence": True,
                },
            }
            return _response(data, request)
        payload = json.loads(request.messages[-1].content)
        quote = "公司生产动力煤"
        data = {
            "schema_version": SEMANTIC_EXTRACTION_SCHEMA_VERSION,
            "instrument_id": payload["instrument_id"],
            "report_period": payload["report_period"],
            "activities": [
                {
                    "subject_scope": "issuer",
                    "action": "produces",
                    "object_raw": "动力煤",
                    "semantic_summary_zh": "公司生产动力煤",
                    "value": None,
                    "unit": None,
                    "evidence_span_ids": _request_span_ids(payload, quote),
                }
            ],
            "relationships": [],
        }
        return _response(data, request)

    async def close(self):
        assert asyncio.get_running_loop() is self.loop
        self.closed = True


class _StructuredSegmentGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            return _response(
                {
                    "decision": "confirmed",
                    "checks": {
                        "subject": True,
                        "action": True,
                        "object": True,
                        "scope": True,
                        "period": True,
                        "evidence": True,
                    },
                },
                request,
            )
        payload = json.loads(request.messages[-1].content)
        quote = "煤炭 100 60 40%"
        return _response(
            {
                "schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
                "field_family": payload["field_family"],
                "instrument_id": payload["instrument_id"],
                "report_period": payload["report_period"],
                "rows": [
                    {
                        "segment_type": "product",
                        "segment_name_raw": "煤炭",
                        "revenue": 100.0,
                        "segment_cost": 60.0,
                        "gross_margin": 0.4,
                        "currency_unit": "万元",
                        "evidence_span_ids": _request_span_ids(
                            payload,
                            "万元",
                            quote,
                        ),
                    }
                ],
            },
            request,
        )


class _StructuredOperatingGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            return _response(
                {
                    "decision": "confirmed",
                    "checks": {
                        "subject": True,
                        "action": True,
                        "object": True,
                        "scope": True,
                        "period": True,
                        "evidence": True,
                    },
                },
                request,
            )
        payload = json.loads(request.messages[-1].content)
        quote = "煤炭 200 210 50"
        return _response(
            {
                "schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
                "field_family": payload["field_family"],
                "instrument_id": payload["instrument_id"],
                "report_period": payload["report_period"],
                "rows": [
                    {
                        "segment_name_raw": "煤炭",
                        "fact_type": "production_volume",
                        "value": 210.0,
                        "unit_raw": "万吨",
                        "fact_scope": "煤炭:产量",
                        "evidence_span_ids": _request_span_ids(
                            payload,
                            "万吨",
                            quote,
                        ),
                    }
                ],
            },
            request,
        )


class _NormalizedUnitOperatingGateway(_StructuredOperatingGateway):
    async def complete(self, request):
        response = await super().complete(request)
        if request.metadata["stage"] == "semantic_verification":
            return response
        data = json.loads(json.dumps(response.data, ensure_ascii=False))
        data["rows"][0]["unit_raw"] = "万公吨"
        raw = json.dumps(data, ensure_ascii=False)
        return replace(
            response,
            data=data,
            raw_content=raw,
            response_hash=hashlib.sha256(raw.encode()).hexdigest(),
        )


class _UnsupportedUnitOperatingGateway(_StructuredOperatingGateway):
    async def complete(self, request):
        response = await super().complete(request)
        if request.metadata["stage"] == "semantic_verification":
            return response
        data = json.loads(json.dumps(response.data, ensure_ascii=False))
        data["rows"][0]["unit_raw"] = "未治理质量单位"
        raw = json.dumps(data, ensure_ascii=False)
        return replace(
            response,
            data=data,
            raw_content=raw,
            response_hash=hashlib.sha256(raw.encode()).hexdigest(),
        )


class _MixedUnitOperatingGateway(_StructuredOperatingGateway):
    async def complete(self, request):
        response = await super().complete(request)
        if request.metadata["stage"] == "semantic_verification":
            return response
        data = json.loads(json.dumps(response.data, ensure_ascii=False))
        pending = dict(data["rows"][0])
        pending.update(
            {
                "segment_name_raw": "化工产品",
                "fact_type": "sales_volume",
                "value": 12.0,
                "unit_raw": "T/KL",
                "fact_scope": "化工产品:销量",
            }
        )
        data["rows"].append(pending)
        raw = json.dumps(data, ensure_ascii=False)
        return replace(
            response,
            data=data,
            raw_content=raw,
            response_hash=hashlib.sha256(raw.encode()).hexdigest(),
        )


class _PartialStructuredSegmentGateway(_StructuredSegmentGateway):
    async def complete(self, request):
        response = await super().complete(request)
        if request.metadata["stage"] == "semantic_verification":
            return response
        data = json.loads(json.dumps(response.data, ensure_ascii=False))
        invalid = dict(data["rows"][0])
        invalid["gross_margin"] = 40.0
        data["rows"].append(invalid)
        raw = json.dumps(data, ensure_ascii=False)
        return replace(
            response,
            data=data,
            raw_content=raw,
            response_hash=hashlib.sha256(raw.encode()).hexdigest(),
        )


class _RecoveringStructuredSegmentGateway(_StructuredSegmentGateway):
    async def complete(self, request):
        response = await super().complete(request)
        if request.metadata["stage"] == "semantic_verification":
            return response
        data = json.loads(json.dumps(response.data, ensure_ascii=False))
        if len(self.requests) == 1:
            invalid = dict(data["rows"][0])
            invalid["gross_margin"] = 40.0
            data["rows"].append(invalid)
        else:
            payload = json.loads(request.messages[-1].content)
            quote = "焦煤 80 50 37.5%"
            recovered = dict(data["rows"][0])
            recovered.update(
                {
                    "segment_name_raw": "焦煤",
                    "revenue": 80.0,
                    "segment_cost": 50.0,
                    "gross_margin": 0.375,
                    "evidence_span_ids": _request_span_ids(payload, quote),
                }
            )
            data["rows"].append(recovered)
        raw = json.dumps(data, ensure_ascii=False)
        return replace(
            response,
            data=data,
            raw_content=raw,
            response_hash=hashlib.sha256(raw.encode()).hexdigest(),
        )


class _EmptyStructuredGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        payload = json.loads(request.messages[-1].content)
        return _response(
            {
                "schema_version": STRUCTURED_EXTRACTION_SCHEMA_VERSION,
                "field_family": payload["field_family"],
                "instrument_id": payload["instrument_id"],
                "report_period": payload["report_period"],
                "rows": [],
            },
            request,
        )


class _EmptyAtomicGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        payload = json.loads(request.messages[-1].content)
        return _response(
            {
                "schema_version": SEMANTIC_EXTRACTION_SCHEMA_VERSION,
                "instrument_id": payload["instrument_id"],
                "report_period": payload["report_period"],
                "activities": [],
                "relationships": [],
            },
            request,
        )


class _OneContextRetryGateway(_FakeGateway):
    async def complete(self, request):
        if request.metadata["stage"] == "semantic_extraction" and not self.requests:
            self.loop = asyncio.get_running_loop()
            self.requests.append(request)
            raise ValueError("context incomplete")
        return await super().complete(request)


class _RelationshipGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            data = {
                "decision": "confirmed",
                "checks": {
                    "subject": True,
                    "action": True,
                    "object": True,
                    "scope": True,
                    "period": True,
                    "evidence": True,
                },
            }
            return _response(data, request)
        payload = json.loads(request.messages[-1].content)
        quote = "公司向客户股份有限公司销售动力煤"
        data = {
            "schema_version": SEMANTIC_EXTRACTION_SCHEMA_VERSION,
            "instrument_id": payload["instrument_id"],
            "report_period": payload["report_period"],
            "activities": [],
            "relationships": [
                {
                    "subject_scope": "issuer",
                    "relationship_type": "sells_to",
                    "counterparty_name_raw": "客户股份有限公司",
                    "object_raw": "动力煤",
                    "semantic_summary_zh": "公司向客户股份有限公司销售动力煤",
                    "evidence_span_ids": _request_span_ids(payload, quote),
                }
            ],
        }
        return _response(data, request)


class _InsufficientRelationshipGateway(_RelationshipGateway):
    async def complete(self, request):
        if request.metadata["stage"] != "semantic_verification":
            return await super().complete(request)
        self.requests.append(request)
        return _response(
            {
                "decision": "insufficient_evidence",
                "checks": {
                    "subject": True,
                    "action": True,
                    "object": True,
                    "scope": True,
                    "period": True,
                    "evidence": False,
                },
            },
            request,
        )


class _AnonymousRelationshipGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            payload = json.loads(request.messages[-1].content)
            if payload.get("records"):
                return _response(
                    {
                        "schema_version": "business_profile_semantic_batch_verifier.v1",
                        "decisions": [
                            _batch_verification_decision(item["target_id"])
                            for item in payload["records"]
                        ],
                    },
                    request,
                )
            return _response(
                {
                    "decision": "confirmed",
                    "checks": {
                        "subject": True,
                        "action": True,
                        "object": True,
                        "scope": True,
                        "period": True,
                        "evidence": True,
                    },
                },
                request,
            )
        payload = json.loads(request.messages[-1].content)
        top_five_quote = "前五大客户销售占比为59.5%"
        related_party_quote = "关联方销售占比为32.3%"
        top_five_supplier_quote = "前五大供应商采购占比为25.8%"
        related_party_supplier_quote = "关联方采购占比为14.4%"
        return _response(
            {
                "schema_version": SEMANTIC_EXTRACTION_SCHEMA_VERSION,
                "instrument_id": payload["instrument_id"],
                "report_period": payload["report_period"],
                "activities": [],
                "relationships": [
                    {
                        "subject_scope": "issuer",
                        "relationship_type": "sells_to",
                        "counterparty_name_raw": "前五大客户",
                        "semantic_summary_zh": "前五大客户销售占比为59.5%",
                        "anonymous": True,
                        "disclosed_share": 0.595,
                        "object_raw": "收入",
                        "evidence_span_ids": _request_span_ids(payload, top_five_quote),
                    },
                    {
                        "subject_scope": "issuer",
                        "relationship_type": "sells_to",
                        "counterparty_name_raw": "关联方",
                        "semantic_summary_zh": "关联方销售占比为32.3%",
                        "anonymous": True,
                        "disclosed_share": 0.323,
                        "object_raw": "收入",
                        "evidence_span_ids": _request_span_ids(
                            payload, related_party_quote
                        ),
                    },
                    {
                        "subject_scope": "issuer",
                        "relationship_type": "buys_from",
                        "counterparty_name_raw": "前五大供应商",
                        "semantic_summary_zh": "前五大供应商采购占比为25.8%",
                        "anonymous": True,
                        "disclosed_share": 0.258,
                        "object_raw": "采购额",
                        "evidence_span_ids": _request_span_ids(
                            payload, top_five_supplier_quote
                        ),
                    },
                    {
                        "subject_scope": "issuer",
                        "relationship_type": "buys_from",
                        "counterparty_name_raw": "关联方",
                        "semantic_summary_zh": "关联方采购占比为14.4%",
                        "anonymous": True,
                        "disclosed_share": 0.144,
                        "object_raw": "采购额",
                        "evidence_span_ids": _request_span_ids(
                            payload, related_party_supplier_quote
                        ),
                    },
                ],
            },
            request,
        )


class _ProductionAndSalesGateway(_FakeGateway):
    async def complete(self, request):
        self.requests.append(request)
        if request.metadata["stage"] == "semantic_verification":
            payload = json.loads(request.messages[-1].content)
            if payload.get("records"):
                return _response(
                    {
                        "schema_version": "business_profile_semantic_batch_verifier.v1",
                        "decisions": [
                            _batch_verification_decision(item["target_id"])
                            for item in payload["records"]
                        ],
                    },
                    request,
                )
            return _response(
                {
                    "decision": "confirmed",
                    "checks": {
                        "subject": True,
                        "action": True,
                        "object": True,
                        "scope": True,
                        "period": True,
                        "evidence": True,
                    },
                },
                request,
            )
        payload = json.loads(request.messages[-1].content)
        activities = []
        for action, quote in (
            ("produces", "公司生产动力煤"),
            ("sells", "公司销售动力煤"),
        ):
            activities.append(
                {
                    "subject_scope": "issuer",
                    "action": action,
                    "object_raw": "动力煤",
                    "semantic_summary_zh": quote,
                    "value": None,
                    "unit": None,
                    "evidence_span_ids": _request_span_ids(payload, quote),
                }
            )
        return _response(
            {
                "schema_version": SEMANTIC_EXTRACTION_SCHEMA_VERSION,
                "instrument_id": payload["instrument_id"],
                "report_period": payload["report_period"],
                "activities": activities,
                "relationships": [],
            },
            request,
        )


class _ConcurrentProductionAndSalesGateway(_ProductionAndSalesGateway):
    def __init__(self):
        super().__init__()
        self.active_verifications = 0
        self.peak_verifications = 0

    async def complete(self, request):
        if request.metadata["stage"] != "semantic_verification":
            return await super().complete(request)
        self.active_verifications += 1
        self.peak_verifications = max(
            self.peak_verifications, self.active_verifications
        )
        try:
            await asyncio.sleep(0.01)
            return await super().complete(request)
        finally:
            self.active_verifications -= 1


class _VerifierFailureGateway(_FakeGateway):
    async def complete(self, request):
        if request.metadata["stage"] == "semantic_verification":
            self.requests.append(request)
            raise ValueError("verifier provider returned malformed content")
        return await super().complete(request)


class _VerifierAuthenticationGateway(_ProductionAndSalesGateway):
    async def complete(self, request):
        if request.metadata["stage"] == "semantic_verification":
            self.requests.append(request)
            raise LlmAuthenticationError("LLM authentication failed")
        return await super().complete(request)


def test_action_only_verification_failure_expands_bounded_annual_report_context(
    tmp_path,
):
    table_text = "外购煤 百万吨 98.6"
    context_text = "2025年外购煤销售量及采购价格下降，公司持续开展外购煤采购。"
    sections = tuple(
        SelectedSection(
            section_id=f"section-{page}-{index}",
            page_number=page,
            section_key="coal_operations",
            text=text,
            normalized_text=text,
            normalized_start=0,
            normalized_end=len(text),
            page_hash=hashlib.sha256(text.encode()).hexdigest(),
            section_hash=hashlib.sha256(text.encode()).hexdigest(),
            selector_reasons=("test",),
            quality="native",
        )
        for index, (page, text) in enumerate(
            (
                (21, table_text),
                (22, context_text),
                (22, "外购煤采购补充说明。"),
            )
        )
    )
    selected = SelectedSectionArtifact(
        artifact_version="business_profile_selected_sections.v1",
        bundle={},
        sections=sections,
        previous_bundle_id=None,
        expansion_reason=None,
        artifact_hash="selected-hash",
    )
    target = {
        "target_type": "activity",
        "selected": selected,
        "verification_target": {
            "activity_id": "activity-external-coal",
            "action": "purchases",
            "object_raw": "外购煤",
            "evidence": {
                "evidence_spans": [
                    {
                        "section_id": "section-21-0",
                        "page_number": 21,
                        "section_hash": sections[0].section_hash,
                        "quote": table_text,
                        "quote_hash": hashlib.sha256(table_text.encode()).hexdigest(),
                    }
                ]
            },
        },
    }
    failed_action = {
        "decision": "insufficient_evidence",
        "checks": {
            "subject": True,
            "action": False,
            "object": True,
            "scope": True,
            "period": True,
            "evidence": True,
        },
    }

    expanded = _expanded_action_verification_target(target, failed_action)

    assert expanded is not None
    spans = expanded["evidence"]["evidence_spans"]
    assert len(spans) == 2
    assert spans[1]["page_number"] == 22
    assert "外购煤" in spans[1]["quote"]
    assert "采购" in spans[1]["quote"]
    assert (
        _expanded_action_verification_target(
            {
                **target,
                "verification_target": {
                    **target["verification_target"],
                    "action": "hedges",
                },
            },
            failed_action,
        )
        is None
    )

    class ActionContextGateway:
        def __init__(self):
            self.requests = []

        async def complete(self, request):
            self.requests.append(request)
            payload = json.loads(request.messages[-1].content)
            target_id = payload["records"][0]["target_id"]
            return _response(
                {
                    "schema_version": "business_profile_semantic_batch_verifier.v1",
                    "decisions": [_batch_verification_decision(target_id)],
                },
                request,
            )

    gateway = ActionContextGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path)),
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
    )
    outcome = asyncio.run(runtime._verify_wave_async([target]))[0]

    assert outcome["verification"]["decision"] == "confirmed"
    assert [item["kind"] for item in outcome["verification"]["attempts"]] == ["batched"]
    assert outcome["retry_calls"] == 0
    assert outcome["usage_tokens"] == 40
    assert len(gateway.requests) == 1

    class FailingExpandedContextGateway(ActionContextGateway):
        async def complete(self, request):
            self.requests.append(request)
            raise RuntimeError("batch verification unavailable")

    failing_gateway = FailingExpandedContextGateway()
    failing_runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path / "failing")),
        artifact_root=tmp_path / "failing-artifacts",
        llm_client=failing_gateway,
    )
    failed = asyncio.run(failing_runtime._verify_wave_async([target]))[0]

    assert isinstance(failed["exception"], RuntimeError)
    assert failed["audit"]["stage"] == "semantic_verification_batch"
    assert failed["retry_calls"] == 0


def test_verification_wave_splits_large_family_without_overflow_rework(tmp_path):
    class ChunkGateway(_FakeGateway):
        async def complete(self, request):
            self.requests.append(request)
            payload = json.loads(request.messages[-1].content)
            return _response(
                {
                    "schema_version": "business_profile_semantic_batch_verifier.v1",
                    "decisions": [
                        _batch_verification_decision(item["target_id"])
                        for item in payload.get("records", [])
                    ],
                },
                request,
            )

    gateway = ChunkGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path)),
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
    )
    selected = SelectedSectionArtifact(
        artifact_version="business_profile_selected_sections.v1",
        bundle={},
        sections=(
            SelectedSection(
                section_id="section-1",
                page_number=1,
                section_key="operations",
                text="公司生产动力煤。",
                normalized_text="公司生产动力煤。",
                normalized_start=0,
                normalized_end=8,
                page_hash="page-hash",
                section_hash="section-hash",
                selector_reasons=("test",),
                quality="native",
            ),
        ),
        previous_bundle_id=None,
        expansion_reason=None,
        artifact_hash="selected-hash",
    )
    targets = [
        {
            "target_id": f"activity-{index}",
            "target_type": "activity",
            "selected": selected,
            "verification_target": {
                "activity_id": f"activity-{index}",
                "action": "produces",
                "object_raw": "动力煤",
                "evidence": {
                    "evidence_spans": [
                        {
                            "section_id": "section-1",
                            "page_number": 1,
                            "section_hash": "section-hash",
                            "quote": "公司生产动力煤。",
                            "quote_hash": hashlib.sha256(
                                "公司生产动力煤。".encode()
                            ).hexdigest(),
                        }
                    ]
                },
            },
        }
        for index in range(51)
    ]

    outcomes = asyncio.run(runtime._verify_wave_async(targets))

    assert len(outcomes) == 51
    assert all("exception" not in outcome for outcome in outcomes)
    assert len(gateway.requests) == 2
    request_sizes = [
        len(json.loads(request.messages[-1].content)["records"])
        for request in gateway.requests
    ]
    assert request_sizes == [50, 1]


def test_verification_wave_reuses_identical_duplicate_target_once(tmp_path):
    class CountingGateway(_FakeGateway):
        async def complete(self, request):
            self.requests.append(request)
            payload = json.loads(request.messages[-1].content)
            return _response(
                {
                    "schema_version": "business_profile_semantic_batch_verifier.v1",
                    "decisions": [
                        _batch_verification_decision(item["target_id"])
                        for item in payload["records"]
                    ],
                },
                request,
            )

    gateway = CountingGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path)),
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
    )
    selected = SelectedSectionArtifact(
        artifact_version="business_profile_selected_sections.v1",
        bundle={},
        sections=(
            SelectedSection(
                section_id="section-1",
                page_number=1,
                section_key="operations",
                text="公司生产动力煤。",
                normalized_text="公司生产动力煤。",
                normalized_start=0,
                normalized_end=8,
                page_hash="page-hash",
                section_hash="section-hash",
                selector_reasons=("test",),
                quality="native",
            ),
        ),
        previous_bundle_id=None,
        expansion_reason=None,
        artifact_hash="selected-hash",
    )
    target = {
        "target_id": "activity-duplicate",
        "target_type": "activity",
        "selected": selected,
        "evidence_context_hash": "context-hash",
        "verification_target": {
            "activity_id": "activity-duplicate",
            "action": "produces",
            "object_raw": "动力煤",
            "evidence": {
                "section_id": "section-1",
                "page_number": 1,
                "section_hash": "section-hash",
                "quote": "公司生产动力煤。",
                "quote_hash": hashlib.sha256("公司生产动力煤。".encode()).hexdigest(),
            },
        },
    }

    outcomes = asyncio.run(runtime._verify_wave_async([target, dict(target)]))

    assert len(outcomes) == 1
    assert outcomes[0]["verification"]["target_id"] == "activity-duplicate"
    assert len(gateway.requests) == 1


def test_verification_wave_isolates_same_id_with_different_content(tmp_path):
    gateway = _FakeGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(_storage(tmp_path)),
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
    )
    first = {
        "target_id": "activity-collision",
        "target_type": "activity",
        "verification_target": {
            "activity_id": "activity-collision",
            "action": "produces",
            "object_raw": "动力煤",
        },
    }
    second = {
        **first,
        "verification_target": {
            **first["verification_target"],
            "action": "sells",
        },
    }

    outcomes = asyncio.run(runtime._verify_wave_async([first, second]))

    assert len(outcomes) == 2
    assert all("identity collision" in str(item["exception"]) for item in outcomes)
    assert gateway.requests == []


def _relationship_runtime(
    tmp_path,
    monkeypatch,
    entities,
    *,
    promote,
    network_disabled=False,
    gateway=None,
    text="主要业务：公司向客户股份有限公司销售动力煤。",
    counterparty_resolver=None,
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 relationship source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"page-artifact").hexdigest(),
            "status": "written",
        },
    )
    plain_scope = _scope("named_relationships")
    manifest = FieldFamilyPromotionManifest(
        field_family="named_relationships",
        enabled=True,
        benchmark_passed=True,
        identities=plain_scope.identities,
    )
    scope = _scope("named_relationships", manifest) if promote else plain_scope
    gateway = gateway or _RelationshipGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests={"named_relationships": manifest} if promote else {},
        counterparty_resolver=(
            counterparty_resolver or GovernedCounterpartyResolver(entities=entities)
        ),
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            promotion_enabled=promote,
            kill_switches={
                "all_writes": False,
                "network_calls": network_disabled,
                "promotion": False,
                "scope_widening": False,
            },
        ),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] in {"success", "stopped"}
    return repository, pipeline, scope, gateway


def _deterministic_runtime(
    tmp_path, monkeypatch, *, family, text, config=None, gateway=None
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 deterministic table source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    manifest_row["metadata"]["industry_group"] = "coal"
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"deterministic-page").hexdigest(),
            "status": "written",
        },
    )
    plain_scope = _scope(family)
    manifest = FieldFamilyPromotionManifest(
        field_family=family,
        enabled=True,
        benchmark_passed=True,
        identities=plain_scope.identities,
    )
    scope = _scope(family, manifest)
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests={family: manifest},
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config or SemanticProductionConfig(enabled=True, promotion_enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    return repository, pipeline, scope


def _force_program_ambiguity(monkeypatch, reason="semantic_context_ambiguous"):
    monkeypatch.setattr(
        runtime_module,
        "_program_validation_decision",
        lambda target, *, target_type: {
            "proof_version": "business_profile_program_validation.v1",
            "classification": "ambiguous",
            "canonical_promotion_allowed": False,
            "promotion_block_reasons": [reason],
            "skip_semantic_verifier": False,
        },
    )


def test_source_revision_binds_selected_document_and_retry_generation(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 revision"
    path = tmp_path / "annual.pdf"
    path.write_bytes(content)
    manifest = _manifest(path, content)
    loader = lambda instrument_id: [manifest]
    revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    changed_manifest = {**manifest, "content_hash": "f" * 64}
    changed_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=lambda instrument_id: [changed_manifest],
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=loader,
    )
    runtime._persist_stage_exceptions(
        [
            {
                "instrument_id": "601088.SH",
                "field_family": "atomic_activities",
                "source_document_id": "source-2025",
                "reason_code": "gateway_failure",
            }
        ],
        scope=_scope("atomic_activities"),
        config=SemanticProductionConfig(enabled=True),
    )
    retry_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )

    assert revision != changed_revision
    assert revision != retry_revision
    assert (
        replace(_scope("atomic_activities"), source_revision=revision).scope_hash
        != replace(
            _scope("atomic_activities"), source_revision=retry_revision
        ).scope_hash
    )


def test_joint_semantic_families_call_llm_once_and_restart_from_artifact(
    tmp_path, monkeypatch
):
    gateway = _FakeGateway()
    repository, pipeline, single_scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="atomic_activities",
        text="公司从事的主要业务：公司生产动力煤。",
        gateway=gateway,
    )
    scope = replace(
        single_scope,
        field_families=("atomic_activities", "named_relationships"),
        promotion_manifest_hashes={},
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    first = pipeline.run("extract", scope=scope)

    assert first["status"] == "success"
    assert first["metrics"].get("joint_semantic_llm_calls") == 1, first
    assert first["metrics"]["joint_semantic_sibling_reuses"] == 1
    assert first["metrics"]["joint_semantic_saved_llm_calls"] == 1
    assert len(gateway.requests) == 1
    assert len(repository.list_records("activities", instrument_id="601088.SH")) == 1
    with repository.storage.get_connection() as conn:
        artifact_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_semantic_artifacts "
            "WHERE field_family = 'annual_report_semantic_bundle'"
        ).fetchone()[0]
    assert artifact_count == 1

    same_scope_restart = BusinessProfileSemanticPipeline(
        config=pipeline.config,
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "same-scope-restart-checkpoint.json"
        ),
        handlers=pipeline.handlers,
    )
    for stage in ("plan", "select"):
        assert same_scope_restart.run(stage, scope=scope)["status"] == "success"
    durable_replay = same_scope_restart.run("extract", scope=scope)

    assert durable_replay["status"] == "success"
    assert len(gateway.requests) == 1, durable_replay

    restarted = BusinessProfileSemanticPipeline(
        config=pipeline.config,
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "restart-checkpoint.json"
        ),
        handlers=pipeline.handlers,
    )
    restarted_scope = replace(
        scope,
        identities={**scope.identities, "model": "model.v2"},
    )
    for stage in ("plan", "select"):
        assert restarted.run(stage, scope=restarted_scope)["status"] == "success"
    replayed = restarted.run("extract", scope=restarted_scope)

    assert replayed["status"] == "success"
    # A changed runtime identity invalidates both the completed run and the
    # raw joint receipt; only the sibling in this new invocation may reuse it.
    assert replayed["metrics"].get("joint_semantic_durable_replays", 0) == 0
    assert replayed["metrics"]["joint_semantic_sibling_reuses"] == 1
    assert replayed["metrics"]["joint_semantic_saved_llm_calls"] == 1
    assert replayed["metrics"]["tokens"] > 0
    assert len(gateway.requests) == 2


def test_joint_deterministic_failure_is_shared_without_second_llm_call(
    tmp_path, monkeypatch
):
    class InvalidShareGateway(_FakeGateway):
        async def complete(self, request):
            self.requests.append(request)
            payload = json.loads(request.messages[-1].content)
            return _response(
                {
                    "schema_version": SEMANTIC_EXTRACTION_SCHEMA_VERSION,
                    "instrument_id": payload["instrument_id"],
                    "report_period": payload["report_period"],
                    "activities": [],
                    "relationships": [
                        {
                            "subject_scope": "issuer",
                            "relationship_type": "sells_to",
                            "counterparty_name_raw": "前五名客户",
                            "anonymous": True,
                            "relationship_scope": "concentration",
                            "disclosed_share_source_value": 0.02,
                            "disclosed_share_source_unit": "占年度销售总额比例",
                            "object_raw": "营业收入",
                            "semantic_summary_zh": "前五名客户销售占比",
                            "evidence_span_ids": [payload["evidence_spans"][0]["evidence_span_id"]],
                        }
                    ],
                },
                request,
            )

    gateway = InvalidShareGateway()
    _repository, pipeline, single_scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="atomic_activities",
        text="公司从事的主要业务：前五名客户销售占比为0.02%。",
        gateway=gateway,
    )
    scope = replace(
        single_scope,
        field_families=("atomic_activities", "named_relationships"),
        promotion_manifest_hashes={},
    )
    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    result = pipeline.run("extract", scope=scope)

    assert len(gateway.requests) == 1
    assert result["status"] == "stopped"

def test_due_context_rework_expands_lineaged_pages_and_recovers(tmp_path, monkeypatch):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 context expansion"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    document_hash = hashlib.sha256(content).hexdigest()
    page_texts = [
        "前置说明",
        "财务摘要",
        "主要业务 公司生产动力煤",
        "经营讨论",
        "补充说明",
    ]
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": index,
                        "text": text,
                        "text_hash": hashlib.sha256(text.encode()).hexdigest(),
                        "page_artifact_hash": hashlib.sha256(text.encode()).hexdigest(),
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                    for index, text in enumerate(page_texts, start=1)
                ],
            },
            "artifact_path": str(tmp_path / "pages.json.gz"),
            "artifact_hash": hashlib.sha256(b"pages").hexdigest(),
            "status": "written",
        },
    )
    loader = lambda instrument_id: [manifest]
    gateway = _OneContextRetryGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=loader,
    )
    config = SemanticProductionConfig(enabled=True)
    first_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    first_scope = replace(_scope("atomic_activities"), source_revision=first_revision)
    first_pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(tmp_path / "first.json"),
        handlers=runtime.handlers(),
    )
    for stage in ("plan", "select", "extract"):
        assert first_pipeline.run(stage, scope=first_scope)["status"] in {
            "success",
            "stopped",
        }
    first_checkpoint = first_pipeline.checkpoint_store.load()
    persisted_retry_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    assert first_checkpoint["scope"]["source_revision"] == persisted_retry_revision
    first_extract_payload = runtime.stage_store.read(
        first_checkpoint["artifacts"]["extract"], expected_stage="extract"
    )
    assert first_extract_payload["scope_hash"] == first_checkpoint["scope_hash"]
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_exceptions SET next_retry_at = ?",
            ("2026-08-01T00:00:00+08:00",),
        )
        conn.commit()
    retry_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    retry_scope = replace(_scope("atomic_activities"), source_revision=retry_revision)
    retry_pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(tmp_path / "retry.json"),
        handlers=runtime.handlers(),
    )
    assert retry_pipeline.run("plan", scope=retry_scope)["status"] == "success"
    selected_result = retry_pipeline.run("select", scope=retry_scope)
    selected_payload = runtime.stage_store.read(
        selected_result["artifact"], expected_stage="select"
    )
    selected_artifact = runtime.section_store.read(
        selected_payload["selected"][0]["selected_artifact_path"]
    )

    assert selected_payload["selected"][0]["expanded_for_missing_context"] is True
    assert selected_artifact["previous_bundle_id"]
    assert selected_artifact["expansion_reason"] == "governed_missing_context"
    assert len(selected_artifact["sections"]) == 5

    extract_result = retry_pipeline.run("extract", scope=retry_scope)
    assert extract_result["metrics"]["machine_rework_recovered"] == 1
    assert repository.list_exceptions(status="open") == []
    assert len(repository.list_exceptions(status="resolved")) == 1
    recovered_revision = compute_business_profile_semantic_source_revision(
        repository,
        instruments=["601088.SH"],
        field_families=["atomic_activities"],
        knowledge_cutoff="2026-08-01",
        manifest_loader=loader,
    )
    assert (
        retry_pipeline.checkpoint_store.load()["scope"]["source_revision"]
        == recovered_revision
    )
    runtime.close()


def test_deterministic_unit_normalization_uses_governed_catalog():
    value, unit = _normalized_value(2, "万吨")
    currency_value, currency = _normalized_value(3, "万元", "currency")

    assert value == 20_000
    assert unit == "tonne"
    assert currency_value == 30_000
    assert currency == "CNY"


def test_deterministic_segment_table_persists_and_promotes_normalized_currency(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
    )
    for stage in ("plan", "select", "extract", "verify", "promote"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    segments = repository.list_records("segments", instrument_id="601088.SH")
    evidence = repository.list_records("evidence", instrument_id="601088.SH")
    assert len(segments) == len(evidence) == 1
    assert segments[0]["revenue"] == 1_000_000
    assert segments[0]["segment_cost"] == 600_000
    assert segments[0]["currency"] == "CNY"
    assert segments[0]["gross_margin"] == 0.4
    assert segments[0]["review_status"] == "approved"
    assert evidence[0]["review_status"] == "approved"


def test_unpromoted_deterministic_parser_record_never_calls_verifier_or_promotes(
    tmp_path, monkeypatch
):
    gateway = _FakeGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
        gateway=gateway,
    )
    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    segment = repository.list_records("segments", instrument_id="601088.SH")[0]
    segment["metadata"]["parser_manifest_promoted"] = False
    repository.upsert("segments", segment)

    verified = pipeline.run("verify", scope=scope)

    assert verified["status"] == "success"
    assert verified["metrics"]["llm_calls"] == 0
    assert verified["metrics"]["errors"] == 0
    assert gateway.requests == []
    verification_artifact = pipeline.checkpoint_store.load()["artifacts"]["verify"]
    verification = pipeline.handlers["verify"].__self__.stage_store.read(
        verification_artifact, expected_stage="verify"
    )["verifications"][0]
    assert verification["decision"] == "held"
    assert verification["proof"]["canonical_promotion_allowed"] is False

    promoted = pipeline.run("promote", scope=scope)
    assert promoted["status"] == "success"
    promotion_artifact = pipeline.handlers["promote"].__self__.stage_store.read(
        promoted["artifact"], expected_stage="promote"
    )
    assert any(
        item["decision"]["classification"] == "machine_rework"
        and "manifest_not_promoted" in item["decision"]["reason_codes"]
        and "failed_gate:semantic_proof" in item["decision"]["reason_codes"]
        for item in promotion_artifact["decisions"]
    )
    held = repository.list_records("segments", instrument_id="601088.SH")[0]
    assert held["review_status"] != "approved"


def test_recomputed_deterministic_proof_removes_stale_verify_rework(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
    )
    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    segment = repository.list_records("segments", instrument_id="601088.SH")[0]
    target_id = segment["record_id"]
    runtime = pipeline.handlers["verify"].__self__
    checkpoint = pipeline.checkpoint_store.load()
    checkpoint["artifacts"]["verify"] = runtime.stage_store.write(
        "verify",
        {
            "runtime_schema_version": runtime_module.RUNTIME_SCHEMA_VERSION,
            "scope_hash": scope.scope_hash,
            "verifications": [],
            "machine_rework": [
                {"target_id": target_id, "reason_code": "gateway_failure"}
            ],
            "exceptions": [],
        },
    )
    pipeline.checkpoint_store.save(checkpoint)

    verified = pipeline.run("verify", scope=scope)

    assert verified["status"] == "success"
    artifact = runtime.stage_store.read(verified["artifact"], expected_stage="verify")
    assert artifact["machine_rework"] == []
    assert artifact["verifications"][0]["target_id"] == target_id
    assert artifact["verifications"][0]["decision"] == "confirmed"


def test_structured_empty_output_reports_expected_non_disclosure(tmp_path, monkeypatch):
    _repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text="分部信息\n公司未按产品披露收入成本明细。",
    )
    assert pipeline.run("plan", scope=scope)["status"] == "success"
    selected = pipeline.run("select", scope=scope)
    assert selected["status"] == "success"
    assert selected["quality"]["outline_confidences"] == {"low": 1}

    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "success"
    assert extracted["quality"]["stage_ready"] is True
    assert extracted["quality"]["expected_non_disclosure_documents"] == 1
    assert extracted["quality"]["empty_output_documents"] == 1
    assert extracted["quality"]["empty_output_reasons"] == {
        "expected_non_disclosure": 1
    }


def test_ambiguous_structured_table_uses_bounded_semantic_fallback(
    tmp_path, monkeypatch
):
    gateway = _StructuredSegmentGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息 单位：万元\n"
            "分产品 营业收入 营业成本 毛利率\n"
            "煤炭 100 60 40%"
        ),
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module, "parse_selected_tables", lambda *args, **kwargs: ([], [])
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "success"
    assert extracted["quality"]["structured_fallback_calls"] == 1
    assert extracted["quality"]["structured_fallback_accepted_records"] == 1
    assert len(gateway.requests) == 1
    segment = repository.list_records("segments", instrument_id="601088.SH")[0]
    assert segment["revenue"] == 1_000_000
    assert segment["extraction_method"] == "semantic_structured_fallback"
    assert segment["metadata"]["numeric_reconciliation_valid"] is True

    verified = pipeline.run("verify", scope=scope)
    assert verified["status"] == "success"
    assert len(gateway.requests) == 1
    assert verified["quality"]["verified_records"] == 1
    assert pipeline.run("promote", scope=scope)["status"] == "success"
    segment = repository.list_records("segments", instrument_id="601088.SH")[0]
    assert segment["review_status"] == "approved"


def test_partial_structured_fallback_persists_valid_rows_and_rework_diagnostics(
    tmp_path, monkeypatch
):
    gateway = _PartialStructuredSegmentGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息 单位：万元\n"
            "分产品 营业收入 营业成本 毛利率\n"
            "煤炭 100 60 40%"
        ),
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "stopped"
    assert extracted["quality"]["stage_ready"] is False
    assert extracted["quality"]["structured_fallback_rejected_rows"] == 1
    assert len(repository.list_records("segments", instrument_id="601088.SH")) == 1
    exceptions = repository.list_exceptions(instrument_id="601088.SH")
    assert exceptions[-1]["reason_codes"] == ["partial_row_rejection"]
    assert (
        exceptions[-1]["metadata"]["diagnostics"]["row_rejections"][0]["row_index"] == 1
    )


def test_partial_structured_fallback_retry_persists_newly_recovered_rows(
    tmp_path, monkeypatch
):
    gateway = _RecoveringStructuredSegmentGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息 单位：万元\n"
            "分产品 营业收入 营业成本 毛利率\n"
            "煤炭 100 60 40%\n"
            "焦煤 80 50 37.5%"
        ),
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    first = pipeline.run("extract", scope=scope)
    second = pipeline.run("extract", scope=scope)

    assert first["status"] == "stopped"
    assert second["status"] == "success"
    assert second["quality"]["stage_ready"] is True
    segments = repository.list_records("segments", instrument_id="601088.SH")
    assert {row["segment_name_raw"] for row in segments} == {"煤炭", "焦煤"}
    assert not repository.list_exceptions(
        instrument_id="601088.SH",
        status="open",
    )


def test_ambiguous_operating_table_uses_bounded_semantic_fallback(
    tmp_path, monkeypatch
):
    gateway = _StructuredOperatingGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "主要产品产销情况 单位：万吨\n"
            "产品 销售量 生产量 库存量\n"
            "煤炭 200 210 50"
        ),
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "success"
    assert extracted["quality"]["structured_fallback_calls"] == 1
    assert extracted["quality"]["structured_fallback_accepted_records"] == 1
    fact = repository.list_records("operating_facts", instrument_id="601088.SH")[0]
    assert fact["fact_type"] == "production_volume"
    assert fact["value_raw"] == 210.0
    assert fact["unit_raw"] == "万吨"
    assert fact["metadata"]["derivation_method"] == "semantic_synthesis"
    assert fact["metadata"]["semantic_synthesis"] is True
    assert fact["metadata"]["numeric_reconciliation_valid"] is True
    with repository.storage.get_connection() as conn:
        semantic_run = json.loads(
            conn.execute(
                "SELECT metadata_json FROM business_profile_semantic_runs"
            ).fetchone()[0]
        )
    assert semantic_run["semantic_family_complete"] is True
    assert semantic_run["origin"] == "llm_extracted"
    assert semantic_run["record_ids"]["operating_facts"] == [fact["record_id"]]
    assert semantic_run["evidence_ids"]
    assert (
        semantic_run["semantic_audit"]["diagnostics"]["semantic_result"]["rows"][0][
            "unit_raw"
        ]
        == "万吨"
    )
    verified = pipeline.run("verify", scope=scope)
    assert verified["status"] == "success"
    assert verified["quality"]["verified_records"] == 1
    assert len(gateway.requests) == 1
    assert pipeline.run("promote", scope=scope)["status"] == "success"
    fact = repository.list_records("operating_facts", instrument_id="601088.SH")[0]
    assert fact["review_status"] == "approved"


def test_semantic_unit_alias_absent_from_source_is_normalized(tmp_path, monkeypatch):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "主要产品产销情况 单位：万吨\n"
            "产品 销售量 生产量 库存量\n"
            "煤炭 200 210 50"
        ),
        gateway=_NormalizedUnitOperatingGateway(),
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "success"
    fact = repository.list_records("operating_facts", instrument_id="601088.SH")[0]
    assert fact["unit_raw"] == "万公吨"
    assert fact["unit_normalized"] == "tonne"


def test_unit_conversion_pending_blocks_family_and_persists_raw_row(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "主要产品产销情况 单位：万吨\n"
            "产品 销售量 生产量 库存量\n"
            "煤炭 200 210 50"
        ),
        gateway=_UnsupportedUnitOperatingGateway(),
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "stopped"
    assert extracted["reason"].startswith("quality_gate:extract:")
    assert extracted["quality"]["stage_ready"] is False
    assert extracted["quality"]["blocking_machine_rework"] == 1
    assert extracted["metrics"]["semantic_rows_unit_pending"] == 1
    assert repository.list_records("operating_facts", instrument_id="601088.SH") == []
    exceptions = repository.list_exceptions(instrument_id="601088.SH")
    assert len(exceptions) == 1
    assert exceptions[0]["reason_codes"] == ["unit_normalization_failed"]
    with repository.storage.get_connection() as conn:
        artifact = conn.execute(
            "SELECT response_json FROM business_profile_semantic_artifacts"
        ).fetchone()
        event = conn.execute(
            "SELECT status, reason_code, metadata_json "
            "FROM business_profile_semantic_artifact_events "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        semantic_run = json.loads(
            conn.execute(
                "SELECT metadata_json FROM business_profile_semantic_runs"
            ).fetchone()[0]
        )
    assert json.loads(artifact["response_json"])["rows"][0]["unit_raw"] == (
        "未治理质量单位"
    )
    assert event["status"] == "conversion_pending"
    assert event["reason_code"] == "unit_normalization_failed"
    pending = json.loads(event["metadata_json"])["pending_rows"][0]
    assert pending["source_value"] == 210.0
    assert pending["source_unit"] == "未治理质量单位"
    assert semantic_run["semantic_family_complete"] is False
    assert semantic_run["unit_conversion_pending"][0]["source_unit"] == (
        "未治理质量单位"
    )


def test_pending_unit_row_keeps_independent_row_candidate_but_blocks_family(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "主要产品产销情况 单位：万吨\n"
            "产品 销售量 生产量 库存量\n"
            "煤炭 200 210 50\n化工产品 12"
        ),
        gateway=_MixedUnitOperatingGateway(),
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)
    facts = repository.list_records("operating_facts", instrument_id="601088.SH")

    assert extracted["status"] == "stopped"
    assert extracted["reason"].startswith("quality_gate:extract:")
    assert extracted["quality"]["stage_ready"] is False
    assert extracted["quality"]["blocking_machine_rework"] == 1
    assert extracted["metrics"]["semantic_rows_unit_pending"] == 1
    assert [(row["segment_id"], row["unit_normalized"]) for row in facts]
    assert len(facts) == 1
    assert facts[0]["unit_raw"] == "万吨"
    with repository.storage.get_connection() as conn:
        metadata = json.loads(
            conn.execute(
                "SELECT metadata_json FROM business_profile_semantic_runs"
            ).fetchone()[0]
        )
    assert metadata["semantic_family_complete"] is False
    assert metadata["unit_conversion_pending"][0]["source_unit"] == "T/KL"
    assert repository.list_records("operating_facts", instrument_id="601088.SH")[0][
        "review_status"
    ] == "candidate"


def test_auto_approved_unit_rule_is_replayed_inline_without_extraction_retry(
    tmp_path, monkeypatch
):
    runtime_rules = []

    def register_proposal(_registry, proposal, **_kwargs):
        rule = {
            "rule_id": "bp-unit-rule-inline",
            "normalized_lexeme": proposal["normalized_lexeme"],
            "source_unit": proposal["source_unit"],
            "status": "auto_approved",
            "dimension": "count",
            "canonical_unit": "unit",
            "multiplier": "1",
            "numerator": [],
            "denominator": [],
            "catalog_version": "runtime-inline",
        }
        runtime_rules[:] = [rule]
        return rule

    monkeypatch.setattr(
        runtime_module.BusinessProfileUnitRuleRegistry,
        "register_proposal",
        register_proposal,
    )
    monkeypatch.setattr(
        runtime_module.BusinessProfileUnitRuleRegistry,
        "overlay_rules",
        lambda _registry, **_kwargs: list(runtime_rules),
    )
    gateway = _UnsupportedUnitOperatingGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "主要产品产销情况 单位：万吨\n"
            "产品 销售量 生产量 库存量\n"
            "煤炭 200 210 50"
        ),
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)
    facts = repository.list_records("operating_facts", instrument_id="601088.SH")

    assert extracted["status"] == "success"
    assert extracted["quality"]["stage_ready"] is True
    assert extracted["quality"]["structured_fallback_calls"] == 1
    assert extracted["metrics"]["semantic_artifact_inline_replays"] == 1
    assert extracted["metrics"]["semantic_rows_unit_pending"] == 0
    assert len(facts) == 1
    assert facts[0]["value_normalized"] == 210.0
    assert facts[0]["unit_normalized"] == "unit"
    with repository.storage.get_connection() as conn:
        latest_status = conn.execute(
            "SELECT status FROM business_profile_semantic_artifact_events "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1"
        ).fetchone()[0]
    assert latest_status == "converted"


def test_unit_proposal_failure_logging_is_bounded_and_has_debug_traceback(monkeypatch):
    logger = SimpleNamespace(warning=Mock(), debug=Mock())
    monkeypatch.setattr(runtime_module, "logger", logger)
    try:
        raise TypeError("Decimal 无法序列化\n" + "x" * 800)
    except TypeError as exc:
        runtime_module._log_unit_proposal_failure("PCS", exc)

    warning_args = logger.warning.call_args.args
    assert warning_args[:3] == (
        "business-profile unit proposal fallback unit=%s "
        "error_type=%s error_message=%s",
        "PCS",
        "TypeError",
    )
    assert warning_args[3].startswith("Decimal 无法序列化 ")
    assert "\n" not in warning_args[3]
    assert len(warning_args[3]) == 500
    debug_call = logger.debug.call_args
    assert debug_call.args[-1] == "PCS"
    assert debug_call.kwargs["exc_info"][0] is TypeError
    assert debug_call.kwargs["exc_info"][2] is not None


def test_semantic_row_failure_categories_are_not_collapsed_to_context():
    numeric = ValueError("structured rows rejected")
    numeric.diagnostics = ({"failure_category": "numeric_validation_failed"},)
    evidence = ValueError("structured rows rejected")
    evidence.diagnostics = ({"failure_category": "evidence_provenance_failed"},)

    assert runtime_module._semantic_failure_reason(numeric) == (
        "numeric_validation_failed"
    )
    assert runtime_module._semantic_failure_reason(evidence) == (
        "evidence_provenance_failed"
    )


def test_composite_semantic_evidence_uses_complete_bundle_hash():
    primary_hash = hashlib.sha256(b"primary").hexdigest()
    composite_hash = hashlib.sha256(b"primary-secondary").hexdigest()
    selected = SimpleNamespace(
        sections=(
            SimpleNamespace(
                section_id="section-primary",
                page_number=1,
                quality="native",
                section_hash=hashlib.sha256(b"section-primary").hexdigest(),
            ),
        )
    )
    item = {
        "instrument_id": "601088.SH",
        "selected_artifact_hash": hashlib.sha256(b"selected").hexdigest(),
        "document": {
            "identity": "source-2025",
            "source": "cninfo",
            "source_tier": "official_primary",
            "document_type": "annual_report",
            "title": "2025 Annual Report",
            "content_hash": hashlib.sha256(b"document").hexdigest(),
            "report_period": "2025-12-31",
            "published_at": "2026-03-30T10:00:00+08:00",
            "metadata": {"source_url": "https://example.invalid/annual.pdf"},
        },
    }
    assertion = {
        "object_raw": "综合能源业务",
        "evidence": {
            "section_id": "section-primary",
            "quote_hash": primary_hash,
            "composite": True,
            "composite_quote_hash": composite_hash,
            "evidence_spans": [
                {"section_id": "section-primary", "quote": "第一段"},
                {"section_id": "section-secondary", "quote": "第二段"},
            ],
        },
    }

    evidence = runtime_module._semantic_evidence(item, selected, assertion)

    assert evidence["evidence_text_hash"] == composite_hash
    assert evidence["metadata"]["composite_evidence"] is True
    assert evidence["metadata"]["semantic_result"]["object_raw"] == ("综合能源业务")


def test_ambiguous_structured_table_empty_model_output_remains_machine_rework(
    tmp_path, monkeypatch
):
    gateway = _EmptyStructuredGateway()
    _repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息 单位：万元\n"
            "分产品 营业收入 营业成本 毛利率\n"
            "煤炭 100 60 40%"
        ),
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module,
        "parse_selected_tables",
        lambda *args, **kwargs: ([], []),
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "stopped"
    assert extracted["quality"]["blocking_machine_rework"] == 1
    assert extracted["quality"]["expected_non_disclosure_documents"] == 0
    assert extracted["quality"]["structured_fallback_rejected"] == 1


def test_empty_atomic_activity_stays_resumable_with_semantic_audit(
    tmp_path, monkeypatch
):
    gateway = _EmptyAtomicGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="atomic_activities",
        text="主要业务：公司生产动力煤。公司销售动力煤。",
        gateway=gateway,
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "stopped"
    assert extracted["quality"]["blocking_machine_rework"] == 1
    assert extracted["quality"]["empty_output_reasons"] == {
        "semantic_no_explicit_facts": 1
    }
    exception = repository.list_exceptions(instrument_id="601088.SH")[-1]
    assert exception["reason_codes"] == ["context_incomplete"]
    assert (
        exception["metadata"]["diagnostics"]["semantic_audit"]["diagnostics"][
            "semantic_result"
        ]["activities"]
        == []
    )
    with repository.storage.get_connection() as conn:
        run_metadata = json.loads(
            conn.execute(
                "SELECT metadata_json FROM business_profile_semantic_runs"
            ).fetchone()[0]
        )
    assert run_metadata["semantic_family_complete"] is False


def test_empty_named_relationships_are_reusable_non_disclosure(tmp_path, monkeypatch):
    gateway = _EmptyAtomicGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="named_relationships",
        text="主要业务：公司向客户销售动力煤，但未披露客户名称。",
        gateway=gateway,
    )

    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "success"
    assert extracted["quality"]["stage_ready"] is True
    assert extracted["quality"]["expected_non_disclosure_documents"] == 1
    assert repository.list_exceptions(instrument_id="601088.SH") == []
    with repository.storage.get_connection() as conn:
        run_metadata = json.loads(
            conn.execute(
                "SELECT metadata_json FROM business_profile_semantic_runs"
            ).fetchone()[0]
        )
    assert run_metadata["semantic_family_complete"] is True
    assert run_metadata["expected_non_disclosure"] is True


def test_deterministic_structured_rows_bypass_semantic_fallback(tmp_path, monkeypatch):
    gateway = _StructuredSegmentGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
        gateway=gateway,
    )
    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    assert gateway.requests == []
    assert len(repository.list_records("segments", instrument_id="601088.SH")) == 1
    extract_artifact = pipeline.checkpoint_store.load()["artifacts"]["extract"]
    extract_payload = pipeline.handlers["extract"].__self__.stage_store.read(
        extract_artifact, expected_stage="extract"
    )
    assert extract_payload["outputs"][0]["origin"] == "program_derived"


def test_ambiguous_structured_table_reports_configuration_blocker(
    tmp_path, monkeypatch
):
    gateway = _StructuredSegmentGateway()
    config = SemanticProductionConfig(
        enabled=True,
        promotion_enabled=False,
        kill_switches={
            "all_writes": False,
            "network_calls": True,
            "promotion": False,
            "scope_widening": False,
        },
    )
    _repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息 单位：万元\n"
            "分产品 营业收入 营业成本 毛利率\n"
            "煤炭 100 60 40%"
        ),
        config=config,
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module, "parse_selected_tables", lambda *args, **kwargs: ([], [])
    )
    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "stopped"
    assert extracted["reason"].startswith("blocked_configuration:extract:")
    assert extracted["quality"]["blocked_configuration"] is True
    assert extracted["quality"]["blocked_configuration_reasons"] == {
        "semantic_network_disabled": 1
    }
    assert gateway.requests == []


def test_llm_authentication_failure_is_a_resumable_configuration_blocker(
    tmp_path, monkeypatch
):
    class _AuthGateway:
        requests = []

        async def complete(self, request):
            self.requests.append(request)
            raise LlmAuthenticationError("LLM API key environment variable is missing")

    gateway = _AuthGateway()
    _repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息 单位：万元\n"
            "分产品 营业收入 营业成本 毛利率\n"
            "煤炭 100 60 40%"
        ),
        gateway=gateway,
    )
    monkeypatch.setattr(
        runtime_module, "parse_selected_tables", lambda *args, **kwargs: ([], [])
    )
    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "stopped"
    assert extracted["quality"]["blocked_configuration"] is True
    assert extracted["quality"]["blocked_configuration_reasons"] == {
        "llm_authentication_error": 1
    }
    assert extracted["quality"]["blocking_machine_rework"] == 0
    assert extracted["metrics"]["llm_calls"] == 1
    assert extracted["metrics"]["evidence_spans_offered"] >= 1


def test_atomic_llm_authentication_failure_is_a_configuration_blocker(
    tmp_path, monkeypatch
):
    class _AuthGateway:
        requests = []

        async def complete(self, request):
            self.requests.append(request)
            raise LlmAuthenticationError("LLM authentication failed")

    gateway = _AuthGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="atomic_activities",
        text="主要业务：公司生产并销售动力煤。",
        gateway=gateway,
    )
    for stage in ("plan", "select"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    extracted = pipeline.run("extract", scope=scope)

    assert extracted["status"] == "stopped"
    assert extracted["reason"].startswith("blocked_configuration:extract:")
    assert extracted["quality"]["blocked_configuration_reasons"] == {
        "llm_authentication_error": 1
    }
    assert extracted["quality"]["blocking_machine_rework"] == 0
    assert extracted["metrics"]["errors"] == 0
    assert repository.list_exceptions(instrument_id="601088.SH") == []


def test_verifier_authentication_failure_is_a_configuration_blocker(
    tmp_path, monkeypatch
):
    _force_program_ambiguity(monkeypatch)
    gateway = _VerifierAuthenticationGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="atomic_activities",
        text="主要业务：公司生产动力煤。公司销售动力煤。",
        gateway=gateway,
    )
    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"

    verified = pipeline.run("verify", scope=scope)

    assert verified["status"] == "stopped"
    assert verified["reason"].startswith("blocked_configuration:verify:")
    assert verified["quality"]["blocked_configuration_reasons"] == {
        "llm_authentication_error": 2
    }
    assert verified["quality"]["blocking_machine_rework"] == 0
    assert verified["metrics"]["errors"] == 0
    assert len(gateway.requests) == 2
    assert repository.list_exceptions(instrument_id="601088.SH") == []


def test_promotion_fails_closed_when_bound_validation_metadata_is_missing(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
    )
    for stage in ("plan", "select", "extract", "verify"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    segment = repository.list_records("segments", instrument_id="601088.SH")[0]
    segment["metadata"].pop("promotion_validation")
    repository.upsert("segments", segment)

    assert pipeline.run("promote", scope=scope)["status"] == "success"

    current = repository.get_record("segments", segment["record_id"])
    assert current["review_status"] == "candidate"
    exception = repository.list_exceptions(instrument_id="601088.SH")[0]
    reason_codes = exception["reason_codes"]
    assert "failed_gate:numeric_reconciliation" in reason_codes
    assert "failed_gate:temporal_scope" in reason_codes
    assert exception["metadata"]["source_document_id"] == "source-2025"


def test_deterministic_operating_table_normalizes_volume_and_unknown_unit_isolated(
    tmp_path, monkeypatch
):
    valid_root = tmp_path / "valid"
    valid_root.mkdir()
    repository, pipeline, scope = _deterministic_runtime(
        valid_root,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "煤炭产销量\n"
            "|项目|原煤产量（万吨）|商品煤产量（万吨）|商品煤销量（万吨）|\n"
            "|一矿|10|8|7|"
        ),
    )
    for stage in ("plan", "select", "extract", "verify", "promote"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    facts = repository.list_records("operating_facts", instrument_id="601088.SH")
    assert {item["unit_normalized"] for item in facts} == {"tonne"}
    assert {item["value_normalized"] for item in facts} == {100_000, 80_000, 70_000}
    assert all(item["review_status"] == "approved" for item in facts)

    invalid_root = tmp_path / "invalid"
    invalid_root.mkdir()
    invalid_repository, invalid_pipeline, invalid_scope = _deterministic_runtime(
        invalid_root,
        monkeypatch,
        family="tabular_operating_facts",
        text=(
            "产销量\n"
            "|主要产品|生产量（箱）|销售量（箱）|库存量（箱）|\n"
            "|产品A|10|8|2|"
        ),
    )
    assert invalid_pipeline.run("plan", scope=invalid_scope)["status"] == "success"
    assert invalid_pipeline.run("select", scope=invalid_scope)["status"] == "success"
    result = invalid_pipeline.run("extract", scope=invalid_scope)
    assert result["status"] == "stopped"
    assert result["reason"].startswith("quality_gate:extract:")
    assert (
        invalid_repository.list_records("operating_facts", instrument_id="601088.SH")
        == []
    )
    # The failed row is isolated, but its source evidence remains available for
    # targeted machine rework rather than being discarded with the row.
    assert len(invalid_repository.list_records("evidence", instrument_id="601088.SH")) == 1


def test_selected_character_budget_stops_before_extraction_and_resume_reuse(
    tmp_path, monkeypatch
):
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="structured_segments",
        text=(
            "分部信息\n"
            "|分产品|营业收入（万元）|营业成本（万元）|毛利率|\n"
            "|煤炭|100|60|40%|"
        ),
        config=SemanticProductionConfig(
            enabled=True,
            promotion_enabled=True,
            budgets=SemanticProductionBudgets(max_characters=10),
        ),
    )

    assert pipeline.run("plan", scope=scope)["status"] == "success"
    selected = pipeline.run("select", scope=scope)
    assert selected["status"] == "stopped"
    assert selected["reason"] == "budget_exhausted:characters"
    assert selected["completed_stages"] == ["plan", "select"]

    resumed = pipeline.run("resume", scope=scope)
    assert resumed["status"] == "stopped"
    assert resumed["completed_stages"] == ["plan", "select"]
    with repository.storage.get_connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_semantic_runs"
            ).fetchone()[0]
            == 0
        )


def test_extract_stops_new_network_calls_when_token_budget_is_reached(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    manifests = []
    for instrument_id in ("601088.SH", "600000.SH"):
        content = f"%PDF-1.7 {instrument_id}".encode()
        pdf = tmp_path / f"{instrument_id}.pdf"
        pdf.write_bytes(content)
        manifests.append(_manifest(pdf, content, instrument_id=instrument_id))
    text = "主要业务：公司生产动力煤。"
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document["content_hash"],
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(
                str(document["content_hash"]).encode()
            ).hexdigest(),
            "status": "written",
        },
    )
    gateway = _FakeGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [
            item for item in manifests if item["instrument_id"] == instrument_id
        ],
    )
    config = SemanticProductionConfig(
        enabled=True,
        budgets=SemanticProductionBudgets(max_tokens=40),
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = replace(
        _scope("atomic_activities"),
        instruments=("601088.SH", "600000.SH"),
    )

    assert pipeline.run("plan", scope=scope)["status"] == "success"
    assert pipeline.run("select", scope=scope)["status"] == "success"
    result = pipeline.run("extract", scope=scope)

    assert result["status"] == "stopped"
    assert result["reason"] == "budget_exhausted:tokens"
    assert result["metrics"]["llm_calls"] == 1
    assert len(gateway.requests) == 1
    assert "extract" not in result["completed_stages"]
    artifact = pipeline.checkpoint_store.load()["artifacts"]["extract"]
    payload = runtime.stage_store.read(artifact, expected_stage="extract")
    assert payload["budget_stop_reason"] == "budget_exhausted:tokens"
    assert len(payload["outputs"]) == 1


def test_verify_program_validates_safe_records_without_spending_llm_tokens(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 verifier budget source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    text = "主要业务：公司生产动力煤。公司销售动力煤。"
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document["content_hash"],
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"verifier-budget-page").hexdigest(),
            "status": "written",
        },
    )
    gateway = _ProductionAndSalesGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            budgets=SemanticProductionBudgets(max_tokens=80),
        ),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = _scope("atomic_activities")

    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    result = pipeline.run("verify", scope=scope)

    assert result["status"] == "success"
    assert result["metrics"]["llm_calls"] == 1
    assert len(gateway.requests) == 1
    assert "verify" in result["completed_stages"]
    artifact = pipeline.checkpoint_store.load()["artifacts"]["verify"]
    payload = runtime.stage_store.read(artifact, expected_stage="verify")
    assert payload["budget_stop_reason"] is None
    assert len(payload["verifications"]) == 2
    assert {item["decision"] for item in payload["verifications"]} == {"validated"}
    assert {item["proof"]["proof_version"] for item in payload["verifications"]} == {
        "business_profile_program_validation.v1"
    }
    assert payload["resume"] == {
        "reused_verifications": 0,
        "new_verifications": 2,
        "batch_llm_calls": 0,
        "batch_tokens": 0,
    }


def test_verify_ambiguous_targets_use_one_batch_without_nested_fanout(
    tmp_path, monkeypatch
):
    _force_program_ambiguity(monkeypatch)
    gateway = _ConcurrentProductionAndSalesGateway()
    _repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="atomic_activities",
        text="主要业务：公司生产动力煤。公司销售动力煤。",
        gateway=gateway,
        config=SemanticProductionConfig(
            enabled=True,
            budgets=SemanticProductionBudgets(
                max_tokens=50_000,
                max_concurrency=2,
            ),
        ),
    )

    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    result = pipeline.run("verify", scope=scope)

    assert result["status"] == "success"
    assert result["metrics"]["verified_records"] == 2
    assert gateway.peak_verifications == 1
    assert len(gateway.requests) == 2
    verifier_payload = json.loads(gateway.requests[1].messages[-1].content)
    assert len(verifier_payload["records"]) == 2
    artifact = pipeline.checkpoint_store.load()["artifacts"]["verify"]
    verified = pipeline.handlers["verify"].__self__.stage_store.read(
        artifact, expected_stage="verify"
    )
    assert {item["batch_size"] for item in verified["verifications"]} == {2}
    assert len({item["request_hash"] for item in verified["verifications"]}) == 1


def test_verify_resumes_partial_batch_without_repeating_completed_targets(
    tmp_path, monkeypatch
):
    _force_program_ambiguity(monkeypatch)

    class PartialBatchGateway(_ProductionAndSalesGateway):
        def __init__(self):
            super().__init__()
            self.verification_calls = 0

        async def complete(self, request):
            if request.metadata["stage"] != "semantic_verification":
                return await super().complete(request)
            self.requests.append(request)
            self.verification_calls += 1
            payload = json.loads(request.messages[-1].content)
            records = payload["records"]
            returned = records[:1] if self.verification_calls == 1 else records
            return _response(
                {
                    "schema_version": "business_profile_semantic_batch_verifier.v1",
                    "decisions": [
                        _batch_verification_decision(item["target_id"])
                        for item in returned
                    ],
                },
                request,
            )

    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 resumable verifier budget source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    text = "主要业务：公司生产动力煤。公司销售动力煤。"
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document["content_hash"],
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"resumable-verifier-page").hexdigest(),
            "status": "written",
        },
    )
    gateway = PartialBatchGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            budgets=SemanticProductionBudgets(
                max_tokens=50_000,
                max_concurrency=1,
            ),
        ),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = _scope("atomic_activities")

    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    first = pipeline.run("verify", scope=scope)

    assert first["status"] == "stopped"
    assert first["reason"].startswith("quality_gate:verify:")
    assert len(gateway.requests) == 2
    partial_reference = pipeline.checkpoint_store.load()["artifacts"]["verify"]
    partial = runtime.stage_store.read(partial_reference, expected_stage="verify")
    assert len(partial["verifications"]) == 1
    assert partial["resume"] == {
        "reused_verifications": 0,
        "new_verifications": 1,
        "batch_llm_calls": 1,
        "batch_tokens": 40,
    }
    assert len(partial["machine_rework"]) == 1

    first_payload = json.loads(gateway.requests[1].messages[-1].content)
    first_missing_id = first_payload["records"][1]["target_id"]

    resumed = pipeline.run("resume", scope=scope)

    assert resumed["status"] == "success"
    assert len(gateway.requests) == 3
    resumed_payload = json.loads(gateway.requests[2].messages[-1].content)
    assert [item["target_id"] for item in resumed_payload["records"]] == [
        first_missing_id
    ]
    final_reference = pipeline.checkpoint_store.load()["artifacts"]["verify"]
    final = runtime.stage_store.read(final_reference, expected_stage="verify")
    # The model-facing aliases are intentionally short; persisted
    # verifications must still carry the original durable activity ids.
    assert len(final["verifications"]) == 2
    assert all(
        str(item["target_id"]).startswith("activity:")
        for item in final["verifications"]
    )
    assert final["machine_rework"] == []
    assert final["resume"] == {
        "reused_verifications": 1,
        "new_verifications": 1,
        "batch_llm_calls": 1,
        "batch_tokens": 40,
    }
    assert repository.list_exceptions(instrument_id="601088.SH", status="open") == []


def test_verify_failure_persists_llm_audit_and_counts_failed_call(
    tmp_path, monkeypatch
):
    _force_program_ambiguity(monkeypatch)
    gateway = _VerifierFailureGateway()
    repository, pipeline, scope = _deterministic_runtime(
        tmp_path,
        monkeypatch,
        family="atomic_activities",
        text="主要业务：公司生产动力煤。",
        gateway=gateway,
    )

    for stage in ("plan", "select", "extract"):
        assert pipeline.run(stage, scope=scope)["status"] == "success"
    result = pipeline.run("verify", scope=scope)

    assert result["status"] == "stopped"
    assert result["metrics"]["llm_calls"] == 2
    assert result["metrics"]["errors"] == 1
    assert len(gateway.requests) == 2
    exception = repository.list_exceptions(instrument_id="601088.SH")[-1]
    assert exception["reason_codes"] == ["gateway_failure"]
    diagnostics = exception["metadata"]["diagnostics"]
    assert diagnostics["exception"]["transformation_stage"] == ("semantic_verification")
    assert diagnostics["exception"]["error_type"] == "ValueError"
    assert diagnostics["semantic_audit"]["stage"] == "semantic_verification_batch"
    assert diagnostics["semantic_audit"]["failure_category"] == (
        "gateway_or_validation_failure"
    )

    resumed = pipeline.run("resume", scope=scope)
    assert resumed["status"] == "stopped"
    runtime = pipeline.handlers["verify"].__self__
    resumed_artifact = runtime.stage_store.read(
        pipeline.checkpoint_store.load()["artifacts"]["verify"],
        expected_stage="verify",
    )
    assert len(resumed_artifact["machine_rework"]) == 1


def test_real_local_pdf_plan_select_and_hash_incremental_discovery(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = _pdf_bytes(
        "Principal Business Segment Information revenue and cost product details"
    )
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = _scope("structured_segments")

    planned = pipeline.run("plan", scope=scope)
    selected = pipeline.run("select", scope=scope)

    assert planned["stage"] == "plan"
    assert selected["stage"] == "select"
    selected_payload = runtime.stage_store.read(
        selected["artifact"], expected_stage="select"
    )
    assert len(selected_payload["selected"]) == 1
    assert selected_payload["selected"][0]["page_artifact_hash"]
    assert selected_payload["selected"][0]["selected_artifact_hash"]
    assert discover_business_profile_semantic_scope(
        repository,
        knowledge_cutoff="2026-08-01",
        max_instruments=3,
        field_families=("structured_segments",),
        runtime_identities=scope.identities,
        source_asset_loader=lambda _instrument_id: [manifest],
        active_universe_loader=lambda: [{"instrument_id": "601088.SH"}],
    ) == ("601088.SH",)


def test_select_shares_page_artifact_across_field_families(tmp_path, monkeypatch):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = _pdf_bytes(
        "Principal Business Segment Information revenue and cost product details"
    )
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest = _manifest(pdf, content)
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "shared-checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = replace(
        _scope("structured_segments"),
        field_families=("structured_segments", "tabular_operating_facts"),
    )
    original = runtime_module.ensure_archived_pdf_page_artifact
    extraction_calls = 0

    def count_page_artifact(document):
        nonlocal extraction_calls
        extraction_calls += 1
        return original(document)

    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        count_page_artifact,
    )

    assert pipeline.run("plan", scope=scope)["status"] == "success"
    selected = pipeline.run("select", scope=scope)

    assert selected["status"] in {"success", "stopped"}
    assert extraction_calls == 1
    assert selected["metrics"]["page_artifact_cache_misses"] == 1
    assert selected["metrics"]["page_artifact_cache_hits"] == 1
    for timing_name in (
        "pdf_hash_read_seconds",
        "pdf_cache_read_seconds",
        "pdf_extract_seconds",
        "page_artifact_write_seconds",
        "outline_seconds",
        "selection_seconds",
        "selected_artifact_write_seconds",
    ):
        assert timing_name in selected["metrics"]
        assert selected["metrics"][timing_name] >= 0


def test_storage_backed_discovery_is_hash_family_identity_and_retry_incremental(
    tmp_path,
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    identities = _scope("structured_segments").identities
    document_hash = hashlib.sha256(b"official annual report").hexdigest()
    storage.financial_statements.upsert_source_file_manifest(
        FinancialSourceFileManifest(
            source_file_id="source-2025",
            instrument_id="601088.SH",
            symbol="601088",
            exchange="SSE",
            report_period="2025-12-31",
            report_type="annual_report",
            filing_id="announcement-2025",
            parser_version="business-profile-test.v1",
            source="cninfo",
            source_mode="direct",
            source_tier="official_primary",
            archive_path=str(tmp_path / "annual.pdf"),
            content_hash=document_hash,
            published_at="2026-03-30T10:00:00+08:00",
            status="verified",
            schema_version="business_profile_source_asset.v1",
        )
    )

    def discover(families, runtime_identities=identities):
        return discover_business_profile_semantic_scope(
            repository,
            knowledge_cutoff="2099-08-01",
            max_instruments=3,
            field_families=families,
            runtime_identities=runtime_identities,
            source_asset_loader=lambda instrument_id: (
                storage.financial_statements.get_source_file_manifests(
                    instrument_id=instrument_id
                )
            ),
            active_universe_loader=lambda: [{"instrument_id": "601088.SH"}],
        )

    assert discover(("structured_segments",)) == ("601088.SH",)
    repository.persist_document_field_family_bundle(
        run={
            "run_id": "run-segment-2025",
            "instrument_id": "601088.SH",
            "source_document_id": "source-2025",
            "field_family": "structured_segments",
            "bundle_hash": "bundle-2025",
            "metadata": {
                "document_hash": document_hash,
                "runtime_identities": dict(identities),
            },
        },
        records_by_type={},
    )
    assert discover(("structured_segments",)) == ()
    assert discover(("atomic_activities",)) == ("601088.SH",)
    assert discover(
        ("structured_segments",), {**identities, "parser": "parser.v2"}
    ) == ("601088.SH",)

    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
    )
    exception = {
        "instrument_id": "601088.SH",
        "field_family": "structured_segments",
        "source_document_id": "source-2025",
        "tier": "machine_rework",
        "reason_code": "selector_gap",
    }
    runtime._persist_stage_exceptions(
        [exception],
        scope=_scope("structured_segments"),
        config=SemanticProductionConfig(enabled=True, retry_limit=1),
    )
    assert discover(("structured_segments",)) == ()
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_exceptions SET next_retry_at = ?",
            ("2000-01-01T00:00:00+08:00",),
        )
        conn.commit()
    assert discover(("structured_segments",)) == ("601088.SH",)

    runtime._persist_stage_exceptions(
        [exception],
        scope=_scope("structured_segments"),
        config=SemanticProductionConfig(enabled=True, retry_limit=1),
    )
    exhausted = repository.list_exceptions(instrument_id="601088.SH")
    assert exhausted[0]["retry_count"] == 1
    assert exhausted[0]["next_retry_at"] is None
    assert discover(("structured_segments",)) == ()


def test_discovery_tracks_each_minimum_plan_document_hash(tmp_path):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    identities = _scope("tabular_operating_facts").identities
    annual_hash = hashlib.sha256(b"annual").hexdigest()
    semi_hash = hashlib.sha256(b"semiannual").hexdigest()
    for source_file_id, report_type, period, published_at, content_hash in (
        (
            "source-annual",
            "annual_report",
            "2025-12-31",
            "2026-03-30T10:00:00+08:00",
            annual_hash,
        ),
        (
            "source-semi",
            "semiannual_report",
            "2026-06-30",
            "2026-08-30T10:00:00+08:00",
            semi_hash,
        ),
    ):
        storage.financial_statements.upsert_source_file_manifest(
            FinancialSourceFileManifest(
                source_file_id=source_file_id,
                instrument_id="601088.SH",
                symbol="601088",
                exchange="SSE",
                report_period=period,
                report_type=report_type,
                filing_id=source_file_id,
                parser_version="business-profile-test.v1",
                source="cninfo",
                source_mode="direct",
                source_tier="official_primary",
                archive_path=str(tmp_path / f"{source_file_id}.pdf"),
                content_hash=content_hash,
                published_at=published_at,
                status="verified",
                schema_version="business_profile_source_asset.v1",
            )
        )
    repository.persist_document_field_family_bundle(
        run={
            "run_id": "run-only-semi",
            "instrument_id": "601088.SH",
            "source_document_id": "source-semi",
            "field_family": "tabular_operating_facts",
            "bundle_hash": "bundle-semi",
            "metadata": {
                "document_hash": semi_hash,
                "runtime_identities": dict(identities),
            },
        },
        records_by_type={},
    )

    assert discover_business_profile_semantic_scope(
        repository,
        knowledge_cutoff="2099-08-01",
        max_instruments=30,
        field_families=("tabular_operating_facts",),
        runtime_identities=identities,
        source_asset_loader=lambda instrument_id: (
            storage.financial_statements.get_source_file_manifests(
                instrument_id=instrument_id
            )
        ),
        active_universe_loader=lambda: [{"instrument_id": "601088.SH"}],
    ) == ("601088.SH",)


def test_shadow_selection_failure_persists_machine_rework_without_promotion_manifest(
    tmp_path,
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    missing = tmp_path / "missing.pdf"
    manifest = _manifest(missing, b"missing")
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        manifest_loader=lambda instrument_id: [manifest],
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True, promotion_enabled=False),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    scope = _scope("structured_segments")

    assert pipeline.run("plan", scope=scope)["status"] == "success"
    selected = pipeline.run("select", scope=scope)

    assert selected["status"] == "stopped"
    assert selected["reason"].startswith("quality_gate:select:")
    exception = repository.list_exceptions(instrument_id="601088.SH")[0]
    assert exception["tier"] == "machine_rework"
    assert exception["reason_codes"] == ["planned_document_missing_or_invalid_locally"]
    assert exception["retry_count"] == 1
    assert exception["next_retry_at"] is not None


def test_semantic_runtime_promotes_after_program_validation(tmp_path, monkeypatch):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 synthetic archived source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    text = (
        "主要业务："
        + "行业背景与一般风险说明。" * 20
        + "公司生产动力煤并销售动力煤。"
        + "会计政策与其他非业务说明。" * 20
    )
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    artifact = {
        "source_content_hash": document_hash,
        "pages": [
            {
                "page_number": 1,
                "text": text,
                "text_hash": page_hash,
                "page_artifact_hash": page_hash,
                "native_text_status": "extracted",
                "ocr_required": False,
            }
        ],
    }
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": artifact,
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"page-artifact").hexdigest(),
            "status": "written",
        },
    )
    scope_without_manifest = _scope("atomic_activities")
    manifest = FieldFamilyPromotionManifest(
        field_family="atomic_activities",
        enabled=True,
        benchmark_passed=True,
        identities=scope_without_manifest.identities,
    )
    scope = _scope("atomic_activities", manifest)
    gateway = _FakeGateway()
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=gateway,
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests={"atomic_activities": manifest},
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True, promotion_enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )

    for stage in ("plan", "select", "extract", "verify", "promote"):
        result = pipeline.run(stage, scope=scope)
        assert result["status"] == "success"

    activities = repository.list_records("activities", instrument_id="601088.SH")
    evidence = repository.list_records("evidence", instrument_id="601088.SH")
    approved = repository.get_approved_as_of(
        "activities", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    assert len(gateway.requests) == 1
    extraction_payload = json.loads(gateway.requests[0].messages[-1].content)
    extraction_text = extraction_payload["evidence_spans"][0]["text"]
    assert "行业背景与一般风险说明" in extraction_text
    assert "公司生产动力煤并销售动力煤" in extraction_text
    assert len(activities) == len(evidence) == len(approved) == 1
    assert activities[0]["review_status"] == "approved"
    assert evidence[0]["review_status"] == "approved"
    assert repository.list_exceptions(instrument_id="601088.SH") == []
    runtime.close()
    assert gateway.closed is True


def test_runtime_primary_key_lookup_does_not_scan_bounded_history(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert(
        "evidence",
        {
            "evidence_id": "evidence-direct-lookup",
            "instrument_id": "601088.SH",
            "source_document_id": "source-1",
            "source_tier": "official_filing",
            "document_hash": hashlib.sha256(b"source").hexdigest(),
            "publish_date": "2026-03-30",
            "data_available_date": "2026-03-30",
            "availability_quality": "actual",
            "page_number": 1,
            "section_path": "section-1",
            "evidence_text_hash": hashlib.sha256(b"quote").hexdigest(),
            "extraction_method": "deterministic_table",
            "parser_version": "parser.v1",
            "ocr_status": "not_required",
            "confidence": 1.0,
            "review_status": "candidate",
        },
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
    )
    monkeypatch.setattr(
        repository,
        "list_records",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("bounded history scan must not be used")
        ),
    )

    assert (
        runtime._find_record("evidence", "evidence-direct-lookup")["evidence_id"]
        == "evidence-direct-lookup"
    )


def test_unique_exact_counterparty_is_verified_and_promoted(tmp_path, monkeypatch):
    repository, pipeline, scope, gateway = _relationship_runtime(
        tmp_path,
        monkeypatch,
        [
            {
                "entity_id": "entity-customer",
                "legal_name": "客户股份有限公司",
                "valid_from": "2000-01-01",
            }
        ],
        promote=True,
    )

    assert pipeline.run("verify", scope=scope)["status"] == "success"
    assert pipeline.run("promote", scope=scope)["status"] == "success"

    relationships = repository.list_records("relationships", instrument_id="601088.SH")
    evidence = repository.list_records("evidence", instrument_id="601088.SH")
    assert len(gateway.requests) == 2
    assert len(relationships) == len(evidence) == 1
    assert relationships[0]["counterparty_entity_id"] == "entity-customer"
    assert relationships[0]["resolution_basis"] == "exact_legal_name"
    assert relationships[0]["metadata"]["semantic_assertion_id"]
    assert relationships[0]["review_status"] == "approved"
    assert repository.list_exceptions(instrument_id="601088.SH") == []


def test_insufficient_verifier_context_routes_to_machine_rework(tmp_path, monkeypatch):
    repository, pipeline, scope, _gateway = _relationship_runtime(
        tmp_path,
        monkeypatch,
        [
            {
                "entity_id": "entity-customer",
                "legal_name": "客户股份有限公司",
                "valid_from": "2000-01-01",
            }
        ],
        promote=True,
        gateway=_InsufficientRelationshipGateway(),
    )

    assert pipeline.run("verify", scope=scope)["status"] == "success"
    assert pipeline.run("promote", scope=scope)["status"] == "success"

    relationship = repository.list_records("relationships", instrument_id="601088.SH")[
        0
    ]
    assert relationship["review_status"] == "candidate"
    exception = repository.list_exceptions(instrument_id="601088.SH", status="open")[0]
    assert exception["tier"] == "machine_rework"
    assert "context_incomplete" in exception["reason_codes"]
    assert "failed_gate:semantic_proof" in exception["reason_codes"]


def test_distinct_anonymous_concentrations_bypass_resolution_and_are_promoted(
    tmp_path, monkeypatch
):
    class FailingResolver:
        def resolve(self, *_args, **_kwargs):
            raise AssertionError(
                "anonymous concentration must bypass entity resolution"
            )

    repository, pipeline, scope, gateway = _relationship_runtime(
        tmp_path,
        monkeypatch,
        [],
        promote=True,
        gateway=_AnonymousRelationshipGateway(),
        text=(
            "主要业务：前五大客户销售占比为59.5%，关联方销售占比为32.3%；"
            "前五大供应商采购占比为25.8%，关联方采购占比为14.4%。"
        ),
        counterparty_resolver=FailingResolver(),
    )

    assert pipeline.run("verify", scope=scope)["status"] == "success"
    candidates = repository.list_records("operating_facts", instrument_id="601088.SH")
    related_purchase = next(
        fact for fact in candidates if fact.get("value_normalized") == 0.144
    )
    assert related_purchase["metadata"]["anonymous_label"] == "关联方"
    assert related_purchase["metadata"]["object_raw"] == "采购额"
    assert related_purchase["fact_scope"].startswith("anonymous-concentration-scope:")
    stale_target_ids = {
        fact["record_id"]
        for fact in sorted(candidates, key=lambda item: item["record_id"])[:3]
    }
    with repository.storage.get_connection() as conn:
        conn.executemany(
            "INSERT INTO business_profile_exceptions ("
            "exception_id, target_type, target_id, instrument_id, field_family, "
            "tier, reason_codes_json, gate_signature, gate_manifest_hash, "
            "created_at, updated_at"
            ") VALUES (?, 'operating_facts', ?, '601088.SH', "
            "'named_relationships', 'machine_rework', ?, ?, 'stale-manifest', ?, ?)",
            [
                (
                    f"stale-catalog-{index}",
                    fact["record_id"],
                    json.dumps(["failed_gate:catalogs_current"]),
                    f"stale-gate-{index}",
                    "2026-08-20T00:00:00+08:00",
                    "2026-08-20T00:00:00+08:00",
                )
                for index, fact in enumerate(
                    fact for fact in candidates if fact["record_id"] in stale_target_ids
                )
            ],
        )
        conn.commit()
    assert pipeline.run("promote", scope=scope)["status"] == "success"

    facts = repository.list_records("operating_facts", instrument_id="601088.SH")
    assert len(gateway.requests) == 1
    assert len(facts) == 4
    assert {fact["fact_type"] for fact in facts} == {
        "customer_concentration_share",
        "supplier_concentration_share",
    }
    assert {fact["value_normalized"] for fact in facts} == {
        0.595,
        0.323,
        0.258,
        0.144,
    }
    assert len({fact["fact_scope"] for fact in facts}) == 4
    assert {fact["review_status"] for fact in facts} == {"approved"}
    assert repository.list_records("relationships", instrument_id="601088.SH") == []
    assert repository.list_exceptions(instrument_id="601088.SH", status="open") == []
    resolved = repository.list_exceptions(instrument_id="601088.SH", status="resolved")
    assert {item["target_id"] for item in resolved} == stale_target_ids


def test_production_counterparty_resolver_reads_governed_a_share_master(tmp_path):
    storage = _storage(tmp_path)
    with sqlite3.connect(storage.quotes_db_path) as conn:
        conn.execute(
            "CREATE TABLE instruments ("
            "instrument_id TEXT PRIMARY KEY, name TEXT, type TEXT, exchange TEXT, "
            "listed_date TEXT, delisted_date TEXT)"
        )
        conn.executemany(
            "INSERT INTO instruments VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    "600000.SH",
                    "浦发银行股份有限公司",
                    "stock",
                    "SSE",
                    "1999-11-10",
                    None,
                ),
                ("00700.HK", "腾讯控股有限公司", "stock", "HKEX", None, None),
            ],
        )
    with storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO company_profiles ("
            "instrument_id, symbol, company_name, short_name, exchange, market, "
            "status, source, source_mode, data_as_of, profile_json, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "600000.SH", "600000", "浦发银行股份有限公司", "浦发银行",
                "SSE", "A股", "active", "official", "direct", "2026-08-04",
                json.dumps({
                    "legal_name": "浦发银行股份有限公司",
                    "legal_name_authority": "official_company_registration",
                }),
                "2026-08-04T00:00:00+08:00", "2026-08-04T00:00:00+08:00",
            ),
        )
        conn.commit()

    resolver = runtime_module.build_business_profile_counterparty_resolver(storage)

    resolved = resolver.resolve("浦发银行股份有限公司", knowledge_cutoff="2026-01-01")
    assert resolved.status == "resolved"
    assert resolved.entity_id == "600000.SH"
    external = resolver.resolve("腾讯控股有限公司")
    assert external.status == "unresolved"
    assert external.entity_id is None
    assert external.basis is None


def test_production_counterparty_resolver_resolves_full_name_not_security_short_name(tmp_path):
    storage = _storage(tmp_path)
    with sqlite3.connect(storage.quotes_db_path) as conn:
        conn.execute(
            "CREATE TABLE instruments ("
            "instrument_id TEXT PRIMARY KEY, name TEXT, type TEXT, exchange TEXT, "
            "listed_date TEXT, delisted_date TEXT)"
        )
        conn.executemany(
            "INSERT INTO instruments VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    "600000.SH",
                    "浦发银行",
                    "stock",
                    "SSE",
                    "1999-11-10",
                    None,
                ),
                (
                    "600001.SH",
                    "示例银行股份有限公司",
                    "stock",
                    "SSE",
                    "2000-01-01",
                    None,
                ),
                (
                    "600002.SH",
                    "唯一示例股份有限公司",
                    "stock",
                    "SSE",
                    "2000-01-01",
                    None,
                ),
            ],
        )
        conn.commit()
    with storage.get_connection() as conn:
        now = "2026-08-04T00:00:00+08:00"
        conn.executemany(
            "INSERT INTO company_profiles ("
            "instrument_id, symbol, company_name, short_name, exchange, market, "
            "status, source, source_mode, data_as_of, profile_json, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "600000.SH",
                    "600000",
                    "上海浦东发展银行股份有限公司",
                    "浦发银行",
                    "SSE",
                    "A股",
                    "active",
                    "official",
                    "direct",
                    "2026-08-04",
                    json.dumps({
                        "legal_name": "上海浦东发展银行股份有限公司",
                        "legal_name_authority": "official_company_registration",
                    }),
                    now,
                    now,
                ),
                (
                    "600001.SH",
                    "600001",
                    "示例银行股份有限公司",
                    "浦发银行",
                    "SSE",
                    "A股",
                    "active",
                    "official",
                    "direct",
                    "2026-08-04",
                    "{}",
                    now,
                    now,
                ),
                (
                    "600002.SH",
                    "600002",
                    "唯一示例股份有限公司",
                    "唯一示例",
                    "SSE",
                    "A股",
                    "active",
                    "official",
                    "direct",
                    "2026-08-04",
                    "{}",
                    now,
                    now,
                ),
            ],
        )
        conn.commit()

    resolver = runtime_module.build_business_profile_counterparty_resolver(
        storage,
        knowledge_cutoff="2026-08-04",
    )

    assert resolver.aliases == ()
    assert resolver.resolve("浦发银行").status == "unresolved"
    assert resolver.resolve("唯一示例").status == "unresolved"
    assert resolver.resolve("上海浦东发展银行股份有限公司").entity_id == "600000.SH"


def test_production_counterparty_resolver_allows_empty_governed_entity_set(tmp_path):
    storage = _storage(tmp_path)
    with sqlite3.connect(storage.quotes_db_path) as conn:
        conn.execute(
            "CREATE TABLE instruments ("
            "instrument_id TEXT PRIMARY KEY, name TEXT, type TEXT, exchange TEXT, "
            "listed_date TEXT, delisted_date TEXT)"
        )
        conn.execute(
            "INSERT INTO instruments VALUES (?, ?, ?, ?, ?, ?)",
            ("600000.SH", "浦发银行", "stock", "SSE", "1999-11-10", None),
        )

    resolver = runtime_module.build_business_profile_counterparty_resolver(storage)

    assert resolver.resolve("供应商甲股份有限公司").status == "unresolved"
    assert resolver.resolve("供应商甲股份有限公司").entity_id is None


def test_disclosed_complete_counterparty_is_published_without_master_registration(
    tmp_path, monkeypatch
):
    repository, pipeline, scope, gateway = _relationship_runtime(
        tmp_path, monkeypatch, [], promote=True
    )

    assert pipeline.run("verify", scope=scope)["status"] == "success"
    assert pipeline.run("promote", scope=scope)["status"] == "success"
    relationships = repository.list_records("relationships", instrument_id="601088.SH")
    assert len(gateway.requests) == 2
    assert len(relationships) == 1
    assert relationships[0]["counterparty_entity_id"] is None
    assert relationships[0]["metadata"]["resolution_status"] == "disclosed_name_only"
    assert relationships[0]["metadata"]["counterparty_catalog_pending"] is False
    assert relationships[0]["review_status"] == "candidate"
    exceptions = repository.list_exceptions(instrument_id="601088.SH")
    assert any("catalog_proposal" in item["reason_codes"] for item in exceptions)


def test_masked_ordinary_counterparty_promotes_without_catalog_proposal(tmp_path, monkeypatch):
    class MaskedOrdinaryGateway(_FakeGateway):
        async def complete(self, request):
            self.requests.append(request)
            if request.metadata["stage"] == "semantic_verification":
                return _response(
                    {
                        "decision": "confirmed",
                        "checks": {
                            "subject": True,
                            "action": True,
                            "object": True,
                            "scope": True,
                            "period": True,
                            "evidence": True,
                        },
                    },
                    request,
                )
            payload = json.loads(request.messages[-1].content)
            return _response(
                {
                    "schema_version": SEMANTIC_EXTRACTION_SCHEMA_VERSION,
                    "instrument_id": payload["instrument_id"],
                    "report_period": payload["report_period"],
                    "activities": [],
                    "relationships": [
                        {
                            "subject_scope": "issuer",
                            "relationship_type": "sells_to",
                            "relationship_scope": "ordinary",
                            "counterparty_name_raw": "客户 A(1)",
                            "anonymous": True,
                            "object_raw": "动力煤",
                            "semantic_summary_zh": "公司向客户 A(1)销售动力煤",
                            "evidence_span_ids": _request_span_ids(
                                payload, "公司向客户 A(1)销售动力煤"
                            ),
                        }
                    ],
                },
                request,
            )

    repository, pipeline, scope, _gateway = _relationship_runtime(
        tmp_path,
        monkeypatch,
        [],
        promote=True,
        gateway=MaskedOrdinaryGateway(),
        text="公司向客户 A(1)销售动力煤。",
    )
    assert pipeline.run("verify", scope=scope)["status"] == "success"
    assert pipeline.run("promote", scope=scope)["status"] == "success"

    relationship = repository.list_records("relationships", instrument_id="601088.SH")[0]
    assert relationship["review_status"] == "approved"
    assert not any(
        "catalog_proposal" in item["reason_codes"]
        for item in repository.list_exceptions(instrument_id="601088.SH")
    )


def test_terminal_verify_cleanup_removes_only_owned_candidates(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    runtime = BusinessProfileSemanticRuntime(
        repository=repository, artifact_root=tmp_path / "artifacts"
    )
    evidence = {
        "evidence_id": "evidence-1",
        "instrument_id": "601088.SH",
        "source_document_id": "annual-report-2025",
        "source_tier": "official_filing",
        "document_hash": "document-hash",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": "evidence-hash",
        "extraction_method": "native_text",
        "parser_version": "test",
        "confidence": 1.0,
        "review_status": "candidate",
        "metadata": {},
    }
    activity = {
        "activity_id": "activity-1",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "subject_scope": "issuer",
        "action": "produces",
        "object_type": "product",
        "object_raw": "动力煤",
        "object_id": "coal.thermal_coal",
        "segment_id": "coal",
        "evidence_id": "evidence-1",
        "run_id": "terminal-run",
        "data_available_date": "2026-03-28",
        "extraction_method": "semantic_verified",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": "2025-12-31",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {},
    }
    repository.persist_document_field_family_bundle(
        run={
            "run_id": "terminal-run",
            "instrument_id": "601088.SH",
            "source_document_id": "annual-report-2025",
            "field_family": "atomic_activities",
            "bundle_hash": "bundle-hash",
            "fact_catalog_version": "business_profile_facts.2026.3",
            "product_catalog_version": "business_profile_products.2026.4",
            "metadata": {"result_policy": "reuse"},
        },
        records_by_type={"evidence": [evidence], "activities": [activity]},
    )
    output = {
        "run_id": "terminal-run",
        "instrument_id": "601088.SH",
        "record_ids": {"activities": ["activity-1"]},
    }

    runtime._cleanup_terminal_verify_output(
        output, reason="business_rule_validation_failed"
    )

    assert repository.get_record("activities", "activity-1") is None
    assert repository.get_record("evidence", "evidence-1") is not None
    with repository.storage.get_connection() as conn:
        row = conn.execute(
            "SELECT status, error_code FROM business_profile_semantic_runs WHERE run_id = ?",
            ("terminal-run",),
        ).fetchone()
    assert tuple(row) == ("failed", "business_rule_validation_failed")


def test_resolved_relationship_closes_prior_catalog_proposal_exceptions(
    tmp_path, monkeypatch
):
    repository, pipeline, scope, _ = _relationship_runtime(
        tmp_path, monkeypatch, [], promote=True
    )
    runtime = pipeline.handlers["promote"].__self__

    assert pipeline.run("verify", scope=scope)["status"] == "success"
    assert pipeline.run("promote", scope=scope)["status"] == "success"
    unresolved = repository.list_records(
        "relationships", instrument_id="601088.SH"
    )[0]
    assertion_id = unresolved["metadata"]["semantic_assertion_id"]
    assert repository.list_exceptions(instrument_id="601088.SH", status="open")

    resolved = {
        **unresolved,
        "relationship_id": "relationship-resolved",
        "counterparty_name_normalized": "客户股份有限公司",
        "counterparty_entity_id": "customer-entity",
        "resolution_basis": "approved_exact_legal_name",
        "review_status": "candidate",
        "metadata": {
            **unresolved["metadata"],
            "resolution_status": "resolved_entity",
            "identity_status": "resolved_entity",
            "counterparty_catalog_pending": False,
            "semantic_assertion_id": assertion_id,
        },
    }
    for generated in ("created_at", "updated_at", "lineage_hash"):
        resolved.pop(generated, None)
    repository.upsert("relationships", resolved)
    current = repository.get_record("relationships", resolved["relationship_id"])
    BusinessProfileReviewService(repository).system_promote_record(
        "relationships",
        resolved["relationship_id"],
        field_family="named_relationships",
        policy_version="test_policy.v1",
        gate_manifest_hash="test-relationship-gates",
        reviewer_version="v1",
        expected_updated_at=current["updated_at"],
        evidence_references=[resolved["evidence_id"]],
    )

    runtime._resolve_relationship_promotion_exceptions(
        resolved, field_family="named_relationships"
    )

    assert repository.list_exceptions(
        instrument_id="601088.SH", status="open"
    ) == []


def test_network_kill_switch_makes_zero_gateway_calls_without_business_rework(
    tmp_path, monkeypatch
):
    repository, pipeline, _, gateway = _relationship_runtime(
        tmp_path,
        monkeypatch,
        [],
        promote=False,
        network_disabled=True,
    )

    assert gateway.requests == []
    assert repository.list_exceptions(instrument_id="601088.SH") == []
    assert repository.list_records("relationships", instrument_id="601088.SH") == []
    assert pipeline.checkpoint_store.load()["stopped_reason"] == (
        "blocked_configuration:extract:semantic_network_disabled"
    )


def test_multiple_exact_counterparties_enter_quick_review_without_fabricated_id(
    tmp_path, monkeypatch
):
    entities = [
        {
            "entity_id": entity_id,
            "legal_name": "客户股份有限公司",
            "valid_from": "2000-01-01",
        }
        for entity_id in ("entity-a", "entity-b")
    ]
    repository, _, _, gateway = _relationship_runtime(
        tmp_path, monkeypatch, entities, promote=False
    )

    exceptions = repository.list_exceptions(instrument_id="601088.SH")
    assert len(gateway.requests) == 1
    assert len(exceptions) == 1
    assert exceptions[0]["tier"] == "quick_review"
    assert exceptions[0]["reason_codes"] == ["entity_ambiguity"]
    assert {item["entity_id"] for item in exceptions[0]["ranked_choices"]} == {
        "entity-a",
        "entity-b",
    }
    assert repository.list_records("relationships", instrument_id="601088.SH") == []
    assert repository.list_records("evidence", instrument_id="601088.SH") == []


def test_approved_atomic_activities_drive_local_roles_and_fail_closed_exposures(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    content = b"%PDF-1.7 activity derivation source"
    pdf = tmp_path / "annual.pdf"
    pdf.write_bytes(content)
    manifest_row = _manifest(pdf, content)
    manifest_row["metadata"]["industry_group"] = "coal"
    text = "主要业务：公司生产动力煤。公司销售动力煤。"
    document_hash = hashlib.sha256(content).hexdigest()
    page_hash = hashlib.sha256(text.encode()).hexdigest()
    monkeypatch.setattr(
        runtime_module,
        "ensure_archived_pdf_page_artifact",
        lambda document: {
            "artifact": {
                "source_content_hash": document_hash,
                "pages": [
                    {
                        "page_number": 1,
                        "text": text,
                        "text_hash": page_hash,
                        "page_artifact_hash": page_hash,
                        "native_text_status": "extracted",
                        "ocr_required": False,
                    }
                ],
            },
            "artifact_path": str(tmp_path / "page.json.gz"),
            "artifact_hash": hashlib.sha256(b"activity-page").hexdigest(),
            "status": "written",
        },
    )
    families = (
        "atomic_activities",
        "derived_value_chain_roles",
        "commodity_exposure_facts",
        "commodity_exposure_publication",
    )
    identities = _scope("atomic_activities").identities
    manifests = {
        family: FieldFamilyPromotionManifest(
            field_family=family,
            enabled=True,
            benchmark_passed=True,
            identities=identities,
        )
        for family in families
    }
    scope = SemanticProductionScope(
        instruments=("601088.SH",),
        field_families=families,
        knowledge_cutoff="2026-08-01",
        identities=identities,
        promotion_manifest_hashes={
            family: manifest.manifest_hash for family, manifest in manifests.items()
        },
    )
    runtime = BusinessProfileSemanticRuntime(
        repository=repository,
        artifact_root=tmp_path / "artifacts",
        llm_client=_ProductionAndSalesGateway(),
        manifest_loader=lambda instrument_id: [manifest_row],
        promotion_manifests=manifests,
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=True, promotion_enabled=True),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=runtime.handlers(),
    )
    promoted = None
    for stage in ("plan", "select", "extract", "verify", "promote"):
        result = pipeline.run(stage, scope=scope)
        assert result["status"] == "success"
        if stage == "promote":
            promoted = result

    activities = repository.get_approved_as_of(
        "activities", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    roles = repository.get_approved_as_of(
        "value_chain_roles", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    facts = repository.get_approved_as_of(
        "exposure_facts", instrument_id="601088.SH", cutoff="2026-08-01"
    )
    assert {item["action"] for item in activities} == {"produces", "sells"}
    assert [item["role"] for item in roles] == ["producer"]
    assert len(facts) == 2
    assert {item["activity_id"] for item in facts} == {
        item["activity_id"] for item in activities
    }
    assert all(item["review_status"] == "approved" for item in facts)
    exposures = repository.list_records("exposures", instrument_id="601088.SH")
    assert exposures == []

    promoted_payload = runtime.stage_store.read(
        promoted["artifact"],
        expected_stage="promote",
    )
    publication_results = promoted_payload["derived"]["publications"]
    assert len(publication_results) == 1
    assert {item["status"] for item in publication_results} == {"fact_only"}
    assert promoted["quality"]["commodity_exposures_published"] == 0
    assert promoted["quality"]["commodity_exposure_facts_published"] == 2
    exceptions = repository.list_exceptions(instrument_id="601088.SH")
    assert exceptions == []
    stale_activity = next(item for item in activities if item["action"] == "produces")
    runtime._persist_runtime_exception(
        {
            "instrument_id": "601088.SH",
            "field_family": "derived_value_chain_roles",
            "source_document_id": stale_activity["evidence_id"],
            "target_id": stale_activity["activity_id"],
            "tier": "machine_rework",
            "reason_code": "transformation_lineage_missing",
            "evidence_reference": stale_activity["evidence_id"],
        },
        scope=scope,
        manifest=manifests["derived_value_chain_roles"],
    )
    assert len(repository.list_exceptions(instrument_id="601088.SH")) == 1
    runtime._derive_and_publish(scope)
    assert repository.list_exceptions(instrument_id="601088.SH") == []
    report = pipeline.run("report", scope=scope)["metrics"]["by_field_family"]
    assert report["atomic_activities"]["llm_calls"] == 1
    assert report["atomic_activities"]["candidates"] == 2
    assert report["derived_value_chain_roles"]["auto_promoted"] == 1
    assert report["commodity_exposure_facts"]["auto_promoted"] == 2
    assert report["commodity_exposure_publication"]["auto_promoted"] == 0

    checkpoint = pipeline.checkpoint_store.load()
    verified = runtime.stage_store.read(
        checkpoint["artifacts"]["verify"], expected_stage="verify"
    )
    verified["machine_rework"] = [
        {"target_id": "shared-gap", "reason_code": "verification_failed"}
    ]
    checkpoint["artifacts"]["verify"] = runtime.stage_store.write("verify", verified)
    monkeypatch.setattr(
        runtime,
        "_derive_and_publish",
        lambda scope: {
            "roles": [
                {
                    "decision": {
                        "target_type": "value_chain_roles",
                        "target_id": "role-approved",
                        "classification": "unchanged",
                    },
                    "promoted": True,
                },
                {
                    "decision": {
                        "target_type": "value_chain_roles",
                        "target_id": "shared-gap",
                        "classification": "quick_review",
                    },
                    "promoted": False,
                },
            ],
            "exposure_facts": [
                {
                    "decision": {
                        "target_type": "exposure_facts",
                        "target_id": "fact-approved",
                        "classification": "unchanged",
                    },
                    "promoted": True,
                }
            ],
            "publications": [
                {"status": "unchanged", "fact_id": "fact-approved"},
                {"status": "input_gap", "fact_id": "shared-gap"},
            ],
            "gaps": [
                {
                    "field_family": "derived_value_chain_roles",
                    "target_id": "shared-gap",
                }
            ],
        },
    )

    rerun = runtime.promote(
        scope=scope,
        config=pipeline.config,
        checkpoint=checkpoint,
    )

    assert rerun["quality"]["promoted_records"] == 2
    assert rerun["quality"]["value_chain_roles_published"] == 1
    assert rerun["quality"]["commodity_exposure_facts_published"] == 1
    assert rerun["quality"]["commodity_exposures_published"] == 1
    assert rerun["quality"]["publication_gaps"] == 1


def test_revised_scope_preserves_expanded_selection_policy(tmp_path, monkeypatch):
    storage = _storage(tmp_path)
    runtime = BusinessProfileSemanticRuntime(
        repository=BusinessProfileRepository(storage),
        artifact_root=tmp_path / "artifacts",
        selection_policy="expanded",
    )
    captured = {}

    def compute_revision(_repository, **kwargs):
        captured.update(kwargs)
        return "expanded-revision"

    monkeypatch.setattr(
        runtime_module,
        "compute_business_profile_semantic_source_revision",
        compute_revision,
    )
    revised = runtime._revised_scope(
        replace(_scope("atomic_activities"), source_revision="initial"),
        SemanticProductionConfig(enabled=True),
    )

    assert revised.source_revision == "expanded-revision"
    assert captured["selection_policy"] == "expanded"
