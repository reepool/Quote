#!/usr/bin/env python3
"""Validate business-profile shadow, pilot, expansion, and scheduler gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.business_profile_rollout import (
    evaluate_rollout_expansion,
    evaluate_scheduler_readiness,
    run_bounded_production_pilot,
    run_shadow_backfill_validation,
)
from research.business_profile_governance import (
    BusinessProfileRepository,
    BusinessProfileResolver,
)
from research.business_profile_promotion import (
    BusinessProfilePromotionService,
    FieldFamilyPromotionManifest,
    PromotionContext,
)
from research.business_profile_review import BusinessProfileReviewService
from research.business_profile_semantic_pipeline import (
    BusinessProfileSemanticPipeline,
    SemanticProductionCheckpointStore,
    SemanticProductionConfig,
    SemanticProductionScope,
)
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


_RUNTIME_IDENTITIES = {
    "schema": "segments.v1",
    "parser": "table.v1",
    "selector": "selector.v1",
    "catalog": "facts.2026.2",
    "policy": "promotion.2026.1",
}
_PROMOTION_GATES = {
    "official_identity": True,
    "artifact_quality": True,
    "exact_evidence": True,
    "catalogs_current": True,
    "temporal_scope": True,
    "numeric_reconciliation": True,
    "no_conflicts": True,
    "field_family_manifest": True,
    "runtime_identity_match": True,
    "candidate_current": True,
    "semantic_proof": True,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(
        prefix="business-profile-rollout-validation-"
    ) as temp_dir:
        root = Path(temp_dir)
        source = root / "production-shaped.db"
        _storage(source).initialize()
        shadow = run_shadow_backfill_validation(
            source,
            lambda path: _run_governed_validation(
                path,
                ("601088.SH",),
                exercise_machine_rework=True,
            ),
        )
        pilot_db = root / "pilot.db"
        pilot = run_bounded_production_pilot(
            instrument_ids=["601088.SH", "600362.SH"],
            enabled_manifests={
                "structured_segments": {
                    "enabled": True,
                    "manifest_hash": "isolated-test",
                }
            },
            runner=lambda instruments: _run_governed_validation(
                pilot_db,
                instruments,
                exercise_machine_rework=False,
            ),
            rollback_drill=lambda: _run_rollback_drill(root / "rollback.db"),
            kill_switch_drill=lambda: _run_kill_switch_drill(root / "kill-switch.json"),
        )
    expansion = evaluate_rollout_expansion(
        {
            "precision": 0.995,
            "drift_rate": 0.01,
            "average_cost": 0.2,
            "human_exception_rate": 0.02,
        },
        {
            "min_precision": 0.99,
            "max_drift_rate": 0.02,
            "max_average_cost": 0.5,
            "max_human_exception_rate": 0.05,
        },
    )
    scheduler = evaluate_scheduler_readiness(
        issuer_count=5500,
        changed_issuer_rate=0.05,
        seconds_per_changed_issuer=10,
        available_window_seconds=3600,
        exception_backlog=20,
        maximum_exception_backlog=100,
    )
    report = {
        "schema_version": "business_profile_rollout_gate_validation.v1",
        "scope": "isolated_temporary_database",
        "production_writes_performed": False,
        "isolated_governed_writes_performed": True,
        "scheduler_enabled": False,
        "shadow": shadow,
        "pilot": pilot,
        "expansion": expansion,
        "scheduler_readiness": scheduler,
    }
    report["report_hash"] = hashlib.sha256(
        json.dumps(
            report, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps({"output": str(args.output), "report_hash": report["report_hash"]})
    )
    return (
        0
        if shadow["passed"]
        and pilot["passed"]
        and expansion["expand"]
        and scheduler["scheduler_ready"]
        else 1
    )


def _storage(path: Path) -> ResearchStorageManager:
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(path),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(path.with_name(f"{path.stem}-quotes.db")),
            financials_db_path=str(path.with_name(f"{path.stem}-financials.db")),
            valuation_db_path=str(path.with_name(f"{path.stem}-valuation.db")),
            interests_db_path=str(path.with_name(f"{path.stem}-interests.db")),
        ),
        budget=ResearchBudgetConfig(),
    )
    return ResearchStorageManager(config)


def _run_governed_validation(
    path: Path,
    instruments: Sequence[str],
    *,
    exercise_machine_rework: bool,
) -> dict[str, Any]:
    storage = _storage(path)
    storage.initialize()
    repository = BusinessProfileRepository(storage)
    manifest = FieldFamilyPromotionManifest(
        field_family="structured_segments",
        enabled=True,
        benchmark_passed=True,
        identities=_RUNTIME_IDENTITIES,
    )
    service = BusinessProfilePromotionService(
        BusinessProfileReviewService(repository),
        max_machine_retries=2,
    )
    evidence_ids = {
        instrument_id: f"pilot-evidence-{instrument_id.replace('.', '-')}"
        for instrument_id in instruments
    }
    missing_candidates = [
        _candidate_evidence(instrument_id, evidence_id)
        for instrument_id, evidence_id in evidence_ids.items()
        if repository.get_record("evidence", evidence_id) is None
    ]
    if missing_candidates:
        repository.upsert_many("evidence", missing_candidates)
    for instrument_id, evidence_id in evidence_ids.items():
        _ensure_promoted_evidence(
            repository,
            service,
            manifest,
            instrument_id=instrument_id,
            evidence_id=evidence_id,
        )
    machine_rework_recovery_ok = True
    if exercise_machine_rework:
        recovery_id = "shadow-machine-rework"
        recovery = repository.get_record("evidence", recovery_id)
        if recovery is None:
            repository.upsert(
                "evidence",
                _candidate_evidence("601088.SH", recovery_id),
            )
            recovery = repository.get_record("evidence", recovery_id)
        if recovery and recovery.get("review_status") == "candidate":
            rework_context = _promotion_context(
                recovery,
                manifest,
                gates={**_PROMOTION_GATES, "artifact_quality": False},
                exception_reasons=("ocr_required",),
            )
            service.process(rework_context, manifest)
            recovery = repository.get_record("evidence", recovery_id)
            service.process(_promotion_context(recovery, manifest), manifest)
        machine_rework_recovery_ok = bool(
            repository.get_record("evidence", recovery_id).get("review_status")
            == "approved"
            and not service.list_exceptions(status="open")
        )
    with storage.get_connection() as conn:
        audit_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_review_audit "
                "WHERE reviewer LIKE 'system:business_profile_auto_promotion.%'"
            ).fetchone()[0]
        )
    governed_output = [
        (item["evidence_id"], item["review_status"])
        for instrument_id in instruments
        for item in repository.list_records(
            "evidence", instrument_id=instrument_id, limit=100
        )
    ]
    profiles = [
        BusinessProfileResolver(repository).resolve(
            instrument_id,
            as_of_date="2026-08-01",
            include_candidates=True,
        )
        for instrument_id in instruments
    ]
    return {
        "governed_output": sorted(governed_output),
        "bulk_transaction_ok": all(
            status == "approved" for _, status in governed_output
        ),
        "point_in_time_reads_ok": all(
            profile.get("data_available_cutoff") == "2026-08-01"
            and profile.get("readiness", {}).get("storage_status") == "ready"
            for profile in profiles
        ),
        "machine_rework_recovery_ok": machine_rework_recovery_ok,
        "zero_valuation_leakage": all(
            not profile.get("approved_exposures") for profile in profiles
        ),
        "zero_candidate_valuation_leakage": all(
            not profile.get("approved_exposures")
            and not profile.get("candidate_exposures")
            for profile in profiles
        ),
        "audited_system_promotion": audit_count >= len(instruments),
        "bounded_scope": {
            item["instrument_id"]
            for item in repository.list_records("evidence", limit=10_000)
        }.issubset(set(instruments)),
    }


def _candidate_evidence(instrument_id: str, evidence_id: str) -> dict[str, Any]:
    return {
        "evidence_id": evidence_id,
        "instrument_id": instrument_id,
        "source_document_id": f"cninfo-{evidence_id}",
        "source_tier": "official_filing",
        "document_hash": hashlib.sha256(evidence_id.encode()).hexdigest(),
        "report_period": "2025-12-31",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "evidence_text_hash": hashlib.sha256(
            f"evidence:{evidence_id}".encode()
        ).hexdigest(),
        "extraction_method": "native_pdf_table",
        "confidence": 1.0,
        "review_status": "candidate",
    }


def _ensure_promoted_evidence(
    repository: BusinessProfileRepository,
    service: BusinessProfilePromotionService,
    manifest: FieldFamilyPromotionManifest,
    *,
    instrument_id: str,
    evidence_id: str,
) -> None:
    record = repository.get_record("evidence", evidence_id)
    if record is None:
        repository.upsert("evidence", _candidate_evidence(instrument_id, evidence_id))
        record = repository.get_record("evidence", evidence_id)
    if record and record.get("review_status") == "candidate":
        service.process(_promotion_context(record, manifest), manifest)


def _promotion_context(
    record: Mapping[str, Any],
    manifest: FieldFamilyPromotionManifest,
    *,
    gates: Mapping[str, bool] = _PROMOTION_GATES,
    exception_reasons: tuple[str, ...] = (),
) -> PromotionContext:
    return PromotionContext(
        target_type="evidence",
        target_id=str(record["evidence_id"]),
        instrument_id=str(record["instrument_id"]),
        field_family=manifest.field_family,
        expected_updated_at=str(record["updated_at"]),
        gates=dict(gates),
        runtime_identities=manifest.identities,
        evidence_references=(
            f"{record['source_document_id']}:{record['document_hash']}",
        ),
        exception_reasons=exception_reasons,
    )


def _run_rollback_drill(path: Path) -> bool:
    storage = _storage(path)
    storage.initialize()
    repository = BusinessProfileRepository(storage)
    try:
        repository.persist_document_field_family_bundle(
            run={
                "run_id": "rollback-run",
                "instrument_id": "601088.SH",
                "source_document_id": "rollback-document",
                "field_family": "atomic_activities",
                "bundle_hash": "rollback-bundle",
                "fact_catalog_version": "business_profile_facts.2026.2",
                "product_catalog_version": "business_profile_products.2026.2",
                "metadata": {"drill": "foreign_key_rollback"},
            },
            records_by_type={
                "activities": [
                    {
                        "activity_id": "rollback-activity",
                        "instrument_id": "601088.SH",
                        "report_period": "2025-12-31",
                        "subject_scope": "issuer",
                        "action": "produces",
                        "object_type": "product",
                        "object_raw": "动力煤",
                        "object_id": "coal.thermal_coal",
                        "evidence_id": "missing-evidence",
                        "run_id": "rollback-run",
                        "data_available_date": "2026-03-28",
                        "extraction_method": "semantic_verified",
                        "confidence": 0.98,
                        "review_status": "candidate",
                        "valid_from": "2025-12-31",
                        "knowledge_from": "2026-03-28",
                        "version": 1,
                        "metadata": {},
                    }
                ],
            },
        )
    except sqlite3.IntegrityError:
        with storage.get_connection() as conn:
            run_count = conn.execute(
                "SELECT COUNT(*) FROM business_profile_semantic_runs "
                "WHERE run_id = 'rollback-run'"
            ).fetchone()[0]
        return repository.list_records("evidence") == [] and run_count == 0
    return False


def _run_kill_switch_drill(checkpoint: Path) -> bool:
    calls = []
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(
            enabled=True,
            kill_switches={
                "all_writes": True,
                "network_calls": False,
                "promotion": False,
                "scope_widening": False,
            },
        ),
        checkpoint_store=SemanticProductionCheckpointStore(checkpoint),
        handlers={"plan": lambda **kwargs: calls.append(kwargs)},
    )
    result = pipeline.run(
        "plan",
        scope=SemanticProductionScope(
            instruments=("601088.SH",),
            field_families=("structured_segments",),
            knowledge_cutoff="2026-08-01",
            identities=_RUNTIME_IDENTITIES,
            source_revision="kill-switch-drill.v1",
        ),
    )
    return (
        result.get("status") == "stopped"
        and result.get("reason") == "kill_switch:all_writes"
        and not calls
    )


if __name__ == "__main__":
    raise SystemExit(main())
