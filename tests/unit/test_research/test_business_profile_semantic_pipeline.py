import json

import pytest

from research.business_profile_semantic_pipeline import (
    BusinessProfileSemanticPipeline,
    SemanticProductionBudgets,
    SemanticProductionCheckpointStore,
    SemanticProductionConfig,
    SemanticProductionScope,
    SemanticProductionThresholds,
    parse_semantic_production_config,
)


def _scope(**overrides):
    values = {
        "instruments": ("601088.SH",),
        "field_families": ("atomic_activities",),
        "knowledge_cutoff": "2026-08-01",
        "identities": {
            "document": "doc-hash",
            "section": "section-hash",
            "selector": "selector.v1",
            "parser": "parser.v1",
            "schema": "schema.v1",
            "catalog": "catalog.v1",
            "model": "model.v1",
            "verifier": "verifier.v1",
            "rules": "rules.v1",
            "policy": "policy.v1",
        },
        "promotion_manifest_hashes": {"atomic_activities": "manifest-hash"},
    }
    values.update(overrides)
    return SemanticProductionScope(**values)


def _config(**overrides):
    values = {
        "enabled": True,
        "promotion_enabled": True,
        "scheduler_enabled": False,
    }
    values.update(overrides)
    return SemanticProductionConfig(**values)


def _handlers(results=None):
    results = results or {}

    def handler(stage):
        return lambda **kwargs: results.get(
            stage,
            {"status": "success", "artifact": {"stage": stage}, "metrics": {}},
        )

    return {stage: handler(stage) for stage in ("plan", "select", "extract", "verify", "promote")}


def test_exact_stage_order_resume_and_unchanged_replay(tmp_path):
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(tmp_path / "checkpoint.json"),
        handlers=_handlers(),
    )
    scope = _scope()

    first = pipeline.run("plan", scope=scope)
    unchanged = pipeline.run("plan", scope=scope)
    resumed = pipeline.run("resume", scope=scope)

    assert first["completed_stages"] == ["plan"]
    assert unchanged["status"] == "unchanged"
    assert resumed["stage"] == "select"
    assert resumed["completed_stages"] == ["plan", "select"]


def test_stale_scope_and_budget_checkpoint_are_rejected(tmp_path):
    path = tmp_path / "checkpoint.json"
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(path),
        handlers=_handlers(),
    )
    pipeline.run("plan", scope=_scope())

    with pytest.raises(ValueError, match="checkpoint scope"):
        pipeline.run(
            "resume",
            scope=_scope(identities={**_scope().identities, "model": "model.v2"}),
        )
    changed_budget = BusinessProfileSemanticPipeline(
        config=_config(budgets=SemanticProductionBudgets(max_tokens=100)),
        checkpoint_store=SemanticProductionCheckpointStore(path),
        handlers=_handlers(),
    )
    with pytest.raises(ValueError, match="checkpoint budgets"):
        changed_budget.run("resume", scope=_scope())


def test_budget_drift_and_exception_backlog_stop_at_checkpoint(tmp_path):
    config = _config(
        budgets=SemanticProductionBudgets(max_tokens=10),
        thresholds=SemanticProductionThresholds(max_drift_rate=0.01, max_exception_backlog=2),
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(tmp_path / "checkpoint.json"),
        handlers=_handlers(
            {
                "plan": {
                    "status": "success",
                    "artifact": {"plan": "hash"},
                    "metrics": {"tokens": 11, "drift_rate": 0.0},
                }
            }
        ),
    )

    result = pipeline.run("plan", scope=_scope())
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text())
    assert result["status"] == "stopped"
    assert result["reason"] == "budget_exhausted:tokens"
    assert checkpoint["artifacts"]["plan"] == {"plan": "hash"}
    assert checkpoint["completed_stages"] == []


def test_cancellation_and_interruption_resume_without_duplicate_completion(tmp_path):
    cancelled = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(tmp_path / "cancel.json"),
        handlers=_handlers(),
        cancellation_requested=lambda: True,
    ).run("plan", scope=_scope())
    assert cancelled["reason"] == "cancelled"

    path = tmp_path / "interrupt.json"
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(path),
        handlers=_handlers(
            {"plan": {"status": "interrupted", "reason": "provider_timeout", "artifact": {"partial": True}}}
        ),
    )
    interrupted = pipeline.run("plan", scope=_scope())
    assert interrupted["reason"] == "provider_timeout"
    assert interrupted["completed_stages"] == []


def test_failed_stage_is_checkpointed_without_being_marked_complete(tmp_path):
    path = tmp_path / "failed.json"
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(path),
        handlers=_handlers(
            {
                "plan": {
                    "status": "failed",
                    "reason": "document_manifest_missing",
                    "artifact": {"diagnostic": "missing"},
                }
            }
        ),
    )

    failed = pipeline.run("plan", scope=_scope())
    checkpoint = json.loads(path.read_text())

    assert failed["status"] == "stopped"
    assert failed["reason"] == "stage_failed:plan:document_manifest_missing"
    assert failed["completed_stages"] == []
    assert checkpoint["artifacts"]["plan"] == {"diagnostic": "missing"}


def test_promotion_requires_enablement_manifest_and_kill_switch(tmp_path):
    scope = _scope()
    path = tmp_path / "checkpoint.json"
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(promotion_enabled=False),
        checkpoint_store=SemanticProductionCheckpointStore(path),
        handlers=_handlers(),
    )
    for stage in ("plan", "select", "extract", "verify"):
        pipeline.run(stage, scope=scope)
    result = pipeline.run("promote", scope=scope)
    assert result["reason"] == "promotion_disabled_or_unmanifested"


def test_config_validation_requires_complete_boolean_kill_switches():
    with pytest.raises(ValueError, match="kill switches"):
        parse_semantic_production_config(
            {"enabled": True, "kill_switches": {"promotion": False}}
        )
