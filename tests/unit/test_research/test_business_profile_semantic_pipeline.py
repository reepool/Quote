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

    return {
        stage: handler(stage)
        for stage in ("plan", "select", "extract", "verify", "promote")
    }


def test_exact_stage_order_resume_and_unchanged_replay(tmp_path):
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
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
        thresholds=SemanticProductionThresholds(
            max_drift_rate=0.01, max_exception_backlog=2
        ),
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
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
    assert checkpoint["completed_stages"] == ["plan"]

    resumed = pipeline.run("resume", scope=_scope())
    assert resumed["status"] == "stopped"
    assert resumed["reason"] == "budget_exhausted:tokens"
    assert resumed["completed_stages"] == ["plan"]


def test_consumable_budget_stops_when_threshold_is_reached(tmp_path):
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(budgets=SemanticProductionBudgets(max_tokens=10)),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=_handlers(
            {
                "plan": {
                    "status": "success",
                    "artifact": {"plan": "hash"},
                    "metrics": {"tokens": 10},
                }
            }
        ),
    )

    result = pipeline.run("plan", scope=_scope())

    assert result["status"] == "stopped"
    assert result["reason"] == "budget_exhausted:tokens"
    assert result["completed_stages"] == ["plan"]


def test_report_aggregates_denominators_and_rates_by_field_family(tmp_path):
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=_handlers(
            {
                "plan": {
                    "status": "success",
                    "artifact": {"plan": "hash"},
                    "metrics": {
                        "by_field_family": {
                            "atomic_activities": {
                                "documents": 2,
                                "selected_documents": 2,
                                "candidates": 4,
                                "llm_calls": 1,
                                "auto_promoted": 3,
                                "quick_review": 1,
                                "reason_code_counts": {"entity_ambiguity": 1},
                            }
                        }
                    },
                }
            }
        ),
    )

    result = pipeline.run("plan", scope=_scope())
    family = result["metrics"]["by_field_family"]["atomic_activities"]
    assert family["documents"] == 2
    assert family["llm_call_rate"] == 0.5
    assert family["auto_promotion_rate"] == 0.75
    assert family["human_exception_rate"] == 0.2
    assert family["reason_code_clusters"] == [
        {"reason_code": "entity_ambiguity", "count": 1}
    ]


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
            {
                "plan": {
                    "status": "interrupted",
                    "reason": "provider_timeout",
                    "artifact": {"partial": True},
                }
            }
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


def test_publication_rebuild_uses_dedicated_handler_without_prior_stages(tmp_path):
    calls = []
    handlers = _handlers()
    handlers["rebuild-publications"] = lambda **kwargs: (
        calls.append(kwargs)
        or {"status": "success", "artifact": {"publication": "hash"}, "metrics": {}}
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(
            tmp_path / "checkpoint.json"
        ),
        handlers=handlers,
    )

    result = pipeline.run("rebuild-publications", scope=_scope())

    assert result["stage"] == "rebuild-publications"
    assert result["artifact"] == {"publication": "hash"}
    assert result["completed_stages"] == []
    assert len(calls) == 1


def test_report_without_checkpoint_is_read_only_and_not_ready(tmp_path):
    checkpoint = tmp_path / "missing.json"
    pipeline = BusinessProfileSemanticPipeline(
        config=SemanticProductionConfig(enabled=False),
        checkpoint_store=SemanticProductionCheckpointStore(checkpoint),
    )

    result = pipeline.run("report", scope=_scope())

    assert result["status"] == "not_ready"
    assert result["reason"] == "semantic_production_checkpoint_missing"
    assert not checkpoint.exists()


def test_checkpoint_path_resolves_rebound_and_latest_logical_scope(tmp_path):
    original_scope = _scope(source_revision="revision.v1")
    rebound_scope = _scope(source_revision="revision.v2")
    original_path = tmp_path / f"{original_scope.scope_hash[:20]}.json"
    pipeline = BusinessProfileSemanticPipeline(
        config=_config(),
        checkpoint_store=SemanticProductionCheckpointStore(original_path),
        handlers=_handlers(
            {
                "plan": {
                    "status": "success",
                    "artifact": {"plan": "hash"},
                    "source_revision": "revision.v2",
                    "metrics": {},
                }
            }
        ),
    )
    pipeline.run("plan", scope=original_scope)

    assert (
        SemanticProductionCheckpointStore.resolve_path(tmp_path, rebound_scope)
        == original_path
    )
    assert (
        SemanticProductionCheckpointStore.resolve_path(
            tmp_path,
            _scope(source_revision=""),
            latest_logical_scope=True,
        )
        == original_path
    )
