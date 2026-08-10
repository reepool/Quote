import asyncio
import sqlite3
from contextlib import contextmanager

import pytest

from research.business_profile_semantic_artifacts import (
    BusinessProfileSemanticArtifactRepository,
    SemanticArtifactIdentity,
)
from research.business_profile_semantic_runtime import (
    _normalized_value_with_resolution,
)
from research.business_profile_unit_conversions import (
    governed_primitive_definitions,
    governed_primitive_multipliers,
)
from research.business_profile_unit_registry import (
    BusinessProfileUnitRuleRegistry,
    unit_proposal_response_schema,
)
from research.storage import ResearchStorageManager


class _Storage:
    def __init__(self, path):
        self.path = str(path)
        with self.get_connection() as conn:
            ResearchStorageManager._create_tables(conn)

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _apply_pragmas(conn):
        conn.execute("PRAGMA foreign_keys = ON")


def _identity(**overrides):
    values = {
        "instrument_id": "600403.SH",
        "source_document_id": "document-1",
        "document_hash": "a" * 64,
        "report_period": "2025-12-31",
        "field_family": "tabular_operating_facts",
        "evidence_scope_hash": "b" * 64,
        "input_hash": "c" * 64,
        "prompt_version": "prompt.v1",
        "schema_version": "schema.v1",
    }
    values.update(overrides)
    return SemanticArtifactIdentity(**values)


def test_semantic_artifact_is_idempotent_and_exact_scope_replayable(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    repository = BusinessProfileSemanticArtifactRepository(storage)
    first = repository.receive(
        _identity(),
        response={"rows": [{"unit_raw": "kW", "value": 10}]},
        response_hash="",
        evidence_ids=["span-1"],
        usage={"input_tokens": 100, "output_tokens": 20},
    )
    second = repository.receive(
        _identity(),
        response={"rows": [{"unit_raw": "kW", "value": 10}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    assert first["artifact_id"] == second["artifact_id"]
    repository.mark(first["artifact_id"], "conversion_pending")
    assert repository.find_replay(_identity())["artifact_id"] == first["artifact_id"]
    assert repository.find_replay(_identity(input_hash="d" * 64)) is None


def test_semantic_artifact_latest_event_uses_insertion_order_for_tied_timestamps(
    tmp_path,
):
    storage = _Storage(tmp_path / "research.db")
    repository = BusinessProfileSemanticArtifactRepository(storage)
    artifact = repository.receive(
        _identity(),
        response={"rows": [{"unit_raw": "kW", "value": 10}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    repository.mark(artifact["artifact_id"], "conversion_pending")
    repository.mark(artifact["artifact_id"], "rejected")
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_semantic_artifact_events "
            "SET created_at = '2026-08-09T12:00:00+08:00'"
        )
        conn.commit()

    assert repository.find_replay(_identity()) is None


def test_unit_rule_is_persistent_proved_notified_and_available_as_overlay(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers={"prefix:万": "10000", "base:unit": "1"},
    )
    proposal = {
        "source_unit": "万盒",
        "normalized_lexeme": "万盒",
        "dimension": "count",
        "canonical_unit": "unit",
        "numerator": ["盒"],
        "denominator": [],
        "primitive_rule_ids": ["prefix:万", "base:unit"],
        "factors": [
            {"primitive_rule_id": "prefix:万", "exponent": 1},
            {"primitive_rule_id": "base:unit", "exponent": 1},
        ],
        "transformation_type": "linear_multiplier",
        "round_trip_vectors": [{"source": "2", "canonical": "20000"}],
        "semantic_summary_zh": "每万盒折算为一万个计数单位",
    }
    rule = registry.register_proposal(proposal, proposal_input_hash="f" * 64)
    assert rule["status"] == "auto_approved"
    assert rule["multiplier"] == "10000"
    assert registry.overlay_rules()[0]["normalized_lexeme"] == "万盒"
    with storage.get_connection() as conn:
        notification_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_unit_rule_notifications"
        ).fetchone()[0]
        catalog_lineage = conn.execute(
            "SELECT parent_catalog_version FROM business_profile_unit_catalog_versions"
        ).fetchone()[0]
    assert notification_count == 1
    assert catalog_lineage == "business_profile_units.2026.5"


def test_governed_llm_alias_is_persisted_and_auto_approved(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers=governed_primitive_multipliers(),
        primitive_definitions=governed_primitive_definitions(),
    )
    proposal = {
        "source_unit": "万箱",
        "normalized_lexeme": "万箱",
        "dimension": "count",
        "canonical_unit": "unit",
        "numerator": ["箱"],
        "denominator": [],
        "primitive_rule_ids": ["magnitude:万", "classifier:件"],
        "factors": [
            {"primitive_rule_id": "magnitude:万", "exponent": 1},
            {"primitive_rule_id": "classifier:件", "exponent": 1},
        ],
        "transformation_type": "linear_multiplier",
        "round_trip_vectors": [{"source": "1", "canonical": "10000"}],
        "semantic_summary_zh": "箱是计数单位，万表示一万倍",
    }

    rule = registry.register_proposal(proposal, proposal_input_hash="9" * 64)

    assert rule["status"] == "auto_approved"
    assert rule["dimension"] == "count"
    assert rule["canonical_unit"] == "unit"
    assert rule["multiplier"] == "10000"


def test_cross_dimension_llm_alias_is_quarantined(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers=governed_primitive_multipliers(),
        primitive_definitions=governed_primitive_definitions(),
    )
    proposal = {
        "source_unit": "神秘电量",
        "normalized_lexeme": "神秘电量",
        "dimension": "electric_charge",
        "canonical_unit": "Ah",
        "numerator": [],
        "denominator": [],
        "primitive_rule_ids": ["classifier:件"],
        "factors": [{"primitive_rule_id": "classifier:件", "exponent": 1}],
        "transformation_type": "linear_multiplier",
        "round_trip_vectors": [{"source": "1", "canonical": "1"}],
        "semantic_summary_zh": "错误地映射到计数 primitive",
    }

    rule = registry.register_proposal(proposal, proposal_input_hash="8" * 64)

    assert rule["status"] == "quarantined"
    assert "primitive_dimension_mismatch" in rule["proof"]["reason_codes"]


def test_unit_proposal_schema_closes_dimension_and_canonical_vocabulary():
    schema = unit_proposal_response_schema()

    assert "electric_charge" in schema["properties"]["dimension"]["enum"]
    assert "Ah" in schema["properties"]["canonical_unit"]["enum"]


def test_catalog_reconciles_quarantined_rule_and_replays_artifact(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    artifacts = BusinessProfileSemanticArtifactRepository(storage)
    artifact = artifacts.receive(
        _identity(instrument_id="688799.SH"),
        response={"rows": [{"unit_raw": "万粒", "value": 2}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers=governed_primitive_multipliers(),
        primitive_definitions=governed_primitive_definitions(),
    )
    old = registry.register_proposal(
        {
            "source_unit": "万粒",
            "normalized_lexeme": "万粒",
            "dimension": "粒",
            "canonical_unit": "粒",
            "numerator": [],
            "denominator": [],
            "primitive_rule_ids": ["magnitude:万"],
            "factors": [{"primitive_rule_id": "magnitude:万", "exponent": 1}],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [{"source": "1万粒", "canonical": "10000粒"}],
            "semantic_summary_zh": "旧版自由文本候选",
        },
        proposal_input_hash="7" * 64,
        artifact_id=artifact["artifact_id"],
        source_document_id="document-1",
        context_hash="scope-1",
        model_identity="model-1",
    )

    report = registry.reconcile_deterministic_rules()

    assert report == {"scanned": 1, "resolved": 1, "superseded": 1, "replayed": 1}
    assert registry.get_rule(old["rule_id"])["status"] == "superseded"
    replacement = next(
        rule for rule in registry.overlay_rules() if rule["source_unit"] == "万粒"
    )
    assert replacement["status"] == "auto_approved"
    assert replacement["multiplier"] == "10000"
    assert artifacts.find_replay(_identity(instrument_id="688799.SH")) is not None

    messages = []

    async def notifier(message):
        messages.append(message)

    delivered = asyncio.run(registry.dispatch_notifications(notifier, limit=10))
    assert delivered == 3
    assert any("已生效=是" in message for message in messages)
    assert any("影响公司=688799.SH" in message for message in messages)


def test_operator_correction_supersedes_rule_and_replays_without_formula_input(
    tmp_path,
):
    storage = _Storage(tmp_path / "research.db")
    artifacts = BusinessProfileSemanticArtifactRepository(storage)
    identity = _identity(instrument_id="300750.SZ")
    artifact = artifacts.receive(
        identity,
        response={"rows": [{"unit_raw": "万Ah", "value": "12"}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    registry = BusinessProfileUnitRuleRegistry(storage)
    old = registry.register_proposal(
        {
            "source_unit": "万Ah",
            "normalized_lexeme": "万Ah",
            "dimension": "未确定（含未登记的 A 维度）",
            "canonical_unit": "Ah",
            "numerator": [],
            "denominator": [],
            "primitive_rule_ids": [],
            "factors": [],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [
                {"source": "1", "canonical": "416.6666666666667"}
            ],
            "semantic_summary_zh": "错误的模型建议",
        },
        proposal_input_hash="7" * 64,
        artifact_id=artifact["artifact_id"],
        source_document_id="document-1",
        context_hash="scope-1",
        model_identity="unit-model-1",
    )

    result = registry.correct_rule(
        old["rule_id"],
        dimension="electric_charge",
        canonical_unit="Ah",
        multiplier="10000",
        reason="万表示一万，Ah 是电荷容量单位",
    )

    replacement = result["replacement_rule"]
    assert registry.get_rule(old["rule_id"])["status"] == "superseded"
    assert replacement["status"] == "auto_approved"
    assert replacement["dimension"] == "electric_charge"
    assert replacement["canonical_unit"] == "Ah"
    assert replacement["multiplier"] == "10000"
    assert result["replayed_artifacts"] == 1
    assert artifacts.find_replay(identity) is not None
    assert [row["status"] for row in registry.get_rule_history(old["rule_id"])] == [
        "proposed",
        "quarantined",
        "superseded",
    ]
    overlay = {
        row["normalized_lexeme"]: row for row in registry.overlay_rules()
    }
    assert overlay["万Ah"]["rule_id"] == replacement["rule_id"]
    repeated = registry.correct_rule(
        old["rule_id"],
        dimension="electric_charge",
        canonical_unit="Ah",
        multiplier="1e4",
        reason="重复执行不应产生新生命周期",
    )
    assert repeated["idempotent_reuse"] is True
    assert repeated["replayed_artifacts"] == 0
    assert len(registry.get_rule_history(old["rule_id"])) == 3
    assert len(registry.get_rule_history(replacement["rule_id"])) == 2


def test_operator_correction_rejects_ungoverned_target(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(storage)
    old = registry.register_proposal(
        {
            "source_unit": "箱当量",
            "normalized_lexeme": "箱当量",
            "dimension": "unknown",
            "canonical_unit": "unknown",
            "numerator": [],
            "denominator": [],
            "primitive_rule_ids": [],
            "factors": [],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [{"source": "1", "canonical": "1"}],
        },
        proposal_input_hash="8" * 64,
    )

    with pytest.raises(ValueError, match="unknown dimension"):
        registry.correct_rule(
            old["rule_id"],
            dimension="custom_dimension",
            canonical_unit="custom",
            multiplier="1",
        )


def test_operator_correction_resumes_after_interrupted_catalog_commit(
    tmp_path, monkeypatch
):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(storage)
    old = registry.register_proposal(
        {
            "source_unit": "未知安时单位",
            "normalized_lexeme": "未知安时单位",
            "dimension": "unknown",
            "canonical_unit": "unknown",
            "numerator": [],
            "denominator": [],
            "primitive_rule_ids": [],
            "factors": [],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [{"source": "1", "canonical": "1"}],
        },
        proposal_input_hash="9" * 64,
    )
    commit_catalog = registry._commit_catalog_version

    def interrupted(_rule_id):
        raise RuntimeError("simulated interruption")

    monkeypatch.setattr(registry, "_commit_catalog_version", interrupted)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        registry.correct_rule(
            old["rule_id"],
            dimension="electric_charge",
            canonical_unit="Ah",
            multiplier="1000",
        )
    assert registry.get_rule(old["rule_id"])["status"] == "superseded"

    monkeypatch.setattr(registry, "_commit_catalog_version", commit_catalog)
    resumed = registry.correct_rule(
        old["rule_id"],
        dimension="electric_charge",
        canonical_unit="Ah",
        multiplier="1000",
    )

    assert resumed["replacement_rule"]["status"] == "auto_approved"
    assert resumed["idempotent_reuse"] is False


def test_unit_rule_without_round_trip_proof_is_quarantined(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers={"prefix:万": "10000", "base:unit": "1"},
    )
    proposal = {
        "source_unit": "万盒",
        "normalized_lexeme": "万盒",
        "dimension": "count",
        "canonical_unit": "unit",
        "numerator": ["盒"],
        "denominator": [],
        "primitive_rule_ids": ["prefix:万", "base:unit"],
        "factors": [
            {"primitive_rule_id": "prefix:万", "exponent": 1},
            {"primitive_rule_id": "base:unit", "exponent": 1},
        ],
        "transformation_type": "linear_multiplier",
        "round_trip_vectors": [],
    }

    rule = registry.register_proposal(proposal, proposal_input_hash="0" * 64)

    assert rule["status"] == "quarantined"
    assert "round_trip_vectors_missing" in rule["proof"]["reason_codes"]


def test_auto_approved_rule_is_reusable_by_proof_and_semantic_conversion(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers={"prefix:万": "10000", "base:unit": "1"},
    )
    first = registry.register_proposal(
        {
            "source_unit": "万盒",
            "normalized_lexeme": "万盒",
            "dimension": "count",
            "canonical_unit": "unit",
            "numerator": ["盒"],
            "denominator": [],
            "primitive_rule_ids": ["prefix:万", "base:unit"],
            "factors": [
                {"primitive_rule_id": "prefix:万", "exponent": 1},
                {"primitive_rule_id": "base:unit", "exponent": 1},
            ],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [{"source": "2", "canonical": "20000"}],
            "semantic_summary_zh": "每万盒折算为一万个计数单位",
        },
        proposal_input_hash="1" * 64,
    )
    second = registry.register_proposal(
        {
            "source_unit": "万箱",
            "normalized_lexeme": "万箱",
            "dimension": "count",
            "canonical_unit": "unit",
            "numerator": ["箱"],
            "denominator": [],
            "primitive_rule_ids": [first["rule_id"]],
            "factors": [
                {"primitive_rule_id": first["rule_id"], "exponent": 1}
            ],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [{"source": "3", "canonical": "30000"}],
            "semantic_summary_zh": "复用已证明的万倍计数规则",
        },
        proposal_input_hash="2" * 64,
    )

    normalized, unit, resolution = _normalized_value_with_resolution(
        2,
        "万盒",
        "count",
        runtime_rules=registry.overlay_rules(),
    )

    assert first["status"] == second["status"] == "auto_approved"
    assert normalized == 20000
    assert unit == "unit"
    assert resolution.runtime_rule_id == first["rule_id"]


def test_unsafe_unit_rule_is_quarantined(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(storage)
    proposal = {
        "source_unit": "美元折人民币",
        "normalized_lexeme": "美元折人民币",
        "dimension": "currency",
        "canonical_unit": "CNY",
        "numerator": [],
        "denominator": [],
        "primitive_rule_ids": [],
        "factors": [],
        "transformation_type": "fx",
        "round_trip_vectors": [],
    }
    rule = registry.register_proposal(proposal, proposal_input_hash="e" * 64)
    assert rule["status"] == "quarantined"
    assert registry.overlay_rules() == []


def test_unit_rule_latest_lifecycle_uses_insertion_order_for_tied_timestamps(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers={"prefix:万": "10000", "base:unit": "1"},
    )
    proposal = {
        "source_unit": "万盒",
        "normalized_lexeme": "万盒",
        "dimension": "count",
        "canonical_unit": "unit",
        "numerator": ["盒"],
        "denominator": [],
        "primitive_rule_ids": ["prefix:万", "base:unit"],
        "factors": [
            {"primitive_rule_id": "prefix:万", "exponent": 1},
            {"primitive_rule_id": "base:unit", "exponent": 1},
        ],
        "transformation_type": "linear_multiplier",
        "round_trip_vectors": [{"source": "2", "canonical": "20000"}],
        "semantic_summary_zh": "每万盒折算为一万个计数单位",
    }
    original = registry.register_proposal(proposal, proposal_input_hash="a" * 64)
    replacement = {
        **proposal,
        "source_unit": "万箱",
        "normalized_lexeme": "万箱",
        "numerator": ["箱"],
    }
    registry.supersede(
        original["rule_id"], replacement, proposal_input_hash="b" * 64
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_unit_rules "
            "SET created_at = '2026-08-09T12:00:00+08:00'"
        )
        conn.commit()

    assert registry.get_rule(original["rule_id"])["status"] == "superseded"
    overlay_ids = {row["rule_id"] for row in registry.overlay_rules()}
    assert original["rule_id"] not in overlay_ids
