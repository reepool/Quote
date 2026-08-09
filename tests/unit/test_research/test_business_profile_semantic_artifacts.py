import sqlite3
from contextlib import contextmanager

from research.business_profile_semantic_artifacts import (
    BusinessProfileSemanticArtifactRepository,
    SemanticArtifactIdentity,
)
from research.business_profile_semantic_runtime import (
    _normalized_value_with_resolution,
)
from research.business_profile_unit_registry import BusinessProfileUnitRuleRegistry
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
    assert catalog_lineage == "business_profile_units.2026.4"


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
