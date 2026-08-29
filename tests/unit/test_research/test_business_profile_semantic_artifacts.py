import asyncio
import json
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

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
    normalize_unit_lexeme,
)
from research.business_profile_unit_registry import (
    BusinessProfileUnitRuleRegistry,
    propose_unknown_unit,
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


def test_semantic_artifact_failed_conversion_is_not_replayable(tmp_path):
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
    assert repository.find_replay(_identity()) is None
    assert repository.find_replay(_identity(input_hash="d" * 64)) is None


def test_semantic_artifact_replay_reopens_only_after_unit_rule_change(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    repository = BusinessProfileSemanticArtifactRepository(storage)
    artifact = repository.receive(
        _identity(),
        response={"rows": [{"unit_raw": "万盒", "value": 10}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    repository.mark(
        artifact["artifact_id"],
        "conversion_pending",
        reason_code="unit_rule_auto_approved",
    )
    assert repository.find_replay(_identity())["artifact_id"] == artifact["artifact_id"]


def test_converted_semantic_artifact_is_replayable(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    repository = BusinessProfileSemanticArtifactRepository(storage)
    artifact = repository.receive(
        _identity(),
        response={"rows": [{"unit_raw": "吨", "value": 10}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    repository.mark(artifact["artifact_id"], "converted")
    replay = repository.find_replay(_identity())
    assert replay is not None
    assert replay["artifact_id"] == artifact["artifact_id"]


def test_partial_row_rejection_is_not_replayable_after_conversion(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    repository = BusinessProfileSemanticArtifactRepository(storage)
    artifact = repository.receive(
        _identity(),
        response={"rows": [{"unit_raw": "吨", "value": 10}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    repository.mark(artifact["artifact_id"], "converted")
    repository.mark(
        artifact["artifact_id"],
        "conversion_pending",
        reason_code="partial_row_rejection",
    )
    assert repository.find_replay(_identity()) is None


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
    assert catalog_lineage == "business_profile_units.2026.7"


def test_unknown_llm_alias_cannot_borrow_unrelated_count_primitive(tmp_path):
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

    assert rule["status"] == "quarantined"
    assert "unproved_source_token" in rule["proof"]["reason_codes"]


@pytest.mark.parametrize(
    ("source_unit", "dimension", "canonical_unit", "primitive_rule_id"),
    [
        ("神秘件", "count", "unit", "classifier:件"),
        ("件/件", "count", "unit", "classifier:件"),
        ("T/KL", "mass", "tonne", "primitive:t"),
    ],
)
def test_partially_matched_unknown_source_unit_is_not_proved(
    tmp_path, source_unit, dimension, canonical_unit, primitive_rule_id
):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers=governed_primitive_multipliers(),
        primitive_definitions=governed_primitive_definitions(),
    )
    proposal = {
        "source_unit": source_unit,
        "normalized_lexeme": source_unit,
        "dimension": dimension,
        "canonical_unit": canonical_unit,
        "numerator": [source_unit],
        "denominator": [],
        "primitive_rule_ids": [primitive_rule_id],
        "factors": [{"primitive_rule_id": primitive_rule_id, "exponent": 1}],
        "transformation_type": "linear_multiplier",
        "round_trip_vectors": [{"source": "1", "canonical": "1"}],
        "semantic_summary_zh": "不能只凭后缀件推断整个未知单位",
    }

    rule = registry.register_proposal(proposal, proposal_input_hash="8" * 64)

    assert rule["status"] == "quarantined"
    assert "unproved_source_token" in rule["proof"]["reason_codes"]


def test_deterministic_reconciliation_supersedes_unsafe_active_weight_case_rule(
    tmp_path,
):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers={
            "prefix:万": "10000",
            "base:unit": "1",
        },
        primitive_definitions={},
    )
    unsafe = registry.register_proposal(
        {
            "source_unit": "万重箱",
            "normalized_lexeme": "万重箱",
            "dimension": "count",
            "canonical_unit": "unit",
            "numerator": ["重箱"],
            "denominator": [],
            "primitive_rule_ids": ["prefix:万", "base:unit"],
            "factors": [
                {"primitive_rule_id": "prefix:万", "exponent": 1},
                {"primitive_rule_id": "base:unit", "exponent": 1},
            ],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [{"source": "1", "canonical": "10000"}],
            "semantic_summary_zh": "历史错误规则",
        },
        proposal_input_hash="7" * 64,
    )

    report = registry.reconcile_deterministic_rules()
    effective = registry.get_unit_state("万重箱")["effective_rule"]

    assert unsafe["status"] == "auto_approved"
    assert report["resolved"] == 1
    assert registry.get_rule(unsafe["rule_id"])["status"] == "superseded"
    assert effective is not None
    assert effective["dimension"] == "mass"
    assert effective["canonical_unit"] == "tonne"
    assert effective["multiplier"] == "500"


def test_deterministic_reconciliation_recovers_owning_completed_work(tmp_path):
    storage = _Storage(tmp_path / "research.db")
    artifacts = BusinessProfileSemanticArtifactRepository(storage)
    identity = _identity(instrument_id="688799.SH")
    artifact = artifacts.receive(
        identity,
        response={"rows": [{"unit_raw": "万重箱", "value": 2}]},
        response_hash="",
        evidence_ids=["span-1"],
    )
    now = "2026-08-10T12:00:00+08:00"
    with storage.get_connection() as conn:
        conn.execute(
            "INSERT INTO business_profile_work_items ("
            "work_id, frontier_id, instrument_id, source, announcement_id, "
            "report_period, document_type, policy, processing_identity_hash, "
            "stage, status, attempt_count, lease_owner, lease_expires_at, "
            "checkpoint_path, metadata_json, completed_at, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "work-weight-case",
                "frontier-weight-case",
                "688799.SH",
                "cninfo",
                "annual-2025",
                "2025-12-31",
                "annual_report",
                "latest_annual_only",
                "identity-weight-case",
                "publish",
                "completed",
                2,
                "worker-1",
                now,
                "checkpoint.json",
                json.dumps({"source_document_id": "document-1"}),
                now,
                now,
                now,
            ),
        )
        conn.commit()
    registry = BusinessProfileUnitRuleRegistry(
        storage,
        primitive_multipliers={"prefix:万": "10000", "base:unit": "1"},
        primitive_definitions={},
    )
    unsafe = registry.register_proposal(
        {
            "source_unit": "万重箱",
            "normalized_lexeme": "万重箱",
            "dimension": "count",
            "canonical_unit": "unit",
            "numerator": ["重箱"],
            "denominator": [],
            "primitive_rule_ids": ["prefix:万", "base:unit"],
            "factors": [
                {"primitive_rule_id": "prefix:万", "exponent": 1},
                {"primitive_rule_id": "base:unit", "exponent": 1},
            ],
            "transformation_type": "linear_multiplier",
            "round_trip_vectors": [{"source": "1", "canonical": "10000"}],
            "semantic_summary_zh": "历史错误规则",
        },
        proposal_input_hash="6" * 64,
        artifact_id=artifact["artifact_id"],
        source_document_id="document-1",
        context_hash="scope-weight-case",
        model_identity="unit-model-1",
    )

    report = registry.reconcile_deterministic_rules()

    assert report["replayed"] == 1
    assert registry.get_rule(unsafe["rule_id"])["status"] == "superseded"
    with storage.get_connection() as conn:
        work = conn.execute(
            "SELECT stage, status, attempt_count, lease_owner, lease_expires_at, "
            "completed_at, last_error FROM business_profile_work_items "
            "WHERE work_id = 'work-weight-case'"
        ).fetchone()
    assert dict(work) == {
        "stage": "semantic",
        "status": "retry_due",
        "attempt_count": 0,
        "lease_owner": None,
        "lease_expires_at": None,
        "completed_at": None,
        "last_error": f"unit_rule_superseded:{unsafe['rule_id']}",
    }


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

    messages = []

    async def notifier(message):
        messages.append(message)

    assert asyncio.run(registry.dispatch_notifications(notifier, limit=10)) == 1
    assert messages[0].startswith("⚠️ [公司画像单位规则]")


def test_unit_proposal_schema_closes_dimension_and_canonical_vocabulary():
    schema = unit_proposal_response_schema()

    assert "electric_charge" in schema["properties"]["dimension"]["enum"]
    assert "Ah" in schema["properties"]["canonical_unit"]["enum"]


def test_unit_proposal_request_serializes_decimal_primitive_definitions():
    class _Client:
        request = None

        async def complete(self, request):
            self.request = request
            return SimpleNamespace(
                data={
                    "source_unit": "箱",
                    "normalized_lexeme": "箱",
                    "dimension": "count",
                    "canonical_unit": "unit",
                    "numerator": ["箱"],
                    "denominator": [],
                    "primitive_rule_ids": ["classifier:件"],
                    "factors": [
                        {"primitive_rule_id": "classifier:件", "exponent": 1}
                    ],
                    "transformation_type": "linear_multiplier",
                    "round_trip_vectors": [{"source": "1", "canonical": "1"}],
                    "semantic_summary_zh": "箱是计数单位",
                }
            )

    client = _Client()
    primitive_multipliers = governed_primitive_multipliers()
    primitive_definitions = governed_primitive_definitions()
    result = asyncio.run(
        propose_unknown_unit(
            client,
            source_unit="箱",
            context_zh="产品产销量单位为箱",
            primitive_multipliers=primitive_multipliers,
            primitive_definitions=primitive_definitions,
        )
    )

    payload = json.loads(client.request.messages[-1].content)
    primitives = payload["governed_primitives"]
    primitive = next(
        item for item in primitives if item["rule_id"] == "classifier:件"
    )
    assert primitive == {
        "rule_id": "classifier:件",
        "multiplier": "1",
        "dimension": "count",
        "canonical_unit": "unit",
        "source_tokens": ["件"],
    }
    assert all(isinstance(item["multiplier"], str) for item in primitives)
    assert result["dimension"] == "count"


@pytest.mark.parametrize(
    ("source_unit", "expected_multiplier"),
    [
        ("万粒", "10000"),
        ("亿吨千米", "100000000"),
        ("元币种：人民币", "1"),
    ],
)
def test_catalog_reconciles_quarantined_rule_and_replays_artifact(
    tmp_path, source_unit, expected_multiplier
):
    storage = _Storage(tmp_path / "research.db")
    artifacts = BusinessProfileSemanticArtifactRepository(storage)
    artifact = artifacts.receive(
        _identity(instrument_id="688799.SH"),
        response={"rows": [{"unit_raw": source_unit, "value": 2}]},
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
            "source_unit": source_unit,
            "normalized_lexeme": normalize_unit_lexeme(source_unit),
            "dimension": "unknown",
            "canonical_unit": "unknown",
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
        rule for rule in registry.overlay_rules() if rule["source_unit"] == source_unit
    )
    assert replacement["status"] == "auto_approved"
    assert replacement["multiplier"] == expected_multiplier
    assert artifacts.find_replay(_identity(instrument_id="688799.SH")) is not None

    messages = []

    async def notifier(message):
        messages.append(message)

    delivered = asyncio.run(registry.dispatch_notifications(notifier, limit=10))
    assert delivered == 3
    assert len(messages) == 1
    assert messages[0].startswith("✅ [公司画像单位规则]")
    assert "当前最终状态=enabled" in messages[0]
    assert "已生效=是" in messages[0]
    assert "本次事件=quarantined,auto_approved,superseded" in messages[0]
    assert "生命周期=" in messages[0]
    assert "proposed>quarantined>superseded" in messages[0]
    assert "隔离原因=" in messages[0]
    assert "new_or_unknown_dimension" in messages[0]
    assert "影响公司=688799.SH" in messages[0]
    assert len(messages[0]) < 4096
    state = registry.get_unit_state(source_unit)
    assert state["effective"] is True
    assert state["effective_rule"]["rule_id"] == replacement["rule_id"]
    assert state["replacements"] == [
        {"rule_id": old["rule_id"], "superseded_by": replacement["rule_id"]}
    ]
    assert state["quarantine_reasons"][0]["rule_id"] == old["rule_id"]


@pytest.mark.parametrize(
    ("source_unit", "expected_status", "expected_multiplier"),
    [
        ("万张", "enabled", "10000"),
        ("点", "enabled", "1"),
        ("万粒/万瓶", "enabled", "10000"),
        ("PCS", "enabled", "1"),
        ("平方", "enabled", "1"),
        ("立方", "enabled", "1"),
        ("亿吨千米", "enabled", "100000000"),
        ("元币种：人民币", "enabled", "1"),
        ("万台（万千瓦时）", "quarantined", None),
    ],
)
def test_catalog_reconciles_only_deterministic_production_units(
    tmp_path,
    source_unit,
    expected_status,
    expected_multiplier,
):
    storage = _Storage(tmp_path / "research.db")
    registry = BusinessProfileUnitRuleRegistry(storage)
    old = registry.register_proposal(
        {
            "source_unit": source_unit,
            "normalized_lexeme": normalize_unit_lexeme(source_unit),
            "dimension": "unknown",
            "canonical_unit": "unknown",
            "numerator": [],
            "denominator": [],
            "primitive_rule_ids": [],
            "factors": [],
            "transformation_type": "contextual",
            "round_trip_vectors": [{"source": "1", "canonical": "1"}],
            "semantic_summary_zh": "旧版未能确定性解析的候选",
        },
        proposal_input_hash=(source_unit.encode("utf-8").hex() + "0" * 64)[:64],
    )

    report = registry.reconcile_deterministic_rules()
    state = registry.get_unit_state(source_unit)

    assert state["final_status"] == expected_status
    if expected_multiplier is None:
        assert report["resolved"] == 0
        assert registry.get_rule(old["rule_id"])["status"] == "quarantined"
        assert state["effective_rule"] is None
    else:
        assert report["resolved"] == 1
        assert registry.get_rule(old["rule_id"])["status"] == "superseded"
        assert state["effective_rule"]["multiplier"] == expected_multiplier


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


def test_auto_approved_rule_converts_exact_unit_but_cannot_prove_new_alias(tmp_path):
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

    assert first["status"] == "auto_approved"
    assert second["status"] == "quarantined"
    assert "unproved_source_token" in second["proof"]["reason_codes"]
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
