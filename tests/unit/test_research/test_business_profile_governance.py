import sqlite3
from contextlib import contextmanager

import pytest

from research.business_profile_governance import (
    BusinessProfileRepository,
    BusinessProfileResolver,
    _build_business_profile_measurement_contract,
    _derive_profile_and_market_link_status,
    build_empty_business_profile_context,
    select_current_business_profile_activities,
)
from research.business_profile_review import BusinessProfileReviewService
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


class _FuturesStorage:
    def get_exposure_mappings(self, *, scope_type, scope_id):
        if scope_type == "industry" and scope_id == "煤炭":
            return [
                {
                    "mapping_id": "industry-coal",
                    "scope_type": "industry",
                    "scope_id": "煤炭",
                    "product_name": "焦煤",
                    "revenue_series_id": "CNF.JM.DCE.main",
                    "cost_series_ids": [],
                    "spread_ids": [],
                    "direction": "positive",
                    "valid_from": "2020-01-01",
                    "valid_to": None,
                }
            ]
        return []

    def get_series(self, series_id):
        if series_id == "CNF.JM.DCE.main":
            return {"series_id": series_id, "active": True}
        return None


class _SpecialCommodityStorage:
    def get_series(self, series_id):
        if series_id == "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY":
            return {
                "series_id": series_id,
                "commodity_id": "CN.CHEMICAL.SODA_ASH.SPOT",
                "active": True,
            }
        return None


def _storage(tmp_path):
    research_db = tmp_path / "research.db"
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(research_db),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage, research_db


def _approved_evidence(instrument_id="601088.SH"):
    return {
        "evidence_id": "evidence-2025-ar",
        "instrument_id": instrument_id,
        "source_document_id": "cninfo-ann-2025-ar",
        "source_institution": "cninfo",
        "source_tier": "official_filing",
        "document_type": "annual_report",
        "title": "2025 annual report",
        "document_hash": "doc-hash",
        "report_period": "2025-12-31",
        "publish_date": "2026-03-28",
        "data_available_date": "2026-03-28",
        "availability_quality": "actual",
        "page_number": 31,
        "section_path": "business review/coal",
        "evidence_text_hash": "text-hash",
        "extraction_method": "manual_gold_standard",
        "confidence": 1.0,
        "review_status": "approved",
        "metadata": {"source_priority": 1},
    }


@pytest.mark.parametrize(
    ("facts", "expected_status", "expected_linked"),
    [
        ([], "not_applicable", 0),
        (
            [{"metadata": {"source_activity_id": "activity-1"}}],
            "linked",
            1,
        ),
        (
            [
                {"metadata": {"source_activity_id": "activity-1"}},
                {
                    "metadata": {
                        "measurement_authority": (
                            "llm_source_fields_program_normalized"
                        )
                    }
                },
            ],
            "partially_linked",
            1,
        ),
        (
            [
                {
                    "metadata": {
                        "measurement_authority": (
                            "llm_source_fields_program_normalized"
                        )
                    }
                }
            ],
            "unlinked",
            0,
        ),
    ],
)
def test_business_profile_measurement_contract_linkage_status(
    facts, expected_status, expected_linked
):
    contract = _build_business_profile_measurement_contract(facts)

    assert contract["authoritative_measurements_path"] == (
        "company_specific_profile.operating_facts"
    )
    assert contract["activity_measurement_role"] == "compatibility_projection"
    assert contract["operating_fact_activity_link_field"] == (
        "metadata.source_activity_id"
    )
    assert contract["operating_fact_count"] == len(facts)
    assert contract["linked_activity_derived_operating_fact_count"] == expected_linked
    assert contract["linkage_status"] == expected_status


def test_empty_business_profile_exposes_measurement_contract():
    context = build_empty_business_profile_context(
        "601088.SH", as_of_date="2026-08-24"
    )

    assert context["status"] == "not_ready"
    assert context["measurement_contract"]["linkage_status"] == "not_applicable"
    assert context["measurement_contract"]["operating_fact_count"] == 0


def test_standalone_operating_facts_do_not_require_activity_links():
    contract = _build_business_profile_measurement_contract(
        [{"metadata": {"semantic_synthesis": True}}]
    )

    assert contract["operating_fact_count"] == 1
    assert contract["activity_derived_operating_fact_count"] == 0
    assert contract["standalone_operating_fact_count"] == 1
    assert contract["linkage_status"] == "not_applicable"


def test_current_activity_selection_prefers_catalog_normalized_replay():
    shared = {
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "evidence_id": "evidence-2025-ar",
        "subject_scope": "consolidated_group",
        "action": "sells",
        "object_type": "product",
        "object_raw": "聚乙烯",
        "segment_id": None,
        "geography": None,
        "value": 373.9,
        "unit": "千吨",
        "share": None,
        "business_regime_id": None,
        "knowledge_from": "2026-03-30",
        "version": 1,
    }

    selected = select_current_business_profile_activities(
        [
            {**shared, "activity_id": "legacy", "object_id": None},
            {
                **shared,
                "activity_id": "normalized",
                "object_id": "polymer.polyethylene",
            },
        ]
    )

    assert [item["activity_id"] for item in selected] == ["normalized"]


def test_relationship_as_of_keeps_latest_cohort_and_same_period_contracts(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    base = {
        "instrument_id": "601088.SH",
        "relationship_type": "buys_from",
        "direction": "inbound",
        "counterparty_name_raw": "供应商甲有限公司",
        "counterparty_name_normalized": None,
        "counterparty_entity_id": None,
        "resolution_basis": None,
        "anonymous": 0,
        "scope_type": "company",
        "scope_id": "601088.SH",
        "object_raw": "煤炭",
        "object_id": None,
        "evidence_id": "evidence-2025-ar",
        "data_available_date": "2026-03-28",
        "confidence": 0.9,
        "review_status": "approved",
        "valid_from": "2025-01-01",
        "metadata": {},
    }
    for record in (
        {**base, "relationship_id": "rel-2024", "report_period": "2024-12-31", "metadata": {"relationship_occurrence_key": "contract-old"}},
        {**base, "relationship_id": "rel-2025-a", "report_period": "2025-12-31", "metadata": {"relationship_occurrence_key": "contract-a"}},
        {**base, "relationship_id": "rel-2025-b", "report_period": "2025-12-31", "metadata": {"relationship_occurrence_key": "contract-b"}},
    ):
        _governed_upsert(repository, "relationships", record)

    current = repository.get_approved_as_of(
        "relationships", instrument_id="601088.SH", cutoff="2026-04-30"
    )

    assert {item["relationship_id"] for item in current} == {"rel-2025-a", "rel-2025-b"}


def test_relationship_as_of_excludes_stale_persistent_evidence(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    _governed_upsert(repository, "relationships", {
        "relationship_id": "rel-stale", "instrument_id": "601088.SH", "report_period": "2024-12-31",
        "relationship_type": "buys_from", "direction": "inbound", "counterparty_name_raw": "供应商甲有限公司",
        "anonymous": 0, "scope_type": "company", "scope_id": "601088.SH", "object_raw": "煤炭",
        "evidence_id": "evidence-2025-ar", "data_available_date": "2026-03-28", "confidence": 0.9,
        "review_status": "approved", "valid_from": "2024-01-01", "metadata": {"relationship_occurrence_key": "contract-a"},
    })

    assert repository.get_approved_as_of(
        "relationships", instrument_id="601088.SH", cutoff="2027-01-01"
    ) == []


@pytest.mark.parametrize(
    ("record_type", "record", "record_id"),
    [
        (
            "activities",
            {
                "activity_id": "ended-activity",
                "report_period": "2025-12-31",
                "subject_scope": "issuer",
                "action": "produces",
                "object_type": "product",
                "object_raw": "煤炭",
                "evidence_id": "evidence-2025-ar",
                "extraction_method": "semantic_verified",
                "valid_from": "2025-01-01",
                "valid_to": "2026-04-30",
            },
            "activity_id",
        ),
        (
            "value_chain_roles",
            {
                "record_id": "ended-role",
                "report_period": "2025-12-31",
                "role": "upstream_producer",
                "materiality": "high",
                "mapping_basis": "company_disclosure",
                "evidence_id": "evidence-2025-ar",
                "valid_from": "2025-01-01",
                "valid_to": "2026-04-30",
            },
            "record_id",
        ),
        (
            "exposure_assumptions",
            {
                "assumption_id": "ended-assumption",
                "scope_type": "instrument",
                "scope_id": "601088.SH",
                "assumption_type": "pass_through",
                "assumption_value": 0.5,
                "unit": "fraction",
                "method": "approved_model",
                "effective_from": "2025-01-01",
                "effective_to": "2026-04-30",
            },
            "assumption_id",
        ),
        (
            "exposures",
            {
                "exposure_id": "ended-exposure",
                "report_period": "2025-12-31",
                "scope_type": "company",
                "scope_id": "601088.SH",
                "commodity_id": "coal",
                "exposure_role": "revenue",
                "direction": "positive",
                "mapping_basis": "test",
                "price_series_id": "coal-price",
                "evidence_id": "evidence-2025-ar",
                "effective_from": "2025-01-01",
                "effective_to": "2026-04-30",
            },
            "exposure_id",
        ),
    ],
)
def test_as_of_excludes_every_expired_validity_interval(
    tmp_path, record_type, record, record_id
):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    payload = {
        **record,
        "instrument_id": "601088.SH",
        "data_available_date": "2026-03-28",
        "confidence": 1.0,
        "review_status": "approved",
    }
    _governed_upsert(repository, record_type, payload)

    before_end = repository.get_approved_as_of(
        record_type, instrument_id="601088.SH", cutoff="2026-04-29"
    )
    at_end = repository.get_approved_as_of(
        record_type, instrument_id="601088.SH", cutoff="2026-04-30"
    )

    assert before_end[0][record_id] == record[record_id]
    assert at_end == []


def _governed_upsert(repository, record_type, record):
    """Insert test records through the same candidate-first audited path as production."""

    payload = dict(record)
    requested_status = str(payload.get("review_status") or "candidate")
    if requested_status == "candidate":
        return repository.upsert(record_type, payload)
    if requested_status != "approved":
        raise ValueError(f"unsupported test review status: {requested_status}")
    payload["review_status"] = "candidate"
    record_id = repository.upsert(record_type, payload)
    spec = BusinessProfileRepository._TABLES[record_type]
    current = next(
        item
        for item in repository.list_records(record_type)
        if str(item[spec["pk"]]) == str(record_id)
    )
    BusinessProfileReviewService(repository).system_promote_record(
        record_type,
        record_id,
        field_family="test_fixture",
        policy_version="test_fixture_policy.v1",
        gate_manifest_hash="test-fixture-gates",
        reviewer_version="v1",
        expected_updated_at=current["updated_at"],
        evidence_references=["test-fixture:official-evidence"],
    )
    return record_id


def test_initialize_creates_business_profile_governance_tables(tmp_path):
    _, research_db = _storage(tmp_path)

    with sqlite3.connect(research_db) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert {
        "business_profile_evidence",
        "company_business_profile_events",
        "company_business_profile_regimes",
        "company_business_segments",
        "company_operating_facts",
        "company_value_chain_roles",
        "company_commodity_exposures",
        "business_profile_review_audit",
    }.issubset(tables)
    with sqlite3.connect(research_db) as conn:
        segment_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(company_business_segments)"
            ).fetchall()
        }

    assert {
        "business_regime_id",
        "knowledge_from",
        "knowledge_to",
        "supersedes_record_id",
    }.issubset(segment_columns)
    with sqlite3.connect(research_db) as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
    assert {
        "idx_business_profile_evidence_asof_v2",
        "idx_company_business_profile_regimes_identity_v2",
        "idx_company_business_segments_identity_v2",
        "idx_company_operating_facts_identity_v2",
        "idx_company_value_chain_roles_identity_v2",
        "idx_company_commodity_exposures_identity_v2",
    }.issubset(indexes)


def test_repository_rejects_unknown_fields_before_persistence(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    evidence = _approved_evidence()
    evidence["model_supplied_policy_id"] = "invented"

    with pytest.raises(ValueError, match="unknown fields"):
        repository.upsert("evidence", evidence)
    assert repository.list_records("evidence") == []


@pytest.mark.parametrize(
    ("facts", "exposures", "mappings", "profile_status", "market_status"),
    [
        (0, [], [], "not_ready", "not_applicable"),
        (1, [{"exposure_id": "e1"}], [], "ready", "unlinked"),
        (
            1,
            [{"exposure_id": "e1"}, {"exposure_id": "e2"}],
            [{"source_exposure_id": "e1"}],
            "ready",
            "partial",
        ),
        (
            1,
            [{"exposure_id": "e1"}],
            [{"source_exposure_id": "e1"}],
            "ready",
            "direct_linked",
        ),
    ],
)
def test_profile_and_market_link_status_matrix(
    facts, exposures, mappings, profile_status, market_status
):
    result = _derive_profile_and_market_link_status(
        approved_company_fact_count=facts,
        approved_exposures=exposures,
        executable_company_mappings=mappings,
    )
    assert result[:2] == (profile_status, market_status)


def test_resolver_reports_migration_required_without_mutating_legacy_database(tmp_path):
    legacy_db = tmp_path / "legacy.db"
    with sqlite3.connect(legacy_db) as conn:
        conn.execute("CREATE TABLE legacy_marker (id INTEGER PRIMARY KEY)")
        conn.commit()

    class _LegacyStorage:
        @staticmethod
        def _apply_pragmas(conn):
            conn.row_factory = sqlite3.Row

        @contextmanager
        def get_connection(self):
            conn = sqlite3.connect(legacy_db)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()

    context = BusinessProfileResolver(
        BusinessProfileRepository(_LegacyStorage())
    ).resolve("601088.SH", as_of_date="2026-04-30")

    assert context["status"] == "not_ready"
    assert context["readiness"]["storage_status"] == "migration_required"
    with sqlite3.connect(legacy_db) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert tables == {"legacy_marker"}


def test_readiness_distinguishes_stale_flow_missing_state_and_human_exception(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    _governed_upsert(
        repository,
        "segments",
        {
            "record_id": "stale-segment",
            "instrument_id": "601088.SH",
            "report_period": "2024-12-31",
            "segment_id": "coal",
            "segment_name_raw": "coal",
            "segment_type": "product",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2025-03-28",
            "confidence": 1.0,
            "review_status": "approved",
            "valid_from": "2024-01-01",
            "valid_to": "2024-12-31",
        },
    )
    _governed_upsert(
        repository,
        "value_chain_roles",
        {
            "record_id": "held-role",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "role": "processor",
            "mapping_basis": "atomic_activity_rule",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 1.0,
            "review_status": "candidate",
            "valid_from": "2026-03-28",
        },
    )
    role = next(
        item
        for item in repository.list_records("value_chain_roles")
        if item["record_id"] == "held-role"
    )
    BusinessProfileReviewService(repository).review_record(
        "value_chain_roles",
        "held-role",
        decision="held",
        reviewer="analyst@example",
        reason="economic scope is ambiguous",
        expected_review_status="candidate",
        expected_updated_at=role["updated_at"],
    )

    readiness = BusinessProfileResolver(repository).resolve(
        "601088.SH",
        as_of_date="2026-12-31",
    )["readiness"]["temporal_coverage"]

    assert readiness["segments"]["status"] == "stale_flow_coverage"
    assert readiness["value_chain_roles"]["status"] == "missing_current_state"
    assert readiness["value_chain_roles"]["unresolved_exception_count"] == 1


def test_reverse_merger_regime_does_not_rewrite_pre_disclosure_profile(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    old_evidence = _approved_evidence("000001.SZ")
    old_evidence.update(
        {
            "evidence_id": "evidence-old-business",
            "source_document_id": "old-annual-report",
            "report_period": "2024-12-31",
            "publish_date": "2025-03-20",
            "data_available_date": "2025-03-20",
        }
    )
    new_evidence = _approved_evidence("000001.SZ")
    new_evidence.update(
        {
            "evidence_id": "evidence-reverse-merger",
            "source_document_id": "reverse-merger-closing",
            "document_type": "profile_change_event",
            "title": "major asset restructuring closing announcement",
            "report_period": "2025-06-30",
            "publish_date": "2025-07-05",
            "data_available_date": "2025-07-05",
            "document_hash": "new-doc-hash",
            "evidence_text_hash": "new-text-hash",
        }
    )
    _governed_upsert(repository, "evidence", old_evidence)
    _governed_upsert(repository, "evidence", new_evidence)
    _governed_upsert(repository,
        "regimes",
        {
            "regime_id": "regime-old-v1",
            "regime_key": "old-business",
            "instrument_id": "000001.SZ",
            "regime_name": "legacy manufacturing business",
            "regime_type": "operating_business",
            "valid_from": "2020-01-01",
            "knowledge_from": "2025-03-20",
            "knowledge_to": "2025-07-05",
            "evidence_id": "evidence-old-business",
            "data_available_date": "2025-03-20",
            "confidence": 1.0,
            "review_status": "approved",
        },
    )
    _governed_upsert(repository,
        "regimes",
        {
            "regime_id": "regime-new-v1",
            "regime_key": "new-mining-business",
            "instrument_id": "000001.SZ",
            "regime_name": "mining business after reverse merger",
            "regime_type": "reverse_merger_result",
            "valid_from": "2025-06-30",
            "knowledge_from": "2025-07-05",
            "trigger_event_id": "event-reverse-merger",
            "evidence_id": "evidence-reverse-merger",
            "data_available_date": "2025-07-05",
            "confidence": 1.0,
            "review_status": "approved",
        },
    )
    _governed_upsert(repository,
        "events",
        {
            "event_id": "event-reverse-merger",
            "instrument_id": "000001.SZ",
            "event_type": "reverse_merger",
            "event_date": "2025-06-30",
            "event_date_quality": "actual_closing_date",
            "prior_regime_id": "regime-old-v1",
            "resulting_regime_id": "regime-new-v1",
            "materiality": "high",
            "description": "principal operating business replaced",
            "evidence_id": "evidence-reverse-merger",
            "data_available_date": "2025-07-05",
            "confidence": 1.0,
            "review_status": "approved",
        },
    )
    for record_id, regime_id, name, available, knowledge_to in (
        (
            "segment-old",
            "regime-old-v1",
            "legacy manufacturing",
            "2025-03-20",
            "2025-07-05",
        ),
        (
            "segment-new",
            "regime-new-v1",
            "copper mining",
            "2025-07-05",
            None,
        ),
    ):
        _governed_upsert(
            repository,
            "segments",
            {
                "record_id": record_id,
                "instrument_id": "000001.SZ",
                "report_period": "2025-06-30",
                "segment_id": record_id,
                "segment_name_raw": name,
                "segment_type": "product",
                "evidence_id": (
                    "evidence-old-business"
                    if regime_id == "regime-old-v1"
                    else "evidence-reverse-merger"
                ),
                "data_available_date": available,
                "knowledge_from": available,
                "knowledge_to": knowledge_to,
                "business_regime_id": regime_id,
                "confidence": 1.0,
                "review_status": "approved",
            },
        )

    pre_disclosure = BusinessProfileResolver(repository).resolve(
        "000001.SZ",
        as_of_date="2025-07-03",
    )
    post_disclosure = BusinessProfileResolver(repository).resolve(
        "000001.SZ",
        as_of_date="2025-07-06",
    )

    assert pre_disclosure["profile_lifecycle"]["active_regime"]["regime_id"] == (
        "regime-old-v1"
    )
    assert [item["record_id"] for item in pre_disclosure["segment_profiles"]] == [
        "segment-old"
    ]
    assert post_disclosure["profile_lifecycle"]["active_regime"]["regime_id"] == (
        "regime-new-v1"
    )
    assert [item["record_id"] for item in post_disclosure["segment_profiles"]] == [
        "segment-new"
    ]


def test_candidate_material_profile_event_does_not_replace_active_regime(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    _governed_upsert(repository,
        "regimes",
        {
            "regime_id": "regime-current",
            "instrument_id": "601088.SH",
            "regime_name": "current coal business",
            "regime_type": "operating_business",
            "valid_from": "2020-01-01",
            "knowledge_from": "2026-03-28",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 1.0,
            "review_status": "approved",
        },
    )
    _governed_upsert(repository,
        "events",
        {
            "event_id": "event-pending-disposal",
            "instrument_id": "601088.SH",
            "event_type": "business_disposal",
            "event_date": "2026-04-15",
            "materiality": "high",
            "description": "proposed disposal of principal business",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-04-15",
            "confidence": 0.8,
            "review_status": "candidate",
        },
    )

    context = BusinessProfileResolver(repository).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
    )

    assert context["profile_lifecycle"]["active_regime"]["regime_id"] == (
        "regime-current"
    )
    assert context["profile_lifecycle"]["candidate_events"][0]["event_id"] == (
        "event-pending-disposal"
    )
    assert (
        "material_profile_change_pending_review:event-pending-disposal"
        in context["warnings"]
    )


def test_material_business_expansion_keeps_existing_segments_in_same_regime(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    _governed_upsert(repository,
        "regimes",
        {
            "regime_id": "regime-integrated",
            "instrument_id": "601088.SH",
            "regime_name": "integrated energy business",
            "regime_type": "operating_business",
            "valid_from": "2020-01-01",
            "knowledge_from": "2026-03-28",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 1.0,
            "review_status": "approved",
        },
    )
    for record_id, segment_name in (
        ("segment-coal", "coal production"),
        ("segment-power", "power generation expansion"),
    ):
        _governed_upsert(repository,
            "segments",
            {
                "record_id": record_id,
                "instrument_id": "601088.SH",
                "report_period": "2025-12-31",
                "segment_id": record_id,
                "segment_name_raw": segment_name,
                "segment_type": "product",
                "business_regime_id": "regime-integrated",
                "evidence_id": "evidence-2025-ar",
                "data_available_date": "2026-03-28",
                "confidence": 1.0,
                "review_status": "approved",
            },
        )

    context = BusinessProfileResolver(repository).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
    )

    assert context["profile_lifecycle"]["active_regime"]["regime_id"] == (
        "regime-integrated"
    )
    assert {item["record_id"] for item in context["segment_profiles"]} == {
        "segment-coal",
        "segment-power",
    }


def test_overlapping_approved_regimes_fail_closed(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    for regime_id, valid_from in (
        ("regime-one", "2020-01-01"),
        ("regime-two", "2025-01-01"),
    ):
        _governed_upsert(repository,
            "regimes",
            {
                "regime_id": regime_id,
                "instrument_id": "601088.SH",
                "regime_name": regime_id,
                "regime_type": "operating_business",
                "valid_from": valid_from,
                "knowledge_from": "2026-03-28",
                "evidence_id": "evidence-2025-ar",
                "data_available_date": "2026-03-28",
                "confidence": 1.0,
                "review_status": "approved",
            },
        )
        _governed_upsert(repository,
            "segments",
            {
                "record_id": f"segment-{regime_id}",
                "instrument_id": "601088.SH",
                "report_period": "2025-12-31",
                "segment_id": regime_id,
                "segment_name_raw": regime_id,
                "segment_type": "product",
                "business_regime_id": regime_id,
                "evidence_id": "evidence-2025-ar",
                "data_available_date": "2026-03-28",
                "confidence": 1.0,
                "review_status": "approved",
            },
        )

    context = BusinessProfileResolver(repository).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
    )

    assert context["profile_lifecycle"]["active_regime"] is None
    assert context["company_specific_profile"]["segments"] == []
    assert "overlapping_active_business_regimes" in context["warnings"]
    assert "overlapping_active_business_regimes" in context["readiness"]["input_gaps"]


def test_resolver_applies_review_date_evidence_and_company_precedence(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    _governed_upsert(repository,
        "segments",
        {
            "record_id": "segment-coal-2025",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "segment_id": "coal",
            "segment_name_raw": "煤炭业务",
            "segment_name_normalized": "煤炭",
            "segment_type": "product",
            "revenue_share": 0.8,
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 0.98,
            "review_status": "approved",
            "valid_from": "2026-03-28",
        },
    )
    _governed_upsert(repository,
        "exposures",
        {
            "exposure_id": "exposure-coal-approved",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "scope_type": "company",
            "scope_id": "601088.SH",
            "commodity_id": "焦煤",
            "exposure_role": "revenue",
            "direction": "positive",
            "materiality": "high",
            "mapping_basis": "company_disclosure",
            "price_series_id": "CNF.JM.DCE.main",
            "lag_days": 30,
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 0.95,
            "review_status": "approved",
            "effective_from": "2026-03-28",
        },
    )
    _governed_upsert(repository,
        "exposures",
        {
            "exposure_id": "exposure-future-candidate",
            "instrument_id": "601088.SH",
            "report_period": "2026-06-30",
            "scope_type": "company",
            "scope_id": "601088.SH",
            "commodity_id": "铜",
            "exposure_role": "revenue",
            "direction": "positive",
            "mapping_basis": "candidate_extraction",
            "price_series_id": "CNF.CU.SHFE.main",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-08-20",
            "confidence": 0.7,
            "review_status": "candidate",
            "effective_from": "2026-08-20",
        },
    )

    context = BusinessProfileResolver(repository, _FuturesStorage()).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
        industry_membership={
            "mapping_status": "authoritative",
            "sw_l1_name": "煤炭",
        },
    )

    assert context["status"] == "ready"
    assert context["market_link_status"] == "direct_linked"
    assert [item["exposure_id"] for item in context["approved_exposures"]] == [
        "exposure-coal-approved"
    ]
    assert (
        context["candidate_exposures"][0]["exposure_id"] == "exposure-future-candidate"
    )
    assert context["executable_exposure_mappings"][0]["source"] == (
        "approved_company_business_profile"
    )
    assert len(context["executable_exposure_mappings"]) == 1
    assert context["readiness"]["approved_company_exposure_count"] == 1
    assert context["model_scores"]["score_version"] == "business_profile_model_score.v1"


def test_approved_special_commodity_series_is_executable(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    _governed_upsert(repository,
        "exposures",
        {
            "exposure_id": "exposure-soda-ash-cost",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "scope_type": "company",
            "scope_id": "601088.SH",
            "commodity_id": "chemical.soda_ash",
            "exposure_role": "feedstock_cost",
            "direction": "negative",
            "materiality": "high",
            "mapping_basis": "official_cost_composition_review",
            "price_series_id": "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 0.95,
            "review_status": "approved",
            "effective_from": "2026-03-28",
        },
    )

    context = BusinessProfileResolver(
        repository,
        futures_storage=_FuturesStorage(),
        special_commodity_storage=_SpecialCommodityStorage(),
    ).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
    )

    mapping = context["executable_exposure_mappings"][0]
    assert mapping["market_data_family"] == "special_commodity"
    assert mapping["revenue_series_id"] is None
    assert mapping["cost_series_ids"] == [
        "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY"
    ]
    assert context["readiness"]["executable_mapping_count"] == 1
    assert context["market_link_status"] == "direct_linked"


def test_approved_profile_without_market_link_is_ready_but_unlinked(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    _governed_upsert(
        repository,
        "value_chain_roles",
        {
            "record_id": "role-processor",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "role": "processor",
            "materiality": "high",
            "mapping_basis": "company_disclosure",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 0.95,
            "review_status": "approved",
            "valid_from": "2026-03-28",
        },
    )
    _governed_upsert(
        repository,
        "exposures",
        {
            "exposure_id": "exposure-unlinked-product",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "scope_type": "company",
            "scope_id": "601088.SH",
            "commodity_id": "company_specific_grade",
            "exposure_role": "revenue",
            "direction": "positive",
            "mapping_basis": "company_disclosure",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 0.95,
            "review_status": "approved",
            "effective_from": "2026-03-28",
        },
    )

    context = BusinessProfileResolver(repository).resolve(
        "601088.SH", as_of_date="2026-04-30"
    )

    assert context["status"] == "ready"
    assert context["market_link_status"] == "unlinked"
    assert context["readiness"]["status"] == "ready"
    assert context["readiness"]["market_link"] == {
        "status": "unlinked",
        "approved_exposure_count": 1,
        "executable_mapping_count": 0,
        "unresolved_exposure_ids": ["exposure-unlinked-product"],
    }
    assert context["approved_exposures"][0]["commodity_id"] == "company_specific_grade"


def test_approved_fact_with_unapproved_evidence_is_not_valuation_eligible(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    _governed_upsert(repository, "evidence", evidence)
    _governed_upsert(
        repository,
        "value_chain_roles",
        {
            "record_id": "role-upstream",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "role": "upstream_producer",
            "materiality": "high",
            "mapping_basis": "company_disclosure",
            "evidence_id": "evidence-2025-ar",
            "data_available_date": "2026-03-28",
            "confidence": 0.95,
            "review_status": "candidate",
            "valid_from": "2026-03-28",
        },
    )
    with sqlite3.connect(research_db) as conn:
        conn.execute(
            """
            UPDATE company_value_chain_roles
            SET review_status = 'approved'
            WHERE record_id = 'role-upstream'
            """
        )
        conn.commit()

    context = BusinessProfileResolver(repository).resolve(
        "601088.SH",
        as_of_date="2026-04-30",
    )

    assert context["company_specific_profile"]["value_chain_roles"] == []
    assert "invalid_evidence:value_chain_roles:role-upstream" in context["warnings"]
    assert (
        context["candidate_facts"]["value_chain_roles"][0]["eligibility"][
            "evidence_valid"
        ]
        is False
    )


def test_review_queue_returns_only_candidates(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    _governed_upsert(repository, "evidence", _approved_evidence())
    candidate = _approved_evidence("600019.SH")
    candidate["evidence_id"] = "evidence-candidate"
    candidate["source_document_id"] = "candidate-doc"
    candidate["review_status"] = "candidate"
    _governed_upsert(repository, "evidence", candidate)

    queue = repository.get_review_queue(record_type="evidence")

    assert len(queue) == 1
    assert queue[0]["evidence_id"] == "evidence-candidate"
    assert queue[0]["record_type"] == "evidence"


def test_review_approval_is_optimistic_and_audit_is_immutable(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    review_service = BusinessProfileReviewService(repository)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    _governed_upsert(repository, "evidence", evidence)
    candidate = repository.list_records("evidence")[0]

    audit = review_service.review_record(
        "evidence",
        evidence["evidence_id"],
        decision="approved",
        reviewer="analyst@example",
        reason="matched official annual report page",
        expected_review_status="candidate",
        expected_updated_at=candidate["updated_at"],
        evidence_references=["annual-report:page-31"],
    )

    reviewed = repository.list_records("evidence")[0]
    assert reviewed["review_status"] == "approved"
    assert reviewed["reviewed_by"] == "analyst@example"
    assert audit["prior_status"] == "candidate"
    assert audit["new_status"] == "approved"
    assert audit["evidence_references"] == ["annual-report:page-31"]
    assert review_service.list_review_audit(record_id=evidence["evidence_id"]) == [
        audit
    ]

    _governed_upsert(repository, "evidence", evidence)
    after_ingestion_rerun = repository.list_records("evidence")[0]
    assert after_ingestion_rerun["review_status"] == "approved"
    assert after_ingestion_rerun["updated_at"] == reviewed["updated_at"]

    with pytest.raises(ValueError, match="stale business profile review state"):
        review_service.review_record(
            "evidence",
            evidence["evidence_id"],
            decision="rejected",
            reviewer="analyst@example",
            reason="stale second decision",
            expected_review_status="candidate",
            expected_updated_at=candidate["updated_at"],
        )

    with sqlite3.connect(research_db) as conn:
        with pytest.raises(
            sqlite3.IntegrityError,
            match="business_profile_review_audit is immutable",
        ):
            conn.execute(
                """
                UPDATE business_profile_review_audit
                SET reason = 'changed'
                WHERE audit_id = ?
                """,
                (audit["audit_id"],),
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="business_profile_review_audit is immutable",
        ):
            conn.execute(
                "DELETE FROM business_profile_review_audit WHERE audit_id = ?",
                (audit["audit_id"],),
            )


def test_non_official_evidence_approval_requires_review_reference(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    review_service = BusinessProfileReviewService(repository)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    evidence["source_tier"] = "structured_aggregator"
    _governed_upsert(repository, "evidence", evidence)
    candidate = repository.list_records("evidence")[0]

    with pytest.raises(
        ValueError,
        match="non-official evidence approval requires evidence_references",
    ):
        review_service.review_record(
            "evidence",
            evidence["evidence_id"],
            decision="approved",
            reviewer="analyst@example",
            reason="reviewed aggregator evidence",
            expected_review_status="candidate",
            expected_updated_at=candidate["updated_at"],
        )

    audit = review_service.review_record(
        "evidence",
        evidence["evidence_id"],
        decision="approved",
        reviewer="analyst@example",
        reason="cross-checked against filing",
        expected_review_status="candidate",
        expected_updated_at=candidate["updated_at"],
        evidence_references=["official-filing:sha256:page-31"],
    )
    assert audit["new_status"] == "approved"


def test_candidate_upsert_cannot_overwrite_concurrent_terminal_review(tmp_path):
    storage, research_db = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    _governed_upsert(repository, "evidence", evidence)

    class _RaceConnection:
        def __init__(self, conn):
            self._conn = conn
            self._triggered = False

        def execute(self, sql, params=()):
            if not self._triggered and sql == "BEGIN IMMEDIATE":
                self._triggered = True
                with sqlite3.connect(research_db) as review_conn:
                    review_conn.execute(
                        """
                        UPDATE business_profile_evidence
                        SET review_status = 'approved',
                            reviewed_by = 'concurrent-reviewer',
                            reviewed_at = '2026-07-18T12:00:00+08:00',
                            updated_at = '2026-07-18T12:00:00+08:00'
                        WHERE evidence_id = ?
                        """,
                        (evidence["evidence_id"],),
                    )
                    review_conn.commit()
            return self._conn.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    class _RaceStorage:
        _apply_pragmas = staticmethod(storage._apply_pragmas)

        @contextmanager
        def get_connection(self):
            with storage.get_connection() as conn:
                yield _RaceConnection(conn)

    BusinessProfileRepository(_RaceStorage()).upsert("evidence", evidence)

    reviewed = repository.list_records("evidence")[0]
    assert reviewed["review_status"] == "approved"
    assert reviewed["reviewed_by"] == "concurrent-reviewer"


def test_fact_approval_requires_same_instrument_approved_evidence(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    review_service = BusinessProfileReviewService(repository)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    _governed_upsert(repository, "evidence", evidence)
    _governed_upsert(repository,
        "segments",
        {
            "record_id": "segment-candidate",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "segment_id": "coal",
            "segment_name_raw": "煤炭",
            "segment_type": "product",
            "evidence_id": evidence["evidence_id"],
            "data_available_date": "2026-03-28",
            "confidence": 0.95,
            "review_status": "candidate",
        },
    )
    segment = repository.list_records("segments")[0]

    with pytest.raises(ValueError, match="approval evidence is not approved"):
        review_service.review_record(
            "segments",
            segment["record_id"],
            decision="approved",
            reviewer="analyst@example",
            reason="official segment table matched",
            expected_review_status="candidate",
            expected_updated_at=segment["updated_at"],
        )

    candidate_evidence = repository.list_records("evidence")[0]
    review_service.review_record(
        "evidence",
        evidence["evidence_id"],
        decision="approved",
        reviewer="analyst@example",
        reason="official filing verified",
        expected_review_status="candidate",
        expected_updated_at=candidate_evidence["updated_at"],
    )
    audit = review_service.review_record(
        "segments",
        segment["record_id"],
        decision="approved",
        reviewer="analyst@example",
        reason="official segment table matched",
        expected_review_status="candidate",
        expected_updated_at=segment["updated_at"],
    )

    assert audit["new_status"] == "approved"
    assert repository.list_records("segments")[0]["review_status"] == "approved"


def test_supersede_requires_an_approved_same_instrument_replacement(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    review_service = BusinessProfileReviewService(repository)
    _governed_upsert(repository, "evidence", _approved_evidence())
    for record_id, supersedes_record_id in (
        ("segment-old", None),
        ("segment-new", "segment-old"),
    ):
        _governed_upsert(
            repository,
            "segments",
            {
                "record_id": record_id,
                "instrument_id": "601088.SH",
                "report_period": "2025-12-31",
                "segment_id": "stable-segment",
                "segment_name_raw": record_id,
                "segment_type": "product",
                "evidence_id": "evidence-2025-ar",
                "data_available_date": "2026-03-28",
                "confidence": 1.0,
                "review_status": "approved",
                "supersedes_record_id": supersedes_record_id,
                "version": 2 if supersedes_record_id else 1,
            },
        )
    old = next(
        item
        for item in repository.list_records("segments")
        if item["record_id"] == "segment-old"
    )

    audit = review_service.review_record(
        "segments",
        "segment-old",
        decision="superseded",
        reviewer="analyst@example",
        reason="replaced by corrected official segment row",
        expected_review_status="approved",
        expected_updated_at=old["updated_at"],
        replacement_record_id="segment-new",
    )

    records = {item["record_id"]: item for item in repository.list_records("segments")}
    assert records["segment-old"]["review_status"] == "superseded"
    assert records["segment-new"]["review_status"] == "approved"
    assert audit["replacement_record_id"] == "segment-new"
