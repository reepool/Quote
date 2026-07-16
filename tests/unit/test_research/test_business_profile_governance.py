import sqlite3

from research.business_profile_governance import (
    BusinessProfileRepository,
    BusinessProfileResolver,
)
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
    repository.upsert("evidence", old_evidence)
    repository.upsert("evidence", new_evidence)
    repository.upsert(
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
    repository.upsert(
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
    repository.upsert(
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
        repository.upsert(
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
    repository.upsert("evidence", _approved_evidence())
    repository.upsert(
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
    repository.upsert(
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
    repository.upsert("evidence", _approved_evidence())
    repository.upsert(
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
        repository.upsert(
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


def test_resolver_applies_review_date_evidence_and_company_precedence(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    repository.upsert("evidence", _approved_evidence())
    repository.upsert(
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
    repository.upsert(
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
    repository.upsert(
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


def test_approved_fact_with_unapproved_evidence_is_not_valuation_eligible(tmp_path):
    storage, _ = _storage(tmp_path)
    repository = BusinessProfileRepository(storage)
    evidence = _approved_evidence()
    evidence["review_status"] = "candidate"
    repository.upsert("evidence", evidence)
    repository.upsert(
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
            "review_status": "approved",
            "valid_from": "2026-03-28",
        },
    )

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
    repository.upsert("evidence", _approved_evidence())
    candidate = _approved_evidence("600019.SH")
    candidate["evidence_id"] = "evidence-candidate"
    candidate["source_document_id"] = "candidate-doc"
    candidate["review_status"] = "candidate"
    repository.upsert("evidence", candidate)

    queue = repository.get_review_queue(record_type="evidence")

    assert len(queue) == 1
    assert queue[0]["evidence_id"] == "evidence-candidate"
    assert queue[0]["record_type"] == "evidence"
