import sqlite3

from research.business_profile_governance import (
    BusinessProfileRepository,
    BusinessProfileResolver,
)
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


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
        "company_business_segments",
        "company_operating_facts",
        "company_value_chain_roles",
        "company_commodity_exposures",
    }.issubset(tables)


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
    assert context["candidate_exposures"][0]["exposure_id"] == "exposure-future-candidate"
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
    assert context["candidate_facts"]["value_chain_roles"][0]["eligibility"][
        "evidence_valid"
    ] is False


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
