import hashlib
from pathlib import Path

from research.business_profile_disclosure_planner import (
    BusinessProfileCoverageInspector,
    BusinessProfileDisclosurePlanner,
)
from research.business_profile_discovery import BusinessProfileDocumentCandidate
from research.business_profile_documents import classify_business_profile_document


class _Repository:
    def __init__(self, *, approved=None, candidates=None):
        self.approved = approved or {}
        self.candidates = candidates or {}

    def get_approved_as_of(self, record_type, **_kwargs):
        return list(self.approved.get(record_type, ()))

    def list_records(self, record_type, **_kwargs):
        return list(self.candidates.get(record_type, ()))


def _candidate(announcement_id, title, published_at):
    return BusinessProfileDocumentCandidate(
        announcement_id=announcement_id,
        title=title,
        announcement_time=published_at,
        symbols=["600000"],
        adjunct_url=f"/finalpage/{announcement_id}.PDF",
        adjunct_type="PDF",
        classification=classify_business_profile_document(title, adjunct_type="PDF"),
        source="cninfo",
        source_tier="official_primary",
    )


def _manifest(tmp_path, source_file_id, announcement_id, title, period, published_at, *, supersedes=None):
    content = f"%PDF-1.4 {announcement_id}".encode()
    path = tmp_path / f"{announcement_id}.pdf"
    path.write_bytes(content)
    classification = classify_business_profile_document(title, adjunct_type="PDF")
    return {
        "source_file_id": source_file_id,
        "instrument_id": "600000.SH",
        "source": "cninfo",
        "source_tier": "official_primary",
        "report_period": period,
        "report_type": classification.document_type,
        "filing_id": announcement_id,
        "archive_path": str(path),
        "content_hash": hashlib.sha256(content).hexdigest(),
        "published_at": published_at,
        "supersedes_source_file_id": supersedes,
        "metadata": {
            "announcement_title": title,
            "document_family": classification.document_type.removesuffix("_correction"),
        },
    }


def _planner(tmp_path, repository=None, **kwargs):
    inspector = BusinessProfileCoverageInspector(repository or _Repository())
    return BusinessProfileDisclosurePlanner(
        coverage_inspector=inspector,
        artifact_root=tmp_path / "plans",
        **kwargs,
    )


def test_annual_only_plan_is_hash_verified_and_deterministic(tmp_path):
    annual = _manifest(
        tmp_path, "sf-annual", "annual", "浦发银行2025年年度报告", "2025-12-31", "2026-03-30"
    )
    older = _manifest(
        tmp_path, "sf-old", "old", "浦发银行2024年年度报告", "2024-12-31", "2025-03-30"
    )
    planner = _planner(tmp_path)

    first = planner.plan(
        instrument_id="600000.SH",
        field_family="atomic_activities",
        knowledge_cutoff="2026-07-01",
        manifests=[older, annual],
    )
    second = planner.plan(
        instrument_id="600000.SH",
        field_family="atomic_activities",
        knowledge_cutoff="2026-07-01",
        manifests=[annual, older],
    )

    assert [item["announcement_id"] for item in first.included] == ["annual"]
    assert first.included[0]["local_status"] == "verified"
    assert first.plan_hash == second.plan_hash
    assert first.complete is True
    assert any(item["decision_reason"] == "supplement_not_required" for item in first.omitted)
    assert (tmp_path / "plans" / "600000.SH" / "2026-07-01" / f"{first.plan_hash}.json").is_file()


def test_newer_semiannual_is_added_only_for_unresolved_time_sensitive_family(tmp_path):
    annual = _manifest(
        tmp_path, "sf-a", "annual", "某公司2024年年度报告", "2024-12-31", "2025-03-30"
    )
    semi = _manifest(
        tmp_path, "sf-h", "semi", "某公司2025年半年度报告", "2025-06-30", "2025-08-30"
    )
    plan = _planner(tmp_path, selection_policy="expanded").plan(
        instrument_id="600000.SH",
        field_family="tabular_operating_facts",
        knowledge_cutoff="2025-12-01",
        manifests=[annual, semi],
    )

    assert [item["announcement_id"] for item in plan.included] == ["annual", "semi"]


def test_correction_replaces_original_and_preserves_lineage(tmp_path):
    original = _manifest(
        tmp_path, "sf-original", "original", "某公司2025年年度报告", "2025-12-31", "2026-03-20"
    )
    correction = _manifest(
        tmp_path, "sf-correction", "correction", "某公司2025年年度报告（修订版）", "2025-12-31", "2026-04-01", supersedes="sf-original"
    )
    plan = _planner(tmp_path).plan(
        instrument_id="600000.SH",
        field_family="structured_segments",
        knowledge_cutoff="2026-05-01",
        manifests=[original, correction],
    )

    assert [item["announcement_id"] for item in plan.included] == ["correction"]
    assert plan.included[0]["supersedes_source_file_id"] == "sf-original"
    assert next(item for item in plan.omitted if item["announcement_id"] == "original")["decision_reason"] == "older_or_superseded_periodic_report"


def test_specialist_rules_are_field_family_specific_and_bounded(tmp_path):
    annual = _manifest(
        tmp_path, "sf-a", "annual", "某公司2025年年度报告", "2025-12-31", "2026-03-20"
    )
    contract = _manifest(
        tmp_path, "sf-c", "contract", "关于签署重大销售合同的公告", "2026-04-01", "2026-04-01"
    )
    capacity = _manifest(
        tmp_path, "sf-p", "capacity", "关于项目建成投产的公告", "2026-04-02", "2026-04-02"
    )
    plan = _planner(
        tmp_path,
        max_documents=2,
        max_specialist_documents=1,
        selection_policy="expanded",
    ).plan(
        instrument_id="600000.SH",
        field_family="named_relationships",
        knowledge_cutoff="2026-05-01",
        manifests=[annual, capacity, contract],
    )

    assert [item["announcement_id"] for item in plan.included] == ["annual", "contract"]
    assert next(item for item in plan.omitted if item["announcement_id"] == "capacity")["decision_reason"] == "specialist_not_required_or_out_of_bound"


def test_missing_local_report_is_acquired_only_when_planned(tmp_path):
    annual = _candidate("annual", "某公司2025年年度报告", "2026-03-30")
    unrelated = _candidate("unrelated", "关于召开股东大会的通知", "2026-04-01")
    planner = _planner(tmp_path)
    plan = planner.plan(
        instrument_id="600000.SH",
        field_family="atomic_activities",
        knowledge_cutoff="2026-05-01",
        discovered=[annual, unrelated],
    )
    calls = []

    class _Archive:
        def archive_candidates(self, instrument, selected, **kwargs):
            calls.append((instrument, selected, kwargs))
            return "archived"

    result = planner.acquire_missing(
        plan,
        instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
        candidates=[annual, unrelated],
        archive_service=_Archive(),
    )

    assert result == "archived"
    assert [item.announcement_id for item in calls[0][1]] == ["annual"]


def test_future_disclosures_are_excluded_and_approved_coverage_short_circuits(tmp_path):
    repository = _Repository(
        approved={
            "activities": [
                {"report_period": "2025-12-31", "data_available_date": "2026-03-30"}
            ]
        }
    )
    future = _candidate("future", "某公司2026年年度报告", "2027-03-30")
    plan = _planner(tmp_path, repository=repository).plan(
        instrument_id="600000.SH",
        field_family="atomic_activities",
        knowledge_cutoff="2026-06-01",
        discovered=[future],
    )

    assert plan.included == ()
    assert plan.coverage.complete is True
    assert plan.omitted[0]["decision_reason"] == "future_knowledge_excluded"


def test_coverage_reports_candidates_and_open_exceptions_without_treating_them_as_approved():
    repository = _Repository(
        candidates={"relationships": [{"relationship_id": "candidate-1"}]}
    )
    coverage = BusinessProfileCoverageInspector(repository).inspect(
        instrument_id="600000.SH",
        field_family="named_relationships",
        knowledge_cutoff="2026-06-01",
        exceptions=[
            {
                "instrument_id": "600000.SH",
                "field_family": "named_relationships",
                "status": "open",
                "tier": "quick_review",
            }
        ],
    )

    assert coverage.approved_count == 0
    assert coverage.candidate_count == 1
    assert coverage.exception_count == 1
    assert coverage.complete is False
    assert coverage.gaps == (
        "missing_approved_coverage",
        "unresolved_exception",
    )


def test_document_bound_exhaustion_is_fail_closed(tmp_path):
    annual = _manifest(
        tmp_path, "sf-a", "annual", "某公司2025年年度报告", "2025-12-31", "2026-03-20"
    )
    semi = _manifest(
        tmp_path, "sf-h", "semi", "某公司2026年半年度报告", "2026-06-30", "2026-08-20"
    )
    specialist = _manifest(
        tmp_path, "sf-r", "resource", "关于矿产资源储量更新的公告", "2026-09-01", "2026-09-01"
    )
    plan = _planner(
        tmp_path,
        max_documents=2,
        max_specialist_documents=1,
        selection_policy="expanded",
    ).plan(
        instrument_id="600000.SH",
        field_family="commodity_exposure_facts",
        knowledge_cutoff="2026-10-01",
        manifests=[annual, semi, specialist],
    )

    assert len(plan.included) == 2
    assert plan.complete is False
    assert "document_bound_exhausted" in plan.completeness_gaps
    assert any(item["decision_reason"] == "document_bound_exhausted" for item in plan.omitted)


def test_default_policy_omits_newer_semiannual_and_specialist_documents(tmp_path):
    annual = _manifest(
        tmp_path,
        "sf-a",
        "annual",
        "某公司2025年年度报告",
        "2025-12-31",
        "2026-03-20",
    )
    semi = _manifest(
        tmp_path,
        "sf-h",
        "semi",
        "某公司2026年半年度报告",
        "2026-06-30",
        "2026-08-20",
    )
    contract = _manifest(
        tmp_path,
        "sf-c",
        "contract",
        "关于签署重大销售合同的公告",
        "2026-09-01",
        "2026-09-01",
    )

    plan = _planner(tmp_path).plan(
        instrument_id="600000.SH",
        field_family="named_relationships",
        knowledge_cutoff="2026-10-01",
        manifests=[annual, semi, contract],
    )

    assert [item["announcement_id"] for item in plan.included] == ["annual"]
    omitted = {item["announcement_id"]: item["decision_reason"] for item in plan.omitted}
    assert omitted["semi"] == "supplement_not_required"
    assert omitted["contract"] == "specialist_not_required_or_out_of_bound"
