"""Source review repairs only flagged facts; uncertainty does not become approval."""
import asyncio
import time
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperDraftResponse, bind_workpaper_draft, render_workpaper,
)
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import REVIEW_POLICY, review_workpaper, workpaper_hash
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _report, _selection, _draft


def material():
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), _selection(index_sections(PAGES)))
    raw = _draft(packet).model_dump(mode="json")
    bad = raw["elements"]["scale_and_mix"]["facts"][0]
    bad["text"] = "净利润100.00元。"
    bad["metrics"][0]["name"] = "净利润"
    paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(),
                               reading_scope=packet.reading_scope, evidence=packet.evidence,
                               origin="controlled_fixture")
    fixed = _draft(packet).elements.scale_and_mix.facts[0].model_dump(mode="json")
    issue = {"finding_id": "wrong-profit-column", "element": "scale_and_mix", "severity": "material",
             "issue_type": "wrong_column", "action": "correct", "affected_fact_id": "revenue",
             "evidence_ids": list(fixed["evidence_ids"]), "explanation": "100.00是营业收入，不是净利润。"}
    review = {"reviewed_fact_ids": ["main", "revenue"], "findings": [issue]}
    verify = {"checked_fact_ids": ["revenue"], "checks": [{"fact_id": "revenue", "accepted": True,
               "evidence_ids": list(fixed["evidence_ids"]), "explanation": "原表本期列为营业收入100.00元。"}]}
    return paper, packet, fixed, review, verify


class Provider:
    offline_only = True

    def __init__(self, replies):
        self.replies = list(replies)
        self.workpaper_receipts = []
        self.calls = []

    def workpaper_request(self, **kwargs):
        self.calls.append(kwargs)
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        self.workpaper_receipts.append({
            "request_id": kwargs["request_id"], "purpose": kwargs["purpose"],
            "contract_status": "accepted", "source_label": "fixture", "model": "reviewer",
            "reasoning_effort": "high", "usage": {"total_tokens": 20},
        })
        return reply


def invoke(replies):
    paper, packet, *_ = material()
    provider = Provider(replies)
    result, receipts = review_workpaper(paper, packet=packet, provider=provider, work_id="review-test",
                                       identity=workpaper_processing_identity(real=False, review=True))
    return paper, result, provider, receipts


def test_source_column_repair_preserves_unrelated_business_and_original():
    original, _, fixed, review, verify = material()
    before, result, provider, receipts = invoke([review, {"repaired_facts": [fixed]}, verify])
    assert result.quality_review.status == "completed"
    assert result.elements.business_structure == original.elements.business_structure
    assert result.elements.scale_and_mix.facts[0].metrics[0].name == "营业收入"
    finding = result.quality_review.findings[0]
    assert finding.original_fact.metrics[0].name == "净利润"
    assert finding.resolution == "corrected_pending_trust"
    assert result.quality_review.original_draft_hash == workpaper_hash(before)
    assert [c["purpose"] for c in provider.calls] == [
        "review_profile_workpaper", "repair_profile_workpaper", "verify_profile_workpaper_repair"]
    assert len(receipts) == 3
    assert "requested_reasoning=high" in render_workpaper(result)
    result.validate_pages(PAGES)


def test_real_review_identity_requires_explicit_profile():
    with pytest.raises(ValueError, match="explicitly selected"):
        workpaper_processing_identity(real=True, review=True)


def test_fresh_extraction_can_review_without_existing_seed():
    from tests.unit.test_research.test_company_profile_workpaper import _Provider

    class FreshProvider(_Provider):
        def workpaper_request(self, **kwargs):
            if kwargs["purpose"] in {"select_profile_sections", "extract_profile_workpaper"}:
                return super().workpaper_request(**kwargs)
            self.calls.append(kwargs)
            return {"reviewed_fact_ids": ["main", "revenue"], "findings": []}

    with TemporaryDirectory(prefix="v9-review-fresh-") as directory:
        provider = FreshProvider()
        runtime = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(Path(directory)), provider=provider)
        item = {"work_id": "fresh-review", "report": _report(), "pages": PAGES,
                "processing_identity": workpaper_processing_identity(real=False, review=True)}
        for stage in ("acquire", "parse", "semantic", "verify", "publish"):
            assert runtime.run_stage(stage, item)["status"] == "success"
        profile = CompanyProfileReadService(Path(directory)).query(("600000.SH",))["profiles"][0]
        assert profile["workpaper"]["quality_review"]["status"] == "completed"
        assert len(profile["workpaper"]["elements"]["scale_and_mix"]["facts"]) == 1
        assert [call["purpose"] for call in provider.calls] == [
            "select_profile_sections", "extract_profile_workpaper", "review_profile_workpaper"]


@pytest.mark.parametrize("failure", ["rejected", "bad_number", "unrelated", "missing_check", "provider_failure"])
def test_invalid_or_unconfirmed_repair_never_revives_bad_fact(failure):
    _, _, fixed, review, verify = material()
    if failure == "rejected":
        verify["checks"][0]["accepted"] = False
    elif failure == "bad_number":
        fixed["metrics"][0].update(raw_value="777.00", value="777.00")
    elif failure == "unrelated":
        fixed["fact_id"] = "main"
    elif failure == "missing_check":
        verify["checks"] = []
    elif failure == "provider_failure":
        verify = ValueError("review transport stopped")
    _, result, provider, _ = invoke([review, {"repaired_facts": [fixed]}, verify])
    assert not result.elements.scale_and_mix.facts
    assert result.elements.business_structure.facts[0].fact_id == "main"
    assert result.quality_review.status != "completed"
    assert len(provider.calls) <= 3
    assert result.quality_review.findings[0].original_fact.fact_id == "revenue"
    if failure in {"rejected", "missing_check", "provider_failure"}:
        assert result.quality_review.findings[0].revised_fact is not None


@pytest.mark.parametrize("bad_contract", ["incomplete", "unknown_fact", "unknown_evidence", "wrong_element"])
def test_invalid_initial_review_does_not_remove_independent_facts(bad_contract):
    paper, _, _, review, _ = material()
    if bad_contract == "incomplete":
        review["reviewed_fact_ids"] = ["main"]
    elif bad_contract == "unknown_fact":
        review["findings"][0]["affected_fact_id"] = "not-a-fact"
    elif bad_contract == "unknown_evidence":
        review["findings"][0]["evidence_ids"] = ["not-a-source"]
    else:
        review["findings"][0]["element"] = "business_structure"
    _, result, provider, _ = invoke([review])
    assert result.elements == paper.elements
    assert result.quality_review.status != "completed"
    assert len(provider.calls) == 1


def test_material_omission_can_be_added_but_optional_gap_does_not_trigger_repair():
    _, _, fixed, _, _ = material()
    added = {**fixed, "fact_id": "review-add:missing-prior", "text": "上年同期营业收入90.00元。", "metrics": []}
    finding = {"finding_id": "missing-prior", "element": "scale_and_mix", "severity": "material",
               "issue_type": "material_omission", "action": "possible_omission", "affected_fact_id": None,
               "evidence_ids": list(fixed["evidence_ids"]), "explanation": "应保留表中上年同期列。"}
    initial = {"reviewed_fact_ids": ["main", "revenue"], "findings": [finding]}
    verify = {"checked_fact_ids": [added["fact_id"]], "checks": [{"fact_id": added["fact_id"],
              "accepted": True, "evidence_ids": list(added["evidence_ids"]), "explanation": "上年同期列有90.00元。"}]}
    _, result, _, _ = invoke([initial, {"repaired_facts": [added]}, verify])
    assert added["fact_id"] in {f.fact_id for f in result.elements.scale_and_mix.facts}
    finding["severity"] = "minor"
    _, limited, provider, _ = invoke([initial])
    assert len(provider.calls) == 1
    assert limited.quality_review.status == "completed_with_open_findings"


def test_runtime_seed_review_gateway_writer_and_three_reads():
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from tests.unit.test_utils.test_llm_gateway import _config, _response
    from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
    from utils.llm.testing import ScriptedTransport

    paper, _, fixed, review, verify = material()
    config = _config(max_schema_repair_attempts=0)
    reviewer = replace(config.profiles["test"], name="review", reasoning_effort="high")
    config = replace(config, profiles={**config.profiles, "review": reviewer})
    transport = ScriptedTransport([_response(review), _response({"repaired_facts": [fixed]}), _response(verify)])
    client = _client(transport, _budget(), config)
    provider = CommonGatewaySemanticProvider(
        client=client, profile="test", max_output_tokens=2500, timeout_seconds=10,
        execution_authorization={"status": "authorized", "document_version": _report().document_version,
                                 "approval_reference": "controlled-gateway-authorization"})
    identity = workpaper_processing_identity(real=False, review=True)
    identity.update(quality_review_profile="review", repair_profile="test",
                    review_existing_draft_hash=workpaper_hash(paper))
    try:
        with TemporaryDirectory(prefix="v9-review-test-") as directory:
            root = Path(directory)
            writer = CompanyProfileResearchWriter(root / "output")
            runtime = CompanyProfileStageRuntime(writer=writer, provider=provider)
            item = {"work_id": "review-entry", "report": _report(), "pages": PAGES,
                    "review_seed": paper.model_dump(mode="json"), "processing_identity": identity}
            for stage in ("acquire", "parse", "semantic", "verify", "publish"):
                assert runtime.run_stage(stage, item)["status"] == "success"
            reader = CompanyProfileReadService(root / "output")
            query = reader.query(("600000.SH",))
            export = reader.export(("600000.SH",), root / "export")
            reload = CompanyProfileReadService(root / "output").query(("600000.SH",))
            assert query["profiles"] == export["profiles"] == reload["profiles"]
            profile = query["profiles"][0]
            assert profile["core_complete"] is False
            assert profile["workpaper_status"] == "model_reviewed_pending_verification"
            assert profile["workpaper"]["quality_review"]["status"] == "completed"
            assert len(profile["workpaper_receipts"]) == 3
            assert transport.calls[0]["payload"]["reasoning_effort"] == "high"
            assert "reasoning_effort" not in transport.calls[1]["payload"]
            assert len(transport.calls) == 3
    finally:
        asyncio.run(client.close())


def test_task_service_review_publishes_original_before_review_and_corrected_afterward():
    from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
    from research.company_profile.operations import CompanyProfileTaskService
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from research.storage import ResearchStorageManager
    from tests.unit.test_research.test_business_profile_production_operations import _announcement
    from tests.unit.test_utils.test_llm_gateway import _config, _response
    from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
    from utils.config_manager import ResearchConfig, ResearchStorageConfig
    from utils.llm.testing import ScriptedTransport

    paper, _, fixed, review, verify = material()
    config = _config(max_schema_repair_attempts=0)
    config = replace(config, profiles={**config.profiles, "review": replace(
        config.profiles["test"], name="review", reasoning_effort="high")})
    with TemporaryDirectory(prefix="v9-review-service-") as directory:
        root = Path(directory)
        db = str(root / "research.db")
        storage = ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(
            db_path=db, financials_db_path=db, valuation_db_path=db, interests_db_path=db,
            quotes_db_path=str(root / "quotes.db"), attach_quotes_db=False)))
        client = None
        try:
            storage.initialize()
            BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
                instrument={"instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE"},
                record=_announcement("review-fixture", "公司2025年年度报告",
                                     published_at="2026-04-01T16:00:00+00:00"))

            class InspectOriginal(ScriptedTransport):
                async def send(self, *args, **kwargs):
                    if not self.calls:
                        original = CompanyProfileReadService(root / "output").query(("600000.SH",))
                        assert original["profiles"][0]["workpaper"]["quality_review"] is None
                        assert original["profiles"][0]["workpaper"]["elements"]["scale_and_mix"]["facts"][0]["text"] == "净利润100.00元。"
                    return await super().send(*args, **kwargs)

            transport = InspectOriginal([_response(review), _response({"repaired_facts": [fixed]}), _response(verify)])
            budget = _budget(deadline=time.monotonic() + 150)
            client = _client(transport, budget, config)
            provider = CommonGatewaySemanticProvider(client=client, profile="test", max_output_tokens=2500, timeout_seconds=10)
            provider.offline_only = True
            identity = workpaper_processing_identity(real=False, review=True)
            identity.update(quality_review_profile="review", repair_profile="test",
                            review_existing_draft_hash=workpaper_hash(paper))
            service = CompanyProfileTaskService(
                storage=storage, output_root=root / "output", checkpoint_root=root / "checkpoints",
                page_source=lambda _: {"report": _report(), "pages": PAGES, "review_seed": paper},
                provider=provider, processing_identity=identity, execution_budget=budget)
            ran = asyncio.run(service.execute("run", knowledge_cutoff="2026-11-12",
                                             instrument_ids=("600000.SH",), max_items=1, max_elapsed_seconds=150))
            assert ran["state"] == "completed"
            # Exhaustion stops model work, not access to the persisted result.
            budget.cancel("bounded_review_finished")
            query = asyncio.run(service.execute("query", instrument_ids=("600000.SH",)))
            export = asyncio.run(service.execute("export", instrument_ids=("600000.SH",), output_directory=root / "export"))
            reload = CompanyProfileReadService(root / "output").query(("600000.SH",))
            assert query["profiles"] == export["profiles"] == reload["profiles"]
            final = query["profiles"][0]
            assert final["workpaper"]["quality_review"]["status"] == "completed"
            assert final["core_complete"] is False
            assert "营业收入100.00" in final["workpaper"]["elements"]["scale_and_mix"]["facts"][0]["text"]
            assert len(transport.calls) == 3
        finally:
            if client is not None:
                asyncio.run(client.close())
