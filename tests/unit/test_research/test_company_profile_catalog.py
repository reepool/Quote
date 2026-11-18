"""Read-only research discovery uses the same grades and lineage as detail."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
import pytest
from fastapi import FastAPI

from api.company_profile_routes import annual_profile_reader, router
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileRuntimeRecord
from research.company_profile.workpaper_delivery import bind_workpaper_use_review
from tests.unit.test_research.test_company_profile_enrichment import bootstrap
from tests.unit.test_research.test_company_profile_workpaper_delivery import model_review, use_review


@pytest.mark.parametrize("excluded", ["withheld", "source_only", "duplicate"])
def test_catalog_counts_only_visible_content_and_retains_incomplete_review(excluded):
    with TemporaryDirectory(prefix="workpaper-do-grades-") as directory:
        root = Path(directory)
        writer, record = bootstrap(root)
        paper = model_review(record.workpaper, status="incomplete")
        paper = bind_workpaper_use_review(paper, use_review(paper, {"revenue": excluded}))
        writer.persist(record.model_copy(update={"workpaper": paper}))
        reader = CompanyProfileReadService(root / "output")
        before = {p: p.read_bytes() for p in writer.output_root.glob("*.json")}

        catalog = reader.research_profile_catalog()
        detail = reader.research_profile("600000.SH")
        assert catalog["state"] == "found" and catalog["total"] == 1
        item = catalog["profiles"][0]
        assert item["report"] == detail["report"]
        assert item["visible_fact_count"] == 1 and item["visible_metric_count"] == 0
        assert item["fact_states"] == {"reviewed": 1, excluded: 1}
        assert item["readings"] == [{"work_id": record.work_id, "origin": "llm_generated", "review_status": "incomplete"}]
        assert item["information_gap_count"] == len(detail["information_gaps"])
        assert "products_services" in item["information_gap_elements"]
        assert item["suggested_acquisition_count"] == len(detail["suggested_acquisitions"])
        assert "100.00" not in json.dumps(catalog) and "facts" not in item
        assert catalog["full_report_completeness"] == "not_assessed"
        assert catalog["production_authorization"] == "not_authorized"
        assert before == {p: p.read_bytes() for p in writer.output_root.glob("*.json")}
    assert not root.exists()


def test_catalog_includes_same_report_supplements_but_not_orphans_or_other_companies():
    with TemporaryDirectory(prefix="workpaper-do-lineage-") as directory:
        root = Path(directory)
        writer, record = bootstrap(root)
        reader = CompanyProfileReadService(root / "output")
        plan = reader.plan_enrichment("600000.SH", request_id="products-read",
            targets=[{"element": "products_services", "question": "产品服务有哪些？"}])["enrichment_plan"]
        identity = record.execution.input_identity
        assert identity is not None
        product = record.workpaper.elements.business_structure.model_copy(update={
            "facts": (record.workpaper.elements.business_structure.facts[0].model_copy(update={"fact_id": "product"}),)})
        elements = record.workpaper.elements.model_copy(update={"products_services": product})
        supplement = record.model_copy(update={
            "work_id": "supplement-1", "workpaper": record.workpaper.model_copy(update={"elements": elements}),
            "execution": record.execution.model_copy(update={
                "input_identity": identity.model_copy(update={
                    "processing_identity": {**identity.processing_identity, "workpaper_enrichment": plan}})})})
        writer.persist(supplement)
        orphan_plan = {**plan, "base_workpaper_sha256": "0" * 64}
        writer.persist(supplement.model_copy(update={"work_id": "orphan", "execution": record.execution.model_copy(update={
            "input_identity": identity.model_copy(update={
                "processing_identity": {**identity.processing_identity, "workpaper_enrichment": orphan_plan}})})}))
        other_report = record.report.model_copy(update={"instrument_id": "600001.SH"})
        writer.persist(record.model_copy(update={"work_id": "another-company", "report": other_report,
            "workpaper": record.workpaper.model_copy(update={"report": other_report})}))

        catalog = reader.research_profile_catalog()
        assert [item["instrument_id"] for item in catalog["profiles"]] == ["600000.SH", "600001.SH"]
        first, second = catalog["profiles"]
        assert first["visible_fact_count"] == 3 and first["visible_metric_count"] == 1
        assert first["fact_states"] == {"pending": 3}
        assert [r["work_id"] for r in first["readings"]] == [record.work_id, "supplement-1"]
        assert first["base_work_id"] == record.work_id and first["latest_reading_work_id"] == "supplement-1"
        assert "products_services" in first["information_gap_elements"]
        assert second["visible_fact_count"] == 2 and len(second["readings"]) == 1
    assert not root.exists()


@pytest.mark.asyncio
async def test_catalog_http_empty_legacy_and_invalid_explicit_root(monkeypatch):
    with TemporaryDirectory(prefix="workpaper-do-api-") as directory:
        root = Path(directory)
        monkeypatch.setenv("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT", str(root / "output"))
        writer, record = bootstrap(root)
        # Old accepted-core records are not fabricated into workpaper entries.
        legacy = CompanyProfileRuntimeRecord.model_validate({
            **record.model_dump(exclude={"schema_version", "workpaper", "workpaper_receipts"}),
            "work_id": "legacy", "report": record.report.model_copy(update={"instrument_id": "600002.SH"})})
        writer.persist(legacy)
        app = FastAPI()
        app.include_router(router, prefix="/api/v1")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local-test") as client:
            response = await client.get("/api/v1/research/company/annual-profiles")
            assert response.status_code == 200
            assert response.json() == annual_profile_reader().research_profile_catalog()
            assert response.json()["total"] == 1
            assert response.json()["profiles"][0]["base_work_id"] == record.work_id
            assert (await client.get("/api/v1/research/company/600000.SH/annual-profile")).status_code == 200

            empty = root / "empty"
            empty.mkdir()
            monkeypatch.setenv("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT", str(empty))
            idle = await client.get("/api/v1/research/company/annual-profiles")
            assert idle.status_code == 200
            assert idle.json()["state"] == "idle" and idle.json()["total"] == 0
            assert idle.json()["profiles"] == []
            assert idle.json()["production_authorization"] == "not_authorized"
            for invalid in ("", str(root / "missing")):
                monkeypatch.setenv("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT", invalid)
                assert (await client.get("/api/v1/research/company/annual-profiles")).status_code == 503
    assert not root.exists()
