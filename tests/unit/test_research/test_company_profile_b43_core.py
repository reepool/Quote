"""Controlled company core through the original task service. Not a real-model score."""

import gzip
import json
import shutil
import uuid
from pathlib import Path

from research.company_profile.bounded_entry import _fixed_core_reply
from research.company_profile.contracts import ReportSemanticResponseV3
from research.company_profile.models import ReportIdentity
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.report_interpretation import direct_core_processing_identity
from tests.unit.test_research.test_company_profile_bounded_entry import _fixed_core_objects


def _pages():
    cache = Path(
        "/home/python/Quote/data/filings/announcements/blobs/68/derived/"
        "business_profile_pdfium_recovery.v4/"
        "6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c_"
        "57e1dd4f16ef40ff.json.gz"
    )
    wanted = {4, 9, 11, 16}
    return tuple(
        {"page": int(item["page_number"]), "text": item["text"], "readable": True}
        for item in json.loads(gzip.open(cache, "rt", encoding="utf-8").read())["pages"]
        if int(item["page_number"]) in wanted
    )


def _provider(objects):
    class _Fixed:
        offline_only = True

        def __init__(self):
            self.claims_by_answer = {}

        def interpret(self, request):
            raw = _fixed_core_reply(
                json.loads(request.model_dump_json()),
                {"fixed_core_objects": objects},
            )
            if request.purpose == "synthesize_views":
                for claim in raw.get("claims") or []:
                    self.claims_by_answer[claim["answer"]] = claim["claim_id"]
            if request.purpose == "verify_views":
                raw["checks"] = []
                supported = set()
                for claim in request.claims:
                    quote = ""
                    if claim.evidence:
                        quote = str(claim.evidence[0].anchor.bounded_quote or "")
                    matched = next(
                        (
                            item
                            for item in objects
                            if item["answer"] == claim.answer and item["quote"] in quote
                        ),
                        None,
                    )
                    ok = matched is not None
                    if ok:
                        supported.add(claim.claim_id)
                    raw["checks"].append(
                        {
                            "target_type": "claim",
                            "target_id": claim.claim_id,
                            "status": "pass" if ok else "unclear",
                            "explanation": "答案等于已核对象，且已核引文包含在本请求证据中。",
                            "covers_target_type": "claim",
                            "covers_target_id": claim.claim_id,
                        }
                    )
                for condition in request.source_conditions:
                    obj = next(
                        (
                            item
                            for item in objects
                            if condition.condition_id.endswith(item["condition_id"])
                            or condition.condition_id == item["condition_id"]
                        ),
                        None,
                    )
                    claim_id = None
                    if obj is not None:
                        claim_id = next(
                            (
                                claim.claim_id
                                for claim in request.claims
                                if claim.answer == obj["answer"] and claim.claim_id in supported
                            ),
                            self.claims_by_answer.get(obj["answer"]),
                        )
                    blob = "".join(
                        str(evidence.anchor.bounded_quote or "")
                        for evidence in condition.evidence
                    )
                    blob += "".join(str(page.get("text") or "") for page in request.source_pages)
                    ok = (
                        obj is not None
                        and claim_id is not None
                        and obj["quote"] in blob
                    )
                    raw["checks"].append(
                        {
                            "target_type": "condition",
                            "target_id": condition.condition_id,
                            "status": "pass" if ok else "unclear",
                            "explanation": "条件覆盖已核主张。",
                            "covers_target_type": "claim" if ok else None,
                            "covers_target_id": claim_id if ok else None,
                        }
                    )
            report = request.report.model_dump(mode="json")

            def fill(item, quote, page, evidence_id):
                item["evidence"] = [
                    {
                        "evidence_id": evidence_id,
                        "report": report,
                        "page": int(page),
                        "section_title": "正文",
                        "anchor": {"anchor_type": "text", "bounded_quote": quote},
                    }
                ]

            for index, condition in enumerate(raw.get("source_conditions") or []):
                evidence = condition["evidence"][0]
                fill(condition, evidence["quote"], evidence["page"], f"cond-{index}")
            for index, claim in enumerate(raw.get("claims") or []):
                evidence = claim["evidence"][0]
                fill(claim, evidence["quote"], evidence["page"], f"claim-{index}")
            return ReportSemanticResponseV3.model_validate_json(
                json.dumps(raw, ensure_ascii=False)
            )

    return _Fixed()


def _run(objects):
    from research.company_profile.runtime import (
        CompanyProfileResearchWriter,
        CompanyProfileStageRuntime,
    )

    root = Path("/home/python/Quote/data/QuoteBak") / f"s43-core-{uuid.uuid4().hex[:8]}"
    root.mkdir()
    try:
        report = ReportIdentity(
            instrument_id="600103.SH",
            report_id="ann-600103-2025",
            document_version="6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c",
            report_period="2025-12-31",
            published_at="2026-04-13T16:00:00+00:00",
        )
        identity = direct_core_processing_identity()
        identity.update(
            chapter_select=True,
            ledger_tokens=480_000,
            max_physical_requests=32,
            meter_profile={
                "model": "deepseek-flash",
                "max_output_tokens_field": "max_tokens",
                "stream": True,
                "stream_include_usage": True,
                "disable_thinking": True,
            },
        )
        runtime = CompanyProfileStageRuntime(
            writer=CompanyProfileResearchWriter(
                root / "output", checkpoint_root=root / "checkpoints"
            ),
            provider=_provider(objects),
            token_budget=480_000,
        )
        item = {
            "work_id": "s43-core",
            "report": report,
            "pages": _pages(),
            "processing_identity": identity,
        }
        for stage in ("acquire", "parse", "semantic", "verify", "publish"):
            runtime.run_stage(stage, item)
        queried = CompanyProfileReadService(root / "output").query(("600103.SH",))
        exported = CompanyProfileReadService(root / "output").export(
            ("600103.SH",), root / "export"
        )
        reloaded = CompanyProfileReadService(root / "output").query(("600103.SH",))
        return queried, exported, reloaded
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _answers(payload):
    profile = payload["profiles"][0]
    three = profile["three_views"]
    return profile, three, {
        claim.get("answer")
        for view in three["views"].values()
        for claim in view.get("claims") or []
    }


def test_controlled_paper_core_is_readable_from_query_export_and_reload():
    queried, exported, reloaded = _run(_fixed_core_objects())
    assert queried["profiles"] == reloaded["profiles"]
    assert exported["profiles"] == queried["profiles"]
    profile, three, answers = _answers(queried)
    assert three["draft_complete"] is True
    assert three["real_semantic_complete"] is False
    assert any("医药" in answer and "光电子" in answer for answer in answers)
    assert any("2,418,990,221.17" in answer for answer in answers)
    assert any("自主生产" in answer for answer in answers)
    assert any("直销" in answer for answer in answers)
    assert not any("2,332,376,693.65" in answer for answer in answers)
    covered = {
        check.get("covers_target_id")
        for receipt in three.get("call_receipts") or []
        for check in (receipt.get("response_payload") or {}).get("checks") or []
        if check.get("status") == "pass" and check.get("covers_target_type") == "claim"
    }
    claim_ids = {
        claim.get("claim_id")
        for view in three["views"].values()
        for claim in view.get("claims") or []
    }
    assert covered & claim_ids


def test_missing_downstream_keeps_the_core_limited():
    objects = [
        item
        for item in _fixed_core_objects()
        if item["condition_id"] != "claim-005"
    ]
    queried, exported, reloaded = _run(objects)
    assert queried["profiles"] == reloaded["profiles"]
    assert exported["profiles"] == queried["profiles"]
    profile, three, answers = _answers(queried)
    assert three["draft_complete"] is False
    assert three["real_semantic_complete"] is False
    assert any("医药" in answer for answer in answers)
