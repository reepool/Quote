"""S41 responses are a fault input. They are not a new company score."""

import gzip
import json
from pathlib import Path

from research.company_profile.contracts import ReportSemanticResponseV3
from research.company_profile.direct_reading import interpret_direct_core
from research.company_profile.report_interpretation import direct_core_processing_identity


def _saved_responses():
    query = json.loads(
        Path(
            "openspec/changes/restore-company-profile-llm-led-generalization/"
            "s41-600103-receipts/query.json"
        ).read_text()
    )
    receipts = query["profiles"][0]["three_views"]["call_receipts"]
    return [
        ReportSemanticResponseV3.model_validate_json(
            json.dumps(item["response_payload"], ensure_ascii=False)
        )
        for item in receipts
        if item.get("response_payload")
    ]


def test_s41_follow_up_sends_the_period_question_and_unread_role_page():
    import time

    responses = _saved_responses()
    assert len(responses) >= 4
    cache = Path(
        "/home/python/Quote/data/filings/announcements/blobs/68/derived/"
        "business_profile_pdfium_recovery.v4/"
        "6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c_"
        "57e1dd4f16ef40ff.json.gz"
    )
    pages = [
        {"page": int(item["page_number"]), "text": item["text"], "readable": True}
        for item in json.loads(gzip.open(cache, "rt", encoding="utf-8").read())["pages"]
    ]
    report = responses[1].source_conditions[0].evidence[0].report
    identity = direct_core_processing_identity()
    identity.update(
        chapter_select=True,
        ledger_tokens=480_000,
        meter_profile={
            "model": "deepseek-flash",
            "max_output_tokens_field": "max_tokens",
            "stream": True,
            "stream_include_usage": True,
            "disable_thinking": True,
        },
    )
    seen = []
    queue = list(responses)
    empty = responses[0].model_copy(
        update={"source_conditions": (), "claims": (), "checks": (), "requested_pages": ()}
    )

    class _Provider:
        offline_only = True

        def interpret(self, request):
            seen.append(request)
            if queue:
                return queue.pop(0)
            if request.purpose == "synthesize_views":
                claims = []
                for condition in request.source_conditions:
                    blob = "".join(
                        str(evidence.anchor.bounded_quote or "")
                        for evidence in condition.evidence
                    )
                    if "2,418,990,221.17" in blob and "2,332,376,693.65" not in condition.description:
                        claims.append(
                            {
                                "claim_id": "period-total",
                                "dimension": "revenue_model",
                                "status": "observed",
                                "answer": "本期营业收入 2,418,990,221.17元。",
                                "condition_ids": [condition.condition_id],
                                "evidence": [
                                    {
                                        "evidence_id": "period-total",
                                        "report": request.report.model_dump(mode="json"),
                                        "page": int(condition.evidence[0].page),
                                        "section_title": "利润表",
                                        "anchor": {
                                            "anchor_type": "text",
                                            "bounded_quote": condition.evidence[0].anchor.bounded_quote,
                                        },
                                    }
                                ],
                            }
                        )
                if claims:
                    return ReportSemanticResponseV3.model_validate_json(
                        json.dumps(
                            {
                                "schema_version": "company_profile_report_response.v3",
                                "request_id": request.request_id,
                                "purpose": request.purpose,
                                "claims": claims,
                            },
                            ensure_ascii=False,
                        )
                    )
            if request.purpose == "verify_views":
                checks = []
                for claim in request.claims:
                    ok = claim.answer == "本期营业收入 2,418,990,221.17元。"
                    checks.append(
                        {
                            "target_type": "claim",
                            "target_id": claim.claim_id,
                            "status": "pass" if ok else "unclear",
                            "explanation": "只接受期间总额这一已核答案。",
                            "covers_target_type": "claim",
                            "covers_target_id": claim.claim_id,
                        }
                    )
                if checks:
                    return ReportSemanticResponseV3.model_validate_json(
                        json.dumps(
                            {
                                "schema_version": "company_profile_report_response.v3",
                                "request_id": request.request_id,
                                "purpose": request.purpose,
                                "checks": checks,
                            },
                            ensure_ascii=False,
                        )
                    )
            return empty

    started = interpret_direct_core(
        report=report,
        pages=pages,
        processing_identity=identity,
        provider=_Provider(),
        work_id="s42-replay",
        token_budget=480_000,
        started_at=time.monotonic(),
    )
    already = {9, 16, 18, 22, 92}
    role_sends = [
        request
        for request in seen
        if request.purpose == "source_inventory"
        and request.source_scope == "full_context"
        and any(int(page["page"]) not in already for page in request.source_pages)
    ]
    assert role_sends, trace
    sent_pages = {int(page["page"]) for request in role_sends for page in request.source_pages}
    assert sent_pages.isdisjoint(already)
    assert any("采购" in str(page.get("text") or "") or "经营模式" in str(page.get("text") or "") for request in role_sends for page in request.source_pages)
    period_sends = [
        request
        for request in seen
        if request.purpose == "synthesize_views"
        and any(
            "period-operating-revenue" in condition.condition_id
            for condition in request.source_conditions
        )
    ]
    assert period_sends
    assert any("2,418,990,221.17" in str(condition.description) + "".join(
        evidence.anchor.bounded_quote for evidence in condition.evidence
    ) for request in period_sends for condition in request.source_conditions)
    answers = {claim.answer for claim in started.claims}
    assert any("制浆、造纸" in answer for answer in answers)
    assert any("伸性纸袋纸" in answer for answer in answers)
    assert any("漳州水仙" in answer for answer in answers)
    assert any(
        "c3" in gap and gap.startswith("major_evidence_not_located:")
        for gap in started.gaps
    )
    assert any("2,418,990,221.17" in answer for answer in answers)
    assert any(
        "2,418,990,221.17" in answer and "2,332,376,693.65" not in answer
        for answer in answers
    )
