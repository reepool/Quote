"""Progressive reading and unverified-draft projection. Not a business score."""

from research.company_profile.contracts import ReportSemanticResponseV3, ReportViewClaim
from research.company_profile.report_interpretation import (
    direct_core_processing_identity,
    research_views,
)
from tests.unit.test_research.test_company_profile_llm_led import (
    _evidence,
    _report,
    _run,
)


def test_a_navigation_batch_is_read_before_the_next_batch_is_sent():
    from research.company_profile import direct_reading

    report = _report()
    text = "公司经营有线电视网络传输，并向用户提供接入服务。"
    quote = text[:12]

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.source_scope == "page_map" and request.source_pages[0]["page"] == 2:
                raise ValueError("later-navigation-failed")
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1,),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="kept",
                            dimension="principal_business",
                            status="observed",
                            answer="已核主营。",
                            evidence=(
                                _evidence(report, text=quote, page=1).model_copy(
                                    update={"evidence_id": "kept-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "kept",
                        "status": "pass",
                        "explanation": "离线",
                    },
                ),
            )

    def batches(pages, report, identity, limit):
        return [
            [{"page": page["page"], "text": "目录 业务", "readable": True}]
            for page in pages
        ]

    original = direct_reading.navigation_batches
    direct_reading.navigation_batches = batches
    try:
        result = _run(
            _Provider(),
            pages=(
                {"page": 1, "text": text, "readable": True},
                {"page": 2, "text": text, "readable": True},
            ),
            processing_identity=direct_core_processing_identity(),
            report=report,
        )
    finally:
        direct_reading.navigation_batches = original
    assert result.call_receipts[0]["source_scope"] == "page_map"
    assert result.call_receipts[1]["purpose"] == "synthesize_views"
    assert any(item.status == "observed" for item in result.claims)
    assert any("later-navigation-failed" in gap for gap in result.gaps)


def test_failed_source_review_is_not_projected_as_verified():
    report = _report()
    text = "公司经营有线电视网络传输，并向用户提供接入服务。"
    quote = text[:12]

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1,),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="draft",
                            dimension="principal_business",
                            status="observed",
                            answer="尚未核验。",
                            evidence=(
                                _evidence(report, text=quote, page=1).model_copy(
                                    update={"evidence_id": "draft-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            raise ValueError("source-review-failed")

    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    rendered = research_views(result)["views"]["company_profile"]["claims"]
    assert rendered
    assert all(item["review_state"] != "verified" for item in rendered)
    assert result.checks == ()


def test_last_allowed_route_attempt_keeps_the_remaining_time():
    from types import SimpleNamespace

    from utils.llm.client import LlmClient

    client = object.__new__(LlmClient)
    client.config = SimpleNamespace(
        profiles={"semantic": SimpleNamespace(enabled=True, timeout_seconds=120)}
    )
    pool = SimpleNamespace(
        failover=SimpleNamespace(enabled=True, max_hops=4, min_attempt_seconds=1),
        members=[
            SimpleNamespace(
                source_label="scorpio:grok",
                profiles={"semantic_extraction": "semantic"},
            ),
            SimpleNamespace(
                source_label="scorpio:deepseek",
                profiles={"semantic_extraction": "semantic"},
            ),
        ],
    )
    request = SimpleNamespace(
        metadata={"max_route_attempts": 2},
        timeout_seconds=120,
    )
    budget = SimpleNamespace(remaining=lambda _now: 60.0)
    profile = client.config.profiles["semantic"]
    result = client._routed_attempt_timeout_seconds(
        request=request,
        profile=profile,
        logical_profile="semantic_extraction",
        pool=pool,
        selected_source="scorpio:deepseek",
        excluded_sources={"scorpio:grok"},
        failover_count=1,
        budget=budget,
    )
    assert result is None or result >= 59
