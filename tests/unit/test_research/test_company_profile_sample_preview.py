"""A sample preview stops after one source read and one company draft."""

from research.company_profile.contracts import (
    ReportSemanticResponseV3,
    ReportSourceCondition,
    ReportViewClaim,
)
from research.company_profile.direct_reading import interpret_direct_core
from research.company_profile.models import Evidence, ReportIdentity, TextAnchor
from research.company_profile.report_interpretation import direct_core_processing_identity


class _PreviewProvider:
    offline_only = True

    def __init__(self) -> None:
        self.purposes: list[str] = []

    def interpret(self, request):
        self.purposes.append(request.purpose)
        report = request.report
        quote = "公司主要从事纸业生产。主要产品为伸性纸袋纸。"
        evidence = Evidence(
            evidence_id="preview-1",
            report=report,
            page=1,
            section_title="经营",
            anchor=TextAnchor(bounded_quote=quote),
        )
        if request.purpose == "source_inventory" and request.source_scope == "page_map":
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                requested_pages=(1,),
            )
        if request.purpose == "source_inventory":
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                source_conditions=(
                    ReportSourceCondition(
                        condition_id="principal",
                        dimension="principal_business",
                        description="公司从事纸业生产。",
                        evidence=(evidence,),
                        major=True,
                        statement_status="current",
                    ),
                ),
            )
        if request.purpose == "synthesize_views":
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                claims=(
                    ReportViewClaim(
                        claim_id="draft-principal",
                        dimension="principal_business",
                        status="observed",
                        answer="公司从事纸业生产，主要产品为伸性纸袋纸。",
                        condition_ids=("principal",),
                        evidence=(evidence,),
                    ),
                ),
            )
        raise AssertionError(request.purpose)


def test_sample_preview_saves_one_draft_without_verification():
    report = ReportIdentity(
        instrument_id="600103.SH",
        report_id="preview",
        document_version="preview-doc",
        report_period="2025-12-31",
        published_at="2026-04-13T16:00:00+00:00",
    )
    identity = direct_core_processing_identity()
    identity.update(
        chapter_select=True,
        sample_preview="company_v1",
        ledger_tokens=60_000,
        max_physical_requests=6,
        request_reservation_limit=65_536,
        max_payload_utf8_bytes=60_000,
        execution_mode="offline_fake",
        meter_profile={
            "model": "deepseek-flash",
            "max_output_tokens_field": "max_tokens",
            "stream": True,
            "stream_include_usage": True,
            "disable_thinking": True,
        },
    )
    provider = _PreviewProvider()
    result = interpret_direct_core(
        report=report,
        pages=(
            {
                "page": 1,
                "text": "一、报告期内公司从事的业务情况\n公司主要从事纸业生产。主要产品为伸性纸袋纸。",
                "readable": True,
            },
            {
                "page": 2,
                "text": "二、其他说明\n这里不进入第一份样稿。",
                "readable": True,
            },
        ),
        processing_identity=identity,
        provider=provider,
        work_id="sample-preview",
        token_budget=60_000,
        started_at=0.0,
    )
    assert provider.purposes == [
        "source_inventory",
        "source_inventory",
        "synthesize_views",
    ]
    assert "verify_views" not in provider.purposes
    assert any("纸业生产" in claim.answer for claim in result.claims)
    assert all(claim.limitation == "pending_verification" for claim in result.claims)
