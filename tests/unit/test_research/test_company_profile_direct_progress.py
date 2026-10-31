"""Progressive reading and unverified-draft projection. Not a business score."""

import json

from research.company_profile.contracts import (
    ReportSemanticResponseV3,
    ReportViewCheck,
    ReportViewClaim,
)
from research.company_profile.report_interpretation import (
    ReportInterpretation,
    direct_core_processing_identity,
    research_views,
)
from tests.unit.test_research.test_company_profile_llm_led import (
    _evidence,
    _report,
    _run,
)


def test_a_failed_slice_does_not_drop_an_earlier_reading():
    report = _report()
    text = "公司经营有线电视网络传输，并向用户提供接入服务。"
    quote = text[:12]

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = tuple(int(page["page"]) for page in request.source_pages)
            if request.purpose == "source_inventory" and 2 in pages and 1 not in pages:
                raise ValueError("later-slice-failed")
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

    from research.company_profile import direct_reading

    def slices(pages, report, identity, limit):
        return [
            [
                {
                    "page": page["page"],
                    "text": page["text"],
                    "readable": True,
                    "offset": 0,
                    "scope": "full_page",
                }
            ]
            for page in pages
        ]

    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = slices
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
        direct_reading.source_discovery_slices = original
    assert any(item["purpose"] == "source_inventory" for item in result.call_receipts)
    assert any(item["purpose"] == "synthesize_views" for item in result.call_receipts)
    assert any(item.status == "observed" for item in result.claims)
    assert any("later-slice-failed" in gap for gap in result.gaps)
    assert any(gap == "necessary_unread:2" for gap in result.gaps)


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


def test_a_source_gap_failure_does_not_block_an_already_verified_claim():
    from research.company_profile.report_interpretation import research_views

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
            if request.purpose == "verify_views":
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
            raise ValueError("gap-failed")

    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    rendered = research_views(result)["views"]["company_profile"]["claims"]
    assert any(item["review_state"] == "verified" for item in rendered)
    assert result.draft_complete is False
    assert any(gap.startswith("direct_core_gap_check_stopped:") for gap in result.gaps)


def test_a_later_verify_unit_failure_keeps_the_earlier_unit():
    report = _report()
    text = "公司经营有线电视网络传输，并向用户提供接入服务。"
    long_text = "公司提供互联网接入服务。" + ("甲" * 4000)

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=tuple(page["page"] for page in request.source_pages),
                )
            if request.purpose == "synthesize_views":
                page = request.source_pages[0]["page"]
                claim_id = "first" if page == 1 else "second"
                source = text if page == 1 else long_text
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id=claim_id,
                            dimension="principal_business" if page == 1 else "products_services",
                            status="observed",
                            answer="已定位。",
                            evidence=(
                                _evidence(report, text=source[:12], page=page).model_copy(
                                    update={"evidence_id": f"{claim_id}-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            if request.purpose == "verify_views" and any(
                item.claim_id == "second" for item in request.claims
            ):
                raise ValueError("second-unit-failed")
            if request.purpose == "verify_views" and request.claims:
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    checks=(
                        {
                            "target_type": "claim",
                            "target_id": item.claim_id,
                            "status": "pass",
                            "explanation": "离线",
                        }
                        for item in request.claims
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
            )

    identity = direct_core_processing_identity()
    identity["request_reservation_limit"] = 20_000
    result = _run(
        _Provider(),
        pages=(
            {"page": 1, "text": text, "readable": True},
            {"page": 2, "text": long_text, "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    assert any(item.claim_id == "first" and item.status == "observed" for item in result.claims)
    assert any(item.claim_id == "second" and item.limitation == "pending_verification" for item in result.claims)


def test_a_condition_pass_without_a_delivered_object_is_not_coverage():
    from research.company_profile.contracts import (
        ReportSourceCondition,
        ReportViewCheck,
    )
    from research.company_profile.direct_reading import _completeness_gaps

    report = _report()
    text = "公司提供云计算服务。"
    quote = text[:8]
    claims = []
    for claim_id, dimension in (
        ("cloud-business", "principal_business"),
        ("cloud-product", "products_services"),
        ("cloud-revenue", "revenue_model"),
        ("cloud-output", "output_exposure"),
        ("cloud-role", "supply_chain_role"),
    ):
        claims.append(
            ReportViewClaim(
                claim_id=claim_id,
                dimension=dimension,
                status="observed",
                answer="只交付了云计算服务。",
                evidence=(_evidence(report, text=quote, page=1),),
            )
        )
    condition = ReportSourceCondition(
        condition_id="design-business",
        dimension="principal_business",
        description="另一主要业务是工业设计。",
        evidence=(_evidence(report, text=quote, page=1),),
        major=True,
        statement_status="current",
    )
    bare = ReportViewCheck(
        target_type="condition",
        target_id="design-business",
        status="pass",
        explanation="只看到了源条件。",
    )
    gaps = _completeness_gaps(claims, [condition], [bare])
    assert any(gap == "major_condition_not_verified:design-business" for gap in gaps)
    unrelated = bare.model_copy(
        update={"covers_target_type": "claim", "covers_target_id": "missing-claim"}
    )
    gaps = _completeness_gaps(claims, [condition], [unrelated])
    assert any(gap == "major_condition_not_verified:design-business" for gap in gaps)
    covered = bare.model_copy(
        update={"covers_target_type": "claim", "covers_target_id": "cloud-business"}
    )
    gaps = _completeness_gaps(
        [
            claims[0].model_copy(update={"answer": "公司从事工业设计。"}),
            *claims[1:],
        ],
        [condition],
        [
            covered,
            ReportViewCheck(
                target_type="claim",
                target_id="cloud-business",
                status="pass",
                explanation="工业设计主张已核。",
            ),
        ],
    )
    assert not any(gap.startswith("major_condition_not_verified:") for gap in gaps)


def test_keywords_and_shared_quotes_do_not_cover_an_unanswered_major_business():
    from research.company_profile.contracts import ReportSourceCondition, ReportViewCheck
    from research.company_profile.direct_reading import _completeness_gaps

    report = _report()
    shared = "公司提供云服务并取得云服务收入；另有独立的工业设计服务，其面向设备制造客户提供收费设计服务。"
    claims = []
    checks = []
    for claim_id, dimension in (
        ("cloud-business", "principal_business"),
        ("cloud-product", "products_services"),
        ("cloud-revenue", "revenue_model"),
        ("cloud-input", "input_exposure"),
        ("cloud-role", "supply_chain_role"),
    ):
        claims.append(
            ReportViewClaim(
                claim_id=claim_id,
                dimension=dimension,
                status="observed",
                answer="公司提供云服务并取得云服务收入。",
                condition_ids=("cloud-only",),
                evidence=(_evidence(report, text=shared, page=1),),
            )
        )
        checks.append(
            ReportViewCheck(
                target_type="claim",
                target_id=claim_id,
                status="pass",
                explanation="只核了云服务。",
            )
        )
    missing = (
        ("usage-fee", "revenue_model", "公司的另一主要变现机制是按使用量收费。"),
        ("design-product", "products_services", "另一主要产品是工业设计服务。"),
        ("cloud-infrastructure", "input_exposure", "公司关键投入是云计算基础设施。"),
        ("design-business", "principal_business", "另一主要业务是工业设计。"),
    )
    conditions = [
        ReportSourceCondition(
            condition_id=condition_id,
            dimension=dimension,
            description=description,
            evidence=(_evidence(report, text=shared, page=1),),
            major=True,
            statement_status="current",
        )
        for condition_id, dimension, description in missing
    ]
    gaps = _completeness_gaps(claims, conditions, checks)
    assert gaps == [f"major_condition_not_verified:{item[0]}" for item in missing]
    covered = [
        *checks,
        ReportViewCheck(
            target_type="condition",
            target_id="design-business",
            status="pass",
            explanation="工业设计已由对应主张覆盖。",
            covers_target_type="claim",
            covers_target_id="cloud-business",
        ),
    ]
    answered = [
        claims[0].model_copy(update={"answer": "公司从事工业设计。"}),
        *claims[1:],
    ]
    remaining = _completeness_gaps(answered, conditions, covered)
    assert "major_condition_not_verified:design-business" not in remaining
    assert "major_condition_not_verified:design-product" in remaining
    planned = conditions[0].model_copy(update={"statement_status": "planned"})
    minor = conditions[1].model_copy(update={"major": False})
    quiet = _completeness_gaps(claims, [planned, minor], checks)
    assert quiet == []


def test_direct_reading_does_not_publish_an_uncovered_major_business():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.report_interpretation import core_assessment

    report = _report()
    text = "公司提供云服务并取得云服务收入；另有独立的工业设计服务，其面向设备制造客户提供收费设计服务。"

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="design-business",
                            dimension="principal_business",
                            description="另一主要业务是工业设计。",
                            evidence=(
                                _evidence(report, text=text, page=1).model_copy(
                                    update={"evidence_id": "design-evidence"}
                                ),
                            ),
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
                            claim_id="cloud-business",
                            dimension="principal_business",
                            status="observed",
                            answer="公司提供云服务并取得云服务收入。",
                            evidence=(
                                _evidence(report, text=text, page=1).model_copy(
                                    update={"evidence_id": "cloud-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "cloud-business",
                        "status": "pass",
                        "explanation": "只核了云服务。",
                    },
                ),
            )

    identity = direct_core_processing_identity()
    identity["discovery_scope"] = "full_text"
    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert any(
        gap.startswith("major_condition_not_verified:") and gap.endswith("design-business")
        for gap in result.gaps
    )
    assert result.draft_complete is False
    rendered = research_views(result)
    assert rendered["draft_complete"] is False
    assert rendered["semantic_complete"] is False
    assert rendered["real_semantic_complete"] is False
    assert core_assessment(result).core_complete is False


def test_coverage_verification_includes_the_verified_claim():
    report = _report()
    text = "公司提供云计算服务，不包含其他主业。"
    quote = text[:8]

    class _Provider:
        offline_only = True
        coverage_claims: tuple = ()

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
                            claim_id="cloud-only",
                            dimension="principal_business",
                            status="observed",
                            answer="只交付了云计算服务。",
                            evidence=(
                                _evidence(report, text=quote, page=1).model_copy(
                                    update={"evidence_id": "cloud-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        {
                            "condition_id": "design-business",
                            "dimension": "principal_business",
                            "description": "另一主要业务是工业设计。",
                            "evidence": (
                                _evidence(report, text=quote, page=1).model_copy(
                                    update={"evidence_id": "design-evidence"}
                                ),
                            ),
                            "major": True,
                            "statement_status": "current",
                        },
                    ),
                )
            if request.source_conditions:
                self.coverage_claims = request.claims
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    checks=(
                        {
                            "target_type": "claim",
                            "target_id": "cloud-only",
                            "status": "pass",
                            "explanation": "主张本身仍成立。",
                        },
                        {
                            "target_type": "condition",
                            "target_id": "design-business",
                            "status": "pass",
                            "explanation": "没有指出覆盖对象。",
                        },
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "cloud-only",
                        "status": "pass",
                        "explanation": "离线",
                    },
                ),
            )

    provider = _Provider()
    result = _run(
        provider,
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    assert provider.coverage_claims
    assert provider.coverage_claims[0].claim_id == "cloud-only"
    assert any(
        gap.startswith("major_condition_not_verified:") and gap.endswith("design-business")
        for gap in result.gaps
    )
    assert result.draft_complete is False
    assert any(item.claim_id == "cloud-only" and item.status == "observed" for item in result.claims)


def test_a_later_block_retracts_that_claim_and_keeps_another():
    report = _report()
    text = "公司提供云计算服务，并向家庭用户收费。"
    quote = text[:8]

    def claim(claim_id, dimension):
        return ReportViewClaim(
            claim_id=claim_id,
            dimension=dimension,
            status="observed",
            answer="云计算服务。",
            evidence=(
                _evidence(report, text=quote, page=1).model_copy(
                    update={"evidence_id": f"{claim_id}-evidence"}
                ),
            ),
        )

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
                        claim("kept", "products_services"),
                        claim("blocked", "principal_business"),
                    ),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        {
                            "condition_id": "design-business",
                            "dimension": "principal_business",
                            "description": "另一主要业务是工业设计。",
                            "evidence": (
                                _evidence(report, text=quote, page=1).model_copy(
                                    update={"evidence_id": "design-evidence"}
                                ),
                            ),
                            "major": True,
                            "statement_status": "current",
                        },
                    ),
                )
            blocked_now = bool(request.source_conditions)
            checks = [
                {
                    "target_type": "claim",
                    "target_id": item.claim_id,
                    "status": "block" if blocked_now and item.claim_id == "blocked" else "pass",
                    "explanation": "离线",
                }
                for item in request.claims
            ]
            checks.extend(
                {
                    "target_type": "condition",
                    "target_id": item.condition_id,
                    "status": "pass",
                    "explanation": "引用了已被否定的主张。",
                    "covers_target_type": "claim",
                    "covers_target_id": "blocked",
                }
                for item in request.source_conditions
            )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(checks),
            )

    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    assert any(item.claim_id == "kept" and item.status == "observed" for item in result.claims)
    assert any(item.claim_id == "blocked" and item.status == "unclear" for item in result.claims)
    assert any("design-business" in gap for gap in result.gaps)
    assert result.draft_complete is False


def test_two_paragraphs_on_one_page_both_reach_verification():
    report = _report()
    first = "公司向家庭用户提供有线电视服务，并按月收取收视维护费。"
    second = "公司采购机顶盒和智能卡，由外部设备制造商供应。"
    text = first + "\n\n" + second
    sent = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1,),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="service",
                            dimension="principal_business",
                            status="observed",
                            answer="向家庭用户提供有线电视服务。",
                            evidence=(
                                _evidence(report, text=first, page=1).model_copy(
                                    update={"evidence_id": "service-evidence"}
                                ),
                            ),
                        ),
                        ReportViewClaim(
                            claim_id="supply",
                            dimension="input_exposure",
                            status="observed",
                            answer="采购机顶盒和智能卡。",
                            evidence=(
                                _evidence(report, text=second, page=1).model_copy(
                                    update={"evidence_id": "supply-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            sent.append("\n".join(page["text"] for page in request.source_pages))
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": item.claim_id,
                        "status": "pass",
                        "explanation": "离线",
                    }
                    for item in request.claims
                ),
            )

    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    assert sent
    assert first in sent[0]
    assert second in sent[0]
    assert any(item.claim_id == "service" and item.status == "observed" for item in result.claims)
    assert any(item.claim_id == "supply" and item.status == "observed" for item in result.claims)


def test_selected_chapters_are_read_before_the_company_draft():
    from research.company_profile.direct_reading import body_packages

    report = _report()
    identity = direct_core_processing_identity()
    text = {
        1: "公司经营有线电视网络传输，并向用户提供接入服务。",
        9: "公司按合同期限确认频道收看维护费，并采购数据传输与系统维护服务。",
    }
    pages = tuple(
        {"page": page, "text": body, "readable": True} for page, body in text.items()
    )
    persists = {"count": 0}

    class _Provider:
        offline_only = True

        def __init__(self):
            self.inventories = 0
            self.events = []

        def interpret(self, request):
            current = tuple(int(page["page"]) for page in request.source_pages)
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1, 9),
                )
            if request.purpose == "source_inventory" and request.source_scope != "page_map":
                from research.company_profile.contracts import ReportSourceCondition

                assert not request.claims
                self.inventories += 1
                self.events.append(("inventory", current))
                if self.inventories >= 2 and persists.get("fail"):
                    raise ValueError("second-package-inventory-failed")
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=f"page-{page}",
                            dimension="principal_business" if page == 1 else "revenue_model",
                            description=text[page],
                            evidence=(
                                _evidence(report, text=text[page][:12], page=page).model_copy(
                                    update={"evidence_id": f"cond-{page}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for page in current
                        if page in text
                    ),
                )
            if request.purpose == "synthesize_views":
                self.events.append(("synthesize", current))
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=f"package-{page}",
                            dimension="principal_business" if page == 1 else "revenue_model",
                            status="observed",
                            answer=f"第{page}页已核。",
                            evidence=(
                                _evidence(report, text=text[page][:12], page=page).model_copy(
                                    update={"evidence_id": f"package-{page}-evidence"}
                                ),
                            ),
                        )
                        for page in current
                    ),
                )
            if request.purpose == "verify_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    checks=tuple(
                        {
                            "target_type": "claim",
                            "target_id": claim.claim_id,
                            "status": "pass",
                            "explanation": "离线",
                        }
                        for claim in request.claims
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
            )

    provider = _Provider()
    result = _run(
        provider,
        pages=pages,
        processing_identity=identity,
        report=report,
        on_progress=lambda _result: persists.__setitem__("count", persists["count"] + 1),
    )
    assert persists["count"] >= 1
    assert {item.claim_id for item in result.claims if item.status == "observed"} >= {
        "package-1",
        "package-9",
    }
    names = [name for name, _pages in provider.events]
    assert "inventory" in names and "synthesize" in names
    assert names.index("synthesize") > names.index("inventory")
    assert {page for name, pages in provider.events if name == "inventory" for page in pages} >= {1, 9}
    assert sum(name == "synthesize" for name, _pages in provider.events) == 1


def test_same_local_claim_id_does_not_drop_a_different_business():
    report = _report()
    text = "公司经营有线电视网络。公司按合同期限确认收看维护费。"

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
                            claim_id="same",
                            dimension="principal_business",
                            status="observed",
                            answer="经营有线电视网络。",
                            evidence=(
                                _evidence(report, text="公司经营有线电视网络", page=1).model_copy(
                                    update={"evidence_id": "biz-1"}
                                ),
                            ),
                        ),
                        ReportViewClaim(
                            claim_id="same",
                            dimension="revenue_model",
                            status="observed",
                            answer="按合同期限确认收看维护费。",
                            evidence=(
                                _evidence(report, text="按合同期限确认收看维护费", page=1).model_copy(
                                    update={"evidence_id": "fee-1"}
                                ),
                            ),
                        ),
                    ),
                )
            if request.purpose == "verify_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    checks=tuple(
                        {
                            "target_type": "claim",
                            "target_id": claim.claim_id,
                            "status": "pass",
                            "explanation": "离线",
                        }
                        for claim in request.claims
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id, purpose=request.purpose
            )

    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    answers = {item.answer for item in result.claims}
    assert "经营有线电视网络。" in answers
    assert "按合同期限确认收看维护费。" in answers
    assert len({item.claim_id for item in result.claims}) == 2


def test_company21_core_pages_pack_inside_the_byte_cap():
    import gzip
    import json
    from pathlib import Path

    from research.company_profile.contracts import ReportSemanticRequestV3
    from research.company_profile.direct_reading import body_packages
    from research.company_profile.reading_budget import request_payload_bytes

    root = Path("/home/python/Quote")
    query = json.loads(
        (
            root
            / "openspec/changes/restore-company-profile-llm-led-generalization/b1-company21-evidence/query.json"
        ).read_text()
    )
    selected = set(
        next(
            item["response_payload"]["requested_pages"]
            for item in query["profiles"][0]["three_views"]["call_receipts"]
            if item.get("source_scope") == "page_map"
        )
    )
    checkpoint = json.loads(
        gzip.decompress(
            (
                root
                / "openspec/changes/restore-company-profile-llm-led-generalization/b1-resource-evidence/iteration2/checkpoint.json.gz"
            ).read_bytes()
        )
    )
    pages = [
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": True,
        }
        for page in checkpoint["pages"]
        if int(page["page"]) in selected
    ]
    assert len(pages) == len(selected) == 22
    report = _report()
    identity = direct_core_processing_identity()
    identity["max_payload_utf8_bytes"] = 60_000
    identity["request_reservation_limit"] = 65_536
    packages = list(body_packages(pages, report, identity, 65_536))
    covered = []
    for package in packages:
        request = ReportSemanticRequestV3(
            request_id="pack-check",
            purpose="source_inventory",
            report=report,
            processing_identity=identity,
            source_pages=tuple(package),
            source_scope="full_context",
            max_output_tokens=2500,
            timeout_seconds=180,
        )
        assert request_payload_bytes(request) <= 60_000
        covered.extend(int(page["page"]) for page in package)
    assert set(covered) == selected


def test_company19_candidates_are_verified_once_and_roles_are_included():
    """Offline replay of the real 7 claims and 15 conditions. Not a company score."""
    import gzip
    import json
    from pathlib import Path

    from research.company_profile.contracts import ReportSourceCondition, ReportViewClaim
    from research.company_profile.direct_reading import interpret_direct_core

    root = Path("/home/python/Quote")
    evidence = root / (
        "openspec/changes/restore-company-profile-llm-led-generalization/"
        "b1-company19-evidence/query.json"
    )
    checkpoint = root / (
        "openspec/changes/restore-company-profile-llm-led-generalization/"
        "b1-resource-evidence/iteration2/checkpoint.json.gz"
    )
    query = json.loads(evidence.read_text())
    receipts = query["profiles"][0]["three_views"]["call_receipts"]
    synthesized = next(
        item["response_payload"]["claims"]
        for item in receipts
        if item.get("purpose") == "synthesize_views"
    )
    conditions = next(
        item["response_payload"]["source_conditions"]
        for item in receipts
        if item.get("purpose") == "source_inventory" and item.get("source_scope") != "page_map"
    )
    assert len(synthesized) == 7
    assert len(conditions) == 15
    saved_claims = tuple(
        ReportViewClaim.model_validate_json(json.dumps(item, ensure_ascii=False))
        for item in synthesized
    )
    saved_conditions = tuple(
        ReportSourceCondition.model_validate_json(json.dumps(item, ensure_ascii=False))
        for item in conditions
    )
    report = _report()
    report = report.model_copy(update={"instrument_id": "600037.SH"})
    # The saved claims cite the real report identity. Use that identity.
    report = saved_claims[0].evidence[0].report
    pages = []
    payload = json.loads(gzip.decompress(checkpoint.read_bytes()))
    for page in payload["pages"]:
        if int(page["page"]) in {15, 16, 17, 18, 19}:
            pages.append(
                {
                    "page": int(page["page"]),
                    "text": str(page.get("text") or ""),
                    "readable": True,
                }
            )
    seen = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=tuple(
                        int(page["page"])
                        for page in request.source_pages
                        if int(page["page"]) in {15, 16, 17, 18, 19}
                    ),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=saved_conditions,
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=saved_claims,
                )
            seen.append(request)
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "离线",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": next(
                            (
                                claim.claim_id
                                for claim in request.claims
                                if claim.dimension == condition.dimension
                            ),
                            request.claims[0].claim_id if request.claims else "",
                        ),
                        "explanation": "离线",
                    }
                    for condition in request.source_conditions
                    if request.claims
                ),
            )

    result = interpret_direct_core(
        report=report,
        pages=pages,
        processing_identity=direct_core_processing_identity(),
        provider=_Provider(),
        work_id="company19-replay",
        token_budget=120_000,
        started_at=0,
    )
    claim_hits = {}
    condition_hits = {}
    for request in seen:
        for claim in request.claims:
            claim_hits[claim.claim_id] = claim_hits.get(claim.claim_id, 0) + 1
        for condition in request.source_conditions:
            key = condition.condition_id.split(":")[-1]
            condition_hits[key] = condition_hits.get(key, 0) + 1
    assert len(seen) <= 6
    assert set(claim_hits) == {claim.claim_id for claim in saved_claims}
    assert max(claim_hits.values()) <= 2
    assert all(count == 1 for count in condition_hits.values())
    missing = [
        condition.condition_id
        for condition in saved_conditions
        if condition.condition_id not in condition_hits
    ]
    assert all(any(condition_id in gap for gap in result.gaps) for condition_id in missing)
    assert len(condition_hits) + len(missing) == 15
    for claim_id in (
        "claim_output_exposure_1",
        "claim_input_exposure_1",
        "claim_supply_chain_role_1",
        "claim_upstream_downstream_1",
    ):
        assert any(claim.claim_id == claim_id and claim.status == "observed" for claim in result.claims)


def test_navigation_does_not_decide_which_body_is_read():
    report = _report()
    seen = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = tuple(int(page["page"]) for page in request.source_pages)
            seen.append((request.purpose, request.source_scope, pages))
            if request.purpose == "source_inventory":
                assert not request.claims
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="fee",
                            dimension="revenue_model",
                            status="observed",
                            answer="按合同期限确认收看维护费。",
                            evidence=(
                                _evidence(
                                    report,
                                    text="按合同期限确认收看维护费",
                                    page=116,
                                ).model_copy(update={"evidence_id": "fee-evidence"}),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "fee",
                        "status": "pass",
                        "explanation": "整句有引文。",
                    },
                ),
            )

    result = _run(
        _Provider(),
        pages=(
            {"page": 1, "text": "公司经营有线电视网络。", "readable": True},
            {"page": 116, "text": "公司按合同期限确认收看维护费。", "readable": True},
        ),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    inventoried = {
        page
        for purpose, _scope, pages in seen
        if purpose == "source_inventory"
        for page in pages
    }
    assert {1, 116} <= inventoried
    assert all(scope != "page_map" for _purpose, scope, _pages in seen)
    assert any(item.claim_id == "fee" and item.status == "observed" for item in result.claims)


def test_an_unclear_composite_claim_is_not_reliable():
    report = _report()
    text = "在北京市拥有有线电视网络的经营权。"

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="role",
                            dimension="supply_chain_role",
                            status="observed",
                            answer="公司拥有网络经营权，并连接上游节目内容与网络设备资源。",
                            evidence=(
                                _evidence(report, text=text, page=1).model_copy(
                                    update={"evidence_id": "role-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            if request.purpose == "verify_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    checks=(
                        {
                            "target_type": "claim",
                            "target_id": "role",
                            "status": "unclear",
                            "explanation": "经营权有引文，上游关系没有引文。",
                        },
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
            )

    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    rendered = research_views(result)
    claim = rendered["views"]["supply_chain_position"]["claims"][0]
    assert claim["review_state"] != "verified"
    assert claim["status"] == "unclear"


def test_synthesis_keeps_the_conditions_that_fit_the_reservation():
    from research.company_profile import direct_reading
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司经营有线电视网络，并按合同期限确认收看维护费。"

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="business",
                            dimension="principal_business",
                            description="经营有线电视网络。",
                            evidence=(_evidence(report, text="公司经营有线电视网络", page=1),),
                            major=True,
                            statement_status="current",
                        ),
                        ReportSourceCondition(
                            condition_id="fee",
                            dimension="revenue_model",
                            description="按合同期限确认收看维护费。",
                            evidence=(_evidence(report, text="按合同期限确认收看维护费", page=1),),
                            major=True,
                            statement_status="current",
                        ),
                    ),
                )
            if request.purpose == "synthesize_views":
                assert len(request.source_conditions) == 1
                assert request.source_conditions[0].condition_id.endswith("business")
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="business-claim",
                            dimension="principal_business",
                            status="observed",
                            answer="公司经营有线电视网络。",
                            evidence=(
                                _evidence(report, text="公司经营有线电视网络", page=1).model_copy(
                                    update={"evidence_id": "business-evidence"}
                                ),
                            ),
                            condition_ids=("business",),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "business-claim",
                        "status": "pass",
                        "explanation": "整句有引文。",
                    },
                ),
            )

    def sized(request):
        if request.purpose == "synthesize_views" and len(request.source_conditions) >= 2:
            return 10**9
        return 100

    identity = direct_core_processing_identity()
    identity["request_reservation_limit"] = 65_536
    original = direct_reading.request_reservation
    direct_reading.request_reservation = sized
    try:
        result = _run(
            _Provider(),
            pages=({"page": 1, "text": text, "readable": True},),
            processing_identity=identity,
            report=report,
        )
    finally:
        direct_reading.request_reservation = original
    assert any(item.claim_id == "business-claim" for item in result.claims)
    assert any("major_condition_not_in_synthesis:" in gap and gap.endswith("fee") for gap in result.gaps)


def test_saved_source_conditions_are_drafted_without_rereading():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司经营有线电视网络，并按合同期限确认收看维护费。"
    condition = ReportSourceCondition(
        condition_id="business",
        dimension="principal_business",
        description="经营有线电视网络。",
        evidence=(
            _evidence(report, text="公司经营有线电视网络", page=1).model_copy(
                update={"evidence_id": "saved-business"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-1",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-1",
            purpose="source_inventory",
            source_conditions=(condition,),
        ).model_dump(mode="json"),
    }

    class _Provider:
        offline_only = True

        def __init__(self):
            self.purposes = []

        def interpret(self, request):
            self.purposes.append(request.purpose)
            assert request.purpose != "source_inventory"
            if request.purpose == "synthesize_views":
                assert request.source_conditions
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="business-claim",
                            dimension="principal_business",
                            status="observed",
                            answer="公司经营有线电视网络。",
                            evidence=(
                                _evidence(report, text="公司经营有线电视网络", page=1).model_copy(
                                    update={"evidence_id": "draft-business"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "business-claim",
                        "status": "pass",
                        "explanation": "整句有引文。",
                    },
                    {
                        "target_type": "condition",
                        "target_id": request.source_conditions[0].condition_id,
                        "status": "pass",
                        "explanation": "已由主张覆盖。",
                        "covers_target_type": "claim",
                        "covers_target_id": "business-claim",
                    },
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["isolated_claim_ids"] = ["claim_cond_sc_01"]
    provider = _Provider()
    result = _run(
        provider,
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert provider.purposes[0] == "synthesize_views"
    assert "source_inventory" not in provider.purposes
    rendered = research_views(result)
    claim = rendered["views"]["company_profile"]["claims"][0]
    assert claim["claim_id"] == "business-claim"
    assert "公司经营有线电视网络" in claim["answer"]
    assert claim["review_state"] == "verified"
    assert any(gap == "isolated_unreliable_claim:claim_cond_sc_01" for gap in result.gaps)


def test_a_kept_claim_survives_and_a_retracted_claim_is_not_verified():
    import json

    from research.company_profile.contracts import ReportSourceCondition, ReportViewCheck

    report = _report()
    text = "有线电视收看维护收入 724,769,470.04。公司为对方销售终端。"
    kept = ReportViewClaim(
        claim_id="kept-role",
        dimension="supply_chain_role",
        status="observed",
        answer="公司经营有线电视网络。",
        evidence=(
            _evidence(report, text="公司经营有线电视网络", page=1).model_copy(
                update={"evidence_id": "kept-evidence"}
            ),
        ),
    )
    retracted = ReportViewClaim(
        claim_id="bad-direction",
        dimension="products_services",
        status="observed",
        answer="公司向其销售终端。",
        evidence=(
            _evidence(report, text="公司为对方销售终端", page=1).model_copy(
                update={"evidence_id": "bad-evidence"}
            ),
        ),
    )
    amount = ReportSourceCondition(
        condition_id="maintenance-revenue",
        dimension="revenue_model",
        description="收看维护收入。",
        evidence=(
            _evidence(report, text="有线电视收看维护收入 724,769,470.04", page=1).model_copy(
                update={"evidence_id": "amount-evidence"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-source",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-source",
            purpose="source_inventory",
            source_conditions=(amount,),
        ).model_dump(mode="json"),
    }

    class _Provider:
        offline_only = True

        def __init__(self):
            self.sent = []

        def interpret(self, request):
            self.sent.append(request)
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="maintenance-claim",
                            dimension="revenue_model",
                            status="observed",
                            answer="本期有线电视收看维护收入 724,769,470.04 元。",
                            evidence=(
                                _evidence(
                                    report,
                                    text="有线电视收看维护收入 724,769,470.04",
                                    page=1,
                                ).model_copy(update={"evidence_id": "maintenance-draft"}),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "金额和方向与引文一致。",
                    }
                    for claim in request.claims
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [
            json.loads(kept.model_dump_json()),
            json.loads(retracted.model_dump_json()),
        ],
        "checks": [
            ReportViewCheck(
                target_type="claim",
                target_id="kept-role",
                status="pass",
                explanation="原核验。",
            ).model_dump(mode="json"),
            ReportViewCheck(
                target_type="claim",
                target_id="bad-direction",
                status="pass",
                explanation="旧的错误通过。",
            ).model_dump(mode="json"),
        ],
    }
    identity["retract_claim_ids"] = ["bad-direction"]
    provider = _Provider()
    result = _run(
        provider,
        pages=({"page": 1, "text": text + "公司经营有线电视网络。", "readable": True},),
        processing_identity=identity,
        report=report,
    )
    rendered = research_views(result)
    found = {
        claim["claim_id"]: claim
        for view in rendered["views"].values()
        for claim in view["claims"]
    }
    assert found["kept-role"]["review_state"] == "verified"
    assert found["kept-role"]["answer"] == "公司经营有线电视网络。"
    assert found["bad-direction"]["review_state"] != "verified"
    assert "向其销售" in found["bad-direction"]["answer"]
    sent_text = " ".join(
        evidence.anchor.bounded_quote
        for request in provider.sent
        if request.purpose == "synthesize_views"
        for item in request.source_conditions
        for evidence in item.evidence
    )
    assert "724,769,470.04" in sent_text
    assert "724,769,470.04" in found["maintenance-claim"]["answer"]


def test_unchanged_claim_keeps_its_check_across_two_disk_loads(tmp_path):
    import json

    from research.company_profile.bounded_entry import load_saved_draft

    root = tmp_path / "check-carry"
    root.mkdir(parents=True, exist_ok=True)
    claim = {
        "claim_id": "kept-role",
        "dimension": "supply_chain_role",
        "status": "observed",
        "answer": "公司经营有线电视网络。",
        "condition_ids": [],
        "supporting_record_ids": [],
        "evidence": [
            {
                "evidence_id": "kept-evidence",
                "page": 1,
                "anchor": {"bounded_quote": "公司经营有线电视网络。"},
            }
        ],
        "limitation": None,
    }
    bad = {
        **claim,
        "claim_id": "bad-direction",
        "answer": "公司向其销售终端。",
    }
    check = {
        "target_type": "claim",
        "target_id": "kept-role",
        "status": "pass",
        "explanation": "第一次核验。",
    }
    bad_check = {**check, "target_id": "bad-direction", "explanation": "错误通过。"}

    def write(path, checks):
        path.write_text(
            json.dumps(
                {
                    "profiles": [
                        {
                            "report": {"document_version": "doc-1"},
                            "three_views": {
                                "views": {
                                    "supply_chain_position": {
                                        "claims": [claim, bad]
                                    }
                                },
                                "call_receipts": [
                                    {
                                        "purpose": "verify_views",
                                        "status": "success",
                                        "response_payload": {"checks": checks},
                                    }
                                ],
                            },
                        }
                    ]
                },
                ensure_ascii=False,
            )
        )

    first = root / "first.json"
    second = root / "second.json"
    write(first, [check, bad_check])
    binding = {
        "accepted_draft_query": str(first),
        "asset": {"content_hash": "doc-1"},
        "verification_history_queries": [str(first)],
        "retract_claim_ids": ["bad-direction"],
    }
    loaded = load_saved_draft(binding)
    assert [item["target_id"] for item in loaded["checks"]] == ["kept-role"]
    write(
        second,
        [
            {
                "target_type": "claim",
                "target_id": "kept-role",
                "status": "pass",
                "explanation": "第一次核验。",
            }
        ],
    )
    again = load_saved_draft(
        {
            **binding,
            "accepted_draft_query": str(second),
            "verification_history_queries": [str(first), str(second)],
        }
    )
    assert [item["target_id"] for item in again["checks"]] == ["kept-role"]
    changed_path = root / "changed.json"
    changed = json.loads(second.read_text())
    changed["profiles"][0]["three_views"]["views"]["supply_chain_position"]["claims"][0][
        "answer"
    ] = "公司改写了经营范围。"
    changed["profiles"][0]["three_views"]["call_receipts"] = [
        {"purpose": "verify_views", "status": "success", "response_payload": {"checks": []}}
    ]
    changed_path.write_text(json.dumps(changed, ensure_ascii=False))
    lost = load_saved_draft(
        {
            **binding,
            "accepted_draft_query": str(changed_path),
            "verification_history_queries": [str(first)],
        }
    )
    assert lost["checks"] == []


def test_same_local_claim_id_keeps_both_revenue_answers():
    import json

    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    kept = ReportViewClaim(
        claim_id="kept-role",
        dimension="supply_chain_role",
        status="observed",
        answer="公司经营有线电视网络。",
        evidence=(
            _evidence(report, text="公司经营有线电视网络", page=1).model_copy(
                update={"evidence_id": "kept-evidence"}
            ),
        ),
    )
    first = ReportSourceCondition(
        condition_id="info-revenue",
        dimension="products_services",
        description="信息业务收入。",
        evidence=(
            _evidence(report, text="信息业务收入 897,897,997.75", page=1).model_copy(
                update={"evidence_id": "info-evidence"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    second = ReportSourceCondition(
        condition_id="maintenance-revenue",
        dimension="products_services",
        description="收看维护收入。",
        evidence=(
            _evidence(report, text="有线电视收看维护收入 724,769,470.04", page=1).model_copy(
                update={"evidence_id": "maintenance-evidence"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-source",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-source",
            purpose="source_inventory",
            source_conditions=(first, second),
        ).model_dump(mode="json"),
    }

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "synthesize_views":
                quote = request.source_conditions[0].evidence[0].anchor.bounded_quote
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="claim-1",
                            dimension="products_services",
                            status="observed",
                            answer=f"本期{quote}元。",
                            condition_ids=(request.source_conditions[0].condition_id,),
                            evidence=(
                                _evidence(report, text=quote, page=1).model_copy(
                                    update={"evidence_id": f"draft-{quote[:4]}"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "金额与引文一致。",
                    }
                    for claim in request.claims
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(kept.model_dump_json())],
        "checks": [],
    }
    result = _run(
        _Provider(),
        pages=(
            {
                "page": 1,
                "text": "信息业务收入 897,897,997.75。有线电视收看维护收入 724,769,470.04。公司经营有线电视网络。",
                "readable": True,
            },
        ),
        processing_identity=identity,
        report=report,
    )
    answers = [claim.answer for claim in result.claims]
    assert sum("897,897,997.75" in answer for answer in answers) == 1
    assert sum("724,769,470.04" in answer for answer in answers) == 1
    rendered = research_views(result)
    found = {
        claim["answer"]: claim
        for view in rendered["views"].values()
        for claim in view["claims"]
        if "897,897,997.75" in claim["answer"] or "724,769,470.04" in claim["answer"]
    }
    assert len(found) == 2
    assert {claim["review_state"] for claim in found.values()} == {"verified"}
    assert len({claim["claim_id"] for claim in found.values()}) == 2


def test_an_unclear_renamed_claim_does_not_replace_the_other_answer():
    import json

    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    kept = ReportViewClaim(
        claim_id="kept-role",
        dimension="supply_chain_role",
        status="observed",
        answer="公司经营有线电视网络。",
        evidence=(
            _evidence(report, text="公司经营有线电视网络", page=1).model_copy(
                update={"evidence_id": "kept-evidence"}
            ),
        ),
    )
    first = ReportSourceCondition(
        condition_id="info-revenue",
        dimension="products_services",
        description="信息业务收入。",
        evidence=(
            _evidence(report, text="信息业务收入 897,897,997.75", page=1).model_copy(
                update={"evidence_id": "info-evidence"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    second = ReportSourceCondition(
        condition_id="maintenance-revenue",
        dimension="products_services",
        description="收看维护收入。",
        evidence=(
            _evidence(report, text="有线电视收看维护收入 724,769,470.04", page=1).model_copy(
                update={"evidence_id": "maintenance-evidence"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-source",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-source",
            purpose="source_inventory",
            source_conditions=(first, second),
        ).model_dump(mode="json"),
    }

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "synthesize_views":
                quote = request.source_conditions[0].evidence[0].anchor.bounded_quote
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="claim-1",
                            dimension="products_services",
                            status="observed",
                            answer=f"本期{quote}元。",
                            condition_ids=(request.source_conditions[0].condition_id,),
                            evidence=(
                                _evidence(report, text=quote, page=1).model_copy(
                                    update={"evidence_id": f"draft-{quote[:4]}"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass" if "897,897,997.75" in claim.answer else "unclear",
                        "explanation": "只否定第二条。",
                    }
                    for claim in request.claims
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(kept.model_dump_json())],
        "checks": [],
    }
    result = _run(
        _Provider(),
        pages=(
            {
                "page": 1,
                "text": "信息业务收入 897,897,997.75。有线电视收看维护收入 724,769,470.04。公司经营有线电视网络。",
                "readable": True,
            },
        ),
        processing_identity=identity,
        report=report,
    )
    rendered = research_views(result)
    found = {
        claim["answer"]: claim["review_state"]
        for view in rendered["views"].values()
        for claim in view["claims"]
        if "897,897,997.75" in claim["answer"] or "724,769,470.04" in claim["answer"]
    }
    assert found["本期信息业务收入 897,897,997.75元。"] == "verified"
    assert found["本期有线电视收看维护收入 724,769,470.04元。"] != "verified"
    assert sum("724,769,470.04" in claim.answer for claim in result.claims) == 1


def test_parent_heading_is_kept_as_the_section_limit():
    from research.company_profile.report_interpretation import _bind_source_evidence

    report = _report()
    evidence = _evidence(
        report,
        text="公司主营业务收入及其他业务收入主要在某一时点确认。",
        page=2,
    )
    bound = _bind_source_evidence(
        evidence,
        report,
        (
            {"page": 1, "text": "十九、母公司财务报表主要项目注释\n其他说明。", "readable": True},
            {
                "page": 2,
                "text": "公司主营业务收入及其他业务收入主要在某一时点确认。",
                "readable": True,
            },
        ),
    )
    assert bound is not None
    assert "母公司" in bound.section_title


def test_core_groups_place_the_role_before_later_subsidiaries():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import _business_groups

    report = _report()

    def condition(condition_id, dimension):
        return ReportSourceCondition(
            condition_id=condition_id,
            dimension=dimension,
            description=condition_id,
            evidence=(_evidence(report, text="公司经营业务。", page=1),),
            major=True,
            statement_status="current",
        )

    groups = _business_groups(
        [
            condition("principal-a", "principal_business"),
            condition("principal-b", "principal_business"),
            condition("revenue", "revenue_model"),
            condition("role", "supply_chain_role"),
        ]
    )
    first = {item.dimension for item in groups[0]}
    assert {"principal_business", "revenue_model", "supply_chain_role"} <= first
    assert all(item.condition_id != "principal-b" for item in groups[0])


def test_one_group_is_verified_before_the_next_group_is_drafted():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司经营整车制造并确认整车收入。公司同时经营零部件。公司向上游采购钢材。"

    def condition(condition_id, dimension, quote):
        return ReportSourceCondition(
            condition_id=condition_id,
            dimension=dimension,
            description=condition_id,
            evidence=(
                _evidence(report, text=quote, page=1).model_copy(
                    update={"evidence_id": condition_id}
                ),
            ),
            major=True,
            statement_status="current",
        )

    saved = (
        condition("principal-a", "principal_business", "公司经营整车制造"),
        condition("principal-b", "principal_business", "公司同时经营零部件"),
        condition("revenue", "revenue_model", "确认整车收入"),
        condition("role", "supply_chain_role", "公司向上游采购钢材"),
    )

    class _Provider:
        offline_only = True

        def __init__(self):
            self.calls = []

        def interpret(self, request):
            ids = tuple(item.condition_id for item in request.source_conditions)
            self.calls.append((request.purpose, ids))
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=saved,
                )
            if request.purpose == "synthesize_views":
                if sum(item[0] == "synthesize_views" for item in self.calls) > 1:
                    raise ValueError("later-group-blocked")
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="core-group",
                            dimension="principal_business",
                            status="observed",
                            answer="公司经营整车制造，并向上游采购钢材。",
                            evidence=saved[0].evidence,
                            condition_ids=("principal-a", "role"),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "core-group",
                        "status": "pass",
                        "explanation": "第一组已核。",
                        "covers_target_type": "claim",
                        "covers_target_id": "core-group",
                    },
                    {
                        "target_type": "condition",
                        "target_id": "principal-a",
                        "status": "pass",
                        "explanation": "第一组覆盖。",
                        "covers_target_type": "claim",
                        "covers_target_id": "core-group",
                    },
                ),
            )

    identity = direct_core_processing_identity()
    identity["discovery_scope"] = "full_text"
    provider = _Provider()
    result = _run(
        provider,
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    purposes = [item[0] for item in provider.calls]
    synth = [index for index, purpose in enumerate(purposes) if purpose == "synthesize_views"]
    verified = [index for index, purpose in enumerate(purposes) if purpose == "verify_views"]
    assert len(synth) == 1
    assert verified and verified[0] > synth[0]

    def has(ids, name):
        return any(item == name or str(item).endswith(":" + name) for item in ids)

    first_ids = provider.calls[synth[0]][1]
    assert has(first_ids, "role") and has(first_ids, "principal-a")
    assert has(first_ids, "principal-b") and has(first_ids, "revenue")
    kept = next(item for item in result.claims if item.claim_id == "core-group")
    assert kept.status == "observed"
    assert any("major_condition_not_verified" in gap for gap in result.gaps)


def test_chapter_select_refuses_saved_semantics():
    import pytest

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    identity["accepted_source_receipts"] = (
        {
            "request_id": "old-source",
            "source_pages": [1],
            "response_payload": {
                "request_id": "old-source",
                "purpose": "source_inventory",
                "source_conditions": [],
            },
        },
    )

    class _Provider:
        offline_only = True

        def interpret(self, request):
            raise AssertionError("saved semantics must not reach the model")

    with pytest.raises(ValueError, match="chapter_select_refuses_saved_semantics"):
        _run(
            _Provider(),
            pages=({"page": 1, "text": "公司主营造纸。", "readable": True},),
            processing_identity=identity,
        )


def test_cross_chapter_majors_enter_one_core_draft_and_merged_verify():
    from research.company_profile import direct_reading
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    business = "公司主营纸袋纸制造并向客户销售。"
    revenue = "本期营业收入为100万元。"
    input_text = "公司采购木片作为主要原料。"
    role = "公司处于造纸产业链中游并向下游客户供货。"

    def condition(condition_id, dimension, quote, page):
        return ReportSourceCondition(
            condition_id=condition_id,
            dimension=dimension,
            description=quote,
            evidence=(
                _evidence(report, text=quote, page=page).model_copy(
                    update={"evidence_id": condition_id}
                ),
            ),
            major=True,
            statement_status="current",
        )

    saved = (
        condition("principal", "principal_business", business, 1),
        condition("product", "products_services", business, 1),
        condition("revenue", "revenue_model", revenue, 2),
        condition("input", "input_exposure", input_text, 2),
        condition("role", "supply_chain_role", role, 2),
    )

    class _Provider:
        offline_only = True

        def __init__(self):
            self.calls = []

        def interpret(self, request):
            pages = {int(page["page"]) for page in request.source_pages}
            ids = tuple(item.condition_id for item in request.source_conditions)
            self.calls.append((request.purpose, ids, pages))
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1, 2),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=saved,
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=item.condition_id,
                            dimension=item.dimension,
                            status="observed",
                            answer=item.description,
                            evidence=item.evidence,
                            condition_ids=(item.condition_id,),
                        )
                        for item in request.source_conditions
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "condition",
                        "target_id": item.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": item.condition_id,
                        "explanation": "跨章已核。",
                    }
                    for item in request.source_conditions
                )
                + tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "正文支持。",
                    }
                    for claim in request.claims
                ),
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    provider = _Provider()
    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = (
        lambda pages, report, identity, limit: [[dict(page, offset=0, scope="full_page") for page in pages]]
    )
    try:
        result = _run(
            provider,
            pages=(
                {"page": 1, "text": "管理层讨论与分析\n" + business, "readable": True},
                {"page": 2, "text": "营业收入与采购\n" + revenue + input_text + role, "readable": True},
                {"page": 3, "text": "附注列示子公司注册地址。", "readable": True},
            ),
            processing_identity=identity,
            report=report,
        )
    finally:
        direct_reading.source_discovery_slices = original
    synth = [item for item in provider.calls if item[0] == "synthesize_views"]
    verified = [item for item in provider.calls if item[0] == "verify_views"]
    assert len(synth) == 1
    assert {item.split(":")[-1] for item in synth[0][1]} == {
        "principal",
        "product",
        "revenue",
        "input",
        "role",
    }
    assert len(verified) == 1
    assert {item.split(":")[-1] for item in verified[0][1]} == {
        "principal",
        "product",
        "revenue",
        "input",
        "role",
    }
    assert any(item["page"] == 3 and item["status"] == "not_read" for item in result.page_coverage)
    assert result.draft_complete is True


def test_parent_limit_is_not_promoted_to_the_group():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    quote = "公司主营业务收入及其他业务收入主要在某一时点确认。"
    pages = (
        {"page": 1, "text": "十九、母公司财务报表主要项目注释\n其他说明。", "readable": True},
        {"page": 2, "text": "上海汽车集团股份有限公司2025年年度报告\n" + quote, "readable": True},
    )

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="parent-income",
                            dimension="revenue_model",
                            description="收入确认时点。",
                            evidence=(
                                _evidence(report, text=quote, page=2).model_copy(
                                    update={"evidence_id": "parent-income"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        ),
                    ),
                )
            if request.purpose == "synthesize_views":
                assert any("母公司" in str(page.get("text") or "") for page in request.source_pages)
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="timing",
                            dimension="revenue_model",
                            status="observed",
                            answer=quote,
                            evidence=(
                                _evidence(report, text=quote, page=2).model_copy(
                                    update={"evidence_id": "timing"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(),
            )

    identity = direct_core_processing_identity()
    identity["discovery_scope"] = "full_text"
    result = _run(_Provider(), pages=pages, processing_identity=identity, report=report)
    claim = next(item for item in result.claims if item.claim_id == "timing")
    assert claim.limitation != "subject_scope_omitted"
    assert "母公司财务报表" in claim.evidence[0].section_title


def test_a_later_heading_and_a_risk_line_do_not_replace_the_section():
    from research.company_profile.report_interpretation import _bind_source_evidence

    report = _report()
    quote = "公司聚焦公共安全、数智产业和智慧能源。"
    pages = (
        {
            "page": 1,
            "text": (
                "十九、母公司财务报表主要项目注释\n"
                "本公司已在本报告中详细描述可能存在的相关风险，敬请查阅第三节管理层讨论\n"
                "公司聚焦公共安全、数智产业和智慧能源。\n"
                "合并资产负债表\n"
            ),
            "readable": True,
        },
    )
    bound = _bind_source_evidence(
        _evidence(report, text=quote, page=1),
        report,
        pages,
    )
    assert bound is not None
    assert bound.section_title == "十九、母公司财务报表主要项目注释"


def test_full_text_packs_the_same_pages_once(monkeypatch):
    from research.company_profile.contracts import ReportSourceCondition
    import research.company_profile.direct_reading as direct

    report = _report()
    text = "公司经营整车制造。"
    calls = {"count": 0}
    original = direct.source_discovery_slices

    def counted(*args, **kwargs):
        calls["count"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(direct, "source_discovery_slices", counted)

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="vehicles",
                            dimension="principal_business",
                            description="整车。",
                            evidence=(_evidence(report, text=text, page=1),),
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
                            claim_id="vehicles",
                            dimension="principal_business",
                            status="observed",
                            answer=text,
                            evidence=(_evidence(report, text=text, page=1),),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(),
            )

    identity = direct_core_processing_identity()
    identity["discovery_scope"] = "full_text"
    _run(
        _Provider(),
        pages=(
            {"page": 1, "text": text, "readable": True},
            {"page": 2, "text": "公司另有金融业务。", "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    assert calls["count"] == 1


def test_shared_evidence_id_does_not_abort_group_verification():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    quote = "公司主营业务收入及其他业务收入主要在某一时点确认。"
    evidence = _evidence(report, text=quote, page=2).model_copy(
        update={"evidence_id": "shared-income"}
    )
    pages = (
        {"page": 1, "text": "十九、母公司财务报表主要项目注释\n其他说明。", "readable": True},
        {"page": 2, "text": "上海汽车集团股份有限公司2025年年度报告\n" + quote, "readable": True},
    )

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="parent-income",
                            dimension="revenue_model",
                            description="母公司收入确认时点。",
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
                            claim_id="timing",
                            dimension="revenue_model",
                            status="observed",
                            answer="母公司主营业务收入及其他业务收入主要在某一时点确认。",
                            evidence=(evidence,),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "timing",
                        "status": "pass",
                        "explanation": "保留了母公司限定。",
                    },
                ),
            )

    identity = direct_core_processing_identity()
    identity["discovery_scope"] = "full_text"
    result = _run(_Provider(), pages=pages, processing_identity=identity, report=report)
    claim = next(item for item in result.claims if item.claim_id == "timing")
    assert claim.status == "observed"
    assert "母公司" in claim.answer
    assert "母公司" in claim.evidence[0].section_title


def test_logical_identity_separates_entries_and_verification_targets():
    from research.company_profile.stage5_provider import _logical_report_request_id
    from utils.llm.budget import LogicalRecoveryPolicy, _recovery_block

    page = {"page": 12, "text": "公司经营有线电视网络。", "offset": 0, "scope": "full_page"}
    base = {
        "purpose": "verify_views",
        "source_scope": "full_context",
        "reading_strategy": "direct_core_v1",
        "source_pages": [page],
    }
    first = _logical_report_request_id({**base, "logical_entry_id": "company24", "logical_targets": ["role"]})
    same = _logical_report_request_id({**base, "logical_entry_id": "company24", "logical_targets": ["role"]})
    other_entry = _logical_report_request_id(
        {**base, "logical_entry_id": "company25", "logical_targets": ["role"]}
    )
    other_target = _logical_report_request_id(
        {**base, "logical_entry_id": "company24", "logical_targets": ["fee"]}
    )
    assert first == same
    assert len({first, other_entry, other_target}) == 3
    policy = LogicalRecoveryPolicy(max_recoveries=1, max_per_logical_request=1)
    history = [{"logical_request_id": first}, {"logical_request_id": first}]
    assert _recovery_block(history, first, policy) is not None
    assert _recovery_block(history, other_target, policy) is None


def test_full_report_slices_fit_one_entry():
    import gzip
    import json
    from pathlib import Path

    from research.company_profile.direct_reading import fulltext_entry_plan
    from research.company_profile.models import ReportIdentity

    checkpoint = json.loads(
        gzip.decompress(
            (
                Path("/home/python/Quote")
                / "openspec/changes/restore-company-profile-llm-led-generalization/b1-resource-evidence/iteration2/checkpoint.json.gz"
            ).read_bytes()
        )
    )
    pages = [
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(str(page.get("text") or "").strip()),
        }
        for page in checkpoint["pages"]
    ]
    report = ReportIdentity.model_validate(checkpoint["report"])
    identity = direct_core_processing_identity()
    identity["max_payload_utf8_bytes"] = 60_000
    identity["request_reservation_limit"] = 65_536
    identity["logical_request_timeout_seconds"] = 300
    identity["logical_entry_id"] = "company24"
    plan = fulltext_entry_plan(
        pages,
        report,
        identity,
        ledger=1_200_000,
        reservation_limit=65_536,
        request_cap=36,
    )
    assert plan["readable_pages"]
    assert set(plan["readable_pages"]) <= set(plan["covered_pages"])
    assert plan["fits_entry"]
    assert max(plan["source_units"]) <= 65_536


def _draft_query(claims, receipts):
    return {
        "profiles": [
            {
                "report": {"document_version": "doc-1"},
                "three_views": {
                    "views": {"company_profile": {"claims": claims}},
                    "call_receipts": receipts,
                },
            }
        ]
    }


def _verify_receipt(request_id, claims, checks):
    return {
        "request_id": request_id,
        "purpose": "verify_views",
        "status": "success",
        "response_payload": {"claims": claims, "checks": checks},
    }


def test_a_later_block_or_scope_change_removes_the_old_pass(tmp_path):
    import json
    from research.company_profile.bounded_entry import load_saved_draft

    def claim(claim_id, answer, title="经营情况", records=()):
        return {
            "claim_id": claim_id,
            "dimension": "principal_business",
            "status": "observed",
            "answer": answer,
            "evidence": [
                {
                    "page": 12,
                    "section_title": title,
                    "anchor": {"bounded_quote": answer[:8]},
                }
            ],
            "supporting_record_ids": list(records),
            "review_state": "verified",
        }

    kept = claim("kept", "公司经营公共安全业务。")
    blocked = claim("blocked", "公司经营数智产业。")
    shifted = claim("shifted", "公司经营智慧能源。", title="母公司财务报表主要项目注释")
    older = tmp_path / "older.json"
    newer = tmp_path / "newer.json"
    older.write_text(
        json.dumps(
            _draft_query(
                [kept, blocked, claim("shifted", "公司经营智慧能源。", title="合并财务报表")],
                [
                    _verify_receipt(
                        "old",
                        [kept, blocked, claim("shifted", "公司经营智慧能源。", title="合并财务报表")],
                        [
                            {"target_type": "claim", "target_id": "kept", "status": "pass", "explanation": "旧通过"},
                            {"target_type": "claim", "target_id": "blocked", "status": "pass", "explanation": "旧通过"},
                            {"target_type": "claim", "target_id": "shifted", "status": "pass", "explanation": "旧通过"},
                        ],
                    )
                ],
            )
        )
    )
    newer.write_text(
        json.dumps(
            _draft_query(
                [kept, blocked, shifted],
                [
                    _verify_receipt(
                        "new",
                        [blocked],
                        [
                            {
                                "target_type": "claim",
                                "target_id": "blocked",
                                "status": "block",
                                "explanation": "后置否定",
                            }
                        ],
                    )
                ],
            )
        )
    )
    loaded = load_saved_draft(
        {
            "accepted_draft_query": str(newer),
            "verification_history_queries": [str(older), str(newer)],
            "asset": {"content_hash": "doc-1"},
        }
    )
    passed = {item["target_id"] for item in loaded["checks"] if item["status"] == "pass"}
    assert "kept" in passed
    assert "blocked" not in passed
    assert "shifted" not in passed
    assert "review_state" not in loaded["claims"][0]


def test_a_requested_claim_without_a_check_does_not_keep_the_old_pass(tmp_path):
    import json
    from research.company_profile.bounded_entry import load_saved_draft

    body = {
        "claim_id": "income",
        "dimension": "revenue_model",
        "status": "observed",
        "answer": "公司实现营业收入。",
        "evidence": [{"page": 21, "section_title": "收入", "anchor": {"bounded_quote": "营业收入"}}],
        "supporting_record_ids": [],
    }
    older = tmp_path / "older.json"
    newer = tmp_path / "newer.json"
    older.write_text(
        json.dumps(
            _draft_query(
                [body],
                [
                    _verify_receipt(
                        "old",
                        [body],
                        [{"target_type": "claim", "target_id": "income", "status": "pass", "explanation": "通过"}],
                    )
                ],
            )
        )
    )
    newer.write_text(
        json.dumps(_draft_query([body], [_verify_receipt("new", [body], [])]))
    )
    loaded = load_saved_draft(
        {
            "accepted_draft_query": str(newer),
            "verification_history_queries": [str(older), str(newer)],
            "asset": {"content_hash": "doc-1"},
        }
    )
    assert not [item for item in loaded["checks"] if item.get("target_id") == "income" and item.get("status") == "pass"]


def test_empty_response_claims_use_saved_request_targets(tmp_path):
    import json
    from research.company_profile.bounded_entry import load_saved_draft

    body = {
        "claim_id": "income",
        "dimension": "revenue_model",
        "status": "observed",
        "answer": "公司实现营业收入。",
        "evidence": [{"page": 71, "section_title": "收入", "anchor": {"bounded_quote": "营业收入"}}],
        "supporting_record_ids": [],
    }
    other = {
        "claim_id": "kept",
        "dimension": "principal_business",
        "status": "observed",
        "answer": "公司提供金融服务。",
        "evidence": [{"page": 62, "section_title": "业务", "anchor": {"bounded_quote": "金融服务"}}],
        "supporting_record_ids": [],
    }
    older = tmp_path / "older.json"
    newer = tmp_path / "newer.json"
    older.write_text(
        json.dumps(
            _draft_query(
                [body, other],
                [
                    {
                        "request_id": "old",
                        "purpose": "verify_views",
                        "status": "success",
                        "request_targets": [
                            {**body, "target_type": "claim", "target_id": "income"},
                            {**other, "target_type": "claim", "target_id": "kept"},
                            {"target_type": "condition", "target_id": "fee"},
                        ],
                        "response_payload": {
                            "claims": [],
                            "checks": [
                                {"target_type": "claim", "target_id": "income", "status": "pass", "explanation": "旧通过"},
                                {"target_type": "claim", "target_id": "kept", "status": "pass", "explanation": "保持"},
                                {"target_type": "condition", "target_id": "fee", "status": "pass", "covers_target_id": "income"},
                            ],
                        },
                    }
                ],
            )
        )
    )
    newer.write_text(
        json.dumps(
            _draft_query(
                [body, other],
                [
                    {
                        "request_id": "new",
                        "purpose": "verify_views",
                        "status": "success",
                        "request_targets": [
                            {**body, "target_type": "claim", "target_id": "income"},
                            {"target_type": "condition", "target_id": "fee"},
                        ],
                        "response_payload": {"claims": [], "checks": []},
                    },
                    {
                        "request_id": "run:carried-checks",
                        "purpose": "verify_views",
                        "status": "success",
                        "carried_verification": True,
                        "response_payload": {
                            "claims": [],
                            "checks": [
                                {"target_type": "claim", "target_id": "income", "status": "pass", "explanation": "汇总复活"},
                                {"target_type": "condition", "target_id": "fee", "status": "pass", "covers_target_id": "income"},
                            ],
                        },
                    },
                ],
            )
        )
    )
    loaded = load_saved_draft(
        {
            "accepted_draft_query": str(newer),
            "verification_history_queries": [str(older), str(newer)],
            "asset": {"content_hash": "doc-1"},
        }
    )
    assert not any(item.get("target_id") == "income" and item.get("status") == "pass" for item in loaded["checks"])
    assert not any(item.get("target_id") == "fee" and item.get("status") == "pass" for item in loaded["checks"])
    assert any(item.get("target_id") == "kept" and item.get("status") == "pass" for item in loaded["checks"])


def test_a_saved_draft_verifies_related_claims_in_one_request():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司经营整车制造，并向上游采购钢材。"
    business = ReportSourceCondition(
        condition_id="vehicles",
        dimension="principal_business",
        description="整车制造。",
        evidence=(_evidence(report, text="公司经营整车制造", page=1).model_copy(update={"evidence_id": "vehicles-evidence"}),),
        major=True,
        statement_status="current",
    )
    role = ReportSourceCondition(
        condition_id="steel",
        dimension="supply_chain_role",
        description="向上游采购钢材。",
        evidence=(_evidence(report, text="向上游采购钢材", page=1).model_copy(update={"evidence_id": "steel-evidence"}),),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-core",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-core",
            purpose="source_inventory",
            source_conditions=(business, role),
        ).model_dump(mode="json"),
    }

    def saved_claim(claim_id, answer, quote):
        return {
            "claim_id": claim_id,
            "dimension": "principal_business" if claim_id == "vehicles" else "supply_chain_role",
            "status": "observed",
            "answer": answer,
            "limitation": "pending_verification",
            "evidence": [
                _evidence(report, text=quote, page=1).model_copy(update={"evidence_id": claim_id}).model_dump(mode="json")
            ],
            "condition_ids": [claim_id],
            "supporting_record_ids": [],
        }

    class _Provider:
        offline_only = True

        def __init__(self):
            self.verified = []

        def interpret(self, request):
            if request.purpose == "verify_views":
                self.verified.append(tuple(claim.claim_id for claim in request.claims))
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "同组已核",
                    }
                    for claim in request.claims
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [
            saved_claim("vehicles", "公司经营整车制造。", "公司经营整车制造"),
            saved_claim("steel", "公司向上游采购钢材。", "向上游采购钢材"),
        ],
        "checks": [],
    }
    provider = _Provider()
    _run(
        provider,
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert any({"vehicles", "steel"} <= set(item) for item in provider.verified)


def test_a_second_generation_keeps_an_earlier_real_pass(tmp_path):
    import json
    from research.company_profile.bounded_entry import load_saved_draft

    body = {
        "claim_id": "kept",
        "dimension": "revenue_model",
        "status": "observed",
        "answer": "知识服务以机构订阅为主。",
        "evidence": [{"page": 12, "section_title": "经营", "anchor": {"bounded_quote": "机构订阅"}}],
        "supporting_record_ids": [],
    }
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text(
        json.dumps(
            _draft_query(
                [body],
                [
                    {
                        "request_id": "verify-1",
                        "purpose": "verify_views",
                        "status": "success",
                        "request_targets": [{**body, "target_type": "claim", "target_id": "kept"}],
                        "response_payload": {
                            "claims": [],
                            "checks": [
                                {"target_type": "claim", "target_id": "kept", "status": "pass", "explanation": "原核验"}
                            ],
                        },
                    }
                ],
            )
        )
    )
    second.write_text(
        json.dumps(
            _draft_query(
                [body],
                [
                    {
                        "request_id": "run:carried-checks",
                        "purpose": "verify_views",
                        "status": "success",
                        "carried_verification": True,
                        "response_payload": {"claims": [], "checks": []},
                    }
                ],
            )
        )
    )
    loaded = load_saved_draft(
        {
            "accepted_draft_query": str(second),
            "verification_history_queries": [str(first), str(second)],
            "asset": {"content_hash": "doc-1"},
        }
    )
    assert any(item.get("target_id") == "kept" and item.get("status") == "pass" for item in loaded["checks"])
    assert any(item.get("request_id") == "verify-1" for item in loaded["verify_receipts"])


def test_role_closeout_sends_source_text_not_page_metadata():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司提供智能检查系统和知网机构订阅服务，并向机构客户交付。"
    condition = ReportSourceCondition(
        condition_id="service",
        dimension="products_services",
        description="检查系统与订阅服务。",
        evidence=(_evidence(report, text="公司提供智能检查系统", page=1),),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-service",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-service",
            purpose="source_inventory",
            source_conditions=(condition,),
        ).model_dump(mode="json"),
    }

    class _Provider:
        offline_only = True

        def __init__(self):
            self.pages = []
            self.purposes = []

        def interpret(self, request):
            self.purposes.append(request.purpose)
            self.pages.append(tuple(page.get("text") for page in request.source_pages))
            if request.purpose == "source_inventory":
                raise AssertionError("role closeout must not send another empty inventory")
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                claims=(
                    ReportViewClaim(
                        claim_id="role",
                        dimension="supply_chain_role",
                        status="observed",
                        answer="公司向机构客户提供智能检查系统和订阅服务。",
                        evidence=(_evidence(report, text="公司提供智能检查系统", page=1),),
                    ),
                )
                if request.purpose == "synthesize_views"
                else (),
                checks=(
                    (
                        {
                            "target_type": "claim",
                            "target_id": "role",
                            "status": "pass",
                            "explanation": "角色有原文。",
                        },
                    )
                    if request.purpose == "verify_views"
                    else ()
                ),
            )

    identity = direct_core_processing_identity()
    identity["core_gap_closeout"] = True
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [
            {
                "claim_id": "service",
                "dimension": "products_services",
                "status": "observed",
                "answer": "公司提供智能检查系统和知网机构订阅服务。",
                "evidence": [
                    _evidence(report, text="公司提供智能检查系统", page=1).model_dump(mode="json")
                ],
                "supporting_record_ids": [],
            }
        ],
        "checks": [],
        "verify_receipts": [],
    }
    provider = _Provider()
    _run(
        provider,
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert "source_inventory" not in provider.purposes
    assert any(text in (page or "") for pages in provider.pages for page in pages)
    assert "status" not in "".join(page or "" for pages in provider.pages for page in pages)


def test_the_same_sentence_cannot_stay_verified_on_another_id():
    report = _report()
    sentence = "公司主营业务包括整车业务。2025年度，整车业务实现主营业务收入57,126,986,013.91元。"
    evidence = (_evidence(report, text="整车业务 57,126,986,013.91", page=203),)
    isolated = ReportViewClaim(
        claim_id="claim-principal-business-1",
        dimension="principal_business",
        status="unclear",
        answer=sentence,
        evidence=evidence,
        limitation="claim_or_dependency_not_verified",
    )
    published = isolated.model_copy(
        update={"claim_id": "claim-principal-business", "status": "observed", "limitation": None}
    )
    purchase = ReportViewClaim(
        claim_id="purchase",
        dimension="input_exposure",
        status="observed",
        answer="向关联方采购商品947,841,284.24元。",
        evidence=(_evidence(report, text="采购商品", page=222),),
    )
    rendered = research_views(
        ReportInterpretation(
            report=report,
            processing_identity=direct_core_processing_identity(),
            execution_mode="offline_fake",
            claims=(isolated, published, purchase),
            checks=(
                ReportViewCheck(
                    target_type="claim",
                    target_id="claim-principal-business",
                    status="pass",
                    explanation="金额与表一致。",
                ),
                ReportViewCheck(
                    target_type="claim",
                    target_id="purchase",
                    status="pass",
                    explanation="采购额有原文。",
                ),
            ),
        )
    )
    company = {
        item["claim_id"]: item["review_state"]
        for item in rendered["views"]["company_profile"]["claims"]
    }
    assert company["claim-principal-business"] == "not_reliable"
    assert company["claim-principal-business-1"] == "not_reliable"
    supply_ids = [item["claim_id"] for item in rendered["views"]["supply_chain_position"]["claims"]]
    product_ids = [item["claim_id"] for item in rendered["views"]["product_exposure"]["claims"]]
    assert supply_ids == ["purchase"]
    assert product_ids == ["purchase"]


def test_a_strategy_sentence_can_be_withdrawn_without_dropping_the_adjacent_fact():
    report = _report()
    strategy = ReportViewClaim(
        claim_id="claim-products-services__2",
        dimension="products_services",
        status="unclear",
        answer="公司绿色低碳业务涉及纯电动、混合动力产品，并推进燃料电池产品投放及V2G应用探索。",
        evidence=(_evidence(report, text="加快燃料电池产品投放，初步探索 V2G", page=28),),
        limitation="retracted_pending_correction",
    )
    other_wording = ReportViewClaim(
        claim_id="products_services-001",
        dimension="products_services",
        status="unclear",
        answer="公司推进纯电动、混合动力及燃料电池等整车产品投放，并初步探索 V2G 应用。",
        evidence=(_evidence(report, text="加快燃料电池产品投放，初步探索 V2G", page=28),),
        limitation="claim_or_dependency_not_verified",
    )
    plates = ReportViewClaim(
        claim_id="claim-principal-business__2",
        dimension="principal_business",
        status="observed",
        answer="公司业务涉及整车板块、零部件板块、移动出行和服务板块以及金融板块。",
        evidence=(_evidence(report, text="整车板块发挥龙头牵引作用", page=28),),
    )
    sales = ReportViewClaim(
        claim_id="nev-sales",
        dimension="products_services",
        status="observed",
        answer="报告期内新能源汽车乘用车累计销量1,481,504辆。",
        evidence=(_evidence(report, text="新能源汽车乘用车累计销量1,481,504辆", page=18),),
    )
    rendered = research_views(
        ReportInterpretation(
            report=report,
            processing_identity=direct_core_processing_identity(),
            execution_mode="offline_fake",
            claims=(strategy, other_wording, plates, sales),
            checks=(
                ReportViewCheck(
                    target_type="claim",
                    target_id="claim-products-services__2",
                    status="pass",
                    explanation="旧通过。",
                ),
                ReportViewCheck(
                    target_type="claim",
                    target_id="claim-principal-business__2",
                    status="pass",
                    explanation="板块概括有原文。",
                ),
                ReportViewCheck(
                    target_type="claim",
                    target_id="nev-sales",
                    status="pass",
                    explanation="销量有原文。",
                ),
            ),
        )
    )
    states = {
        item["claim_id"]: item["review_state"]
        for view in rendered["views"].values()
        for item in view["claims"]
    }
    assert states["claim-products-services__2"] == "not_reliable"
    assert states["products_services-001"] == "not_reliable"
    assert states["claim-principal-business__2"] == "verified"
    assert states["nev-sales"] == "verified"


def test_a_saved_query_keeps_a_real_receipt_without_a_hand_merge(tmp_path):
    import json

    from research.company_profile.bounded_entry import load_saved_draft

    body = {
        "claim_id": "kept",
        "dimension": "revenue_model",
        "status": "observed",
        "answer": "知识服务以机构订阅为主。",
        "evidence": [{"page": 12, "section_title": "经营", "anchor": {"bounded_quote": "机构订阅"}}],
        "supporting_record_ids": [],
    }
    saved = tmp_path / "saved.json"
    saved.write_text(
        json.dumps(
            _draft_query(
                [body],
                [
                    {
                        "request_id": "verify-1",
                        "purpose": "verify_views",
                        "status": "success",
                        "physical_request": False,
                        "reused_verification": True,
                        "request_targets": [{**body, "target_type": "claim", "target_id": "kept"}],
                        "response_payload": {
                            "claims": [],
                            "checks": [
                                {
                                    "target_type": "claim",
                                    "target_id": "kept",
                                    "status": "pass",
                                    "explanation": "原核验",
                                }
                            ],
                        },
                    },
                    {
                        "request_id": "run:carried-checks",
                        "purpose": "verify_views",
                        "status": "success",
                        "carried_verification": True,
                        "response_payload": {"claims": [], "checks": []},
                    },
                ],
            )
        )
    )
    loaded = load_saved_draft(
        {
            "accepted_draft_query": str(saved),
            "verification_history_queries": [str(saved)],
            "asset": {"content_hash": "doc-1"},
        }
    )
    assert any(item.get("target_id") == "kept" and item.get("status") == "pass" for item in loaded["checks"])
    assert any(item.get("request_id") == "verify-1" for item in loaded["verify_receipts"])


def test_a_supplementary_detail_can_remain_while_a_missing_major_business_blocks():
    from research.company_profile.contracts import ReportSourceCondition, ReportViewCheck
    from research.company_profile.direct_reading import _completeness_gaps

    report = _report()
    claims = [
        ReportViewClaim(
            claim_id=claim_id,
            dimension=dimension,
            status="observed",
            answer="公司提供云服务并取得云服务收入。",
            evidence=(_evidence(report, text="公司提供云服务", page=1),),
        )
        for claim_id, dimension in (
            ("business", "principal_business"),
            ("product", "products_services"),
            ("revenue", "revenue_model"),
            ("input", "input_exposure"),
            ("role", "supply_chain_role"),
        )
    ]
    footnote = ReportSourceCondition(
        condition_id="footnote",
        dimension="products_services",
        description="附注中的次要注册范围。",
        evidence=(_evidence(report, text="注册范围包括技术服务。", page=2),),
        major=True,
        statement_status="current",
    )
    other_business = ReportSourceCondition(
        condition_id="design",
        dimension="principal_business",
        description="另一主要业务是工业设计。",
        evidence=(_evidence(report, text="工业设计服务面向设备制造客户。", page=3),),
        major=True,
        statement_status="current",
    )
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id=claim.claim_id,
            status="pass",
            explanation="核心主张已核。",
        )
        for claim in claims
    ] + [
        ReportViewCheck(
            target_type="condition",
            target_id="footnote",
            status="supplementary",
            explanation="不改变主营、变现、收入、投入或角色。",
        )
    ]
    closed = _completeness_gaps(claims, [footnote], checks)
    assert closed == ["supplementary_gap:footnote"]
    blocked = _completeness_gaps(claims, [footnote, other_business], checks)
    assert "supplementary_gap:footnote" in blocked
    assert "major_condition_not_verified:design" in blocked


def _one_page_slices(pages, report, identity, limit):
    return [
        [
            {
                "page": page["page"],
                "text": page["text"],
                "readable": True,
                "offset": 0,
                "scope": "full_page",
            }
        ]
        for page in pages
    ]


def test_core_success_survives_the_next_package_timeout_and_exports():
    from research.company_profile import direct_reading

    report = _report()
    kept = "公司主营影视制作并取得制作收入。"
    other = "附注列示子公司注册地。"
    progress = []

    class _Provider:
        offline_only = True

        def __init__(self):
            self.calls = []

        def interpret(self, request):
            self.calls.append(request)
            pages = tuple(int(page["page"]) for page in request.source_pages)
            if 2 in pages:
                raise ValueError("card_recovery_exhausted")
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="kept-business",
                            dimension="principal_business",
                            status="observed",
                            answer="公司主营影视制作。",
                            evidence=(_evidence(report, text=kept, page=1),),
                        ),
                    ),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        __import__(
                            "research.company_profile.contracts",
                            fromlist=["ReportSourceCondition"],
                        ).ReportSourceCondition(
                            condition_id="kept-business",
                            dimension="principal_business",
                            description="主营是影视制作。",
                            evidence=(_evidence(report, text=kept, page=1),),
                            major=True,
                            statement_status="current",
                        ),
                        __import__(
                            "research.company_profile.contracts",
                            fromlist=["ReportSourceCondition"],
                        ).ReportSourceCondition(
                            condition_id="kept-revenue",
                            dimension="revenue_model",
                            description="制作收入已经实现。",
                            evidence=(_evidence(report, text=kept, page=1),),
                            major=True,
                            statement_status="current",
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "kept-business",
                        "status": "pass",
                        "explanation": "主营仍在。",
                    },
                ),
            )

    provider = _Provider()
    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = _one_page_slices
    try:
        result = _run(
            provider,
            pages=(
                {"page": 1, "text": kept, "readable": True},
                {"page": 2, "text": other, "readable": True},
            ),
            processing_identity=direct_core_processing_identity(),
            report=report,
            on_progress=progress.append,
        )
    finally:
        direct_reading.source_discovery_slices = original
    assert any(item.claim_id == "kept-business" and item.status == "observed" for item in result.claims)
    assert any(
        any(claim.claim_id == "kept-business" for claim in snapshot.claims)
        for snapshot in progress
    )
    assert sum(1 for call in provider.calls if 2 in {int(page["page"]) for page in call.source_pages}) == 1
    assert result.draft_complete is False
    assert result.real_semantic_complete is False
    assert any(gap == "necessary_unread:2" for gap in result.gaps)


def test_verified_core_leaves_supplementary_unread_and_a_missing_core_does_not():
    from decimal import Decimal
    import time

    from research.company_profile import direct_reading
    from research.company_profile.contracts import ReportSourceCondition
    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget

    report = _report()
    sentences = {
        "principal_business": "公司主营影视制作。",
        "products_services": "公司提供设备租赁服务。",
        "revenue_model": "公司营业收入为100万元。",
        "input_exposure": "公司采购原材料。",
        "supply_chain_role": "公司向下游客户交付影片。",
    }
    page_text = "".join(sentences.values())

    class _Provider:
        def __init__(self, budget):
            self.run_budget = budget
            self.execution_authorization = {"status": "authorized"}
            self.calls = []

        def interpret(self, request):
            self.calls.append(request)
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=sentence,
                            evidence=(
                                _evidence(report, text=sentence, page=1).model_copy(
                                    update={"evidence_id": f"cond-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension, sentence in sentences.items()
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=dimension,
                            dimension=dimension,
                            status="observed",
                            answer=sentence,
                            evidence=(
                                _evidence(report, text=sentence, page=1).model_copy(
                                    update={"evidence_id": f"claim-{dimension}"}
                                ),
                            ),
                        )
                        for dimension, sentence in sentences.items()
                    ),
                )
            claim_by_dimension = {claim.dimension: claim.claim_id for claim in request.claims}
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "与原文一致。",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": claim_by_dimension[condition.dimension],
                        "explanation": "核心条件已覆盖。",
                    }
                    for condition in request.source_conditions
                ),
            )

    budget = LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=120_000,
        max_requests=16,
        max_cost_cny=Decimal(10),
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
    )
    provider = _Provider(budget)
    identity = direct_core_processing_identity()
    identity["execution_mode"] = "provider"
    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = _one_page_slices
    try:
        ready = _run(
            provider,
            pages=(
                {"page": 1, "text": page_text, "readable": True},
                {"page": 2, "text": "附注列示子公司注册地。", "readable": True},
            ),
            processing_identity=identity,
            report=report,
            execution_budget=budget,
        )
    finally:
        direct_reading.source_discovery_slices = original
    purposes = [call.purpose for call in provider.calls]
    assert purposes.index("synthesize_views") > purposes.index("source_inventory")
    assert any(
        call.purpose == "source_inventory" and 2 in {int(page["page"]) for page in call.source_pages}
        for call in provider.calls
    )
    assert any(
        call.purpose == "source_inventory"
        and 2 in {int(page["page"]) for page in call.source_pages}
        and provider.calls.index(call) < purposes.index("synthesize_views")
        for call in provider.calls
    )
    assert ready.draft_complete is True
    assert ready.real_semantic_complete is True
    assert not any(str(gap).startswith("necessary_unread:") for gap in ready.gaps)
    assert not any(str(gap).startswith("supplementary_unread:") for gap in ready.gaps)

    class _Partial(_Provider):
        def interpret(self, request):
            self.calls.append(request)
            if request.purpose != "synthesize_views":
                if request.purpose == "source_inventory":
                    return ReportSemanticResponseV3(
                        request_id=request.request_id,
                        purpose=request.purpose,
                    )
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                claims=(
                    ReportViewClaim(
                        claim_id="only-business",
                        dimension="principal_business",
                        status="observed",
                        answer=sentences["principal_business"],
                        evidence=(
                            _evidence(report, text=sentences["principal_business"], page=1),
                        ),
                    ),
                ),
            )

    partial_budget = LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        max_tokens=120_000,
        max_requests=16,
        max_cost_cny=Decimal(10),
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
    )
    partial = _Partial(partial_budget)
    direct_reading.source_discovery_slices = _one_page_slices
    try:
        missing = _run(
            partial,
            pages=(
                {"page": 1, "text": page_text, "readable": True},
                {"page": 2, "text": "附注列示子公司注册地。", "readable": True},
            ),
            processing_identity=identity,
            report=report,
            execution_budget=partial_budget,
        )
    finally:
        direct_reading.source_discovery_slices = original
    assert missing.draft_complete is False
    assert missing.real_semantic_complete is False


def test_a_claim_without_source_text_is_not_verified():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司主营影视制作。"

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="real-business",
                            dimension="principal_business",
                            description="主营是影视制作。",
                            evidence=(
                                _evidence(report, text=text, page=1).model_copy(
                                    update={"evidence_id": "real-evidence"}
                                ),
                            ),
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
                            claim_id="invented",
                            dimension="principal_business",
                            status="observed",
                            answer="公司主营航天发射。",
                            evidence=(
                                _evidence(report, text="公司主营航天发射。", page=1).model_copy(
                                    update={"evidence_id": "invented-evidence"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "invented",
                        "status": "pass",
                        "explanation": "模型自说自话。",
                    },
                ),
            )

    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=direct_core_processing_identity(),
        report=report,
    )
    invented = [item for item in result.claims if item.claim_id == "invented"]
    assert invented
    assert invented[0].status != "observed" or invented[0].limitation == "evidence_not_located"
    assert not any(
        check.target_id == "invented" and check.status == "pass" for check in result.checks
    )


def test_an_unread_later_page_cannot_be_marked_supplementary():
    from research.company_profile import direct_reading
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    first = "公司主营影视制作并取得制作收入。公司提供设备租赁。公司采购原材料。公司向下游客户交付影片。"
    later = "公司另有独立的海上运输主业，并确认运输收入。"

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = {int(page["page"]) for page in request.source_pages}
            if request.purpose == "source_inventory" and pages == {2}:
                raise ValueError("later-page-not-sent")
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=first,
                            evidence=(
                                _evidence(report, text=first, page=1).model_copy(
                                    update={"evidence_id": f"later-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension in (
                            "principal_business",
                            "products_services",
                            "revenue_model",
                            "input_exposure",
                            "supply_chain_role",
                        )
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=dimension,
                            dimension=dimension,
                            status="observed",
                            answer=first,
                            evidence=(
                                _evidence(report, text=first, page=1).model_copy(
                                    update={"evidence_id": f"later-claim-{dimension}"}
                                ),
                            ),
                        )
                        for dimension in (
                            "principal_business",
                            "products_services",
                            "revenue_model",
                            "input_exposure",
                            "supply_chain_role",
                        )
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": dimension,
                        "status": "pass",
                        "explanation": "已读部分成立。",
                    }
                    for dimension in (
                        "principal_business",
                        "products_services",
                        "revenue_model",
                        "input_exposure",
                        "supply_chain_role",
                    )
                ),
            )

    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = _one_page_slices
    try:
        result = _run(
            _Provider(),
            pages=(
                {"page": 1, "text": first, "readable": True},
                {"page": 2, "text": later, "readable": True},
            ),
            processing_identity=direct_core_processing_identity(),
            report=report,
        )
    finally:
        direct_reading.source_discovery_slices = original
    assert result.draft_complete is False
    assert result.real_semantic_complete is False
    assert any(gap == "necessary_unread:2" for gap in result.gaps)
    assert not any(str(gap).startswith("supplementary_unread:") for gap in result.gaps)


def test_chapter_selection_can_finish_without_reading_an_ordinary_note():
    from research.company_profile import direct_reading
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    body = "公司主营造纸并销售纸袋纸。公司营业收入为100万元。公司采购木片。公司向下游客户销售纸制品。"
    note = "附注列示子公司注册地址。"
    dimensions = (
        "principal_business",
        "products_services",
        "revenue_model",
        "input_exposure",
        "supply_chain_role",
    )

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = {int(page["page"]) for page in request.source_pages}
            if request.source_scope == "page_map":
                assert all(len(str(page.get("text") or "")) <= 180 for page in request.source_pages)
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1,),
                )
            if 2 in pages:
                raise AssertionError("ordinary note was sent as body text")
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=body,
                            evidence=(
                                _evidence(report, text=body, page=1).model_copy(
                                    update={"evidence_id": f"ch-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension in dimensions
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=dimension,
                            dimension=dimension,
                            status="observed",
                            answer=body,
                            evidence=(
                                _evidence(report, text=body, page=1).model_copy(
                                    update={"evidence_id": f"ch-claim-{dimension}"}
                                ),
                            ),
                        )
                        for dimension in dimensions
                    ),
                )
            claim_by_dimension = {claim.dimension: claim.claim_id for claim in request.claims}
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "选自正文。",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": claim_by_dimension[condition.dimension],
                        "explanation": "核心条件已覆盖。",
                    }
                    for condition in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = _one_page_slices
    try:
        result = _run(
            _Provider(),
            pages=(
                {"page": 1, "text": "管理层讨论\n" + body, "readable": True},
                {"page": 2, "text": note, "readable": True},
            ),
            processing_identity=identity,
            report=report,
        )
    finally:
        direct_reading.source_discovery_slices = original
    assert result.draft_complete is True
    assert any(item["page"] == 2 and item["status"] == "not_read" for item in result.page_coverage)
    assert not any(gap == "necessary_unread:2" for gap in result.gaps)


def test_a_requested_continuation_page_still_blocks_when_unread():
    from research.company_profile import direct_reading

    report = _report()

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = {int(page["page"]) for page in request.source_pages}
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1, 2),
                )
            if 2 in pages:
                raise ValueError("continuation-not-read")
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = _one_page_slices
    try:
        result = _run(
            _Provider(),
            pages=(
                {"page": 1, "text": "营业收入构成", "readable": True},
                {"page": 2, "text": "手续费及佣金净收入续", "readable": True},
            ),
            processing_identity=identity,
            report=report,
        )
    finally:
        direct_reading.source_discovery_slices = original
    assert result.draft_complete is False
    assert any(gap == "necessary_unread:2" for gap in result.gaps)


def test_a_body_requested_continuation_is_read_after_the_core_dimensions_pass():
    from research.company_profile import direct_reading
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    body = "公司主营影视制作。公司营业收入为100万元。公司采购设备。公司向客户交付影片。"
    sent = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = tuple(int(page["page"]) for page in request.source_pages)
            sent.append((request.source_scope, request.purpose, pages))
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1,),
                )
            if request.purpose == "source_inventory" and pages == (1,):
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(2,),
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=body,
                            evidence=(
                                _evidence(report, text=body, page=1).model_copy(
                                    update={"evidence_id": f"follow-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension in (
                            "principal_business",
                            "products_services",
                            "revenue_model",
                            "input_exposure",
                            "supply_chain_role",
                        )
                    ),
                )
            if request.purpose == "source_inventory" and 2 in pages:
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="revenue-total",
                            dimension="revenue_model",
                            description="本期营业收入总额。",
                            evidence=(
                                _evidence(report, text="续页营业收入为200万元。", page=2).model_copy(
                                    update={"evidence_id": "follow-total"}
                                ),
                            ),
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
                            claim_id="revenue-total",
                            dimension="revenue_model",
                            status="observed",
                            answer="公司本期营业收入为200万元。",
                            evidence=(
                                _evidence(report, text="续页营业收入为200万元。", page=2).model_copy(
                                    update={"evidence_id": "follow-total-claim"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "revenue-total",
                        "status": "pass",
                        "explanation": "总额已核。",
                    },
                    {
                        "target_type": "condition",
                        "target_id": "revenue-total",
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": "revenue-total",
                        "explanation": "覆盖总额。",
                    },
                ),
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    original = direct_reading.source_discovery_slices
    direct_reading.source_discovery_slices = _one_page_slices
    try:
        result = _run(
            _Provider(),
            pages=(
                {"page": 1, "text": body, "readable": True},
                {"page": 2, "text": "续页营业收入为200万元。", "readable": True},
            ),
            processing_identity=identity,
            report=report,
        )
    finally:
        direct_reading.source_discovery_slices = original
    stopped = [gap for gap in result.gaps if gap.startswith("direct_core_gap_check_stopped")]
    assert not stopped, stopped
    assert any(item[0] == "full_context" and 2 in item[2] for item in sent)
    assert any(item["page"] == 2 and item["status"] == "read" for item in result.page_coverage)


def test_chapter_navigation_is_admitted_without_reserving_the_unread_report():
    report = _report()
    pages = tuple(
        {
            "page": page,
            "text": f"第{page}节 经营情况讨论与分析。" + ("公司主营造纸并销售纸袋纸。" * 40),
            "readable": True,
        }
        for page in range(1, 61)
    )

    class _Provider:
        offline_only = True

        def __init__(self):
            self.scopes = []

        def interpret(self, request):
            self.scopes.append(request.source_scope)
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1, 2),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    identity["ledger_tokens"] = 120_000
    provider = _Provider()
    result = _run(
        provider,
        pages=pages,
        processing_identity=identity,
        report=report,
        token_budget=120_000,
    )
    assert "page_map" in provider.scopes
    assert "full_context" in provider.scopes
    assert "chapter_selection_empty" not in result.gaps
    assert not any("direct_core_admission_stopped" in gap for gap in result.gaps)
    assert not any(gap == "direct_core_ledger_hold" for gap in result.gaps)


def test_navigation_failure_does_not_fall_back_to_the_full_report():
    identity = direct_core_processing_identity()
    identity["chapter_select"] = True

    class _Provider:
        offline_only = True

        def interpret(self, request):
            raise ValueError("navigation-down")

    result = _run(
        _Provider(),
        pages=(
            {"page": 1, "text": "公司主营造纸并销售纸袋纸。", "readable": True},
            {"page": 2, "text": "附注披露折旧年限。", "readable": True},
        ),
        processing_identity=identity,
    )
    assert "chapter_selection_limited" in result.gaps
    assert result.draft_complete is False
    assert all(item["status"] != "read" for item in result.page_coverage)
    assert not any(
        item.get("source_scope") == "full_context" for item in result.call_receipts
    )


def test_a_product_line_subtotal_does_not_answer_operating_revenue():
    from research.company_profile.direct_reading import _completeness_gaps

    report = _report()
    claim = ReportViewClaim(
        claim_id="lines",
        dimension="revenue_model",
        status="observed",
        answer="2025年主营业务分产品营业收入为纸100元。",
        evidence=(_evidence(report, text="分产品营业收入", page=1),),
    )
    other = [
        ReportViewClaim(
            claim_id=claim_id,
            dimension=dimension,
            status="observed",
            answer="公司经营造纸并采购木片，向客户销售纸。",
            evidence=(_evidence(report, text="公司经营造纸。", page=1),),
        )
        for claim_id, dimension in (
            ("business", "principal_business"),
            ("product", "products_services"),
            ("input", "input_exposure"),
            ("role", "supply_chain_role"),
        )
    ]
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id=item.claim_id,
            status="pass",
            explanation="已核。",
        )
        for item in [claim, *other]
    ]
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import _core_answers_ready

    total = claim.model_copy(
        update={
            "claim_id": "period-total",
            "answer": "本期营业收入100元。",
            "evidence": (
                _evidence(report, text="本期营业收入100元。主营业务分产品收入另列。", page=1),
            ),
        }
    )
    covered = ReportSourceCondition(
        condition_id="period-total",
        dimension="revenue_model",
        description="本期营业收入总额。",
        evidence=total.evidence,
        major=True,
        statement_status="current",
    )
    second = ReportSourceCondition(
        condition_id="second-major",
        dimension="principal_business",
        description="另一主要业务是工业设计。",
        evidence=(_evidence(report, text="另一主要业务是工业设计。", page=1),),
        major=True,
        statement_status="current",
    )
    total_checks = [
        ReportViewCheck(
            target_type="claim",
            target_id=item.claim_id,
            status="pass",
            explanation="已核。",
        )
        for item in [total, *other]
    ] + [
        ReportViewCheck(
            target_type="condition",
            target_id="period-total",
            status="pass",
            explanation="总额已由主张覆盖。",
            covers_target_type="claim",
            covers_target_id="period-total",
        )
    ]
    quiet = _completeness_gaps([total, *other], [covered], total_checks)
    assert "core_question_unanswered:operating_revenue_scale" not in quiet
    assert not any(gap.startswith("major_condition_not_verified:") for gap in quiet)
    blocked = _completeness_gaps([total, *other], [covered, second], total_checks)
    assert "major_condition_not_verified:second-major" in blocked
    assert _core_answers_ready([total, *other], [covered, second], total_checks) is False


def test_a_later_block_replaces_an_older_pass():
    from research.company_profile.direct_reading import _completeness_gaps
    from research.company_profile.report_interpretation import _claim_is_trusted

    report = _report()
    claim = ReportViewClaim(
        claim_id="kept",
        dimension="principal_business",
        status="observed",
        answer="公司主营造纸。",
        evidence=(_evidence(report, text="公司主营造纸。", page=1),),
    )
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id="kept",
            status="pass",
            explanation="先通过。",
        ),
        ReportViewCheck(
            target_type="claim",
            target_id="kept",
            status="block",
            explanation="后否定。",
        ),
    ]
    assert _claim_is_trusted(claim, tuple(checks)) is False
    gaps = _completeness_gaps([claim], [], checks)
    assert "core_dimension_missing:principal_business" in gaps


def test_a_core_repair_rereads_only_the_requested_page_and_drafts_the_new_total():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    saved = ReportSourceCondition(
        condition_id="product-lines",
        dimension="revenue_model",
        description="分产品收入。",
        evidence=(
            _evidence(report, text="分产品营业收入", page=7).model_copy(
                update={"evidence_id": "saved-lines"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-page",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [7],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-page",
            purpose="source_inventory",
            source_conditions=(saved,),
        ).model_dump(mode="json"),
    }
    sent = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = tuple(int(page["page"]) for page in request.source_pages)
            sent.append((request.purpose, pages))
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=(
                        ReportSourceCondition(
                            condition_id="period-total",
                            dimension="revenue_model",
                            description="本期营业收入总额。",
                            evidence=(
                                _evidence(report, text="营业收入 2418990221.17元", page=16).model_copy(
                                    update={"evidence_id": "period-total"}
                                ),
                            ),
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
                            claim_id="period-total",
                            dimension="revenue_model",
                            status="observed",
                            answer="本期营业收入为2418990221.17元。",
                            evidence=(
                                _evidence(report, text="营业收入 2418990221.17元", page=16).model_copy(
                                    update={"evidence_id": "period-total-claim"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "period-total",
                        "status": "pass",
                        "explanation": "总额已核。",
                    },
                    {
                        "target_type": "condition",
                        "target_id": "period-total",
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": "period-total",
                        "explanation": "覆盖总额。",
                    },
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["core_repair_pages"] = [16]
    result = _run(
        _Provider(),
        pages=(
            {"page": 7, "text": "分产品营业收入合计。", "readable": True},
            {"page": 16, "text": "营业收入 2418990221.17元。", "readable": True},
            {"page": 99, "text": "附注披露折旧年限。", "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    assert sent[0] == ("source_inventory", (16,))
    assert not any(7 in pages or 99 in pages for _purpose, pages in sent)
    assert any(claim.claim_id == "period-total" for claim in result.claims)
    assert not any(gap == "necessary_unread:99" for gap in result.gaps)


def test_an_isolated_conflict_is_not_trusted_and_is_not_redrafted():
    import json

    from research.company_profile.contracts import ReportSourceCondition as _Condition
    from research.company_profile.report_interpretation import _claim_is_trusted

    report = _report()

    saved = _Condition(
        condition_id="custody",
        dimension="revenue_model",
        description="托管收入。",
        evidence=(
            _evidence(report, text="托管收入24.81亿元", page=1).model_copy(
                update={"evidence_id": "custody-source"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    claim = ReportViewClaim(
        claim_id="claim-sc-011",
        dimension="revenue_model",
        status="observed",
        answer="托管收入24.81亿元，同比增长7.23%，托管规模增长11.45%。",
        condition_ids=("custody",),
        evidence=(
            _evidence(report, text="托管收入24.81亿元", page=1).model_copy(
                update={"evidence_id": "custody-claim"}
            ),
        ),
    )
    other = _Condition(
        condition_id="housing",
        dimension="products_services",
        description="个人住房贷款是另一项主要业务。",
        evidence=(
            _evidence(report, text="个人住房贷款是主要业务。", page=1).model_copy(
                update={"evidence_id": "housing-source"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-custody",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-custody",
            purpose="source_inventory",
            source_conditions=(saved, other),
        ).model_dump(mode="json"),
    }
    sent = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            sent.append(request.purpose)
            if request.purpose == "synthesize_views":
                assert any(
                    item.condition_id.endswith("housing")
                    for item in request.source_conditions
                )
                assert all(
                    not item.condition_id.endswith("custody")
                    for item in request.source_conditions
                )
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="housing",
                            dimension="products_services",
                            status="observed",
                            answer="个人住房贷款是主要业务。",
                            evidence=other.evidence,
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "housing",
                        "status": "pass",
                        "explanation": "住房已核。",
                    },
                    {
                        "target_type": "condition",
                        "target_id": "housing",
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": "housing",
                        "explanation": "覆盖住房。",
                    },
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(claim.model_dump_json())],
        "checks": [
            {
                "target_type": "claim",
                "target_id": "claim-sc-011",
                "status": "pass",
                "explanation": "旧通过。",
            }
        ],
    }
    identity["isolated_claim_ids"] = ["claim-sc-011"]
    result = _run(
        _Provider(),
        pages=(
            {"page": 1, "text": "托管收入24.81亿元。个人住房贷款是主要业务。", "readable": True},
            {"page": 2, "text": "附注披露折旧年限。", "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    isolated = next(item for item in result.claims if item.claim_id == "claim-sc-011")
    assert isolated.status == "unclear"
    assert _claim_is_trusted(isolated, result.checks) is False
    assert "synthesize_views" in sent
    assert not any(gap == "necessary_unread:2" for gap in result.gaps)
    assert "isolated_unreliable_claim:claim-sc-011" in result.gaps
    assert any(item.claim_id == "housing" for item in result.claims)


def test_a_saved_source_review_follow_page_is_read_on_the_repair_path():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    saved = ReportSourceCondition(
        condition_id="lines",
        dimension="revenue_model",
        description="分行业收入。",
        evidence=(
            _evidence(report, text="分行业营业收入", page=16).model_copy(
                update={"evidence_id": "lines"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-16",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [16],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-16",
            purpose="source_inventory",
            requested_pages=(17,),
            source_conditions=(saved,),
        ).model_dump(mode="json"),
    }
    sent = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            pages = tuple(int(page["page"]) for page in request.source_pages)
            sent.append((request.purpose, pages))
            if request.purpose == "source_inventory":
                raise AssertionError(pages)
            if request.purpose == "synthesize_views":
                if any(
                    item.condition_id.startswith("core-question:")
                    for item in request.source_conditions
                ):
                    assert any(
                        "本期营业收入100元。" in str(page.get("text") or "")
                        for page in request.source_pages
                    )
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="period-total",
                            dimension="revenue_model",
                            status="observed",
                            answer="本期营业收入100元。主营业务分产品收入另列。",
                            evidence=(
                                _evidence(report, text="本期营业收入100元。", page=17).model_copy(
                                    update={"evidence_id": "period-total-claim"}
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "period-total",
                        "status": "pass",
                        "explanation": "总额已核。",
                    },
                    {
                        "target_type": "condition",
                        "target_id": "period-total",
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": "period-total",
                        "explanation": "覆盖总额。",
                    },
                ),
            )

    earlier = {
        "request_id": "saved-17-broad",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [17],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-17-broad",
            purpose="source_inventory",
            source_conditions=(),
        ).model_dump(mode="json"),
    }
    import json

    kept = ReportViewClaim(
        claim_id="kept-revenue",
        dimension="revenue_model",
        status="observed",
        answer="分行业营业收入已列。",
        evidence=(
            _evidence(report, text="本期营业收入100元。", page=17).model_copy(
                update={"evidence_id": "kept-revenue"}
            ),
        ),
    )
    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [earlier, receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(kept.model_dump_json())],
        "checks": [
            {
                "target_type": "claim",
                "target_id": "kept-revenue",
                "status": "pass",
                "explanation": "分项已核。",
            }
        ],
    }
    result = _run(
        _Provider(),
        pages=(
            {"page": 16, "text": "分行业营业收入。", "readable": True},
            {"page": 17, "text": "本期营业收入100元。", "readable": True},
            {"page": 99, "text": "附注披露折旧年限。", "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    assert not any(purpose == "source_inventory" for purpose, _pages in sent)
    assert any(purpose == "synthesize_views" and 17 in pages for purpose, pages in sent)
    assert not any(99 in pages for _purpose, pages in sent)
    assert not any(gap == "necessary_unread:99" for gap in result.gaps)
    assert any(item.claim_id == "period-total" for item in result.claims)


def test_a_resolved_gap_drops_and_a_new_block_remains():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import _completeness_gaps, _current_gaps

    report = _report()
    claim = ReportViewClaim(
        claim_id="claim_principal_business_bank_profile",
        dimension="principal_business",
        status="observed",
        answer="公司为全国性银行。",
        condition_ids=("principal_bank_profile",),
        evidence=(_evidence(report, text="公司为全国性银行。", page=1),),
    )
    condition = ReportSourceCondition(
        condition_id="principal_bank_profile",
        dimension="principal_business",
        description="公司定位。",
        evidence=claim.evidence,
        major=True,
        statement_status="current",
    )
    passed = ReportViewCheck(
        target_type="condition",
        target_id="principal_bank_profile",
        status="pass",
        explanation="已覆盖。",
        covers_target_type="claim",
        covers_target_id=claim.claim_id,
    )
    claim_pass = ReportViewCheck(
        target_type="claim",
        target_id=claim.claim_id,
        status="pass",
        explanation="已核。",
    )
    stale = ["major_condition_not_verified:principal_bank_profile"]
    current = _current_gaps(stale, [claim], [condition], [passed, claim_pass])
    assert "major_condition_not_verified:principal_bank_profile" not in current
    blocked = ReportViewCheck(
        target_type="condition",
        target_id="principal_bank_profile",
        status="block",
        explanation="后否定。",
    )
    gaps = _completeness_gaps([claim], [condition], [passed, claim_pass, blocked])
    assert "major_condition_not_verified:principal_bank_profile" in gaps


def test_a_deposit_condition_uses_the_deposit_claim_not_an_unrelated_product():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import claims_covering_condition

    report = _report()
    condition = ReportSourceCondition(
        condition_id="bp-work:report:3:input_deposits_funding",
        dimension="input_exposure",
        description="存款总额。",
        evidence=(_evidence(report, text="存款总额55,824.35亿元。", page=1),),
        major=True,
        statement_status="current",
    )
    deposit = ReportViewClaim(
        claim_id="claim_input_exposure",
        dimension="input_exposure",
        status="observed",
        answer="集团本外币存款总额为55,824.35亿元。",
        condition_ids=("bp-work:report:3:input_deposits_funding",),
        evidence=condition.evidence,
    )
    product = ReportViewClaim(
        claim_id="claim-products-services-006",
        dimension="products_services",
        status="observed",
        answer="另一项产品。",
        evidence=(_evidence(report, text="另一项产品。", page=1),),
    )
    matched = claims_covering_condition(condition, [deposit, product])
    assert [item.claim_id for item in matched] == ["claim_input_exposure"]


def test_operating_revenue_is_taken_from_the_statement_page_not_the_old_quote():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    saved = ReportSourceCondition(
        condition_id="lines",
        dimension="revenue_model",
        description="分产品合计。",
        evidence=(
            _evidence(report, text="主营业务分产品合计。", page=17).model_copy(
                update={"evidence_id": "lines"}
            ),
        ),
        major=True,
        statement_status="current",
    )
    receipt = {
        "request_id": "saved-17",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [17],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-17",
            purpose="source_inventory",
            source_conditions=(saved,),
        ).model_dump(mode="json"),
    }
    sent = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            sent.append((request.purpose, tuple(int(page["page"]) for page in request.source_pages)))
            if request.purpose == "synthesize_views" and any(
                item.condition_id.startswith("core-question:")
                for item in request.source_conditions
            ):
                assert any(int(page["page"]) == 16 for page in request.source_pages)
                assert any(
                    "营业收入 100元" in str(page.get("text") or "")
                    for page in request.source_pages
                    if int(page["page"]) == 16
                )
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="period-total",
                            dimension="revenue_model",
                            status="observed",
                            answer="本期营业收入100元。",
                            evidence=(
                                _evidence(report, text="营业收入 100元", page=16).model_copy(
                                    update={"evidence_id": "period-total"}
                                ),
                            ),
                        ),
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    {
                        "target_type": "claim",
                        "target_id": "period-total",
                        "status": "pass",
                        "explanation": "总额已核。",
                    },
                    {
                        "target_type": "condition",
                        "target_id": "core-question:period-operating-revenue",
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": "period-total",
                        "explanation": "覆盖总额。",
                    },
                ),
            )

    import json

    kept = ReportViewClaim(
        claim_id="kept-lines",
        dimension="revenue_model",
        status="observed",
        answer="主营业务分产品合计。",
        evidence=saved.evidence,
    )
    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(kept.model_dump_json())],
        "checks": [
            {
                "target_type": "claim",
                "target_id": "kept-lines",
                "status": "pass",
                "explanation": "分项已核。",
            }
        ],
    }
    result = _run(
        _Provider(),
        pages=(
            {
                "page": 16,
                "text": "利润表及现金流量表相关科目变动分析表\n科目 本期数 上年同期数\n营业收入 100元",
                "readable": True,
            },
            {"page": 17, "text": "主营业务分产品合计。", "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    assert any(purpose == "synthesize_views" and 16 in pages for purpose, pages in sent)
    assert any(item.claim_id == "period-total" for item in result.claims)


def test_cited_conditions_without_a_coverage_pass_are_still_verified():
    import json

    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    saved = []
    claims = []
    for index in range(8):
        condition = ReportSourceCondition(
            condition_id=f"item-{index}",
            dimension="products_services",
            description=f"第{index}项主要产品。",
            evidence=(
                _evidence(report, text=f"第{index}项主要产品。", page=1).model_copy(
                    update={"evidence_id": f"item-{index}"}
                ),
            ),
            major=True,
            statement_status="current",
        )
        saved.append(condition)
        claims.append(
            ReportViewClaim(
                claim_id=f"claim-{index}",
                dimension="products_services",
                status="observed",
                answer=f"第{index}项主要产品已经经营。",
                condition_ids=(f"item-{index}",),
                evidence=condition.evidence,
            )
        )
    receipt = {
        "request_id": "saved-items",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-items",
            purpose="source_inventory",
            source_conditions=tuple(saved),
        ).model_dump(mode="json"),
    }
    seen = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.purpose == "synthesize_views":
                raise AssertionError("cited conditions must be checked, not rewritten")
            ids = tuple(item.condition_id.split(":")[-1] for item in request.source_conditions)
            seen.extend(ids)
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "condition",
                        "target_id": item.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": f"claim-{item.condition_id.split(':')[-1].split('-')[-1]}",
                        "explanation": "已有正文覆盖。",
                    }
                    for item in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["request_reservation_limit"] = 65536
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(item.model_dump_json()) for item in claims],
        "checks": [
            {
                "target_type": "claim",
                "target_id": item.claim_id,
                "status": "pass",
                "explanation": "主张已核。",
            }
            for item in claims
        ],
    }
    result = _run(
        _Provider(),
        pages=({"page": 1, "text": " ".join(f"第{index}项主要产品。" for index in range(8)), "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert {f"item-{index}" for index in range(8)} <= set(seen), (seen, result.gaps[:12])
    assert all(item.status == "observed" for item in result.claims)
    assert not any(
        gap.startswith("major_condition_not_verified:saved-items:item-") for gap in result.gaps
    )


def test_same_dimension_does_not_hide_another_unanswered_business():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import _completeness_gaps

    report = _report()
    claims = [
        ReportViewClaim(
            claim_id=claim_id,
            dimension=dimension,
            status="observed",
            answer="公司经营造纸并取得收入。",
            evidence=(_evidence(report, text="公司经营造纸。", page=1),),
        )
        for claim_id, dimension in (
            ("business", "principal_business"),
            ("product", "products_services"),
            ("revenue", "revenue_model"),
            ("input", "input_exposure"),
            ("role", "supply_chain_role"),
        )
    ]
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id=item.claim_id,
            status="pass",
            explanation="已核。",
        )
        for item in claims
    ]
    other = ReportSourceCondition(
        condition_id="other-product",
        dimension="products_services",
        description="另一主要产品是工业设计。",
        evidence=(_evidence(report, text="另一主要产品是工业设计。", page=2),),
        major=True,
        statement_status="current",
    )

    def gap_ids(extra_checks=()):
        gaps = _completeness_gaps(claims, [other], [*checks, *extra_checks])
        return [gap for gap in gaps if gap.startswith("major_condition_not_verified:")]

    assert gap_ids() == ["major_condition_not_verified:other-product"]
    blocked = ReportViewCheck(
        target_type="condition",
        target_id="other-product",
        status="block",
        explanation="没有对应主张。",
    )
    assert "major_condition_not_verified:other-product" in gap_ids((blocked,))
    unclear = ReportViewCheck(
        target_type="condition",
        target_id="other-product",
        status="unclear",
        explanation="对象还不能覆盖。",
    )
    claims[1] = claims[1].model_copy(update={"condition_ids": ("other-product",)})
    assert "major_condition_not_verified:other-product" in _completeness_gaps(
        claims, [other], [*checks, unclear]
    )
    supplementary = ReportViewCheck(
        target_type="condition",
        target_id="other-product",
        status="supplementary",
        explanation="只是举例。",
    )
    assert "major_condition_not_verified:other-product" not in gap_ids((supplementary,))


def test_output_exposure_is_checked_and_unclear_is_not_repeated():
    import json

    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司发放制造业贷款，并引导信贷投向。"
    output = ReportSourceCondition(
        condition_id="loan-output",
        dimension="output_exposure",
        description="制造业贷款投放。",
        evidence=(_evidence(report, text=text, page=1).model_copy(update={"evidence_id": "loan-output"}),),
        major=True,
        statement_status="current",
    )
    direction = ReportSourceCondition(
        condition_id="credit-direction",
        dimension="principal_business",
        description="引导信贷投向。",
        evidence=(_evidence(report, text=text, page=1).model_copy(update={"evidence_id": "credit-direction"}),),
        major=True,
        statement_status="current",
    )
    output_claim = ReportViewClaim(
        claim_id="claim-output",
        dimension="output_exposure",
        status="observed",
        answer="公司发放制造业贷款。",
        condition_ids=("loan-output",),
        evidence=output.evidence,
    )
    direction_claim = ReportViewClaim(
        claim_id="claim-direction",
        dimension="principal_business",
        status="observed",
        answer="公司发放贷款。",
        condition_ids=("credit-direction",),
        evidence=direction.evidence,
    )
    receipt = {
        "request_id": "saved-items",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-items",
            purpose="source_inventory",
            source_conditions=(output, direction),
        ).model_dump(mode="json"),
    }
    purposes = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            purposes.append(request.purpose)
            claim_ids = {item.claim_id for item in request.claims}
            if request.purpose == "verify_views" and claim_ids == {"claim-direction"}:
                raise AssertionError("unclear package was sent again")
            if request.purpose == "synthesize_views":
                condition = request.source_conditions[0]
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="claim-new",
                            dimension=condition.dimension,
                            status="observed",
                            answer="公司引导信贷投向。",
                            condition_ids=(condition.condition_id,),
                            evidence=condition.evidence,
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "condition",
                        "target_id": item.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": request.claims[0].claim_id,
                        "explanation": "已有正文覆盖。",
                    }
                    for item in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["request_reservation_limit"] = 65536
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [
            json.loads(output_claim.model_dump_json()),
            json.loads(direction_claim.model_dump_json()),
        ],
        "checks": [
            {"target_type": "claim", "target_id": "claim-output", "status": "pass", "explanation": "已核。"},
            {"target_type": "claim", "target_id": "claim-direction", "status": "pass", "explanation": "已核。"},
            {
                "target_type": "condition",
                "target_id": "credit-direction",
                "status": "unclear",
                "explanation": "还没有投向。",
            },
        ],
        "verify_receipts": [
            {
                "request_id": "old-direction",
                "purpose": "verify_views",
                "status": "success",
                "request_targets": [
                    {"target_type": "claim", "target_id": "claim-direction"},
                    {"target_type": "condition", "target_id": "credit-direction"},
                ],
                "response_payload": {
                    "checks": [
                        {
                            "target_type": "condition",
                            "target_id": "credit-direction",
                            "status": "unclear",
                        }
                    ]
                },
            }
        ],
    }
    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert "synthesize_views" in purposes
    assert "verify_views" in purposes
    assert not any(gap.endswith("loan-output") for gap in result.gaps)


def test_a_rejected_payload_is_repacked_in_the_same_entry():
    from research.company_profile.contracts import ReportSourceCondition
    from utils.llm.errors import LlmDeadlineExceededError

    report = _report()
    text = "公司经营两项贷款服务。"
    conditions = []
    for name in ("east", "west"):
        conditions.append(
            ReportSourceCondition(
                condition_id=name,
                dimension="output_exposure",
                description=f"{name}贷款。",
                evidence=(_evidence(report, text=text, page=1).model_copy(update={"evidence_id": name}),),
                major=True,
                statement_status="current",
            )
        )
    claim = ReportViewClaim(
        claim_id="claim-loans",
        dimension="output_exposure",
        status="observed",
        answer="公司经营两项贷款服务。",
        condition_ids=("east", "west"),
        evidence=conditions[0].evidence,
    )
    receipt = {
        "request_id": "saved-items",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [1],
        "physical_request": True,
        "response_payload": ReportSemanticResponseV3(
            request_id="saved-items",
            purpose="source_inventory",
            source_conditions=tuple(conditions),
        ).model_dump(mode="json"),
    }
    seen = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            ids = tuple(item.condition_id.split(":")[-1] for item in request.source_conditions)
            if request.purpose == "verify_views" and len(ids) > 1:
                raise LlmDeadlineExceededError("payload_not_admitted")
            seen.extend(ids)
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "condition",
                        "target_id": item.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": "claim-loans",
                        "explanation": "已覆盖。",
                    }
                    for item in request.source_conditions
                ),
            )

    import json

    identity = direct_core_processing_identity()
    identity["request_reservation_limit"] = 65536
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(claim.model_dump_json())],
        "checks": [
            {"target_type": "claim", "target_id": "claim-loans", "status": "pass", "explanation": "已核。"}
        ],
    }
    result = _run(
        _Provider(),
        pages=({"page": 1, "text": text, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert {"east", "west"} <= set(seen), (seen, result.gaps[:8])
    assert not any("payload_not_admitted" in gap for gap in result.gaps)


def test_saved_answers_are_sent_to_coverage_on_the_real_continuation():
    """D8b/D8 disk continuation: existing claims enter coverage, the total is not redrafted."""
    import gzip
    import json
    import time
    from pathlib import Path

    from research.company_profile.bounded_entry import (
        load_saved_draft,
        load_saved_source_discovery,
    )
    from research.company_profile.direct_reading import interpret_direct_core
    from research.company_profile.models import ReportIdentity

    change = Path(
        "/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization"
    )

    def pages_from(cache: Path):
        with gzip.open(cache, "rt") as handle:
            payload = json.load(handle)
        return tuple(
            {
                "page": int(item["page_number"]),
                "text": item.get("text") or "",
                "readable": True,
            }
            for item in payload["pages"]
        )

    def continue_saved(query_path: Path, cache: Path, *, isolated=()):
        query = json.loads(query_path.read_text())
        report = ReportIdentity.model_validate(query["profiles"][0]["report"])
        binding = {
            "accepted_source_query": str(query_path),
            "accepted_draft_query": str(query_path),
            "asset": {"content_hash": report.document_version},
        }
        calls = []

        class _Provider:
            offline_only = True

            def interpret(self, request):
                calls.append(
                    {
                        "purpose": request.purpose,
                        "conditions": tuple(
                            item.condition_id for item in request.source_conditions
                        ),
                        "claims": tuple(item.claim_id for item in request.claims),
                    }
                )
                checks = []
                if request.purpose == "verify_views":
                    for condition in request.source_conditions:
                        tail = condition.condition_id.split(":")[-1]
                        cover = next(
                            (
                                claim.claim_id
                                for claim in request.claims
                                if any(
                                    str(item) == tail or str(item).endswith(":" + tail)
                                    for item in (claim.condition_ids or ())
                                )
                            ),
                            request.claims[0].claim_id if request.claims else "",
                        )
                        checks.append(
                            {
                                "target_type": "condition",
                                "target_id": condition.condition_id,
                                "status": "pass",
                                "covers_target_type": "claim",
                                "covers_target_id": cover,
                                "explanation": "已有正文覆盖。",
                            }
                        )
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    checks=tuple(checks),
                )

        identity = direct_core_processing_identity()
        identity["request_reservation_limit"] = 65536
        identity["max_physical_requests"] = 80
        identity["ledger_tokens"] = 800_000
        identity["accepted_source_receipts"] = load_saved_source_discovery(binding)
        identity["accepted_prior_draft"] = load_saved_draft(binding)
        if isolated:
            identity["isolated_claim_ids"] = list(isolated)
        result = interpret_direct_core(
            report=report,
            pages=pages_from(cache),
            processing_identity=identity,
            provider=_Provider(),
            work_id="saved-coverage",
            token_budget=800_000,
            started_at=time.monotonic(),
        )
        return calls, result

    paper_calls, paper = continue_saved(
        change / "s1-600103-d8b-result" / "query.json",
        Path(
            "/home/python/Quote/data/QuoteBak/s1-600103-d8b-20261028/pdf-cache/derived/"
            "business_profile_pdfium_recovery.v4/"
            "6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c_57e1dd4f16ef40ff.json.gz"
        ),
    )
    paper_verify = [item for item in paper_calls if item["purpose"] == "verify_views"]

    def paired(tail, claim_id):
        return any(
            any(cond.endswith(tail) for cond in item["conditions"])
            and claim_id in item["claims"]
            for item in paper_verify
        )

    assert paired("industry_segments_001", "claim_principal_business_001"), paper_verify
    assert paired("product_portfolio_001", "claim_products_services_001"), paper_verify
    assert not any(
        "period-operating-revenue" in cond
        for item in paper_calls
        for cond in item["conditions"]
    ), paper_calls
    assert not any(
        gap.startswith("major_condition_not_verified:")
        and gap.endswith(("industry_segments_001", "product_portfolio_001"))
        for gap in paper.gaps
    )

    bank_cache = next(
        Path("/home/python/Quote/data/QuoteBak/s1-600000-d8-20261028/pdf-cache").rglob(
            "*.json.gz"
        )
    )
    bank_calls, bank = continue_saved(
        change / "s1-600000-d8-result" / "query.json",
        bank_cache,
        isolated=("claim-sc-011",),
    )
    bank_verify = [item for item in bank_calls if item["purpose"] == "verify_views"]
    covered = {
        cond.split(":")[-1]
        for item in bank_verify
        for cond in item["conditions"]
    }
    assert len(covered) > 6, covered
    deposit = [
        item
        for item in bank_verify
        if any(cond.endswith("input_deposits_funding") for cond in item["conditions"])
    ]
    assert deposit and any("claim_input_exposure" in item["claims"] for item in deposit)
    assert all("claim-sc-011" not in item["claims"] for item in bank_verify)
    assert not any(
        gap.startswith("major_condition_not_verified:")
        and gap.endswith("input_deposits_funding")
        for gap in bank.gaps
    )


def test_cross_page_existing_answer_enters_coverage():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import recheck_claims_for

    report = _report()
    condition = ReportSourceCondition(
        condition_id="manufacturing_credit_exposure",
        dimension="output_exposure",
        description="重点聚焦集成电路、生物医药、人工智能、新能源汽车、高端装备、新材料。",
        evidence=(
            _evidence(
                report,
                text="重点聚焦集成电路、生物医药、人工智能、新能源汽车、高端装备、新材料。上海地区制造业贷款余额达568亿元。",
                page=54,
            ),
        ),
        major=True,
        statement_status="current",
    )
    same_page = ReportViewClaim(
        claim_id="claim_output_exposure_shanghai_financing",
        dimension="output_exposure",
        status="observed",
        answer="报告期末上海地区制造业贷款余额达568亿元。",
        condition_ids=("C017",),
        evidence=condition.evidence,
    )
    other_page = ReportViewClaim(
        claim_id="claim-manufacturing-loans",
        dimension="products_services",
        status="observed",
        answer="公司聚焦集成电路、生物医药、人工智能、高端装备，加大对制造业特别是中长期制造业贷款的投入。",
        condition_ids=("C005",),
        evidence=(_evidence(report, text="集成电路、生物医药、人工智能、高端装备。", page=49),),
    )
    found = recheck_claims_for(condition, [same_page, other_page])
    assert {item.claim_id for item in found} == {
        "claim_output_exposure_shanghai_financing",
        "claim-manufacturing-loans",
    }


def test_same_tail_ids_do_not_mix_two_businesses():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import claims_covering_condition

    report = _report()
    transaction = ReportSourceCondition(
        condition_id="bp-work-a:report:6:sc-003",
        dimension="products_services",
        description="交易银行场景服务。",
        evidence=(_evidence(report, text="北斗司库、云资金监管。", page=63),),
        major=True,
        statement_status="current",
    )
    fund = ReportSourceCondition(
        condition_id="bp-work-a:report:7:sc-003",
        dimension="principal_business",
        description="基金子公司。",
        evidence=(_evidence(report, text="浦银安盛基金管理有限公司。", page=92),),
        major=True,
        statement_status="current",
    )
    transaction_claim = ReportViewClaim(
        claim_id="claim-transaction",
        dimension="products_services",
        status="observed",
        answer="公司提供北斗司库和云资金监管。",
        condition_ids=("bp-work-a:report:6:sc-003",),
        evidence=transaction.evidence,
    )
    fund_claim = ReportViewClaim(
        claim_id="claim-fund",
        dimension="principal_business",
        status="observed",
        answer="浦银安盛基金管理有限公司持有51%股份。",
        condition_ids=("bp-work-a:report:7:sc-003",),
        evidence=fund.evidence,
    )
    assert [item.claim_id for item in claims_covering_condition(transaction, [transaction_claim, fund_claim])] == [
        "claim-transaction"
    ]
    assert [item.claim_id for item in claims_covering_condition(fund, [transaction_claim, fund_claim])] == [
        "claim-fund"
    ]


def test_new_unclear_in_this_entry_is_drafted():
    import json

    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    text = "公司开展自营投资交易，并以浦银避险向客户提供代客避险服务。"
    condition = ReportSourceCondition(
        condition_id="financial-markets",
        dimension="principal_business",
        description="自营投资交易与代客避险。",
        evidence=(_evidence(report, text=text, page=1).model_copy(update={"evidence_id": "markets"}),),
        major=True,
        statement_status="current",
    )
    old = ReportViewClaim(
        claim_id="claim-old-markets",
        dimension="revenue_model",
        status="observed",
        answer="公司开展自营投资交易，并以浦银避险向客户提供债券投资。",
        condition_ids=("financial-markets",),
        evidence=(_evidence(report, text="人民币债券投资。", page=2),),
    )
    purposes = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            purposes.append(request.purpose)
            if request.purpose == "verify_views" and "synthesize_views" not in purposes:
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    checks=tuple(
                        {
                            "target_type": "condition",
                            "target_id": item.condition_id,
                            "status": "unclear",
                            "explanation": "现有收入叙述没有写出代客避险。",
                        }
                        for item in request.source_conditions
                    ),
                )
            if request.purpose == "synthesize_views":
                target = request.source_conditions[0]
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="claim-markets",
                            dimension="principal_business",
                            status="observed",
                            answer="公司开展自营投资交易，并以浦银避险向客户提供代客避险。",
                            condition_ids=(target.condition_id,),
                            evidence=target.evidence,
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    [
                        {
                            "target_type": "claim",
                            "target_id": item.claim_id,
                            "status": "pass",
                            "explanation": "已核。",
                        }
                        for item in request.claims
                    ]
                    + [
                        {
                            "target_type": "condition",
                            "target_id": item.condition_id,
                            "status": "pass",
                            "covers_target_type": "claim",
                            "covers_target_id": request.claims[0].claim_id if request.claims else "",
                            "explanation": "新正文覆盖。",
                        }
                        for item in request.source_conditions
                    ]
                ),
            )

    identity = direct_core_processing_identity()
    identity["request_reservation_limit"] = 65536
    identity["accepted_source_receipts"] = [
        {
            "request_id": "saved-markets",
            "purpose": "source_inventory",
            "status": "success",
            "source_scope": "full_context",
            "source_pages": [1],
            "physical_request": True,
            "response_payload": ReportSemanticResponseV3(
                request_id="saved-markets",
                purpose="source_inventory",
                source_conditions=(condition,),
            ).model_dump(mode="json"),
        }
    ]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(old.model_dump_json())],
        "checks": [
            {"target_type": "claim", "target_id": "claim-old-markets", "status": "pass", "explanation": "已核。"}
        ],
    }
    result = _run(
        _Provider(),
        pages=(
            {"page": 1, "text": text, "readable": True},
            {"page": 2, "text": "人民币债券投资。", "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    assert "synthesize_views" in purposes
    assert "verify_views" in purposes
    assert not any(gap.endswith("financial-markets") for gap in result.gaps)


def test_page63_title_pile_does_not_label_transaction_banking_as_tech_finance():
    from research.company_profile.report_interpretation import _bind_source_evidence

    report = _report()
    pages = (
        {
            "page": 62,
            "text": "3.6 公司主要业务情况\n3.6.1 公司所处行业发展情况\n3.6.2 公司金融业务\n对公贷款保持增长。",
            "readable": True,
        },
        {
            "page": 63,
            "text": (
                "投资银行业务\n"
                "交易银行业务\n"
                "科技金融业务\n"
                "浦赢并购：公司打造并购加组合融资。\n"
                "报告期内，全行国内贸易融资总业务量突破2.8万亿元。\n"
                "公司以客户为中心，构建G端（政府机关）、B端（企业法人）、C端（零售个人）场景圈链体系，持续完善北斗司库、云资金监管、浦掌柜等拳头产品。\n"
                "报告期内，公司坚持将科技金融作为企业级战略。\n"
            ),
            "readable": True,
        },
    )
    quote = "公司以客户为中心，构建G端（政府机关）、B端（企业法人）、C端（零售个人）场景圈链体系，持续完善北斗司库、云资金监管、浦掌柜等拳头产品。"
    bound = _bind_source_evidence(_evidence(report, text=quote, page=63), report, pages)
    assert bound is not None
    assert "科技金融" not in (bound.section_title or "")
    assert "公司金融" in (bound.section_title or "")


def test_unlocated_major_still_blocks_core_closeout():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import _completeness_gaps

    report = _report()
    condition = ReportSourceCondition(
        condition_id="sc-010",
        dimension="principal_business",
        description="自营投资交易与浦银避险。",
        evidence=(_evidence(report, text="原文没有的错引。", page=70),),
        major=True,
        statement_status="current",
    )
    claim = ReportViewClaim(
        claim_id="other-revenue",
        dimension="revenue_model",
        status="observed",
        answer="公司取得利息收入。",
        evidence=(_evidence(report, text="公司取得利息收入。", page=1),),
    )
    gaps = _completeness_gaps(
        [claim],
        [condition],
        [
            ReportViewCheck(
                target_type="claim",
                target_id="other-revenue",
                status="pass",
                explanation="已核。",
            )
        ],
    )
    assert "major_condition_not_verified:sc-010" in gaps


def test_chapter_candidates_come_from_toc_and_headings_not_every_page():
    from research.company_profile.direct_reading import chapter_candidate_pages

    pages = [
        {
            "page": 4,
            "text": (
                "目录\n"
                "第一节 释义 ......................................................... 5\n"
                "第三节 管理层讨论与分析 ............................................. 9\n"
                "第四节 公司治理、环境和社会 ........................................ 37\n"
                "第八节 财务报告 .................................................... 85\n"
            ),
            "readable": True,
        },
        {"page": 5, "text": "第一节 释义\n常用词语释义\n报告期 指 2025 年年度", "readable": True},
        {
            "page": 9,
            "text": "第三节 管理层讨论与分析\n一、经营情况讨论与分析\n公司主营造纸并销售纸袋纸。",
            "readable": True,
        },
        {"page": 10, "text": "公司营业收入为100万元。", "readable": True},
        {"page": 37, "text": "第四节 公司治理、环境和社会\n董事会出席情况。", "readable": True},
        {"page": 55, "text": "附注列示子公司注册地址。", "readable": True},
        {"page": 85, "text": "第八节 财务报告\n合并利润表\n营业收入 100", "readable": True},
    ]
    chosen = chapter_candidate_pages(pages)
    assert 4 in chosen and 9 in chosen and 85 in chosen
    assert 37 not in chosen
    assert 55 not in chosen
    assert len(chosen) < len(pages)


def test_compact_source_conditions_keep_distinct_majors():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.direct_reading import compact_source_conditions

    report = _report()
    paper = ReportSourceCondition(
        condition_id="paper",
        dimension="principal_business",
        description="公司主营造纸。",
        evidence=(_evidence(report, text="公司主营造纸。", page=9),),
        major=True,
        statement_status="current",
    )
    repeat = paper.model_copy(update={"condition_id": "paper-repeat"})
    pharma = ReportSourceCondition(
        condition_id="pharma",
        dimension="principal_business",
        description="公司另有药品销售。",
        evidence=(_evidence(report, text="公司另有药品销售。", page=12),),
        major=True,
        statement_status="current",
    )
    same_quote = ReportSourceCondition(
        condition_id="paper-sales",
        dimension="principal_business",
        description="公司另有纸制品销售渠道。",
        evidence=(_evidence(report, text="公司主营造纸。", page=9),),
        major=True,
        statement_status="current",
    )
    kept = compact_source_conditions((paper, repeat, pharma, same_quote))
    assert {item.condition_id for item in kept} == {"paper", "pharma", "paper-sales"}


def test_chapter_select_does_not_send_every_page_opening_as_navigation():
    report = _report()
    sent = []
    body = "公司主营造纸并销售纸袋纸。公司营业收入为100万元。公司采购木片。公司向下游客户销售纸制品。"
    pages = [
        {
            "page": 4,
            "text": "目录\n第三节 管理层讨论与分析 ............................................. 9\n第八节 财务报告 .................................................... 12\n",
            "readable": True,
        },
        {"page": 9, "text": "第三节 管理层讨论与分析\n" + body, "readable": True},
        {"page": 10, "text": "附注列示子公司注册地址。", "readable": True},
        {"page": 12, "text": "第八节 财务报告\n合并利润表\n营业收入 100万元。", "readable": True},
    ]
    for page in range(13, 41):
        pages.append({"page": page, "text": f"第{page}页普通附注。", "readable": True})

    class _Provider:
        offline_only = True

        def interpret(self, request):
            current = tuple(int(page["page"]) for page in request.source_pages)
            sent.append((request.source_scope, request.purpose, current))
            if request.source_scope == "page_map":
                assert set(current) <= {4, 9, 12}
                assert 10 not in current
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(9, 12),
                )
            if request.purpose == "source_inventory":
                from research.company_profile.contracts import ReportSourceCondition

                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=body,
                            evidence=(
                                _evidence(report, text=body[:12], page=9).model_copy(
                                    update={"evidence_id": f"nav-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension in (
                            "principal_business",
                            "products_services",
                            "revenue_model",
                            "input_exposure",
                            "supply_chain_role",
                        )
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=item.condition_id,
                            dimension=item.dimension,
                            status="observed",
                            answer=body,
                            evidence=item.evidence,
                        )
                        for item in request.source_conditions
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "已核。",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": condition.condition_id,
                        "explanation": "覆盖。",
                    }
                    for condition in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    result = _run(_Provider(), pages=tuple(pages), processing_identity=identity, report=report)
    nav_pages = {page for scope, _purpose, pages in sent if scope == "page_map" for page in pages}
    assert nav_pages <= {4, 9, 12}
    assert 10 not in nav_pages
    assert result.draft_complete is True
    assert any(item["page"] == 10 and item["status"] == "not_read" for item in result.page_coverage)


def test_a_local_conflict_is_isolated_on_the_new_chapter_path():
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.report_interpretation import _claim_is_trusted

    report = _report()
    dimensions = (
        "principal_business",
        "products_services",
        "revenue_model",
        "input_exposure",
        "supply_chain_role",
    )
    body = {
        "principal_business": "公司经营公司金融。",
        "products_services": "个人住房贷款是主要业务。",
        "revenue_model": "托管收入24.81亿元，该口径已更正，不再采用。",
        "input_exposure": "公司吸收存款。",
        "supply_chain_role": "公司向企业客户提供贷款。",
    }
    sent = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            sent.append(request.purpose)
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1,),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=body[dimension],
                            evidence=(
                                _evidence(report, text=body[dimension], page=1).model_copy(
                                    update={"evidence_id": f"iso-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension in dimensions
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=dimension,
                            dimension=dimension,
                            status="observed",
                            answer=body[dimension],
                            evidence=(
                                _evidence(report, text=body[dimension], page=1).model_copy(
                                    update={"evidence_id": f"iso-claim-{dimension}"}
                                ),
                            ),
                        )
                        for dimension in dimensions
                    ),
                )
            checks = []
            for claim in request.claims:
                checks.append(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "block" if claim.claim_id == "revenue_model" else "pass",
                        "explanation": "托管口径已否定。" if claim.claim_id == "revenue_model" else "已核。",
                    }
                )
            for condition in request.source_conditions:
                if condition.dimension == "revenue_model":
                    continue
                checks.append(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": condition.dimension,
                        "explanation": "覆盖。",
                    }
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(checks),
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    result = _run(
        _Provider(),
        pages=(
            {
                "page": 1,
                "text": "管理层讨论\n" + "".join(body.values()),
                "readable": True,
            },
            {"page": 2, "text": "附注披露折旧年限。", "readable": True},
        ),
        processing_identity=identity,
        report=report,
    )
    housing = next(item for item in result.claims if item.claim_id == "products_services")
    custody = next(item for item in result.claims if item.claim_id == "revenue_model")
    assert _claim_is_trusted(housing, result.checks) is True
    assert _claim_is_trusted(custody, result.checks) is False
    assert sent.count("synthesize_views") == 1
    assert any(item["page"] == 2 and item["status"] == "not_read" for item in result.page_coverage)


def test_missing_another_major_keeps_the_company_incomplete():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    body = "公司主营造纸并销售纸袋纸。公司营业收入为100万元。公司采购木片。"

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1,),
                )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=body,
                            evidence=(
                                _evidence(report, text=body[:12], page=1).model_copy(
                                    update={"evidence_id": f"miss-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension in (
                            "principal_business",
                            "products_services",
                            "revenue_model",
                            "input_exposure",
                        )
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=item.condition_id,
                            dimension=item.dimension,
                            status="observed",
                            answer=body,
                            evidence=item.evidence,
                        )
                        for item in request.source_conditions
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "已核。",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": condition.condition_id,
                        "explanation": "覆盖。",
                    }
                    for condition in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    result = _run(
        _Provider(),
        pages=({"page": 1, "text": "管理层讨论\n" + body, "readable": True},),
        processing_identity=identity,
        report=report,
    )
    assert result.draft_complete is False
    assert any(item.status == "observed" for item in result.claims)
    assert any(
        gap.startswith("core_dimension_missing:") or gap.startswith("view_not_substantive:")
        for gap in result.gaps
    )


def test_official_parse_first_core_pack_has_tail_room():
    import gzip
    import json
    from pathlib import Path

    from research.company_profile.direct_reading import (
        SOURCE_OUTPUT,
        _compose_tail,
        _first_delivery_tail,
        _units,
        chapter_candidate_pages,
        navigation_batches,
    )
    from research.company_profile.report_interpretation import direct_core_processing_identity

    caches = {
        "paper": Path(
            "/home/python/Quote/data/QuoteBak/s1-600103-c1-20261029/pdf-cache/derived/"
            "business_profile_pdfium_recovery.v4/"
            "6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c_57e1dd4f16ef40ff.json.gz"
        ),
        "bank": Path(
            "/home/python/Quote/data/QuoteBak/s1-600000-c1-20261029/pdf-cache/derived/"
            "business_profile_pdfium_recovery.v4/"
            "e4d1cff0461c0ef24d26551ca68e31ad323a1b3eadd8a3c03f00feada364de22_57e1dd4f16ef40ff.json.gz"
        ),
    }
    report = _report()
    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    identity["ledger_tokens"] = 120_000
    for name, path in caches.items():
        assert path.is_file(), name
        data = json.loads(gzip.open(path, "rt", encoding="utf-8").read())
        pages = [
            {
                "page": int(item["page_number"]),
                "text": str(item.get("text") or ""),
                "readable": bool(str(item.get("text") or "").strip()),
            }
            for item in data["pages"]
        ]
        chosen = chapter_candidate_pages(pages)
        assert chosen, name
        assert len(chosen) <= 16, (name, chosen)
        if name == "bank":
            assert len(chosen) < 40
        selected = [page for page in pages if int(page["page"]) in set(chosen)]
        batches = navigation_batches(selected, report, identity, 24_000)
        assert batches, name
        nav = _units(
            batches[0],
            purpose="source_inventory",
            source_scope="page_map",
            report=report,
            identity=identity,
            output=1000,
        )
        from research.company_profile.direct_reading import source_discovery_slices

        first = list(source_discovery_slices(selected, report, identity, 24_000))[0]
        inventory = _units(
            first,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            output=SOURCE_OUTPUT,
        )
        tail = _compose_tail(first, report=report, identity=identity)
        assert nav + inventory + tail < 120_000, (
            name,
            nav,
            inventory,
            tail,
            chosen,
            [int(page["page"]) for page in first],
        )
        assert inventory > 1_000, (name, inventory)
        identity["request_reservation_limit"] = 65_536
        later = _first_delivery_tail(
            selected, report=report, identity=identity, limit=65_536
        )
        assert nav + later < 120_000, (name, nav, later, chosen)


def test_representative_source_return_drafts_and_closes_the_first_core_pack():
    from research.company_profile.contracts import ReportSourceCondition

    report = _report()
    pages = tuple(
        {
            "page": page,
            "text": (
                f"第{page}节 经营情况讨论与分析。\n"
                "公司主营造纸并销售纸袋纸。"
                + ("纸袋纸产线与销售渠道说明。" * 24)
                + "公司营业收入为173964000000元。公司采购木片并向下游客户销售纸制品。"
            ),
            "readable": True,
        }
        for page in (9, 12, 16)
    )
    body = pages[0]["text"]
    quote = "公司主营造纸并销售纸袋纸。"

    class _Provider:
        offline_only = True

        def interpret(self, request):
            current = tuple(int(page["page"]) for page in request.source_pages)
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=current,
                )
            if request.purpose == "source_inventory":
                assert sum(len(str(page.get("text") or "")) for page in request.source_pages) > 200
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=quote,
                            evidence=(
                                _evidence(report, text=quote, page=current[0]).model_copy(
                                    update={"evidence_id": f"size-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for dimension in (
                            "principal_business",
                            "products_services",
                            "revenue_model",
                            "input_exposure",
                            "supply_chain_role",
                        )
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=item.condition_id,
                            dimension=item.dimension,
                            status="observed",
                            answer=quote,
                            condition_ids=(item.condition_id,),
                            evidence=item.evidence,
                        )
                        for item in request.source_conditions
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "已核。",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": next(
                            (
                                claim.claim_id
                                for claim in request.claims
                                if claim.dimension == condition.dimension
                            ),
                            condition.condition_id,
                        ),
                        "explanation": "覆盖。",
                    }
                    for condition in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    identity["ledger_tokens"] = 120_000
    result = _run(
        _Provider(),
        pages=pages,
        processing_identity=identity,
        report=report,
        token_budget=120_000,
    )
    assert result.draft_complete is True
    assert {item.dimension for item in result.claims if item.status == "observed"} >= {
        "principal_business",
        "revenue_model",
    }
    assert not any(gap.startswith("direct_core_ledger_hold") for gap in result.gaps)
    assert not any(gap.startswith("direct_core_admission_stopped") for gap in result.gaps)


def _official_bank_pages(*wanted):
    import gzip
    from pathlib import Path

    path = Path(
        "/home/python/Quote/data/QuoteBak/s1-600000-c1-20261029/pdf-cache/derived/"
        "business_profile_pdfium_recovery.v4/"
        "e4d1cff0461c0ef24d26551ca68e31ad323a1b3eadd8a3c03f00feada364de22_57e1dd4f16ef40ff.json.gz"
    )
    data = json.loads(gzip.open(path, "rt", encoding="utf-8").read())
    wanted = {int(page) for page in wanted}
    return [
        {
            "page": int(item["page_number"]),
            "text": str(item.get("text") or ""),
            "readable": True,
        }
        for item in data["pages"]
        if int(item["page_number"]) in wanted
    ]


def _c1b_targets():
    from pathlib import Path

    from research.company_profile.contracts import ReportSourceCondition

    root = Path(
        "/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/"
        "s1-600000-c1b-result"
    )
    claims = [
        ReportViewClaim.model_validate_json(json.dumps(item, ensure_ascii=False))
        for item in json.loads((root / "claims.json").read_text())
    ]
    conditions = [
        ReportSourceCondition.model_validate_json(json.dumps(item, ensure_ascii=False))
        for item in json.loads((root / "conditions.json").read_text())
    ]
    report = claims[0].evidence[0].report
    return root, report, claims, conditions


def test_real_twelve_verify_pack_is_split_by_remaining_ledger():
    from research.company_profile.direct_reading import (
        compact_source_pages,
        compact_verify_targets,
        compatible_evidence_targets,
        joint_verify_reservation,
    )
    from research.company_profile.report_interpretation import direct_core_processing_identity

    _root, report, claims, conditions = _c1b_targets()
    identity = direct_core_processing_identity()
    identity["ledger_tokens"] = 120_000
    identity["request_reservation_limit"] = 65_536
    conditions = compatible_evidence_targets(claims, conditions)
    pages = []
    for item in (*claims, *conditions):
        for evidence in item.evidence:
            pages.append(
                {
                    "page": int(evidence.page),
                    "text": str(evidence.anchor.context_before or evidence.anchor.bounded_quote),
                    "readable": True,
                }
            )
    raw = joint_verify_reservation(report, identity, pages, claims, conditions, ())
    assert raw > 58_785
    packed_claims, packed_conditions, packed_records = compact_verify_targets(
        claims, conditions, ()
    )
    packed_conditions = compatible_evidence_targets(packed_claims, packed_conditions)
    packed_pages = compact_source_pages(pages, [*packed_claims, *packed_conditions])
    room = 58_785
    first = []
    rest = []
    for claim in packed_claims:
        trial = first + [claim]
        units = joint_verify_reservation(
            report, identity, packed_pages, trial, packed_conditions[:2], packed_records
        )
        if first and units > room:
            rest.append(claim)
        else:
            first.append(claim)
    first_units = joint_verify_reservation(
        report, identity, packed_pages, first, packed_conditions[:2], packed_records
    )
    assert first
    assert first_units <= room
    assert {item.claim_id for item in (*first, *rest)} == {item.claim_id for item in claims}


def test_read_pages_surface_missed_operating_revenue_totals():
    from research.company_profile.direct_reading import missed_core_revenue_facts

    _root, _report, claims, conditions = _c1b_targets()
    pages = _official_bank_pages(32, 71)
    missed = missed_core_revenue_facts(pages, conditions, claims)
    amounts = {item["amount"].replace(",", "") for item in missed}
    assert "1739.64" in amounts
    assert {"173964", "120483", "22725", "30756"} & amounts


def test_custody_conflict_pages_are_kept_with_the_draft():
    from research.company_profile.direct_reading import related_conflict_pages

    _root, _report, claims, _conditions = _c1b_targets()
    custody = next(item for item in claims if item.claim_id == "claim-asset-custody-business")
    extra = related_conflict_pages(custody, _official_bank_pages(69, 89))
    assert {int(page["page"]) for page in extra} == {89}
    assert "11.47" in extra[0]["text"]


def test_c1b_pending_targets_verify_under_remaining_and_reread():
    from pathlib import Path

    from research.company_profile.report_interpretation import research_views

    root, report, claims, conditions = _c1b_targets()
    pages = _official_bank_pages(
        *{
            int(evidence.page)
            for item in (*claims, *conditions)
            for evidence in item.evidence
        },
        32,
        71,
    )
    receipt = {
        "request_id": "c1b-source",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [int(page["page"]) for page in pages],
        "physical_request": False,
        "response_payload": ReportSemanticResponseV3(
            request_id="c1b-source",
            purpose="source_inventory",
            source_conditions=tuple(conditions),
        ).model_dump(mode="json"),
    }
    sent = []
    verify_batches = []

    class _Provider:
        offline_only = True

        def interpret(self, request):
            sent.append(request.purpose)
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=f"sub-{item.condition_id.split(':')[-1]}",
                            dimension=item.dimension,
                            status="observed",
                            answer="替换传输：仅证明漏项问题进入成稿协议。",
                            condition_ids=(item.condition_id,),
                            evidence=item.evidence,
                        )
                        for item in request.source_conditions
                    ),
                )
            texts = "\n".join(str(page.get("text") or "") for page in request.source_pages)
            checks = []
            for claim in request.claims:
                conflict = (
                    claim.claim_id == "claim-asset-custody-business"
                    and "11.45" in (claim.answer or "")
                    and "11.47" in texts
                )
                missed_claim = "period-operating-revenue" in str(claim.condition_ids or ())
                checks.append(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "block" if conflict else "unclear" if missed_claim else "pass",
                        "explanation": "替换核验。",
                    }
                )
            for condition in request.source_conditions:
                missed = "period-operating-revenue" in condition.condition_id
                cover = next(
                    (
                        claim.claim_id
                        for claim in request.claims
                        if condition.condition_id in (claim.condition_ids or ())
                    ),
                    request.claims[0].claim_id if request.claims else condition.condition_id,
                )
                checks.append(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "unclear" if missed else "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": cover,
                        "explanation": "漏项待核。" if missed else "覆盖。",
                    }
                )
            verify_batches.append([claim.claim_id for claim in request.claims])
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(checks),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(item.model_dump_json()) for item in claims]
    }
    identity["prior_charged_tokens"] = 61_215
    identity["ledger_tokens"] = 120_000
    identity["request_reservation_limit"] = 65_536
    result = _run(
        _Provider(),
        pages=tuple(pages),
        processing_identity=identity,
        report=report,
        token_budget=120_000,
    )
    assert "verify_views" in sent
    assert result.checks
    sent_ids = {item for batch in verify_batches for item in batch}
    pending_ids = {
        gap.split(":", 1)[-1]
        for gap in result.gaps
        if gap.startswith(("verify_unit_not_admitted:", "claim_pending:"))
    }
    assert {item.claim_id for item in claims} <= sent_ids | pending_ids
    assert any(gap == "read_but_missed:period-operating-revenue" for gap in result.gaps)
    out = Path("/home/python/Quote/data/QuoteBak/s1-600000-c1b-offline-s15")
    out.mkdir(parents=True, exist_ok=True)
    work = out / "work.json"
    work.write_text(result.model_dump_json(), encoding="utf-8")
    loaded = type(result).model_validate_json(work.read_text(encoding="utf-8"))
    first = research_views(result)
    second = research_views(loaded)
    assert first == second
    assert {item.claim_id for item in loaded.claims} == {item.claim_id for item in result.claims}
    assert result.draft_complete is False


def test_later_verify_pack_failure_keeps_the_earlier_group():
    report = _report()
    dimensions = (
        "principal_business",
        "products_services",
        "revenue_model",
        "input_exposure",
        "supply_chain_role",
    )
    texts = {
        "principal_business": "公司经营公司金融业务。",
        "products_services": "公司提供零售银行服务。",
        "revenue_model": "公司按利息和手续费确认当期收入。",
        "input_exposure": "公司吸收客户存款作为资金来源。",
        "supply_chain_role": "公司向企业客户发放贷款。",
    }
    from research.company_profile.contracts import ReportSourceCondition

    conditions = [
        ReportSourceCondition(
            condition_id=dimension,
            dimension=dimension,
            description=texts[dimension],
            evidence=(
                _evidence(report, text=texts[dimension], page=index + 1).model_copy(
                    update={"evidence_id": f"keep-{dimension}"}
                ),
            ),
            major=True,
            statement_status="current",
        )
        for index, dimension in enumerate(dimensions)
    ]
    claims = [
        ReportViewClaim(
            claim_id=dimension,
            dimension=dimension,
            status="observed",
            answer=texts[dimension],
            condition_ids=(dimension,),
            evidence=item.evidence,
            limitation="pending_verification",
        )
        for item, dimension in zip(conditions, dimensions)
    ]
    pages = [
        {"page": index + 1, "text": texts[dimension], "readable": True}
        for index, dimension in enumerate(dimensions)
    ]
    receipt = {
        "request_id": "keep-source",
        "purpose": "source_inventory",
        "status": "success",
        "source_scope": "full_context",
        "source_pages": [page["page"] for page in pages],
        "physical_request": False,
        "response_payload": ReportSemanticResponseV3(
            request_id="keep-source",
            purpose="source_inventory",
            source_conditions=tuple(conditions),
        ).model_dump(mode="json"),
    }

    class _Provider:
        offline_only = True

        def __init__(self):
            self.verifies = 0

        def interpret(self, request):
            if request.purpose != "verify_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(),
                )
            self.verifies += 1
            if self.verifies >= 2:
                raise ValueError("later-verify-failed")
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "先核。",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": condition.condition_id,
                        "explanation": "覆盖。",
                    }
                    for condition in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [receipt]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(item.model_dump_json()) for item in claims]
    }
    identity["ledger_tokens"] = 120_000
    identity["request_reservation_limit"] = 12_000
    identity["prior_charged_tokens"] = 0
    provider = _Provider()
    result = _run(
        provider,
        pages=tuple(pages),
        processing_identity=identity,
        report=report,
        token_budget=120_000,
    )
    assert any(check.status == "pass" for check in result.checks)
    assert any("later-verify-failed" in gap or "direct_core_verify_stopped" in gap for gap in result.gaps)
    assert any(item.status == "observed" for item in result.claims)


def test_distinct_majors_can_finish_after_verify():
    report = _report()
    pages = (
        {"page": 1, "text": "一、主营业务\n公司主营造纸。", "readable": True},
        {"page": 2, "text": "二、主要产品\n公司销售纸袋纸和包装纸。", "readable": True},
        {"page": 3, "text": "三、营业收入\n公司按利息和手续费确认当期收入。", "readable": True},
        {"page": 4, "text": "四、采购与投入\n公司采购木片作为原材料。", "readable": True},
        {"page": 5, "text": "五、销售与客户\n公司向下游包装厂销售纸制品。", "readable": True},
    )
    bodies = {
        1: ("principal_business", "公司主营造纸。"),
        2: ("products_services", "公司销售纸袋纸和包装纸。"),
        3: ("revenue_model", "公司按利息和手续费确认当期收入。"),
        4: ("input_exposure", "公司采购木片作为原材料。"),
        5: ("supply_chain_role", "公司向下游包装厂销售纸制品。"),
    }

    class _Provider:
        offline_only = True

        def interpret(self, request):
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1, 2, 3, 4, 5),
                )
            if request.purpose == "source_inventory":
                from research.company_profile.contracts import ReportSourceCondition

                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    source_conditions=tuple(
                        ReportSourceCondition(
                            condition_id=dimension,
                            dimension=dimension,
                            description=text,
                            evidence=(
                                _evidence(report, text=text, page=page).model_copy(
                                    update={"evidence_id": f"fin-{dimension}"}
                                ),
                            ),
                            major=True,
                            statement_status="current",
                        )
                        for page, (dimension, text) in bodies.items()
                        if page in {int(item["page"]) for item in request.source_pages}
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        ReportViewClaim(
                            claim_id=item.condition_id,
                            dimension=item.dimension,
                            status="observed",
                            answer=item.description,
                            condition_ids=(item.condition_id,),
                            evidence=item.evidence,
                        )
                        for item in request.source_conditions
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": "claim",
                        "target_id": claim.claim_id,
                        "status": "pass",
                        "explanation": "已核。",
                    }
                    for claim in request.claims
                )
                + tuple(
                    {
                        "target_type": "condition",
                        "target_id": condition.condition_id,
                        "status": "pass",
                        "covers_target_type": "claim",
                        "covers_target_id": condition.condition_id,
                        "explanation": "覆盖。",
                    }
                    for condition in request.source_conditions
                ),
            )

    identity = direct_core_processing_identity()
    identity["ledger_tokens"] = 120_000
    result = _run(
        _Provider(),
        pages=pages,
        processing_identity=identity,
        report=report,
        token_budget=120_000,
    )
    assert result.draft_complete is True
    assert result.checks
    assert {item.dimension for item in result.claims if item.status == "observed"} >= set(
        dimension for dimension, _text in bodies.values()
    )


def test_declared_output_fields_do_not_hide_actual_usage():
    from pathlib import Path

    from research.company_profile.reading_budget import _profiles, request_reservation
    from research.company_profile.report_wire import compact_request
    from research.company_profile.contracts import ReportSemanticRequestV3

    _root, report, claims, conditions = _c1b_targets()
    identity = direct_core_processing_identity()
    request = ReportSemanticRequestV3(
        request_id="output-fields",
        purpose="verify_views",
        report=report,
        processing_identity=identity,
        source_pages=({"page": 71, "text": "营业收入 173,964", "readable": True},),
        source_scope="full_context",
        claims=tuple(claims[:1]),
        source_conditions=tuple(conditions[:1]),
        max_output_tokens=2000,
        timeout_seconds=120,
    )
    payload = compact_request(request)
    assert "max_output_tokens" not in payload
    fields = {profile.max_output_tokens_field for profile in _profiles()}
    assert "max_completion_tokens" in fields
    reservation = request_reservation(request)
    assert reservation >= 2000
    budget = json.loads(
        Path(
            "/home/python/Quote/openspec/changes/restore-company-profile-llm-led-generalization/"
            "s1-600000-c1b-result/budget-final.json"
        ).read_text()
    )
    observed = [item["usage"]["output_tokens"] for item in budget["attempts"]]
    declared = [item["reserved_output_tokens"] for item in budget["attempts"]]
    assert declared == [1000, 4000, 2500]
    assert observed == [1990, 17788, 16415]
    assert all(left > right for left, right in zip(observed, declared))


def test_whitespace_only_quote_keeps_subject_unit_and_negation():
    from research.company_profile.direct_reading import unique_context_prefix

    kept = unique_context_prefix(
        "母公司单体，单位：亿元，2025年。\n公司实现 营业收入 1,739.64 亿元。\n不包括子公司。",
        "公司实现营业收入1,739.64亿元。",
    )
    assert "母公司单体" in kept
    assert "亿元" in kept
    assert "不包括子公司" in kept


def test_quote_amount_does_not_count_as_answered_revenue():
    from research.company_profile.contracts import ReportViewClaim
    from research.company_profile.direct_reading import missed_core_revenue_facts
    from tests.unit.test_research.test_company_profile_llm_led import _evidence, _report

    report = _report()
    text = "2025年本集团营业收入1,739.64亿元；公司提供贷款与结算服务。"
    claim = ReportViewClaim(
        claim_id="loan-only",
        dimension="principal_business",
        status="observed",
        answer="公司提供贷款与结算服务。",
        evidence=(_evidence(report, text=text, page=1),),
    )
    missed = missed_core_revenue_facts(
        ({"page": 1, "text": text, "readable": True},),
        (),
        (claim,),
    )
    assert any(item["amount"].replace(",", "") == "1739.64" for item in missed)


def _labeled_c1b_reply(raw, *, fail_after=None, seen=None):
    if seen is not None and raw["purpose"] == "verify_views":
        seen["verifies"] = seen.get("verifies", 0) + 1
        seen.setdefault("claim_ids", set()).update(
            claim.get("claim_id") for claim in raw.get("claims") or []
        )
        if fail_after is not None and seen["verifies"] > fail_after:
            raise ValueError("later-verify-failed")
    result = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": raw["request_id"],
        "purpose": raw["purpose"],
        "substitute": "c1b_core_verify_v1",
    }
    if raw["purpose"] == "synthesize_views":
        result["claims"] = [
            {
                "claim_id": f"sub-{item.get('condition_id', '').split(':')[-1]}",
                "dimension": item.get("dimension") or "revenue_model",
                "status": "observed",
                "answer": "替换传输：仅证明漏项问题进入成稿协议。",
                "condition_ids": [item["condition_id"]] if item.get("condition_id") else [],
                "evidence": item.get("evidence") or [],
            }
            for item in raw.get("source_conditions") or []
        ]
        return result
    texts = "\n".join(str(page.get("text") or "") for page in raw.get("source_pages") or [])
    checks = []
    for claim in raw.get("claims") or []:
        conflict = (
            claim.get("claim_id") == "claim-asset-custody-business"
            and "11.45" in str(claim.get("answer") or "")
            and "11.47" in texts
        )
        missed = "period-operating-revenue" in str(claim.get("condition_ids") or [])
        checks.append(
            {
                "target_type": "claim",
                "target_id": claim.get("claim_id"),
                "status": "block" if conflict else "unclear" if missed else "pass",
                "explanation": "替换核验。",
            }
        )
    for condition in raw.get("source_conditions") or []:
        missed = "period-operating-revenue" in str(condition.get("condition_id") or "")
        cover = next(
            (
                claim.get("claim_id")
                for claim in raw.get("claims") or []
                if condition.get("condition_id") in (claim.get("condition_ids") or [])
            ),
            (raw.get("claims") or [{}])[0].get("claim_id")
            if raw.get("claims")
            else condition.get("condition_id"),
        )
        checks.append(
            {
                "target_type": "condition",
                "target_id": condition.get("condition_id"),
                "status": "unclear" if missed else "pass",
                "covers_target_type": "claim",
                "covers_target_id": cover,
                "explanation": "漏项待核。" if missed else "覆盖。",
            }
        )
    result["checks"] = checks
    return result


def _persist_and_reread(tmp_path, result, report, identity):
    from research.company_profile.execution import (
        CompanyProfileTokenBudget,
        build_execution_record,
    )
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.report_interpretation import core_assessment
    from research.company_profile.runtime import (
        CompanyProfileResearchWriter,
        CompanyProfileRuntimeRecordV2,
    )

    work_id = "offline-s16"
    writer = CompanyProfileResearchWriter(tmp_path / "output")
    used = int(result.tokens_charged)
    record = CompanyProfileRuntimeRecordV2(
        work_id=work_id,
        report=report,
        assessment=core_assessment(result),
        activated_chapters=(),
        evidence_gap_codes=tuple(result.gaps),
        provider_calls=tuple(
            str(item.get("purpose") or "") for item in result.call_receipts
        ),
        provider_blocked=False,
        execution=build_execution_record(
            report=report,
            processing_identity=identity,
            scope_digests={},
            model_attempts=(),
            token_budget=CompanyProfileTokenBudget(
                total_token_budget=120_000,
                extract_max_output_tokens=4_000,
                verify_max_output_tokens=2_000,
                tokens_used=used,
                tokens_remaining=max(0, 120_000 - used),
            ),
            transport_retries=0,
            semantic_disputes=(),
            predecessor_lineage=(),
        ),
        report_interpretation=result,
    )
    writer.persist(record)
    first = CompanyProfileReadService(tmp_path / "output")
    queried = first.query((report.instrument_id,), work_id=work_id)
    exported = first.export(
        (report.instrument_id,),
        tmp_path / "export",
        work_id=work_id,
    )
    reloaded = CompanyProfileReadService(tmp_path / "output").query(
        (report.instrument_id,),
        work_id=work_id,
    )
    return queried, exported, reloaded


def _fresh_budget(token_budget=120_000, max_requests=16):
    import time

    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget

    return LlmRunBudget(
        deadline=time.monotonic() + 180,
        prices={},
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        max_tokens=token_budget,
        max_requests=max_requests,
    )


def test_c1b_targets_reach_query_export_and_new_instance(tmp_path):
    _root, report, claims, conditions = _c1b_targets()
    pages = _official_bank_pages(
        *{
            int(evidence.page)
            for item in (*claims, *conditions)
            for evidence in item.evidence
        },
        32,
        71,
    )
    verify_ids = set()

    class _Provider:
        offline_only = True

        def interpret(self, request):
            raw = json.loads(request.model_dump_json())
            if request.purpose == "verify_views":
                verify_ids.update(claim.claim_id for claim in request.claims)
            payload = _labeled_c1b_reply(raw)
            return ReportSemanticResponseV3.model_validate(payload)

    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [
        {
            "request_id": "c1b-source",
            "purpose": "source_inventory",
            "status": "success",
            "source_scope": "full_context",
            "source_pages": [int(page["page"]) for page in pages],
            "physical_request": False,
            "response_payload": ReportSemanticResponseV3(
                request_id="c1b-source",
                purpose="source_inventory",
                source_conditions=tuple(conditions),
            ).model_dump(mode="json"),
        }
    ]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(item.model_dump_json()) for item in claims]
    }
    identity["prior_charged_tokens"] = 61_215
    identity["ledger_tokens"] = 120_000
    identity["request_reservation_limit"] = 65_536
    budget = _fresh_budget()
    result = _run(
        _Provider(),
        pages=tuple(pages),
        processing_identity=identity,
        report=report,
        token_budget=120_000,
        execution_budget=budget,
    )
    pending_ids = {
        gap.split(":", 1)[-1]
        for gap in result.gaps
        if gap.startswith(("verify_unit_not_admitted:", "claim_pending:"))
    }
    assert {item.claim_id for item in claims} <= verify_ids | pending_ids
    queried, exported, reloaded = _persist_and_reread(tmp_path, result, report, identity)
    assert queried["delivered"] == 1
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    profile = queried["profiles"][0]["three_views"]
    assert profile["draft_complete"] is False
    assert any(
        gap == "read_but_missed:period-operating-revenue" for gap in profile.get("gaps") or []
    )


def test_later_verify_failure_keeps_earlier_query_export(tmp_path):
    report = _report()
    dimensions = (
        "principal_business",
        "products_services",
        "revenue_model",
        "input_exposure",
        "supply_chain_role",
    )
    texts = {
        "principal_business": "公司经营公司金融业务。",
        "products_services": "公司提供零售银行服务。",
        "revenue_model": "公司按利息和手续费确认当期收入。",
        "input_exposure": "公司吸收客户存款作为资金来源。",
        "supply_chain_role": "公司向企业客户发放贷款。",
    }
    from research.company_profile.contracts import ReportSourceCondition

    conditions = [
        ReportSourceCondition(
            condition_id=dimension,
            dimension=dimension,
            description=texts[dimension],
            evidence=(
                _evidence(report, text=texts[dimension], page=index + 1).model_copy(
                    update={"evidence_id": f"keep-{dimension}"}
                ),
            ),
            major=True,
            statement_status="current",
        )
        for index, dimension in enumerate(dimensions)
    ]
    claims = [
        ReportViewClaim(
            claim_id=dimension,
            dimension=dimension,
            status="observed",
            answer=texts[dimension],
            condition_ids=(dimension,),
            evidence=item.evidence,
            limitation="pending_verification",
        )
        for item, dimension in zip(conditions, dimensions)
    ]
    pages = [
        {"page": index + 1, "text": texts[dimension], "readable": True}
        for index, dimension in enumerate(dimensions)
    ]
    seen = {}
    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [
        {
            "request_id": "keep-source",
            "purpose": "source_inventory",
            "status": "success",
            "source_scope": "full_context",
            "source_pages": [page["page"] for page in pages],
            "physical_request": False,
            "response_payload": ReportSemanticResponseV3(
                request_id="keep-source",
                purpose="source_inventory",
                source_conditions=tuple(conditions),
            ).model_dump(mode="json"),
        }
    ]
    identity["accepted_prior_draft"] = {
        "claims": [json.loads(item.model_dump_json()) for item in claims]
    }
    identity["ledger_tokens"] = 120_000
    identity["request_reservation_limit"] = 12_000

    class _Provider:
        offline_only = True

        def interpret(self, request):
            raw = json.loads(request.model_dump_json())
            payload = _labeled_c1b_reply(raw, fail_after=1, seen=seen)
            return ReportSemanticResponseV3.model_validate(payload)

    budget = _fresh_budget()
    result = _run(
        _Provider(),
        pages=tuple(pages),
        processing_identity=identity,
        report=report,
        token_budget=120_000,
        execution_budget=budget,
    )
    queried, exported, reloaded = _persist_and_reread(tmp_path, result, report, identity)
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    profile = queried["profiles"][0]["three_views"]
    assert any(
        claim.get("status") == "observed"
        for view in profile["views"].values()
        for claim in view.get("claims") or []
    )
    assert seen.get("verifies", 0) >= 1
    assert any("later-verify-failed" in gap or "direct_core_verify_stopped" in gap for gap in result.gaps)


def _isolated_gateway(budget, reply, *, sends=None, usage=120):
    from utils.llm import CallableTransport, LlmClient, LlmConfig, LlmPoolCoordinatorRegistry
    from utils.llm.orchestration import ProviderCoordinatorRegistry
    from utils.llm.rate_limit import ProfileLimiterRegistry
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider

    async def transport(_url, _headers, payload, _timeout):
        request = json.loads(
            next(item["content"] for item in payload["messages"] if item["role"] == "user")
        )
        if sends is not None:
            sends.append(request.get("purpose"))
        encoded = json.dumps(reply(request), ensure_ascii=False)
        return {
            "status_code": 200,
            "data": {
                "choices": [{"message": {"content": encoded}}],
                "usage": {
                    "prompt_tokens": 80,
                    "completion_tokens": 40,
                    "total_tokens": usage,
                },
            },
        }

    config = LlmConfig.from_mapping(
        {
            "enabled": True,
            "provider_resources": {
                "openai_compatible:TEST_LLM_KEY": {"requests_per_minute": 0}
            },
            "profiles": {
                "test": {
                    "enabled": True,
                    "base_url": "https://provider.example/v1",
                    "endpoint": "/v1/chat/completions",
                    "api_key_env": "TEST_LLM_KEY",
                    "model": "test-model",
                    "structured_output_mode": "json_object",
                    "supported_structured_output_modes": ["json_object"],
                    "timeout_seconds": 8,
                    "queue_timeout_seconds": 2,
                    "attempt_timeout_seconds": 8,
                    "max_retries": 0,
                    "max_schema_repair_attempts": 0,
                    "max_concurrency": 2,
                    "requests_per_minute": 0,
                    "retry_backoff_seconds": 0,
                }
            },
        }
    )
    client = LlmClient(
        config,
        transport=CallableTransport(transport),
        environment={"TEST_LLM_KEY": "fake"},
        physical_budget=budget,
        limiter_registry=ProfileLimiterRegistry(),
        provider_coordinator_registry=ProviderCoordinatorRegistry(),
        pool_coordinator_registry=LlmPoolCoordinatorRegistry(),
    )

    class _Provider(CommonGatewaySemanticProvider):
        offline_only = True

    return _Provider(
        client=client,
        profile="test",
        max_output_tokens=4000,
        timeout_seconds=30,
    )


def test_gateway_usage_is_booked_once_not_reserved_again():
    report = _report()
    text = "公司经营公司金融业务，并向企业客户发放贷款。"
    pages = ({"page": 1, "text": text, "readable": True},)
    condition = {
        "condition_id": "principal_business",
        "dimension": "principal_business",
        "description": text,
        "evidence": [
            json.loads(
                _evidence(report, text=text[:12], page=1)
                .model_copy(update={"evidence_id": "once-e"})
                .model_dump_json()
            )
        ],
        "major": True,
        "statement_status": "current",
    }
    claim = {
        "claim_id": "principal_business",
        "dimension": "principal_business",
        "status": "observed",
        "answer": text,
        "condition_ids": ["principal_business"],
        "evidence": condition["evidence"],
        "limitation": "pending_verification",
    }
    identity = direct_core_processing_identity()
    identity["accepted_source_receipts"] = [
        {
            "request_id": "once-source",
            "purpose": "source_inventory",
            "status": "success",
            "source_scope": "full_context",
            "source_pages": [1],
            "physical_request": False,
            "response_payload": {
                "request_id": "once-source",
                "purpose": "source_inventory",
                "source_conditions": [condition],
            },
        }
    ]
    identity["accepted_prior_draft"] = {"claims": [claim]}
    identity["ledger_tokens"] = 120_000
    identity["request_reservation_limit"] = 12_000
    sends = []
    budget = _fresh_budget()
    result = _run(
        _isolated_gateway(budget, _labeled_c1b_reply, sends=sends, usage=120),
        pages=pages,
        processing_identity=identity,
        report=report,
        token_budget=120_000,
        execution_budget=budget,
    )
    snapshot = budget.snapshot()
    assert sends
    assert snapshot["physical_requests"] == len(sends)
    assert snapshot["charged_tokens"] == 120 * len(sends)
    assert all(item.get("source_label") != "offline_substitute" for item in snapshot["attempts"])
    assert result.tokens_charged == snapshot["charged_tokens"]


def test_concentration_does_not_complete_operating_role():
    from research.company_profile.direct_reading import _completeness_gaps

    report = _report()
    concentration = ReportViewClaim(
        claim_id="concentration",
        dimension="upstream_downstream",
        status="observed",
        answer="2025年度前五名客户销售额52,052.72万元，占年度销售总额21.52%。",
        evidence=(_evidence(report, text="前五名客户销售额52,052.72万元", page=21),),
    )
    business = ReportViewClaim(
        claim_id="business",
        dimension="principal_business",
        status="observed",
        answer="2025年度主营业务分行业涉及纸行业、浆行业。",
        evidence=(_evidence(report, text="纸行业、浆行业", page=16),),
    )
    products = ReportViewClaim(
        claim_id="products",
        dimension="products_services",
        status="observed",
        answer="主要产品包括纸、浆粕、浆板。",
        evidence=(_evidence(report, text="纸、浆粕、浆板", page=17),),
    )
    revenue = ReportViewClaim(
        claim_id="revenue",
        dimension="revenue_model",
        status="observed",
        answer="青山纸业公司2025年度营业收入为241,899.02万元。",
        evidence=(_evidence(report, text="营业收入为241,899.02万元", page=85),),
    )
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id=item.claim_id,
            status="pass",
            explanation="已核。",
        )
        for item in (concentration, business, products, revenue)
    ]
    gaps = _completeness_gaps(
        [concentration, business, products, revenue],
        [],
        checks,
    )
    assert "core_dimension_missing:operating_role" in gaps


def test_audit_wan_yuan_covers_income_statement_yuan():
    from research.company_profile.direct_reading import (
        _answer_covers_revenue_fact,
        missed_core_revenue_facts,
    )

    pages = [
        {
            "page": 16,
            "text": "利润表 本期 营业收入 2,418,990,221.17",
            "readable": True,
        },
        {
            "page": 85,
            "text": "青山纸业公司2025年度营业收入为241,899.02万元。",
            "readable": True,
        },
    ]
    covered = ReportViewClaim(
        claim_id="claim_operating_revenue_2025",
        dimension="revenue_model",
        status="observed",
        answer="青山纸业公司2025年度营业收入为241,899.02万元。",
        evidence=(_evidence(_report(), text="营业收入为241,899.02万元", page=85),),
    )
    assert _answer_covers_revenue_fact(
        covered.answer,
        {
            "kind": "operating_revenue_total",
            "amount": "2,418,990,221.17",
            "unit": "元",
        },
    )
    assert missed_core_revenue_facts(pages, (), [covered]) == []


def test_resolved_revenue_miss_is_not_kept_after_equivalent_pass():
    from research.company_profile.direct_reading import _current_gaps

    report = _report()
    claim = ReportViewClaim(
        claim_id="claim_operating_revenue_2025",
        dimension="revenue_model",
        status="observed",
        answer="青山纸业公司2025年度营业收入为241,899.02万元。",
        evidence=(_evidence(report, text="营业收入为241,899.02万元", page=85),),
    )
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id="claim_operating_revenue_2025",
            status="pass",
            explanation="总额已核。",
        ),
        ReportViewCheck(
            target_type="condition",
            target_id="core-question:period-operating-revenue",
            status="pass",
            explanation="总额已核。",
        ),
    ]
    live = _current_gaps(
        ["read_but_missed:period-operating-revenue", "chapter_gap_expansion"],
        [claim],
        [],
        checks,
    )
    assert "read_but_missed:period-operating-revenue" not in live
    assert "chapter_gap_expansion" in live


def test_unrelated_growth_rates_do_not_bloat_wealth_verify():
    from research.company_profile.direct_reading import related_conflict_pages

    report = _report()
    wealth = ReportViewClaim(
        claim_id="products_services_wealth",
        dimension="products_services",
        status="observed",
        answer="财富管理个人AUM余额46,645.33亿元，较上年末增长20.26%。",
        evidence=(_evidence(report, text="个人AUM余额46,645.33亿元，较上年末增长20.26%", page=39),),
    )
    extra = related_conflict_pages(
        wealth,
        [
            {
                "page": 39,
                "text": "个人AUM余额46,645.33亿元，较上年末增长20.26%。",
                "readable": True,
            },
            {
                "page": 57,
                "text": "本外币贷款总额较上年末增长5.80%，其中五大赛道贷款增长12.15%。托管条线协同推进。",
                "readable": True,
            },
            {
                "page": 69,
                "text": "报告期末托管规模20.21万亿元，较上年末增长11.45%。",
                "readable": True,
            },
            {
                "page": 32,
                "text": "本集团资产总额增长6.55%，营业收入增长1.88%。",
                "readable": True,
            },
        ],
    )
    assert {int(page["page"]) for page in extra} == set()


def test_s16_bank_single_target_verify_stays_under_payload():
    from pathlib import Path

    from research.company_profile.contracts import ReportSemanticRequestV3
    from research.company_profile.direct_reading import (
        compact_source_pages,
        compact_verify_targets,
        related_conflict_pages,
    )
    from research.company_profile.reading_budget import request_payload_bytes
    from research.company_profile.report_interpretation import (
        direct_core_processing_identity,
    )

    query = json.loads(
        Path(
            "/home/python/Quote/openspec/changes/"
            "restore-company-profile-llm-led-generalization/"
            "s1-600000-s16-result/query.json"
        ).read_text()
    )
    report = None
    pending = []
    seen = set()
    for view in (query["profiles"][0].get("three_views") or {}).get("views", {}).values():
        for raw in view.get("claims") or []:
            claim_id = raw.get("claim_id")
            if claim_id in seen:
                continue
            seen.add(claim_id)
            if claim_id not in {
                "claim-asset-custody-services",
                "principal_business",
                "revenue_model",
                "products_services_wealth",
            }:
                continue
            payload = {key: value for key, value in raw.items() if key != "review_state"}
            claim = ReportViewClaim.model_validate_json(
                json.dumps(payload, ensure_ascii=False)
            )
            pending.append(claim)
            if report is None and claim.evidence:
                report = claim.evidence[0].report
    assert len(pending) == 4
    pages = _official_bank_pages(*range(1, 91))
    identity = direct_core_processing_identity()
    for claim in pending:
        extra = related_conflict_pages(claim, pages)
        if claim.claim_id == "claim-asset-custody-services":
            extras = {int(page["page"]) for page in extra}
            assert 89 in extras
            assert len(extras) <= 3
        group, conditions, related = compact_verify_targets([claim], (), ())
        attached = [
            {"page": int(item.page), "text": item.anchor.bounded_quote, "readable": True}
            for item in claim.evidence
        ]
        for page in extra:
            if int(page["page"]) not in {int(item["page"]) for item in attached}:
                attached.append(page)
        packed = compact_source_pages(attached, [*group, *conditions, *related])
        sized = ReportSemanticRequestV3(
            request_id="s17-verify-size",
            purpose="verify_views",
            report=report,
            processing_identity=identity,
            source_pages=tuple(packed),
            source_scope="full_context",
            claims=tuple(group),
            max_output_tokens=4000,
            timeout_seconds=120,
        )
        assert request_payload_bytes(sized) <= 60_000


def test_chapter_select_compatible_continue_keeps_saved_semantics():
    report = _report()
    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    identity["compatible_continue"] = True
    identity["accepted_source_receipts"] = [
        {
            "request_id": "s16-source",
            "purpose": "source_inventory",
            "status": "success",
            "source_pages": [16, 21],
            "response_payload": {
                "request_id": "s16-source",
                "purpose": "source_inventory",
                "source_conditions": [],
            },
        }
    ]
    identity["accepted_prior_draft"] = {
        "claims": [
            {
                "claim_id": "concentration",
                "dimension": "upstream_downstream",
                "status": "observed",
                "answer": "2025年度前五名客户销售额52,052.72万元，占年度销售总额21.52%。",
                "evidence": [
                    _evidence(
                        report, text="前五名客户销售额52,052.72万元", page=21
                    ).model_dump(mode="json")
                ],
                "supporting_record_ids": [],
            }
        ],
        "checks": [
            {
                "target_type": "claim",
                "target_id": "concentration",
                "status": "pass",
                "explanation": "集中度已核。",
            }
        ],
        "verify_receipts": [],
    }

    class _Provider:
        offline_only = True
        purposes = []

        def interpret(self, request):
            self.purposes.append(request.purpose)
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="operating-role",
                            dimension="supply_chain_role",
                            status="observed",
                            answer="公司已形成林-浆-纸产业链协同，拥有制浆造纸生产线。",
                            evidence=(
                                _evidence(
                                    report,
                                    text="公司目前已形成“林-浆-纸”产业链协同效应",
                                    page=13,
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    ReportViewCheck(
                        target_type="claim",
                        target_id="operating-role",
                        status="pass",
                        explanation="角色有原文。",
                    ),
                    ReportViewCheck(
                        target_type="condition",
                        target_id="core-question:operating-role",
                        status="pass",
                        explanation="经营角色已核。",
                        covers_target_type="claim",
                        covers_target_id="operating-role",
                    ),
                ),
            )

    provider = _Provider()
    result = _run(
        provider,
        pages=(
            {
                "page": 13,
                "text": "三、经营情况讨论与分析 公司目前已形成“林-浆-纸”产业链协同效应，拥有多条制浆造纸生产线。",
                "readable": True,
            },
            {"page": 16, "text": "主营业务分行业纸行业、浆行业。", "readable": True},
            {
                "page": 21,
                "text": "前五名客户销售额52,052.72万元，占年度销售总额21.52%。",
                "readable": True,
            },
        ),
        processing_identity=identity,
        report=report,
    )
    assert "core_dimension_missing:operating_role" not in result.gaps
    assert any(claim.claim_id == "operating-role" for claim in result.claims)


def test_toc_uses_printed_section_start_not_the_toc_page():
    from research.company_profile.direct_reading import chapter_candidate_pages

    pages = [
        {
            "page": 4,
            "text": (
                "目录\n"
                "第一节 释义 ......................................................... 5\n"
                "第三节 管理层讨论与分析 ............................................. 9\n"
                "第八节 财务报告 .................................................... 85\n"
            ),
            "readable": True,
        },
        {
            "page": 9,
            "text": (
                "福建省青山纸业股份有限公司2025 年年度报告\n"
                "9 / 220\n"
                "非经常性损益项目和金额\n"
                "十一、 存在股权激励\n"
                "第三节 管理层讨论与分析\n"
            ),
            "readable": True,
        },
        {
            "page": 11,
            "text": "作为制浆造纸企业，公司具备制浆造纸生产线，主要经营模式如下。",
            "readable": True,
        },
        {"page": 16, "text": "主营业务分行业纸行业、浆行业。", "readable": True},
        {"page": 85, "text": "第八节 财务报告\n合并利润表", "readable": True},
    ]
    chosen = chapter_candidate_pages(pages)
    assert 9 in chosen
    assert chosen.count(4) <= 1


def test_compatible_continue_drafts_existing_role_from_unread_section():
    report = _report()
    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    identity["compatible_continue"] = True
    identity["accepted_source_receipts"] = [
        {
            "request_id": "s16-source",
            "purpose": "source_inventory",
            "status": "success",
            "source_pages": [16, 21],
            "response_payload": {
                "request_id": "s16-source",
                "purpose": "source_inventory",
                "source_conditions": [
                    {
                        "condition_id": "core-question:operating-role",
                        "dimension": "supply_chain_role",
                        "description": "说明生产加工销售角色",
                        "major": True,
                        "statement_status": "current",
                        "evidence": [
                            _evidence(
                                report, text="主营业务分行业纸行业、浆行业。", page=16
                            ).model_dump(mode="json")
                        ],
                    }
                ],
            },
        }
    ]
    identity["accepted_prior_draft"] = {
        "claims": [
            {
                "claim_id": "concentration",
                "dimension": "upstream_downstream",
                "status": "observed",
                "answer": "2025年度前五名客户销售额52,052.72万元，占年度销售总额21.52%。",
                "evidence": [
                    _evidence(
                        report, text="前五名客户销售额52,052.72万元", page=21
                    ).model_dump(mode="json")
                ],
                "supporting_record_ids": [],
            }
        ],
        "checks": [
            {
                "target_type": "claim",
                "target_id": "concentration",
                "status": "pass",
                "explanation": "集中度已核。",
            },
            {
                "target_type": "condition",
                "target_id": "core-question:operating-role",
                "status": "unclear",
                "explanation": "现有声称没有说明生产加工销售角色。",
            },
        ],
        "verify_receipts": [],
    }

    class _Provider:
        offline_only = True
        inventoried = []
        drafted = []

        def interpret(self, request):
            pages = [int(page["page"]) for page in request.source_pages]
            if request.purpose == "source_inventory":
                self.inventoried.extend(pages)
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                )
            if request.purpose == "synthesize_views":
                self.drafted.extend(
                    item.condition_id for item in request.source_conditions
                )
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id="operating-role",
                            dimension="supply_chain_role",
                            status="observed",
                            answer="公司浆纸主业自主生产，具备制浆造纸生产线，并形成林-浆-纸一体化。",
                            evidence=(
                                _evidence(
                                    report,
                                    text="具备制浆造纸生产线及配套系统，主要经营模式如下",
                                    page=11,
                                ),
                            ),
                        ),
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=(
                    ReportViewCheck(
                        target_type="claim",
                        target_id="operating-role",
                        status="pass",
                        explanation="角色有原文。",
                    ),
                    ReportViewCheck(
                        target_type="condition",
                        target_id="core-question:operating-role",
                        status="pass",
                        explanation="经营角色已核。",
                        covers_target_type="claim",
                        covers_target_id="operating-role",
                    ),
                ),
            )

    provider = _Provider()
    result = _run(
        provider,
        pages=(
            {
                "page": 4,
                "text": (
                    "目录\n"
                    "第三节 管理层讨论与分析 ............................................. 9\n"
                    "第八节 财务报告 .................................................... 85\n"
                ),
                "readable": True,
            },
            {
                "page": 9,
                "text": (
                    "9 / 220\n非经常性损益项目和金额\n"
                    "十一、 存在股权激励\n第三节 管理层讨论与分析"
                ),
                "readable": True,
            },
            {
                "page": 11,
                "text": (
                    "作为制浆造纸企业，公司浆纸主业一直秉承自主生产的经营模式，"
                    "具备制浆造纸生产线及配套系统，主要经营模式如下。"
                ),
                "readable": True,
            },
            {"page": 16, "text": "主营业务分行业纸行业、浆行业。", "readable": True},
            {
                "page": 21,
                "text": "前五名客户销售额52,052.72万元，占年度销售总额21.52%。",
                "readable": True,
            },
        ),
        processing_identity=identity,
        report=report,
    )
    assert 11 in provider.inventoried
    assert any(item.endswith("operating-role") for item in provider.drafted)
    assert any(claim.claim_id == "operating-role" for claim in result.claims)
    assert "core_dimension_missing:operating_role" not in result.gaps


def test_concentration_and_contract_liability_do_not_complete_operating_role():
    from research.company_profile.direct_reading import _operating_role_established

    report = _report()
    claims = [
        ReportViewClaim(
            claim_id="concentration",
            dimension="upstream_downstream",
            status="observed",
            answer="前五大客户销售额占比21.52%。",
            evidence=(_evidence(report, text="前五大客户销售额占比21.52%", page=21),),
        ),
        ReportViewClaim(
            claim_id="liability",
            dimension="upstream_downstream",
            status="observed",
            answer="合同负债预收货款余额3295822047.40元。",
            evidence=(_evidence(report, text="合同负债余额3295822047.40元", page=16),),
        ),
    ]
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id="concentration",
            status="pass",
            explanation="集中度。",
        ),
        ReportViewCheck(
            target_type="claim",
            target_id="liability",
            status="pass",
            explanation="合同负债。",
        ),
        ReportViewCheck(
            target_type="condition",
            target_id="core-question:operating-role",
            status="pass",
            explanation="标签不能代替覆盖。",
        ),
    ]
    assert _operating_role_established(claims, checks) is False


def test_direct_sales_revenue_does_not_complete_period_total():
    from research.company_profile.direct_reading import _period_revenue_verified

    report = _report()
    claims = [
        ReportViewClaim(
            claim_id="direct-sales",
            dimension="revenue_model",
            status="observed",
            answer="直销业务营业收入233200万元。",
            evidence=(_evidence(report, text="直销营业收入233200万元", page=16),),
        )
    ]
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id="direct-sales",
            status="pass",
            explanation="分项收入。",
        ),
    ]
    assert _period_revenue_verified(claims, checks) is False


def test_historical_role_pass_does_not_cover_later_deny():
    from research.company_profile.direct_reading import _operating_role_established

    report = _report()
    claims = [
        ReportViewClaim(
            claim_id="operating-role",
            dimension="supply_chain_role",
            status="observed",
            answer="公司向客户提供定制加工并收取加工费。",
            evidence=(_evidence(report, text="提供定制加工并收取加工费", page=11),),
        )
    ]
    checks = [
        ReportViewCheck(
            target_type="condition",
            target_id="core-question:operating-role",
            status="pass",
            explanation="旧通过。",
            covers_target_type="claim",
            covers_target_id="operating-role",
        ),
        ReportViewCheck(
            target_type="claim",
            target_id="operating-role",
            status="pass",
            explanation="旧通过。",
        ),
        ReportViewCheck(
            target_type="condition",
            target_id="core-question:operating-role",
            status="block",
            explanation="后一次否定。",
        ),
    ]
    assert _operating_role_established(claims, checks) is False


def test_role_without_keywords_completes_after_object_covering_pass():
    from research.company_profile.direct_reading import _operating_role_established

    report = _report()
    claims = [
        ReportViewClaim(
            claim_id="operating-role",
            dimension="supply_chain_role",
            status="observed",
            answer="公司向客户提供定制加工并收取加工费。",
            evidence=(_evidence(report, text="提供定制加工并收取加工费", page=11),),
        )
    ]
    checks = [
        ReportViewCheck(
            target_type="claim",
            target_id="operating-role",
            status="pass",
            explanation="对象已核。",
        ),
        ReportViewCheck(
            target_type="condition",
            target_id="core-question:operating-role",
            status="pass",
            explanation="对象覆盖通过。",
            covers_target_type="claim",
            covers_target_id="operating-role",
        ),
    ]
    assert _operating_role_established(claims, checks) is True


def test_toc_printed_offset_uses_title_page_not_leftover_or_cross_ref():
    from research.company_profile.direct_reading import chapter_candidate_pages

    chosen = chapter_candidate_pages(
        [
            {
                "page": 3,
                "text": "查阅第三节 管理层讨论与分析。",
                "readable": True,
            },
            {
                "page": 4,
                "text": (
                    "目录\n"
                    "第三节 管理层讨论与分析 ............................................. 9\n"
                    "第八节 财务报告 .................................................... 85\n"
                ),
                "readable": True,
            },
            {
                "page": 9,
                "text": "9 / 220\n非经常性损益项目和金额\n十一、 存在股权激励",
                "readable": True,
            },
            {
                "page": 11,
                "text": "11 / 220\n第三节 管理层讨论与分析\n公司向客户提供定制加工并收取加工费。",
                "readable": True,
            },
        ]
    )
    assert 11 in chosen
    assert 3 not in chosen
    assert 9 not in chosen


def test_unclear_role_without_new_body_records_no_progress():
    report = _report()
    identity = direct_core_processing_identity()
    identity["chapter_select"] = True
    identity["compatible_continue"] = True
    identity["accepted_source_receipts"] = [
        {
            "request_id": "s16-source",
            "purpose": "source_inventory",
            "status": "success",
            "source_pages": [4, 16, 21],
            "response_payload": {
                "request_id": "s16-source",
                "purpose": "source_inventory",
                "source_conditions": [
                    {
                        "condition_id": "core-question:operating-role",
                        "dimension": "supply_chain_role",
                        "description": "说明生产加工销售角色",
                        "major": True,
                        "statement_status": "current",
                        "evidence": [
                            _evidence(
                                report, text="主营业务分行业纸行业、浆行业。", page=16
                            ).model_dump(mode="json")
                        ],
                    }
                ],
            },
        }
    ]
    identity["accepted_prior_draft"] = {
        "claims": [
            {
                "claim_id": "sales",
                "dimension": "revenue_model",
                "status": "observed",
                "answer": "直销营业收入233200万元。",
                "evidence": [
                    _evidence(report, text="直销营业收入233200万元", page=16).model_dump(
                        mode="json"
                    )
                ],
                "supporting_record_ids": [],
            }
        ],
        "checks": [
            {
                "target_type": "claim",
                "target_id": "sales",
                "status": "pass",
                "explanation": "分项已核。",
            },
            {
                "target_type": "condition",
                "target_id": "core-question:operating-role",
                "status": "unclear",
                "explanation": "现有声称没有说明经营角色。",
            },
        ],
        "verify_receipts": [
            {
                "purpose": "verify_views",
                "request_targets": [
                    {
                        "target_type": "condition",
                        "target_id": "core-question:operating-role",
                    },
                    {"target_type": "claim", "target_id": "sales"},
                ],
            }
        ],
    }

    class _Provider:
        offline_only = True
        drafted = []
        verified = []

        def interpret(self, request):
            if request.purpose == "synthesize_views":
                self.drafted.extend(
                    item.condition_id for item in request.source_conditions
                )
            if request.purpose == "verify_views":
                self.verified.extend(
                    item.claim_id for item in request.claims
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
            )

    provider = _Provider()
    result = _run(
        provider,
        pages=(
            {
                "page": 4,
                "text": (
                    "目录\n"
                    "第三节 管理层讨论与分析 ............................................. 16\n"
                ),
                "readable": True,
            },
            {"page": 16, "text": "主营业务分行业纸行业、浆行业。直销营业收入233200万元。", "readable": True},
            {
                "page": 21,
                "text": "前五名客户销售额52,052.72万元，占年度销售总额21.52%。",
                "readable": True,
            },
        ),
        processing_identity=identity,
        report=report,
    )
    assert "core-question:operating-role" not in provider.drafted
    assert "sales" not in provider.verified
    assert "direct_core_no_new_input:operating_role" in result.gaps


def test_bank_period_revenue_on_p71_is_kept_in_packed_pages():
    from research.company_profile.direct_reading import (
        compact_source_pages,
        make_operating_revenue_question,
    )

    pages = _official_bank_pages(71)
    report = _report()
    question = make_operating_revenue_question(
        report,
        ({"kind": "period-operating-revenue", "page": 71, "quote": "3.7 利润表分析"},),
    )
    packed = compact_source_pages(pages, [question])
    text = "\n".join(str(page.get("text") or "") for page in packed)
    assert "173,964" in text
    assert "120,483" in text
    assert "22,725" in text
    assert "30,756" in text



