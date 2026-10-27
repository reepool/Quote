"""Progressive reading and unverified-draft projection. Not a business score."""

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
        [covered],
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
                assert not request.claims
                self.inventories += 1
                self.events.append(("inventory", current))
                if self.inventories >= 2 and persists.get("fail"):
                    raise ValueError("second-package-inventory-failed")
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
                            condition_ids=ids,
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
    assert verified and synth and verified[0] < synth[1]

    def has(ids, name):
        return any(item == name or str(item).endswith(":" + name) for item in ids)

    first_ids = provider.calls[synth[0]][1]
    assert has(first_ids, "role") and has(first_ids, "principal-a")
    assert not has(first_ids, "principal-b")
    assert len(first_ids) > 1
    kept = next(item for item in result.claims if item.claim_id == "core-group")
    assert kept.status == "observed"
    assert any(gap.startswith("direct_core_package_stopped:") for gap in result.gaps)


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



