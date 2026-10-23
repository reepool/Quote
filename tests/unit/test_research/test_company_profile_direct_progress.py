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
