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
    assert any(item["purpose"] == "synthesize_views" for item in result.call_receipts)
    assert result.call_receipts[0]["purpose"] == "source_inventory"
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


def test_the_first_package_is_persisted_before_the_second_package_is_inventoried():
    from research.company_profile.direct_reading import body_packages

    report = _report()
    identity = direct_core_processing_identity()
    text = {
        1: "公司经营有线电视网络传输，并向用户提供接入服务。" + ("甲" * 4500),
        2: "公司采购节目传输与系统维护服务，面向家庭用户提供接入。" + ("乙" * 4500),
    }
    pages = tuple(
        {"page": page, "text": body, "readable": True} for page, body in text.items()
    )
    assert len(list(body_packages(pages, report, identity, 24_000))) >= 2
    persists = {"count": 0}

    class _Provider:
        offline_only = True

        def __init__(self):
            self.inventories = 0

        def interpret(self, request):
            current = tuple(int(page["page"]) for page in request.source_pages)
            if request.source_scope == "page_map":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(1, 2),
                )
            if request.purpose == "source_inventory" and request.source_scope != "page_map":
                self.inventories += 1
                if self.inventories >= 2:
                    assert persists["count"] >= 1
                    if persists.get("fail"):
                        raise ValueError("second-package-inventory-failed")
            if request.purpose == "synthesize_views":
                page = current[0]
                quote = text[page][:12]
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=(
                        ReportViewClaim(
                            claim_id=f"package-{page}",
                            dimension="principal_business",
                            status="observed",
                            answer=f"第{page}包已核。",
                            evidence=(
                                _evidence(report, text=quote, page=page).model_copy(
                                    update={"evidence_id": f"package-{page}-evidence"}
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
        "package-2",
    }

    persists["count"] = 0
    persists["fail"] = True
    failed = _run(
        _Provider(),
        pages=pages,
        processing_identity=identity,
        report=report,
        on_progress=lambda _result: persists.__setitem__("count", persists["count"] + 1),
    )
    assert any(item.claim_id == "package-1" and item.status == "observed" for item in failed.claims)
    assert any("second-package-inventory-failed" in gap for gap in failed.gaps)
    assert failed.draft_complete is False


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
