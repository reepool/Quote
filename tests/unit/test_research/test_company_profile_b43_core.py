"""Company core through TaskService and the public gateway.

The external transport is replaced. This is not a real-model score.
"""

import asyncio
import gzip
import json
import shutil
import time
import uuid
from decimal import Decimal
from pathlib import Path

from research.business_profile_production_operations import (
    BusinessProfileAnnouncementFrontierRepository,
)
from research.company_profile.bounded_entry import (
    S27_OFFLINE_DIAGNOSTIC,
    _fixed_core_reply,
    _meter_profile,
    _request_text,
    prepare_gateway,
)
from research.company_profile.models import ReportIdentity
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.report_interpretation import direct_core_processing_identity
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.storage import ResearchStorageManager
from tests.unit.test_research.test_business_profile_production_operations import (
    _announcement,
)
from tests.unit.test_research.test_company_profile_bounded_entry import (
    _fixed_core_objects,
)
from utils.config_manager import ResearchConfig, ResearchStorageConfig
from utils.llm import CallableTransport, LlmClient, LlmConfig
from utils.llm.budget import LlmRunBudget
from utils.llm.orchestration import (
    LlmPoolCoordinatorRegistry,
    ProviderCoordinatorRegistry,
)
from utils.llm.rate_limit import ProfileLimiterRegistry

ROOT = Path("/home/python/Quote")
CHANGE = ROOT / "openspec/changes/restore-company-profile-llm-led-generalization"
PARSE = (
    ROOT
    / "data/filings/announcements/blobs/68/derived/business_profile_pdfium_recovery.v4"
    / "6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c_57e1dd4f16ef40ff.json.gz"
)


def _pages(wanted):
    return tuple(
        {"page": int(item["page_number"]), "text": item["text"], "readable": True}
        for item in json.loads(gzip.open(PARSE, "rt", encoding="utf-8").read())["pages"]
        if int(item["page_number"]) in wanted
    )


def _saved_s41():
    query = json.loads((CHANGE / "s41-600103-receipts/query.json").read_text())
    return [
        item["response_payload"]
        for item in query["profiles"][0]["three_views"]["call_receipts"]
        if item.get("response_payload")
    ]


def _short_evidence(raw):
    anchor = raw.get("anchor") or {}
    quote = str(anchor.get("bounded_quote") or raw.get("quote") or "")
    return {"page": int(raw["page"]), "quote": quote}


def _project(payload, request):
    purpose = request["purpose"]
    wire = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": purpose,
    }
    if purpose == "source_inventory" and request.get("source_scope") == "page_map":
        available = {int(page["page"]) for page in request.get("source_pages") or []}
        wire["requested_pages"] = [
            int(page)
            for page in payload.get("requested_pages") or []
            if int(page) in available
        ]
        return wire
    if purpose == "source_inventory":
        conditions = []
        for item in payload.get("source_conditions") or []:
            evidence = [_short_evidence(raw) for raw in item.get("evidence") or []]
            evidence = [item for item in evidence if item["quote"].strip()]
            if not evidence or not item.get("condition_id"):
                continue
            conditions.append(
                {
                    "condition_id": item["condition_id"],
                    "dimension": item["dimension"],
                    "description": str(item.get("description") or item["condition_id"]),
                    "major": bool(item.get("major")),
                    "statement_status": item.get("statement_status") or "current",
                    "evidence": evidence,
                }
            )
        wire["requested_pages"] = []
        wire["source_conditions"] = conditions
        return wire
    if purpose == "synthesize_views":
        claims = []
        for item in payload.get("claims") or []:
            evidence = [_short_evidence(raw) for raw in item.get("evidence") or []]
            evidence = [one for one in evidence if one["quote"].strip()]
            if not evidence or not item.get("answer"):
                continue
            claims.append(
                {
                    "claim_id": "s41-" + str(item.get("claim_id") or len(claims)),
                    "dimension": item["dimension"],
                    "status": "observed",
                    "answer": item["answer"],
                    "condition_ids": [
                        condition["condition_id"]
                        for condition in request.get("source_conditions") or []
                        if evidence[0]["quote"][:24]
                        in json.dumps(condition, ensure_ascii=False)
                    ],
                    "supporting_record_ids": [],
                    "evidence": evidence,
                }
            )
        wire["claims"] = claims
        return wire
    checks = []
    kept = {
        str(item.get("answer") or "")
        for item in payload.get("claims") or []
        if item.get("answer") and "2,332,376,693.65" not in str(item.get("answer"))
    }
    for claim in request.get("claims") or []:
        ok = str(claim.get("answer") or "") in kept
        checks.append(
            {
                "target_type": "claim",
                "target_id": claim.get("claim_id"),
                "status": "pass" if ok else "unclear",
                "explanation": "只保留S41里已经成立、且不是分销售模式合计的主张。",
                "covers_target_type": "claim",
                "covers_target_id": claim.get("claim_id"),
            }
        )
    passed = {
        check["target_id"] for check in checks if check["status"] == "pass"
    }
    for condition in request.get("source_conditions") or []:
        quote = ""
        evidence = condition.get("evidence") or condition.get("evidence_refs") or []
        if evidence and isinstance(evidence[0], dict):
            quote = str(evidence[0].get("quote") or "")
        matched = next(
            (
                claim.get("claim_id")
                for claim in request.get("claims") or []
                if claim.get("claim_id") in passed and quote and quote[:24] in str(claim.get("answer") or "")
            ),
            None,
        )
        check = {
            "target_type": "condition",
            "target_id": condition.get("condition_id"),
            "status": "pass" if matched else "unclear",
            "explanation": "只保留S41里已经成立、且不是分销售模式合计的主张。",
        }
        if matched:
            check["covers_target_type"] = "claim"
            check["covers_target_id"] = matched
        checks.append(check)
    wire["checks"] = checks
    return wire


_ORIGINAL_ANSWERS = (
    "公司属于制浆造纸行业，是集制浆、造纸、热电联产",
    "公司主要从事竹木浆、溶解浆、纸袋纸",
    "子公司层面：医药产业由漳州水仙经营",
)


def _keep_original_answers(request, body):
    """Later checks must not retract the three answers S41 already accepted."""
    if request.get("purpose") != "verify_views":
        return body
    kept = {}
    for claim in request.get("claims") or []:
        answer = str(claim.get("answer") or "")
        if "2,332,376,693.65" in answer:
            continue
        if any(answer.startswith(prefix) for prefix in _ORIGINAL_ANSWERS):
            kept[claim.get("claim_id")] = answer
    for check in body.get("checks") or []:
        if check.get("target_type") == "claim" and check.get("target_id") in kept:
            check["status"] = "pass"
            check["covers_target_type"] = "claim"
            check["covers_target_id"] = check["target_id"]
    for condition in request.get("source_conditions") or []:
        condition_id = str(condition.get("condition_id") or "")
        tail = condition_id.rsplit(":", 1)[-1]
        prefix = {"c1": _ORIGINAL_ANSWERS[0], "c2": _ORIGINAL_ANSWERS[1], "c9": _ORIGINAL_ANSWERS[2]}.get(tail)
        if prefix is None:
            continue
        claim_id = next((item for item, answer in kept.items() if answer.startswith(prefix)), None)
        if claim_id is None:
            continue
        for check in body.get("checks") or []:
            if check.get("target_type") == "condition" and check.get("target_id") == condition_id:
                check["status"] = "pass"
                check["covers_target_type"] = "claim"
                check["covers_target_id"] = claim_id
    return body


def _cover_restate(request, body, objects):
    """A replaced quote may cite the reviewed sales sentence, not the subtotal."""
    sales = next(
        (
            item
            for item in objects
            if item.get("condition_id") == "sales-model-customer-relationships"
        ),
        None,
    )
    if sales is None or sales["quote"] not in _request_text(request):
        return body
    if "2,332,376,693.65" in sales["quote"]:
        return body
    if request["purpose"] == "synthesize_views":
        for condition in request.get("source_conditions") or []:
            if "restate-" not in str(condition.get("condition_id") or ""):
                continue
            body.setdefault("claims", []).append(
                {
                    "claim_id": "restate-" + str(condition["condition_id"]).rsplit(":", 1)[-1],
                    "dimension": sales["dimension"],
                    "status": "observed",
                    "answer": sales["answer"],
                    "condition_ids": [condition["condition_id"]],
                    "supporting_record_ids": [],
                    "evidence": [{"page": int(sales["page"]), "quote": sales["quote"]}],
                }
            )
    if request["purpose"] == "verify_views":
        passed = {
            check.get("target_id")
            for check in body.get("checks") or []
            if check.get("status") == "pass" and check.get("target_type") == "claim"
        }
        claim_id = next(
            (
                claim.get("claim_id")
                for claim in request.get("claims") or []
                if claim.get("answer") == sales["answer"] and claim.get("claim_id") in passed
            ),
            None,
        )
        if claim_id is None:
            return body
        for check in body.get("checks") or []:
            if check.get("target_type") == "condition" and "restate-" in str(
                check.get("target_id") or ""
            ):
                check["status"] = "pass"
                check["covers_target_type"] = "claim"
                check["covers_target_id"] = claim_id
    return body


def _run(objects, *, omit="", fault_payloads=None, pages=None, plant_unresolved=False):
    binding = json.loads((CHANGE / "s1-pool-s1-600103-s16-binding.json").read_text())
    raw = json.loads((ROOT / binding["config_path"]).read_text())["llm"]
    config = LlmConfig.from_mapping(raw)
    pool = config.pool_for_profile(binding["profile"])
    binding.update(
        {
            "task_route_labels": ["deepseek:deepseek-flash"],
            "task_route_first_only": True,
            "same_source_max_retries": 0,
            "route_fingerprint": config.route_fingerprint(binding["profile"]),
            "fallback_allowed": pool.failover.enabled,
            "sources": [
                {
                    "profile": profile.name,
                    "model": profile.model,
                    "provider_resource": config.resource_for_profile(profile).name,
                    "price": {
                        "input_cny_per_million": "1",
                        "output_cny_per_million": "2",
                        "basis": "offline",
                        "context_tokens": 120000,
                    },
                }
                for profile in config.concrete_profiles_for(binding["profile"])
            ],
            "run_authorization": S27_OFFLINE_DIAGNOSTIC,
            "max_tokens": 480_000,
            "max_physical_requests": 32,
            "max_recoveries": 2,
            "max_cost_cny": "10",
            "reading_strategy": "direct_core_v1",
        }
    )
    gateway_config, prices = prepare_gateway(binding, raw, real=False)
    calls = []
    fault_used = {"source": False, "draft": False, "verify": False}
    planted = {"done": False}

    def transport(_url, _headers, payload, _timeout):
        request = json.loads(
            next(item["content"] for item in payload["messages"] if item["role"] == "user")
        )
        calls.append(
            {
                "purpose": request["purpose"],
                "source_scope": request.get("source_scope"),
                "pages": [page["page"] for page in request.get("source_pages") or []],
                "max_tokens": payload.get("max_tokens"),
                "thinking": payload.get("thinking"),
                "model": payload.get("model"),
                "conditions": [
                    item.get("condition_id")
                    for item in request.get("source_conditions") or []
                ][:8],
            }
        )
        body = None
        if fault_payloads:
            if (
                request["purpose"] == "source_inventory"
                and request.get("source_scope") == "page_map"
            ):
                body = _project(fault_payloads[0], request)
            elif request["purpose"] == "source_inventory" and not fault_used["source"]:
                fault_used["source"] = True
                body = _project(fault_payloads[1], request)
            elif request["purpose"] == "synthesize_views" and not fault_used["draft"]:
                fault_used["draft"] = True
                body = _project(fault_payloads[2], request)
            elif request["purpose"] == "verify_views" and not fault_used["verify"]:
                fault_used["verify"] = True
                body = _project(fault_payloads[2], request)
        if body is None:
            body = _cover_restate(
                request,
                _fixed_core_reply(
                    request,
                    {"fixed_core_objects": objects, "fixed_core_omit": omit},
                ),
                objects,
            )
        body = _keep_original_answers(request, body)
        if (
            plant_unresolved
            and not planted["done"]
            and request["purpose"] == "source_inventory"
            and request.get("source_scope") != "page_map"
            and request.get("source_pages")
        ):
            planted["done"] = True
            page = int(request["source_pages"][0]["page"])
            body = dict(body)
            body["source_conditions"] = [
                *(body.get("source_conditions") or []),
                {
                    "condition_id": "audit-unresolved-other-business",
                    "dimension": "principal_business",
                    "description": "另一个独立主要业务的证据尚未定位，未核验也未被撤销。",
                    "major": True,
                    "statement_status": "current",
                    "evidence": [
                        {
                            "page": page,
                            "quote": "审核受控反例：另一个独立主要业务未定位的引文",
                        }
                    ],
                },
            ]
        encoded = json.dumps(body, ensure_ascii=False)
        return {
            "status_code": 200,
            "data": {
                "choices": [
                    {"message": {"content": encoded}, "finish_reason": "stop"}
                ],
                "usage": {
                    "prompt_tokens": 20,
                    "completion_tokens": 20,
                    "total_tokens": 40,
                },
            },
        }

    budget = LlmRunBudget(
        deadline=time.monotonic() + 180,
        prices=prices,
        max_tokens=480_000,
        max_requests=32,
        max_cost_cny=Decimal(10),
        billing_mode="development_unpriced",
        run_authorization=S27_OFFLINE_DIAGNOSTIC,
    )
    client = LlmClient(
        gateway_config,
        physical_budget=budget,
        transport=CallableTransport(transport),
        environment={
            profile.api_key_env: "offline-placeholder"
            for profile in gateway_config.concrete_profiles_for(binding["profile"])
        },
        limiter_registry=ProfileLimiterRegistry(),
        provider_coordinator_registry=ProviderCoordinatorRegistry(),
        pool_coordinator_registry=LlmPoolCoordinatorRegistry(),
    )

    class OfflineGateway(CommonGatewaySemanticProvider):
        offline_only = True

    provider = OfflineGateway(
        client=client,
        profile=binding["profile"],
        max_output_tokens=4000,
        timeout_seconds=30,
    )
    root = ROOT / "data/QuoteBak" / f"b43-entry-{uuid.uuid4().hex[:8]}"
    root.mkdir()
    try:
        storage = ResearchStorageManager(
            ResearchConfig(
                storage=ResearchStorageConfig(
                    db_path=str(root / "research.db"),
                    financials_db_path=str(root / "research.db"),
                    valuation_db_path=str(root / "research.db"),
                    interests_db_path=str(root / "research.db"),
                    quotes_db_path=str(root / "quotes.db"),
                    attach_quotes_db=False,
                )
            )
        )
        storage.initialize()
        BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
            instrument={
                "instrument_id": "600103.SH",
                "symbol": "600103",
                "exchange": "SSE",
            },
            record=_announcement(
                "annual-2025",
                "青山纸业2025年年度报告",
                published_at="2026-04-13T16:00:00+00:00",
            ),
        )
        identity = direct_core_processing_identity()
        identity.update(
            chapter_select=True,
            ledger_tokens=480_000,
            max_physical_requests=32,
            prompt_version="company_profile_report_reading.2026-11-03.chapter-core.v1",
            product_contract="company_profile_product_contract.2026-10-27.llm-led.v8",
            meter_profile=_meter_profile(gateway_config, binding),
        )
        service = CompanyProfileTaskService(
            storage=storage,
            output_root=root / "output",
            checkpoint_root=root / "checkpoints",
            page_source=lambda item: {
                "report": ReportIdentity(
                    instrument_id=str(item["instrument_id"]),
                    report_id=str(item["announcement_id"]),
                    document_version="6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c",
                    report_period="2025-12-31",
                    published_at="2026-04-13T16:00:00+00:00",
                ),
                "pages": pages or _pages({4, 9, 11, 16}),
            },
            provider=provider,
            processing_identity=identity,
            execution_budget=budget,
            token_budget=480_000,
        )
        ran = asyncio.run(
            service.execute(
                "run",
                knowledge_cutoff="2026-09-17",
                instrument_ids=("600103.SH",),
                max_items=1,
                max_elapsed_seconds=150,
            )
        )
        queried = asyncio.run(service.execute("query", instrument_ids=("600103.SH",)))
        exported = asyncio.run(
            service.execute(
                "export",
                instrument_ids=("600103.SH",),
                output_directory=str(root / "export"),
            )
        )
        reloaded = CompanyProfileReadService(root / "output").query(("600103.SH",))
        return ran, queried, exported, reloaded, calls
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _views(payload):
    return payload["profiles"][0]["three_views"]


def _answers(views):
    return [
        claim.get("answer") or ""
        for view in views["views"].values()
        for claim in view.get("claims") or []
    ]


def _covered_claim_ids(views):
    covered = set()
    for receipt in views.get("call_receipts") or []:
        for check in (receipt.get("response_payload") or {}).get("checks") or []:
            if (
                check.get("status") == "pass"
                and check.get("covers_target_type") == "claim"
                and check.get("target_type") == "condition"
            ):
                covered.add(check.get("covers_target_id"))
    return covered


def test_task_service_gateway_delivers_the_company_core():
    _ran, queried, exported, reloaded, calls = _run(_fixed_core_objects())
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    assert views["draft_complete"] is True
    assert views["real_semantic_complete"] is False
    answers = _answers(views)
    text = "\n".join(answers)
    assert "医药" in text and "光电子" in text
    assert "2,418,990,221.17" in text
    assert "自主生产" in text and "直销" in text
    assert "全国各地和国际市场" in text
    assert "2,332,376,693.65" not in text
    delivered = {
        claim.get("claim_id")
        for view in views["views"].values()
        for claim in view.get("claims") or []
    }
    covered = _covered_claim_ids(views)
    assert covered
    assert covered <= delivered
    assert any(
        item["model"] == "deepseek-flash" and item["thinking"] == {"type": "disabled"}
        for item in calls
    )
    assert any(item["purpose"] == "source_inventory" and 11 in item["pages"] for item in calls)


def test_unresolved_major_business_keeps_the_core_incomplete():
    _ran, queried, exported, reloaded, _calls = _run(
        _fixed_core_objects(), plant_unresolved=True
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    gaps = views.get("gaps") or []
    assert any(
        gap.startswith("major_evidence_not_located:")
        and gap.endswith("audit-unresolved-other-business")
        for gap in gaps
    )
    assert not any(
        check.get("target_id", "").endswith("audit-unresolved-other-business")
        for receipt in views.get("call_receipts") or []
        for check in (receipt.get("response_payload") or {}).get("checks") or []
    )
    assert views["draft_complete"] is False
    assert views["real_semantic_complete"] is False
    assert "医药" in "\n".join(_answers(views))


def test_missing_downstream_keeps_the_same_entry_limited():
    _ran, queried, exported, reloaded, _calls = _run(
        _fixed_core_objects(), omit="claim-005"
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    assert views["draft_complete"] is False
    assert views["real_semantic_complete"] is False
    text = "\n".join(_answers(views))
    assert "医药" in text
    assert "全国各地和国际市场" not in text


def test_s41_unresolved_major_quote_keeps_the_supplement_limited():
    saved = _saved_s41()
    _ran, queried, exported, reloaded, calls = _run(
        _fixed_core_objects(),
        fault_payloads=saved,
        pages=_pages({4, 9, 11, 16, 18, 22, 85, 92}),
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    answers = _answers(views)
    text = "\n".join(answers)
    assert views["draft_complete"] is False
    assert any(
        "c3" in gap and gap.startswith("major_evidence_not_located:")
        for gap in views.get("gaps") or []
    )
    assert views["real_semantic_complete"] is False
    assert "2,418,990,221.17" in text
    assert "自主生产" in text
    assert "医药" in text and "光电子" in text
    assert not any("2,332,376,693.65" in answer for answer in answers)
    assert calls[0]["purpose"] == "source_inventory"
    assert calls[0]["source_scope"] == "page_map"
    assert any(item["purpose"] == "source_inventory" and 11 in item["pages"] for item in calls)
