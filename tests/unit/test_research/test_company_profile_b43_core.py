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
from research.company_profile.report_interpretation import (
    _spans_in_text,
    direct_core_processing_identity,
)
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
from utils.llm.budget import LlmRunBudget, LogicalRecoveryPolicy
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


INDUSTRY_QUOTES = (
    ("16", "单位：元 币种：人民币\r\n主营业务分行业情况"),
    ("16", "纸行业 1,197,089,976.81 1,068,486,208.58 10.74 -9.40 -7.88 减少 1.48"),
    ("16", "浆行业 358,768,319.71 331,734,674.66 7.54 -17.85 -18.90 增加 1.19"),
    (
        "16",
        "纸制品加工行\r\n业\r\n284,450,942.34 285,672,734.79 -0.43 -15.03 -16.39 增加 1.63\r\n",
    ),
    ("16", "医药行业 303,680,631.79 159,829,111.78 47.37 -2.06 -0.81 减少 0.66"),
    ("16", "光电子行业 206,025,784.54 152,773,265.40 25.85 53.54 45.93 增加 3.87"),
    (
        "17",
        "营林业 30,598,132.37 23,466,931.09 23.31 64.21 72.09 减少 3.51\r\n"
        "个百分点\r\n商贸业 2,034,990.83 1,999,114.27 1.76 -93.46 -93.35 减少 1.66\r\n"
        "个百分点\r\n环保业 63,795,075.10 62,228,582.85 2.46 19.00 22.94 减少 3.12\r\n"
        "个百分点\r\n机电维修安装 53,693,710.43 46,499,806.32 13.40 9.04 13.30 减少 3.25\r\n"
        "个百分点\r\n内部抵销 -167,760,870.27 -178,451,173.85\r\n"
        "合计 2,332,376,693.65 1,954,239,255.89 16.21 -8.35 -9.35 增加 0.92\r\n"
        "个百分点\r\n",
    ),
)


def _s41_c3_wire():
    saved = _saved_s41()
    source = next(
        item
        for payload in saved
        if payload.get("purpose") == "source_inventory"
        for item in payload.get("source_conditions") or []
        if item.get("condition_id") == "c3"
    )
    return {
        "condition_id": "c3",
        "dimension": source["dimension"],
        "description": source["description"],
        "major": bool(source.get("major")),
        "statement_status": source.get("statement_status") or "current",
        "evidence": [
            {
                "page": int(evidence["page"]),
                "quote": (evidence.get("anchor") or {})["bounded_quote"],
            }
            for evidence in source["evidence"]
        ],
    }


def _evidence_items(item, request=None):
    catalog = (request or {}).get("evidence_catalog") or {}
    found = []
    raws = list(item.get("evidence") or [])
    raws.extend(item.get("evidence_refs") or [])
    for evidence in raws:
        if isinstance(evidence, str):
            evidence = catalog.get(evidence) or {}
        if not isinstance(evidence, dict):
            continue
        quote = str(
            evidence.get("quote")
            or (evidence.get("anchor") or {}).get("bounded_quote")
            or ""
        )
        if not quote:
            continue
        page = evidence.get("page") or (evidence.get("anchor") or {}).get("page") or 16
        found.append({"page": int(page), "quote": quote})
    return found


def _quote_list(item, request=None):
    return [item["quote"] for item in _evidence_items(item, request)]


def _industry_supported(quotes):
    blob = "\n".join(quotes)
    return (
        "主营业务分行业情况" in blob
        and "纸行业" in blob
        and "浆行业" in blob
        and "纸制品加工" in blob
        and "医药行业" in blob
        and "光电子行业" in blob
        and "2,332,376,693.65" in blob
        and "主营业务分产品情况" not in blob
    )


def _repair_response(request, mode):
    failed = [
        condition
        for condition in request.get("source_conditions") or []
        if str(condition.get("condition_id") or "").endswith(":c3")
        or str(condition.get("condition_id") or "") == "c3"
    ]
    conditions = []
    if mode == "located_wrong":
        for condition in failed:
            conditions.append(
                {
                    "condition_id": condition["condition_id"],
                    "dimension": condition.get("dimension") or "revenue_model",
                    "description": condition.get("description") or "待修条件",
                    "major": True,
                    "statement_status": "current",
                    "evidence": [
                        {"page": 17, "quote": "主营业务分产品情况"},
                        {"page": 16, "quote": INDUSTRY_QUOTES[1][1]},
                        {"page": 16, "quote": INDUSTRY_QUOTES[2][1]},
                        {"page": 16, "quote": INDUSTRY_QUOTES[5][1]},
                        {
                            "page": 17,
                            "quote": "营林业 30,598,132.37 23,466,931.09 23.31 64.21 72.09 减少 3.51",
                        },
                    ],
                }
            )
    elif mode == "full":
        for condition in failed:
            conditions.append(
                {
                    "condition_id": condition["condition_id"],
                    "dimension": condition.get("dimension") or "revenue_model",
                    "description": condition.get("description") or "待修条件",
                    "major": True,
                    "statement_status": "current",
                    "evidence": [
                        {"page": int(page), "quote": quote}
                        for page, quote in INDUSTRY_QUOTES
                    ],
                }
            )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "source_conditions": conditions,
    }


def _bind_industry_repair(request, body):
    """Pass the repaired industry condition only when its quotes support it."""
    if request.get("purpose") == "synthesize_views":
        for condition in request.get("source_conditions") or []:
            condition_id = str(condition.get("condition_id") or "")
            quotes = _quote_list(condition, request)
            if not condition_id.endswith(":c3") and condition_id != "c3":
                continue
            if not _industry_supported(quotes):
                continue
            body.setdefault("claims", []).append(
                {
                    "claim_id": "repaired-c3",
                    "dimension": "revenue_model",
                    "status": "observed",
                    "answer": (
                        "主营业务分行业情况按人民币元列示纸行业、浆行业、纸制品加工行业、"
                        "医药行业和光电子行业。该表合计2,332,376,693.65元，不是公司营业收入总额。"
                    ),
                    "condition_ids": [condition_id],
                    "supporting_record_ids": [],
                    "evidence": _evidence_items(condition, request),
                }
            )
    if request.get("purpose") != "verify_views":
        return body
    claim_id = next(
        (
            claim.get("claim_id")
            for claim in request.get("claims") or []
            if _industry_supported(_quote_list(claim, request))
        ),
        None,
    )
    if claim_id is not None:
        for check in body.setdefault("checks", []):
            if check.get("target_type") == "claim" and check.get("target_id") == claim_id:
                check["status"] = "pass"
                check["covers_target_type"] = "claim"
                check["covers_target_id"] = claim_id
    for condition in request.get("source_conditions") or []:
        condition_id = str(condition.get("condition_id") or "")
        if not _industry_supported(_quote_list(condition, request)):
            continue
        replaced = False
        for check in body.setdefault("checks", []):
            if check.get("target_type") == "condition" and check.get("target_id") == condition_id:
                check["status"] = "pass"
                check["covers_target_type"] = "claim"
                check["covers_target_id"] = claim_id or "repaired-c3"
                replaced = True
        if not replaced:
            body["checks"].append(
                {
                    "target_type": "condition",
                    "target_id": condition_id,
                    "status": "pass",
                    "explanation": "引文包含分行业表头和纸、浆、医药、光电子各行，不是下一张表的标题。",
                    "covers_target_type": "claim",
                    "covers_target_id": claim_id or "repaired-c3",
                }
            )
    return body


def _observation_source_receipts():
    """§223 successful source responses. Reconstructed, not the original bytes."""
    query = json.loads(
        (CHANGE / "company-core-observation-600103-20261108-receipts/query.json").read_text()
    )
    return [
        item
        for item in query["profiles"][0]["three_views"]["call_receipts"]
        if item.get("purpose") == "source_inventory" and item.get("response_payload")
    ]


def _folded(text) -> str:
    return "".join(str(text or "").split())


def _quotes_support_answer(answer, quotes) -> bool:
    """A controlled answer is supported only when it is drawn from its quotes."""
    folded = _folded(answer)
    blob = _folded("\n".join(str(quote) for quote in quotes))
    return bool(folded) and folded in blob


def _condition_wire(condition, request=None):
    """Project one saved condition onto this request without dropping evidence.

    A unique span may be stored as the page's own text. A quote that does not
    locate, or locates more than once, stays in the original wording.
    """
    if not condition.get("condition_id"):
        return None
    evidence = []
    pages = {
        int(page["page"]): str(page.get("text") or "")
        for page in (request or {}).get("source_pages") or []
    }
    for raw in condition.get("evidence") or []:
        if not isinstance(raw, dict):
            continue
        quote = (raw.get("anchor") or {}).get("bounded_quote") or raw.get("quote")
        page = raw.get("page")
        if not quote or page is None:
            continue
        text = pages.get(int(page), "")
        spans = _spans_in_text(str(quote), text) or [] if text else []
        if len(spans) == 1:
            start, end = spans[0]
            evidence.append({"page": int(page), "quote": text[start:end]})
        else:
            evidence.append({"page": int(page), "quote": str(quote)})
    return {
        "condition_id": condition["condition_id"],
        "dimension": condition.get("dimension") or "principal_business",
        "description": str(condition.get("description") or condition["condition_id"]),
        "major": bool(condition.get("major")),
        "statement_status": condition.get("statement_status") or "current",
        "evidence": evidence,
    }


def _observation_source_body(request, receipts):
    pages = {int(page["page"]) for page in request.get("source_pages") or []}
    if request.get("source_scope") == "page_map":
        saved = next(item for item in receipts if item.get("source_scope") == "page_map")
        wanted = [
            int(page)
            for page in (saved["response_payload"].get("requested_pages") or [])
            if int(page) in pages
        ]
        return {
            "schema_version": "company_profile_report_response.v3",
            "request_id": request["request_id"],
            "purpose": request["purpose"],
            "requested_pages": wanted,
        }
    exact = []
    for item in receipts:
        if item.get("source_scope") != "full_context":
            continue
        saved_pages = {
            int(page.get("page") if isinstance(page, dict) else page)
            for page in item.get("source_pages") or []
        }
        if saved_pages == pages:
            exact.append(item)
    selected = exact[:1] or [
        item for item in receipts if item.get("source_scope") == "full_context"
    ]
    conditions = []
    seen = set()
    for item in selected:
        for condition in (item.get("response_payload") or {}).get("source_conditions") or []:
            wire = _condition_wire(condition, request)
            if wire is None or wire["condition_id"] in seen:
                continue
            if not exact and not any(evidence["page"] in pages for evidence in wire["evidence"]):
                continue
            seen.add(wire["condition_id"])
            conditions.append(wire)
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "requested_pages": [],
        "source_conditions": conditions,
    }


def _quote_evidence(condition, request):
    catalog = request.get("evidence_catalog") or {}
    found = []
    for raw in list(condition.get("evidence") or []) + list(condition.get("evidence_refs") or []):
        if isinstance(raw, str):
            raw = catalog.get(raw) or {}
        if not isinstance(raw, dict):
            continue
        quote = raw.get("quote") or (raw.get("anchor") or {}).get("bounded_quote")
        if quote:
            found.append({"page": raw.get("page") or 1, "quote": str(quote)})
    return found


def _controlled_observation_draft(request):
    """Labeled controlled draft. The answer is the supporting quote, not the description."""
    claims = []
    for condition in request.get("source_conditions") or []:
        evidence = _quote_evidence(condition, request)
        if not evidence:
            continue
        condition_id = str(condition.get("condition_id") or "")
        quoted = [item["quote"] for item in evidence]
        if not (
            condition.get("major")
            or condition.get("dimension") == "output_exposure"
            or "period-operating-revenue" in condition_id
            or "output-exposure" in condition_id
            or any("2,418,990,221.17" in quote for quote in quoted)
        ):
            continue
        if "period-operating-revenue" in condition_id or any(
            "2,418,990,221.17" in quote for quote in quoted
        ):
            answer = next(quote for quote in quoted if "2,418,990,221.17" in quote)
            kept = [item for item in evidence if "2,418,990,221.17" in item["quote"]]
        else:
            answer = "\n".join(quoted)
            kept = evidence
        if not _quotes_support_answer(answer, [item["quote"] for item in kept]):
            continue
        claims.append(
            {
                "claim_id": "draft-" + condition_id.replace(":", "-"),
                "dimension": condition.get("dimension") or "principal_business",
                "status": "observed",
                "answer": answer,
                "condition_ids": [condition_id],
                "supporting_record_ids": [],
                "evidence": kept[:4],
            }
        )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "claims": claims,
    }


def _controlled_observation_verify(request):
    """Labeled controlled verification. A pass requires the answer to be inside its quotes."""
    checks = []
    covered = {}
    claims_by_id = {}
    for claim in request.get("claims") or []:
        claim_id = claim.get("claim_id")
        quotes = [item["quote"] for item in _quote_evidence(claim, request)]
        if not _quotes_support_answer(claim.get("answer"), quotes):
            checks.append(
                {
                    "target_type": "claim",
                    "target_id": claim_id,
                    "status": "unclear",
                    "explanation": "受控核验：答案不是本主张引文中的原文。",
                    "covers_target_type": "claim",
                    "covers_target_id": claim_id,
                }
            )
            continue
        claims_by_id[claim_id] = claim
        checks.append(
            {
                "target_type": "claim",
                "target_id": claim_id,
                "status": "pass",
                "explanation": "受控核验：答案来自本主张引文。",
                "covers_target_type": "claim",
                "covers_target_id": claim_id,
            }
        )
        for condition_id in claim.get("condition_ids") or []:
            covered.setdefault(condition_id, claim_id)
    for condition in request.get("source_conditions") or []:
        condition_id = condition.get("condition_id")
        claim_id = covered.get(condition_id)
        claim = claims_by_id.get(claim_id)
        if claim is None:
            continue
        condition_quotes = [item["quote"] for item in _quote_evidence(condition, request)]
        if not _quotes_support_answer(claim.get("answer"), condition_quotes):
            continue
        checks.append(
            {
                "target_type": "condition",
                "target_id": condition_id,
                "status": "pass",
                "explanation": "受控核验：条件引文包含该主张。",
                "covers_target_type": "claim",
                "covers_target_id": claim_id,
            }
        )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "checks": checks,
    }


def _run(
    objects,
    *,
    omit="",
    fault_payloads=None,
    pages=None,
    plant_unresolved=False,
    plant_s41_c3=False,
    repair_mode=None,
    attach_failed_id=False,
    drop_first_claims=False,
    truncate_first_verify=False,
    replay_observation=False,
    routine_contract=False,
    anchor_contract=False,
    reviewed_input=False,
):
    if reviewed_input:
        anchor_contract = True
    if anchor_contract:
        routine_contract = True
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
            "max_tokens": 120_000 if routine_contract else 480_000,
            "max_physical_requests": 16 if routine_contract else 32,
            "max_recoveries": 2,
            "max_cost_cny": "10",
            "reading_strategy": "direct_core_v1",
        }
    )
    gateway_config, prices = prepare_gateway(binding, raw, real=False)
    class _Calls(list):
        pass

    calls = _Calls()
    fault_used = {"source": False, "draft": False, "verify": False}
    planted = {"done": False}
    seen_failed = {"id": None}
    dropped_claims = {"done": False}
    truncated_verify = {"done": False}
    observation_receipts = _observation_source_receipts() if replay_observation else []

    def transport(_url, _headers, payload, _timeout):
        user = next(item["content"] for item in payload["messages"] if item["role"] == "user")
        request = json.loads(user)
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
                "page_text": "".join(
                    str(page.get("text") or "") for page in request.get("source_pages") or []
                ),
                "anchor_ids": [item.get("id") for item in request.get("source_anchors") or []],
            }
        )
        body = None
        if reviewed_input and request.get("purpose") == "source_inventory" and request.get("source_scope") == "page_map":
            body = _anchor_page_map(request)
        elif reviewed_input and request.get("purpose") == "source_inventory" and request.get("source_scope") != "evidence_repair":
            body = _reviewed_source_body(request)
        elif reviewed_input and request.get("purpose") == "synthesize_views":
            body = _reviewed_draft(request)
        elif reviewed_input and request.get("purpose") == "verify_views":
            body = _reviewed_verify(request)
        elif anchor_contract and request.get("purpose") == "source_inventory" and request.get("source_scope") == "page_map":
            body = _anchor_page_map(request)
        elif anchor_contract and request.get("purpose") == "source_inventory" and request.get("source_scope") != "evidence_repair":
            body = _anchor_source_body(request, plant_other=plant_unresolved)
        elif anchor_contract and request.get("purpose") == "synthesize_views":
            body = _anchor_draft(request)
        elif anchor_contract and request.get("purpose") == "verify_views":
            body = _anchor_verify(request)
        elif replay_observation and request.get("purpose") == "source_inventory" and request.get("source_scope") != "evidence_repair":
            body = _observation_source_body(request, observation_receipts)
        elif replay_observation and request.get("purpose") == "synthesize_views":
            body = _controlled_observation_draft(request)
        elif replay_observation and request.get("purpose") == "verify_views":
            body = _controlled_observation_verify(request)
        elif request.get("source_scope") == "evidence_repair":
            seen_failed["id"] = next(
                (
                    str(item.get("condition_id") or "")
                    for item in request.get("source_conditions") or []
                ),
                None,
            )
            body = (
                _repair_response(request, repair_mode)
                if repair_mode
                else {
                    "schema_version": "company_profile_report_response.v3",
                    "request_id": request["request_id"],
                    "purpose": request["purpose"],
                    "source_conditions": [],
                }
            )
            calls[-1]["returned_quotes"] = [
                str(evidence.get("quote") or "")
                for condition in body.get("source_conditions") or []
                for evidence in condition.get("evidence") or []
            ]
        elif fault_payloads:
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
            and not anchor_contract
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
        if plant_s41_c3 and (
            request["purpose"] == "source_inventory"
            and request.get("source_scope") not in {"page_map", "evidence_repair"}
            and not planted["done"]
            and request.get("source_pages")
        ):
            planted["done"] = True
            body = dict(body)
            body["source_conditions"] = [
                *(body.get("source_conditions") or []),
                _s41_c3_wire(),
            ]
        if attach_failed_id and seen_failed["id"] and request.get("purpose") == "synthesize_views":
            claims = body.setdefault("claims", [])
            if claims:
                claims[0]["condition_ids"] = [
                    *(claims[0].get("condition_ids") or []),
                    seen_failed["id"],
                ]
        if repair_mode == "full":
            body = _bind_industry_repair(request, body)
        if (
            drop_first_claims
            and not dropped_claims["done"]
            and request.get("purpose") == "synthesize_views"
        ):
            dropped_claims["done"] = True
            body = dict(body)
            body.pop("claims", None)
        finish_reason = "stop"
        if (
            truncate_first_verify
            and not truncated_verify["done"]
            and request.get("purpose") == "verify_views"
        ):
            truncated_verify["done"] = True
            finish_reason = "length"
        if (
            body
            and request.get("purpose") == "source_inventory"
            and request.get("source_scope") not in {"page_map", "evidence_repair"}
        ):
            calls[-1]["returned_evidence"] = [
                {
                    "id": item.get("condition_id"),
                    "page": evidence.get("page"),
                    "quote": evidence.get("quote"),
                }
                for item in body.get("source_conditions") or []
                for evidence in item.get("evidence") or []
                if isinstance(evidence, dict)
            ]
        encoded = json.dumps(body, ensure_ascii=False)
        if routine_contract:
            usage = {
                "prompt_tokens": len(user),
                "completion_tokens": max(1, len(encoded)),
                "total_tokens": len(user) + max(1, len(encoded)),
            }
        else:
            usage = {
                "prompt_tokens": 20,
                "completion_tokens": 20,
                "total_tokens": 40,
            }
        return {
            "status_code": 200,
            "data": {
                "choices": [
                    {"message": {"content": encoded}, "finish_reason": finish_reason}
                ],
                "usage": usage,
            },
        }

    budget = LlmRunBudget(
        deadline=time.monotonic() + 900,
        prices=prices,
        max_tokens=120_000 if routine_contract else 480_000,
        max_requests=16 if routine_contract else 32,
        max_cost_cny=Decimal(10),
        billing_mode="development_unpriced",
        recovery_policy=(
            LogicalRecoveryPolicy(max_recoveries=2, max_per_logical_request=1)
            if routine_contract
            else None
        ),
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
            ledger_tokens=120_000 if routine_contract else 480_000,
            max_physical_requests=16 if routine_contract else 32,
            prompt_version="company_profile_report_reading.2026-11-03.chapter-core.v1",
            product_contract="company_profile_product_contract.2026-10-27.llm-led.v8",
            meter_profile=_meter_profile(gateway_config, binding),
        )
        if anchor_contract:
            routine_contract = True
            identity["source_reference"] = "anchor_v1"
        if routine_contract or anchor_contract:
            identity["request_reservation_limit"] = int(
                binding.get("request_reservation_limit") or 65_536
            )
            if binding.get("max_payload_utf8_bytes"):
                identity["max_payload_utf8_bytes"] = int(
                    binding["max_payload_utf8_bytes"]
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
            token_budget=120_000 if routine_contract else 480_000,
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
        calls.budget_snapshot = budget.snapshot()
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


def test_minor_company_total_is_a_core_draft_target():
    from types import SimpleNamespace

    from research.company_profile.direct_reading import _carries_company_period_total

    page = {
        "page": 9,
        "text": "单位：元 币种：人民币\n本期\n营业收入 2,418,990,221.17\n",
        "readable": True,
    }
    condition = SimpleNamespace(
        condition_id="c4",
        description="报告期公司营业收入2,418,990,221.17元，营业成本1,965,747,663.71元。",
        evidence=(),
    )
    assert _carries_company_period_total(condition, (page,))
    subtotal = SimpleNamespace(
        condition_id="c3",
        description="主营业务分行业合计2,332,376,693.65元。",
        evidence=(),
    )
    assert _carries_company_period_total(subtotal, (page,)) is False


_OBSERVATION_PAGES = {4, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 22, 85, 92}


def _stored_quotes(claim):
    found = []
    for evidence in claim.get("evidence") or []:
        if not isinstance(evidence, dict):
            continue
        quote = (evidence.get("anchor") or {}).get("bounded_quote") or evidence.get("quote") or ""
        if quote:
            found.append(str(quote))
    return found


def _first_observation_c3():
    for item in _observation_source_receipts():
        if item.get("source_scope") != "full_context":
            continue
        for condition in (item.get("response_payload") or {}).get("source_conditions") or []:
            if condition.get("condition_id") == "c3" and condition.get("major"):
                return condition
    raise AssertionError("saved c3 is missing")


def test_replay_keeps_unlocated_c3_evidence_and_refuses_an_unsupported_pass():
    saved = _first_observation_c3()
    request = {
        "source_pages": [
            {"page": page["page"], "text": page["text"]}
            for page in _pages({9, 16, 18, 22, 92})
        ]
    }
    wire = _condition_wire(saved, request)
    assert wire["condition_id"] == "c3"
    assert wire["major"] is True
    assert "2,332,376,693.65" in wire["description"]
    assert any(
        item["page"] == 18 and "2,332,376,693.65" in item["quote"]
        for item in wire["evidence"]
    )
    page16 = [item for item in wire["evidence"] if item["page"] == 16]
    assert page16
    assert all("2,332,376,693.65" not in item["quote"] for item in page16)
    unsupported = {
        "request_id": "probe",
        "purpose": "verify_views",
        "claims": [
            {
                "claim_id": "copied-description",
                "answer": wire["description"],
                "condition_ids": ["c3"],
                "evidence": page16,
            }
        ],
        "source_conditions": [{**wire, "evidence": page16}],
    }
    checks = _controlled_observation_verify(unsupported)["checks"]
    assert checks
    assert all(check["status"] != "pass" for check in checks)
    draft = _controlled_observation_draft(
        {
            "request_id": "probe",
            "purpose": "synthesize_views",
            "source_conditions": [{**wire, "evidence": page16}],
        }
    )
    for claim in draft["claims"]:
        assert "2,332,376,693.65" not in claim["answer"]
        assert _quotes_support_answer(
            claim["answer"], [item["quote"] for item in claim["evidence"]]
        )


def test_preserved_observation_evidence_keeps_c3_blocking():
    """One entry replay. The original unlocated total stays a blocking gap."""
    _ran, queried, exported, reloaded, calls = _run(
        _fixed_core_objects(),
        pages=_pages(_OBSERVATION_PAGES),
        replay_observation=True,
        routine_contract=True,
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    assert views["real_semantic_complete"] is False
    assert views["draft_complete"] is False
    assert any(
        gap.startswith("major_evidence_not_located:") and gap.endswith("c3")
        or (gap.startswith("major_evidence_not_located:") and ":c3" in gap)
        for gap in views.get("gaps") or []
    )
    assert any(
        item.get("id") == "c3"
        and item.get("page") == 18
        and "2,332,376,693.65" in str(item.get("quote") or "")
        for call in calls
        for item in call.get("returned_evidence") or []
    )
    assert any(call.get("source_scope") == "evidence_repair" for call in calls)
    for view in views["views"].values():
        for claim in view.get("claims") or []:
            if "2,332,376,693.65" not in str(claim.get("answer") or ""):
                continue
            assert "2,332,376,693.65" in "\n".join(_stored_quotes(claim))
    assert not any(
        check.get("status") == "pass"
        and check.get("target_type") == "condition"
        and str(check.get("target_id") or "").endswith("c3")
        for receipt in views.get("call_receipts") or []
        for check in (receipt.get("response_payload") or {}).get("checks") or []
    )
    snapshot = calls.budget_snapshot
    assert snapshot["physical_requests"] <= 16, snapshot
    assert snapshot["charged_tokens"] <= 120_000, snapshot


def test_missing_claims_splits_once_and_still_delivers_the_core():
    _ran, queried, exported, reloaded, calls = _run(
        _fixed_core_objects(), drop_first_claims=True
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    assert views["draft_complete"] is True
    assert views["real_semantic_complete"] is False
    drafts = [item for item in calls if item["purpose"] == "synthesize_views"]
    assert len(drafts) >= 2
    assert drafts[0]["conditions"] != drafts[1]["conditions"]
    text = "\n".join(_answers(views))
    assert "医药" in text and "2,418,990,221.17" in text


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
    assert any(
        "2,418,990,221.17" in answer and "2,332,376,693.65" not in answer
        for answer in answers
    )
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
    assert any(item["source_scope"] == "evidence_repair" for item in _calls)


def test_condition_id_on_another_claim_does_not_clear_the_gap():
    _ran, queried, exported, reloaded, _calls = _run(
        _fixed_core_objects(),
        plant_unresolved=True,
        attach_failed_id=True,
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    gaps = views.get("gaps") or []
    gap = next(
        item
        for item in gaps
        if item.startswith("major_evidence_not_located:")
        and item.endswith("audit-unresolved-other-business")
    )
    scoped = gap.split(":", 1)[1]
    assert any(
        scoped in (claim.get("condition_ids") or [])
        for view in views["views"].values()
        for claim in view.get("claims") or []
    )
    assert not any(
        check.get("status") == "pass"
        and check.get("target_type") == "condition"
        and str(check.get("target_id") or "").endswith("audit-unresolved-other-business")
        for receipt in views.get("call_receipts") or []
        for check in (receipt.get("response_payload") or {}).get("checks") or []
    )
    assert views["draft_complete"] is False
    assert views["real_semantic_complete"] is False
    assert "医药" in "\n".join(_answers(views))


def test_s41_industry_condition_repairs_with_its_header_and_rows():
    _ran, queried, exported, reloaded, calls = _run(
        _fixed_core_objects(),
        plant_s41_c3=True,
        repair_mode="full",
        pages=_pages({4, 9, 11, 15, 16, 17, 18}),
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    answers = _answers(views)
    text = "\n".join(answers)
    repair = next(item for item in calls if item["source_scope"] == "evidence_repair")
    assert {16, 17, 18} <= set(repair["pages"])
    assert "主营业务分行业情况" in repair["page_text"]
    assert "单位：元" in repair["page_text"]
    assert views["draft_complete"] is True
    assert views["real_semantic_complete"] is False
    assert "医药" in text and "光电子" in text and "伸性纸袋纸" in text
    assert "纸行业" in text and "浆行业" in text
    assert "自主生产" in text and "直销" in text
    assert "全国各地和国际市场" in text
    assert any("招标" in answer or "木竹片" in answer for answer in answers)
    assert any(
        "2,418,990,221.17" in answer and "2,332,376,693.65" not in answer
        for answer in answers
    )
    repaired = [
        claim
        for view in views["views"].values()
        for claim in view.get("claims") or []
        if claim.get("claim_id") == "repaired-c3"
        or "主营业务分行业情况" in str(claim.get("answer") or "")
    ]
    assert repaired
    quotes = []
    for claim in repaired:
        for evidence in claim.get("evidence") or []:
            quotes.append(str((evidence.get("anchor") or {}).get("bounded_quote") or evidence.get("quote") or ""))
    assert _industry_supported(quotes)
    assert not any(
        gap.startswith("major_evidence_not_located:") and gap.endswith("c3")
        for gap in views.get("gaps") or []
    )
    assert any(
        check.get("status") == "pass"
        and check.get("target_type") == "condition"
        and str(check.get("target_id") or "").endswith(":c3")
        and check.get("covers_target_type") == "claim"
        and check.get("covers_target_id") == "repaired-c3"
        for receipt in views.get("call_receipts") or []
        for check in (receipt.get("response_payload") or {}).get("checks") or []
    )


def test_located_wrong_header_does_not_repair_the_multi_part_condition():
    _ran, queried, exported, reloaded, calls = _run(
        _fixed_core_objects(),
        plant_s41_c3=True,
        repair_mode="located_wrong",
        pages=_pages({4, 9, 11, 15, 16, 17, 18}),
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    repair = next(item for item in calls if item["source_scope"] == "evidence_repair")
    assert {16, 17} <= set(repair["pages"])
    assert "主营业务分行业情况" in repair["page_text"]
    returned = "\n".join(repair["returned_quotes"])
    assert "主营业务分产品情况" in returned
    assert "医药行业" not in returned
    assert len(repair["returned_quotes"]) == 5
    assert any(
        item["purpose"] == "synthesize_views"
        and any(str(condition).endswith(":c3") for condition in item["conditions"])
        for item in calls[calls.index(repair) + 1 :]
    )
    assert views["draft_complete"] is False
    assert views["real_semantic_complete"] is False
    assert any(
        gap.startswith("major_evidence_not_located:") and gap.endswith("c3")
        for gap in views.get("gaps") or []
    )
    assert not any(
        check.get("status") == "pass"
        and check.get("target_type") == "condition"
        and str(check.get("target_id") or "").endswith(":c3")
        for receipt in views.get("call_receipts") or []
        for check in (receipt.get("response_payload") or {}).get("checks") or []
    )
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
    assert any(
        "2,418,990,221.17" in answer and "2,332,376,693.65" not in answer
        for answer in answers
    )
    assert calls[0]["purpose"] == "source_inventory"
    assert calls[0]["source_scope"] == "page_map"
    assert any(item["purpose"] == "source_inventory" and 11 in item["pages"] for item in calls)


_DOCUMENT = "6865d58c27331e60d4db83632076baa3cf237769994df6c2aa6dfe51e97fc63c"
_KIND_DIMENSION = {
    "principal": "principal_business",
    "products": "products_services",
    "period": "revenue_model",
    "industry": "revenue_model",
    "input": "input_exposure",
    "role": "supply_chain_role",
    "output": "output_exposure",
    "chain": "upstream_downstream",
}


def _page_text(request, page_no) -> str:
    for page in request.get("source_pages") or []:
        if int(page.get("page") or 0) == int(page_no):
            return str(page.get("text") or "")
    return ""


def _anchor_slice(anchor, request) -> str:
    text = _page_text(request, anchor["page"])
    return text[int(anchor["start"]) : int(anchor["end"])]


def _anchor_kind(text: str) -> str | None:
    if "主营业务分产品情况" in text:
        return None
    if "2,418,990,221.17" in text and "营业收入" in text:
        return "period"
    if "主营业务分行业情况" in text or ("纸行业" in text and "医药行业" in text):
        return "industry"
    if "伸性纸袋纸" in text or "主要产品为" in text:
        return "products"
    if "自主生产" in text or "自行生产" in text:
        return "role"
    if "产品行销" in text:
        return "output"
    if "采购" in text and ("浆" in text or "原料" in text):
        return "input"
    if "上下游" in text or "林-浆-纸" in text or "林浆纸" in text:
        return "chain"
    if ("主要从事" in text or "主营业务" in text) and "纸" in text and "合计" not in text:
        return "principal"
    return None


def _supporting_anchor_ids(kind, anchors, request) -> list[str]:
    """The shortest spans that actually support this kind. No extra pages."""
    dimension = _KIND_DIMENSION[kind]
    matched = []
    for anchor in anchors:
        text = _anchor_slice(anchor, request)
        if _anchor_kind(text) != kind:
            continue
        matched.append((anchor, text))
    singles = [
        (len(text), [str(anchor["id"])])
        for anchor, text in matched
        if _anchor_fact_supported(dimension, [text])
    ]
    if singles:
        return min(singles)[1]
    chosen_ids: list[str] = []
    chosen_text: list[str] = []
    for anchor, text in matched:
        if "主营业务分产品情况" in text:
            break
        chosen_ids.append(str(anchor["id"]))
        chosen_text.append(text)
        if _anchor_fact_supported(dimension, chosen_text):
            return chosen_ids
        if len(chosen_ids) == 3:
            break
    return []


def _wire_evidence_items(item):
    return list(item.get("evidence") or []) + list(item.get("evidence_refs") or [])


def _evidence_text(item, request) -> str:
    catalog = request.get("evidence_catalog") or {}
    if isinstance(item, str):
        body = catalog.get(item) if isinstance(catalog, dict) else None
        if isinstance(body, dict):
            quote = (body.get("anchor") or {}).get("bounded_quote")
            if quote:
                return str(quote)
        parts = item.rsplit(":", 3)
        if len(parts) == 4 and parts[1].isdigit():
            return _page_text(request, int(parts[1]))[int(parts[2]) : int(parts[3])]
        return ""
    if isinstance(item, dict):
        if item.get("quote"):
            return str(item["quote"])
        quote = (item.get("anchor") or {}).get("bounded_quote")
        if quote:
            return str(quote)
        if item.get("evidence_id"):
            return _evidence_text(item["evidence_id"], request)
    return ""


def _anchor_fact_supported(dimension, quotes) -> bool:
    blob = "\n".join(quote for quote in quotes if quote)
    if not blob.strip():
        return False
    if dimension == "principal_business":
        return ("主要从事" in blob or "主营业务" in blob) and "纸" in blob and "分产品情况" not in blob
    if dimension == "products_services":
        return "伸性纸袋纸" in blob or "主要产品" in blob
    if dimension == "revenue_model":
        if "2,418,990,221.17" in blob:
            return "营业收入" in blob and "2,332,376,693.65" not in blob
        if "2,332,376,693.65" in blob:
            return (
                "主营业务分行业情况" in blob
                and "医药" in blob
                and "光电子" in blob
                and "主营业务分产品情况" not in blob
            )
        return False
    if dimension == "input_exposure":
        return "采购" in blob or "原料" in blob
    if dimension == "supply_chain_role":
        return "自主生产" in blob or "自行生产" in blob or "制浆" in blob
    if dimension == "output_exposure":
        return "行销" in blob and "下游需求" not in blob
    if dimension == "upstream_downstream":
        return "上下游" in blob or "上游" in blob or "林" in blob
    return False


def _anchor_page_map(request):
    pages = [int(page["page"]) for page in request.get("source_pages") or []]
    chosen = [page for page in pages if page in _OBSERVATION_PAGES]
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "requested_pages": chosen or pages,
    }


def _anchor_source_body(request, plant_other=False):
    groups: dict[str, list[str]] = {}
    for kind in _KIND_DIMENSION:
        ids = _supporting_anchor_ids(kind, request.get("source_anchors") or [], request)
        if ids:
            groups[kind] = ids
    conditions = []
    for kind, ids in groups.items():
        conditions.append(
            {
                "condition_id": f"anchor-{kind}-{ids[0].rsplit(':', 1)[-1]}",
                "dimension": _KIND_DIMENSION[kind],
                "description": "受控选择：引用本次请求已经给出的原文锚点。",
                "major": True,
                "statement_status": "current",
                "evidence": ids[:8],
            }
        )
    if plant_other:
        product = next(
            (
                anchor
                for anchor in request.get("source_anchors") or []
                if "主营业务分产品情况" in _anchor_slice(anchor, request)
            ),
            None,
        )
        if product is not None:
            conditions.append(
                {
                    "condition_id": "anchor-wrong-table",
                    "dimension": "revenue_model",
                    "description": "把产品表锚点当作分行业合计。",
                    "major": True,
                    "statement_status": "current",
                    "evidence": [product["id"]],
                }
            )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "requested_pages": [],
        "source_conditions": conditions,
    }


def _anchor_draft(request):
    claims = []
    for condition in request.get("source_conditions") or []:
        evidence = _wire_evidence_items(condition)
        quotes = [text for text in (_evidence_text(item, request) for item in evidence) if text]
        refs = []
        for item in evidence:
            if isinstance(item, str):
                refs.append(item)
            elif isinstance(item, dict) and item.get("evidence_id"):
                refs.append(item["evidence_id"])
        if not quotes or not refs:
            continue
        claims.append(
            {
                "claim_id": "draft-" + str(condition.get("condition_id")).replace(":", "-"),
                "dimension": condition.get("dimension") or "principal_business",
                "status": "observed",
                "answer": "\n".join(quotes),
                "condition_ids": [condition.get("condition_id")],
                "supporting_record_ids": [],
                "evidence": refs,
            }
        )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "claims": claims,
    }


def _anchor_verify(request):
    checks = []
    for claim in request.get("claims") or []:
        quotes = [
            text
            for text in (_evidence_text(item, request) for item in _wire_evidence_items(claim))
            if text
        ]
        supported = _anchor_fact_supported(claim.get("dimension"), quotes)
        checks.append(
            {
                "target_type": "claim",
                "target_id": claim.get("claim_id"),
                "status": "pass" if supported else "unclear",
                "explanation": "受控核验：按原文锚点核对主体、表头和数字。",
                "covers_target_type": "claim",
                "covers_target_id": claim.get("claim_id"),
            }
        )
        if not supported:
            continue
        for condition in request.get("source_conditions") or []:
            if condition.get("condition_id") not in (claim.get("condition_ids") or []):
                continue
            condition_quotes = [
                text
                for text in (
                    _evidence_text(item, request) for item in _wire_evidence_items(condition)
                )
                if text
            ]
            if not _anchor_fact_supported(condition.get("dimension"), condition_quotes):
                continue
            checks.append(
                {
                    "target_type": "condition",
                    "target_id": condition.get("condition_id"),
                    "status": "pass",
                    "explanation": "受控核验：条件锚点与主张指向同一段原文。",
                    "covers_target_type": "claim",
                    "covers_target_id": claim.get("claim_id"),
                }
            )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "checks": checks,
    }


def test_same_text_tables_keep_separate_anchors_and_reject_bad_ids():
    from research.company_profile.report_interpretation import (
        restore_source_anchor,
        source_anchor_catalog,
    )

    pages = list(_pages({16, 17}))
    catalog = source_anchor_catalog(pages, _DOCUMENT)
    assert catalog
    assert set(catalog[0]) == {"id", "page", "start", "end"}

    def text_of(item):
        page = next(page for page in pages if page["page"] == item["page"])
        return page["text"][item["start"] : item["end"]]

    industry = [item for item in catalog if "主营业务分行业情况" in text_of(item)]
    product = [item for item in catalog if "主营业务分产品情况" in text_of(item)]
    assert industry and product
    assert industry[0]["id"] != product[0]["id"]
    report = ReportIdentity(
        instrument_id="600103.SH",
        report_id="annual-2025",
        document_version=_DOCUMENT,
        report_period="2025-12-31",
        published_at="2026-04-13T16:00:00+00:00",
    )
    restored = restore_source_anchor(industry[0]["id"], report, pages)
    assert restored["anchor"]["bounded_quote"] == text_of(industry[0])
    assert restored["anchor"]["source_offset"] == industry[0]["start"]
    wrong_version = "0" * 64 + industry[0]["id"][len(_DOCUMENT) :]
    assert restore_source_anchor(wrong_version, report, pages) is None
    start = industry[0]["id"].rsplit(":", 2)[0]
    assert restore_source_anchor(f"{start}:0:999999", report, pages) is None
    assert _anchor_fact_supported("revenue_model", [text_of(product[0])]) is False


def test_anchor_contract_stops_before_principal_and_role():
    """Second normal-chain run. Principal and operating role stay unresolved."""
    _ran, queried, exported, reloaded, calls = _run(
        _fixed_core_objects(),
        pages=_pages(_OBSERVATION_PAGES),
        anchor_contract=True,
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    views = _views(queried)
    assert views["real_semantic_complete"] is False
    assert views["draft_complete"] is False
    gaps = set(views.get("gaps") or [])
    assert "core_dimension_missing:principal_business" in gaps
    assert "core_dimension_missing:operating_role" in gaps
    assert any(gap.startswith("major_condition_not_verified:core-question:operating-role") or gap == "major_condition_not_verified:core-question:operating-role" for gap in gaps)
    assert "view_not_substantive:company_profile" in gaps
    assert "view_not_substantive:supply_chain_position" in gaps
    assert any(call.get("anchor_ids") for call in calls)
    snapshot = calls.budget_snapshot
    assert snapshot["physical_requests"] <= 16, snapshot["physical_requests"]
    assert snapshot["charged_tokens"] <= 120_000, snapshot["charged_tokens"]


# Hand-checked against the formal 600103.SH 2025 annual report. One passage may
# support more than one dimension. This table is test input, not a production rule.
_REVIEWED = (
    {
        "condition_id": "reviewed-principal",
        "dimension": "principal_business",
        "quotes": (
            "公司主要从事竹木浆、溶解浆、纸袋纸、纸制品、光电子产品、医药产品、林木产品等产品\r\n"
            "产销及贸易经营。主要产品为“青山牌”系列伸性纸袋纸、环保纸袋纸、精制牛皮纸、溶解浆、\r\n"
            "竹木浆、风油精、光器件、光模块等产品，产品行销全国各地和国际市场",
        ),
        "answer": "公司主要从事竹木浆、溶解浆、纸袋纸、纸制品、光电子产品、医药产品和林木产品的产销及贸易经营。",
    },
    {
        "condition_id": "reviewed-products",
        "dimension": "products_services",
        "quotes": (
            "公司主要从事竹木浆、溶解浆、纸袋纸、纸制品、光电子产品、医药产品、林木产品等产品\r\n"
            "产销及贸易经营。主要产品为“青山牌”系列伸性纸袋纸、环保纸袋纸、精制牛皮纸、溶解浆、\r\n"
            "竹木浆、风油精、光器件、光模块等产品，产品行销全国各地和国际市场",
        ),
        "answer": "主要产品包括“青山牌”系列伸性纸袋纸、环保纸袋纸、精制牛皮纸、溶解浆、竹木浆、风油精、光器件和光模块。",
    },
    {
        "condition_id": "reviewed-output",
        "dimension": "output_exposure",
        "quotes": (
            "公司主要从事竹木浆、溶解浆、纸袋纸、纸制品、光电子产品、医药产品、林木产品等产品\r\n"
            "产销及贸易经营。主要产品为“青山牌”系列伸性纸袋纸、环保纸袋纸、精制牛皮纸、溶解浆、\r\n"
            "竹木浆、风油精、光器件、光模块等产品，产品行销全国各地和国际市场",
        ),
        "answer": "产品行销全国各地和国际市场。",
    },
    {
        "condition_id": "reviewed-role",
        "dimension": "supply_chain_role",
        "quotes": (
            "公司浆纸主业一直秉承自主生产的经营模式，具备制浆造纸生产线及配套的碱回收、热电、\r\n"
            "污水处理系统。",
        ),
        "answer": "公司浆纸主业秉承自主生产的经营模式，具备制浆造纸生产线。",
    },
    {
        "condition_id": "reviewed-input",
        "dimension": "input_exposure",
        "quotes": (
            "大宗原材料、\r\n"
            "燃料、设备等主要采用公开招标、竞争性谈判等方式，国内木竹片采购定价主要采取挂牌采购",
        ),
        "answer": "大宗原材料、燃料和设备主要采用公开招标或竞争性谈判，国内木竹片采购定价主要采取挂牌采购。",
    },
    {
        "condition_id": "reviewed-sales",
        "dimension": "revenue_model",
        "quotes": ("公司销售采取“产品直销，以销定产”的销售模式。",),
        "answer": "公司销售采取产品直销、以销定产的销售模式。",
    },
    {
        "condition_id": "reviewed-period",
        "dimension": "revenue_model",
        "quotes": ("单位：元 币种：人民币", "营业收入 2,418,990,221.17"),
        "answer": "营业收入2,418,990,221.17元，单位为人民币元。",
    },
    {
        "condition_id": "reviewed-industry",
        "dimension": "revenue_model",
        "quotes": (
            "主营业务分行业情况",
            "医药行业 303,680,631.79",
            "光电子行业 206,025,784.54",
        ),
        "answer": "主营业务分行业情况按人民币元列示，其中包括医药行业和光电子行业。",
    },
    {
        "condition_id": "reviewed-chain",
        "dimension": "upstream_downstream",
        "quotes": ("公司目前已形成“林-浆-纸”产业链协同效应",),
        "answer": "公司目前已形成“林-浆-纸”产业链协同效应。",
    },
    {
        "condition_id": "reviewed-chain-plan",
        "dimension": "upstream_downstream",
        "quotes": ("推动上下游协同创新",),
        "answer": "展望提出推动上下游协同创新，这不是已经实现的关系。",
        "statement_status": "planned",
        "major": False,
    },
)
_REVIEWED_BY_ID = {item["condition_id"]: item for item in _REVIEWED}


def _reviewed_quotes_are_on_the_formal_pages() -> None:
    pages = {page["page"]: page["text"] for page in _pages({9, 11, 13, 16})}
    blob = "\n".join(pages.values())
    for item in _REVIEWED:
        for quote in item["quotes"]:
            assert quote in blob, quote


def _ids_for_quote(request, quote: str) -> list[str]:
    hits = []
    for anchor in request.get("source_anchors") or []:
        text = _anchor_slice(anchor, request)
        if quote in text:
            hits.append((len(text), str(anchor["id"])))
    if not hits:
        return []
    return [min(hits)[1]]


def _condition_blob(condition, request) -> str:
    return "\n".join(
        text
        for text in (_evidence_text(item, request) for item in _wire_evidence_items(condition))
        if text
    )


def _reviewed_for_condition(condition, request):
    direct = _REVIEWED_BY_ID.get(str(condition.get("condition_id") or ""))
    if direct is not None:
        return direct
    blob = _condition_blob(condition, request)
    matches = [
        item
        for item in _REVIEWED
        if item["dimension"] == condition.get("dimension")
        and all(quote in blob for quote in item["quotes"])
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def _reviewed_source_body(request):
    conditions = []
    for item in _REVIEWED:
        ids: list[str] = []
        missing = False
        for quote in item["quotes"]:
            found = _ids_for_quote(request, quote)
            if not found:
                missing = True
                break
            if found[0] not in ids:
                ids.append(found[0])
        if missing:
            continue
        conditions.append(
            {
                "condition_id": item["condition_id"],
                "dimension": item["dimension"],
                "description": item["answer"],
                "major": item.get("major", True),
                "statement_status": item.get("statement_status", "current"),
                "evidence": ids,
            }
        )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "requested_pages": [],
        "source_conditions": conditions,
    }


def _reviewed_draft(request):
    claims = []
    for condition in request.get("source_conditions") or []:
        item = _reviewed_for_condition(condition, request)
        if item is None:
            continue
        refs = []
        for raw in _wire_evidence_items(condition):
            if isinstance(raw, str):
                refs.append(raw)
            elif isinstance(raw, dict) and raw.get("evidence_id"):
                refs.append(raw["evidence_id"])
        if not refs:
            continue
        claims.append(
            {
                "claim_id": "draft-" + str(condition.get("condition_id")).replace(":", "-"),
                "dimension": item["dimension"],
                "status": "observed",
                "answer": item["answer"],
                "condition_ids": [condition.get("condition_id")],
                "supporting_record_ids": [],
                "evidence": refs,
            }
        )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
        "claims": claims,
    }


def _match_reviewed_claim(claim, request):
    """Support comes from this claim's answer and its own quotes."""
    blob = _condition_blob(claim, request)
    matches = [
        item
        for item in _REVIEWED
        if claim.get("answer") == item["answer"]
        and all(quote in blob for quote in item["quotes"])
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def _covers_supported_claim(condition, supported, request):
    """A condition is covered only when its own text supports one trusted claim."""
    blob = _condition_blob(condition, request)
    condition_id = str(condition.get("condition_id") or "")
    for claim_id, item in supported.items():
        if not all(quote in blob for quote in item["quotes"]):
            continue
        if condition_id == item["condition_id"] or item["dimension"] == condition.get(
            "dimension"
        ):
            return claim_id, item
    return None, None


def _reviewed_verify(request):
    checks = []
    supported = {}
    for claim in request.get("claims") or []:
        item = _match_reviewed_claim(claim, request)
        accepted = item is not None
        checks.append(
            {
                "target_type": "claim",
                "target_id": claim.get("claim_id"),
                "status": "pass" if accepted else "unclear",
                "explanation": "受控核验：主张是否由本主张自己的原文支持。",
                "covers_target_type": "claim",
                "covers_target_id": claim.get("claim_id"),
            }
        )
        if accepted:
            supported[claim.get("claim_id")] = item
    for condition in request.get("source_conditions") or []:
        claim_id, _item = _covers_supported_claim(condition, supported, request)
        checks.append(
            {
                "target_type": "condition",
                "target_id": condition.get("condition_id"),
                "status": "pass" if claim_id else "unclear",
                "explanation": "受控核验：条件是否覆盖本请求里已经支持的主张。",
                "covers_target_type": "claim" if claim_id else None,
                "covers_target_id": claim_id,
            }
        )
    return {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request.get("request_id") or "reviewed-verify",
        "purpose": request.get("purpose") or "verify_views",
        "checks": checks,
    }


def test_reviewed_passage_keeps_principal_and_products_together():
    _reviewed_quotes_are_on_the_formal_pages()
    pages = list(_pages({9}))
    from research.company_profile.report_interpretation import source_anchor_catalog

    anchors = source_anchor_catalog(pages, _DOCUMENT)
    request = {
        "request_id": "component",
        "purpose": "source_inventory",
        "source_pages": pages,
        "source_anchors": anchors,
    }
    body = _reviewed_source_body(request)
    by_id = {item["condition_id"]: item for item in body["source_conditions"]}
    assert "reviewed-principal" in by_id
    assert "reviewed-products" in by_id
    assert by_id["reviewed-principal"]["evidence"] == by_id["reviewed-products"]["evidence"]
    assert by_id["reviewed-principal"]["evidence"]


def test_unlisted_anchor_id_is_rejected_and_fragment_offset_rebinds():
    import pytest

    from research.company_profile.contracts import ReportSemanticRequestV3
    from research.company_profile.report_interpretation import (
        restore_source_anchor,
        source_anchor_catalog,
        source_anchor_id,
    )
    from research.company_profile.report_wire import expand_response

    full = next(page for page in _pages({16}))
    fragment = {
        "page": 16,
        "offset": 13,
        "text": full["text"][13:93],
        "readable": True,
    }
    catalog = source_anchor_catalog([fragment], _DOCUMENT)
    assert catalog
    assert min(item["start"] for item in catalog) >= 13
    report = ReportIdentity(
        instrument_id="600103.SH",
        report_id="annual-2025",
        document_version=_DOCUMENT,
        report_period="2025-12-31",
        published_at="2026-04-13T16:00:00+00:00",
    )
    restored = restore_source_anchor(catalog[0]["id"], report, [fragment])
    quote = restored["anchor"]["bounded_quote"]
    offset = restored["anchor"]["source_offset"]
    assert offset == catalog[0]["start"]
    assert full["text"][offset : offset + len(quote)] == quote
    request = ReportSemanticRequestV3(
        request_id="anchor-component",
        purpose="source_inventory",
        report=report,
        processing_identity={"source_reference": "anchor_v1"},
        source_pages=({"page": 16, "offset": 0, "text": full["text"], "readable": True},),
        max_output_tokens=200,
        timeout_seconds=30,
    )
    listed = source_anchor_catalog(request.source_pages, _DOCUMENT)
    forged = source_anchor_id(_DOCUMENT, 16, 1, 2)
    assert forged not in {item["id"] for item in listed}
    with pytest.raises(ValueError, match="unknown source anchor"):
        expand_response(
            {
                "source_conditions": [
                    {
                        "condition_id": "forged",
                        "dimension": "principal_business",
                        "description": "目录外范围",
                        "major": True,
                        "statement_status": "current",
                        "evidence": [forged],
                    }
                ]
            },
            request,
        )


def test_claim_support_does_not_wait_for_a_source_condition():
    from research.company_profile.contracts import (
        ReportSemanticRequestV3,
        ReportViewCheck,
        ReportViewClaim,
    )
    from research.company_profile.direct_reading import _operating_role_established, _verified_claims
    from research.company_profile.models import Evidence, TextAnchor
    from research.company_profile.report_interpretation import source_anchor_catalog
    from research.company_profile.report_wire import expand_response

    _reviewed_quotes_are_on_the_formal_pages()
    role = _REVIEWED_BY_ID["reviewed-role"]
    pages = list(_pages({11, 13}))
    anchors = source_anchor_catalog(pages, _DOCUMENT)
    role_ids = []
    for quote in role["quotes"]:
        found = _ids_for_quote({"source_pages": pages, "source_anchors": anchors}, quote)
        assert found, quote
        role_ids.extend(found)
    claim = {
        "claim_id": "draft-reviewed-role",
        "dimension": role["dimension"],
        "answer": role["answer"],
        "condition_ids": ["reviewed-role"],
        "evidence": role_ids,
    }
    alone = {
        "request_id": "support-only",
        "purpose": "verify_views",
        "source_pages": pages,
        "source_anchors": anchors,
        "claims": [claim],
        "source_conditions": [],
    }
    alone_checks = _reviewed_verify(alone)["checks"]
    assert [(item["target_type"], item["status"]) for item in alone_checks] == [
        ("claim", "pass")
    ]
    report = ReportIdentity(
        instrument_id="600103.SH",
        report_id="annual-2025",
        document_version=_DOCUMENT,
        report_period="2025-12-31",
        published_at="2026-04-13T16:00:00+00:00",
    )
    request = ReportSemanticRequestV3(
        request_id="support-only",
        purpose="verify_views",
        report=report,
        processing_identity={"source_reference": "anchor_v1"},
        source_pages=tuple(pages),
        claims=(
            ReportViewClaim(
                claim_id="draft-reviewed-role",
                dimension="supply_chain_role",
                status="observed",
                answer=role["answer"],
                condition_ids=("reviewed-role",),
                evidence=(
                    Evidence(
                        evidence_id=role_ids[0],
                        report=report,
                        page=11,
                        section_title="原文",
                        anchor=TextAnchor(
                            bounded_quote=role["quotes"][0],
                            source_offset=0,
                        ),
                    ),
                ),
            ),
        ),
        max_output_tokens=200,
        timeout_seconds=30,
    )
    expanded = expand_response(_reviewed_verify(alone), request)
    assert expanded["checks"][0]["status"] == "pass"
    assert expanded["checks"][0]["target_type"] == "claim"
    stored_claim = ReportViewClaim(
        claim_id="draft-reviewed-role",
        dimension="supply_chain_role",
        status="observed",
        answer=role["answer"],
        evidence=(),
    )
    stored_check = ReportViewCheck.model_validate(expanded["checks"][0])
    assert _verified_claims((stored_claim,), (stored_check,))
    joined = _reviewed_verify(
        {
            **alone,
            "source_conditions": [
                {
                    "condition_id": "core-question:operating-role",
                    "dimension": "supply_chain_role",
                    "evidence": role_ids,
                }
            ],
        }
    )["checks"]
    assert ("claim", "pass") in [(item["target_type"], item["status"]) for item in joined]
    coverage = next(item for item in joined if item["target_type"] == "condition")
    assert coverage["status"] == "pass"
    assert coverage["covers_target_id"] == "draft-reviewed-role"
    role_check = ReportViewCheck(
        target_type="condition",
        target_id="core-question:operating-role",
        status="pass",
        explanation="受控核验：条件是否覆盖本请求里已经支持的主张。",
        covers_target_type="claim",
        covers_target_id="draft-reviewed-role",
    )
    assert _operating_role_established((stored_claim,), (stored_check, role_check))
    wrong = _reviewed_verify(
        {
            **alone,
            "source_conditions": [
                {
                    "condition_id": "reviewed-role",
                    "dimension": "supply_chain_role",
                    "evidence": ["not-the-role-quote"],
                }
            ],
        }
    )["checks"]
    assert next(item for item in wrong if item["target_type"] == "condition")["status"] == "unclear"
    chain = _REVIEWED_BY_ID["reviewed-chain"]
    plan = _REVIEWED_BY_ID["reviewed-chain-plan"]
    assert "林-浆-纸" in chain["quotes"][0]
    assert chain.get("statement_status", "current") == "current"
    assert plan["statement_status"] == "planned"
    assert plan["major"] is False
    assert "木竹片" in _REVIEWED_BY_ID["reviewed-input"]["answer"]
    assert "挂牌采购" in _REVIEWED_BY_ID["reviewed-input"]["quotes"][0]
    assert "推动上下游协同创新" not in chain["quotes"][0]


def test_reviewed_input_delivers_the_company_core():
    """The one section-5 entry. A correct reviewed response must come back complete."""
    result_path = Path("/home/python/Quote/data/QuoteBak/explicit-input-result.json")
    _ran, queried, exported, reloaded, calls = _run(
        _fixed_core_objects(),
        pages=_pages(_OBSERVATION_PAGES),
        reviewed_input=True,
    )
    views = _views(queried)
    snapshot = calls.budget_snapshot
    result_path.write_text(
        __import__("json").dumps(
            {
                "physical": snapshot.get("physical_requests"),
                "charged": snapshot.get("charged_tokens"),
                "stopped": snapshot.get("stopped_reason"),
                "draft_complete": views.get("draft_complete"),
                "gaps": views.get("gaps"),
                "answers": _answers(views),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    assert queried["profiles"] == exported["profiles"] == reloaded["profiles"]
    assert views["draft_complete"] is True, views.get("gaps")
    assert views["real_semantic_complete"] is False
    answers = _answers(views)
    text = "\n".join(answers)
    assert "主要从事" in text and "伸性纸袋纸" in text and "医药" in text and "光电子" in text
    assert "自主生产" in text and "直销" in text and "木竹片" in text and "挂牌采购" in text
    assert "林-浆-纸" in text and "行销" in text
    assert any(
        "2,418,990,221.17" in answer and "2,332,376,693.65" not in answer
        for answer in answers
    )
    assert "推动上下游协同创新" not in text or "不是已经实现" in text
    covered = _covered_claim_ids(views)
    assert covered
    assert snapshot["physical_requests"] <= 16, snapshot.get("physical_requests")
    assert snapshot["charged_tokens"] <= 120_000, snapshot.get("charged_tokens")
    result_path.unlink(missing_ok=True)

