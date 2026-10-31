"""Nonempty offline protocol stress replies, never a business interpreter.

Only the supervised offline entry imports this transport fixture. It has no
company names, page whitelist or acceptance answers. All synthetic claims and
conditions are explicitly unclear and cannot produce usable research results.
"""

from __future__ import annotations

from .report_interpretation import DIMENSIONS


def entry_proof_reply(request: dict) -> dict:
    """Offline stand-in for one entry proof. Not a business reading.

    One current quote can be verified. A quote that is not in the supplied
    text is left for the program to isolate. A planned statement stays
    unverified. Missing dimensions stay missing.
    """
    result = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
    }
    if request.get("source_scope") == "page_map":
        readable = [
            page
            for page in request["source_pages"]
            if page.get("readable", True) and str(page.get("text") or "").strip()
        ]
        result["requested_pages"] = (
            [readable[len(readable) // 2]["page"]] if readable else []
        )
        return result
    if request["purpose"] == "source_inventory":
        pages = [page for page in request["source_pages"] if str(page.get("text") or "").strip()]
        result["requested_pages"] = []
        if not pages:
            result["source_conditions"] = []
            return result
        text = str(pages[0]["text"])
        compact = "".join(text.split())

        def take(start: int, length: int = 80) -> str:
            if len(compact) <= length:
                return compact
            start = min(start, len(compact) - length)
            return compact[start : start + length]

        # Slices drop PDF spaces. The program's whitespace locator binds each
        # unique sequence back to the sent page.
        result["source_conditions"] = [
            {
                "condition_id": "proof-current",
                "dimension": "principal_business",
                "description": "OFFLINE current disclosure in the supplied page.",
                "evidence": [{"page": pages[0]["page"], "quote": take(0)}],
                "major": True,
                "statement_status": "current",
            },
            {
                "condition_id": "proof-output",
                "dimension": "output_exposure",
                "description": "OFFLINE current output disclosure in the supplied page.",
                "evidence": [{"page": pages[0]["page"], "quote": take(80)}],
                "major": False,
                "statement_status": "current",
            },
            {
                "condition_id": "proof-upstream",
                "dimension": "upstream_downstream",
                "description": "OFFLINE current counterparty disclosure in the supplied page.",
                "evidence": [{"page": pages[0]["page"], "quote": take(160)}],
                "major": False,
                "statement_status": "current",
            },
            {
                "condition_id": "proof-planned",
                "dimension": "revenue_model",
                "description": "OFFLINE planned activity, not a current fact.",
                "evidence": [{"page": pages[0]["page"], "quote": take(240)}],
                "major": False,
                "statement_status": "planned",
            },
            {
                "condition_id": "proof-bad-quote",
                "dimension": "products_services",
                "description": "OFFLINE quote that is not in the supplied text.",
                "evidence": [{"page": pages[0]["page"], "quote": "OFFLINE-NOT-IN-SOURCE"}],
                "major": False,
                "statement_status": "current",
            },
        ]
        return result
    if request["purpose"] == "synthesize_views":
        result["claims"] = [
            {
                "claim_id": f"proof-claim-{item['condition_id']}",
                "dimension": item["dimension"],
                "status": "observed",
                "answer": item["description"],
                "condition_ids": [item["condition_id"]],
                "supporting_record_ids": [],
                "evidence": list(item.get("evidence_refs") or []),
            }
            for item in request.get("source_conditions") or []
        ]
        return result
    catalog = request.get("evidence_catalog") or {}

    def bundled(item: dict) -> str:
        parts = [str(item.get("description") or ""), str(item.get("answer") or "")]
        for ref in item.get("evidence_refs") or []:
            evidence = catalog.get(ref) or {}
            anchor = evidence.get("anchor") or {}
            parts.append(str(anchor.get("context_before") or ""))
            parts.append(str(anchor.get("bounded_quote") or ""))
        return "\n".join(parts)

    def needs_isolation(item: dict) -> bool:
        text = bundled(item)
        return (
            item.get("statement_status") == "planned"
            or "计划" in str(item.get("description") or "")
            or "技术环境" in text
        )

    result["checks"] = []
    isolated_ids = {
        item["condition_id"]
        for item in request.get("source_conditions") or []
        if needs_isolation(item)
    }
    for item in request.get("source_conditions") or []:
        result["checks"].append(
            {
                "target_type": "condition",
                "target_id": item["condition_id"],
                "status": "unclear" if item["condition_id"] in isolated_ids else "pass",
                "explanation": "OFFLINE verifier. Location is not semantic support.",
            }
        )
    for item in request.get("claims") or []:
        unclear = needs_isolation(item) or any(
            key in isolated_ids for key in item.get("condition_ids") or []
        )
        result["checks"].append(
            {
                "target_type": "claim",
                "target_id": item["claim_id"],
                "status": "unclear" if unclear else "pass",
                "explanation": "OFFLINE verifier. Location is not semantic support.",
            }
        )
    for item in request.get("records") or []:
        result["checks"].append(
            {
                "target_type": "record",
                "target_id": item["record_id"],
                "status": "pass",
                "explanation": "OFFLINE record shape only.",
            }
        )
    return result


def batch_commit_reply(request: dict) -> dict:
    """Offline stand-in for one batch-commit proof. Not a business reading."""
    result = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
    }
    if request.get("source_scope") == "page_map":
        readable = [
            page
            for page in request["source_pages"]
            if page.get("readable", True) and str(page.get("text") or "").strip()
        ]
        result["requested_pages"] = (
            [readable[len(readable) // 2]["page"]] if readable else []
        )
        return result
    if request["purpose"] == "source_inventory":
        pages = [
            page
            for page in request["source_pages"]
            if str(page.get("text") or "").strip()
        ]
        result["requested_pages"] = []
        result["source_conditions"] = []
        if not pages:
            return result
        compact = "".join(str(pages[0]["text"]).split())

        def take(start: int) -> str:
            if len(compact) <= 40:
                return compact
            start = min(start, len(compact) - 40)
            return compact[start : start + 40]

        result["source_conditions"] = [
            {
                "condition_id": "batch-a",
                "dimension": "principal_business",
                "description": "OFFLINE current disclosure in the supplied page.",
                "evidence": [{"page": pages[0]["page"], "quote": take(0)}],
                "major": True,
                "statement_status": "current",
            },
            {
                "condition_id": "batch-b",
                "dimension": "products_services",
                "description": "OFFLINE second batch. " + ("补充说明。" * 1040),
                "evidence": [{"page": pages[0]["page"], "quote": take(40)}],
                "major": False,
                "statement_status": "current",
            },
        ]
        return result
    if request["purpose"] == "synthesize_views":
        result["claims"] = [
            {
                "claim_id": f"batch-claim-{item['condition_id']}",
                "dimension": item["dimension"],
                "status": "observed",
                "answer": item["description"][:80],
                "condition_ids": [item["condition_id"]],
                "supporting_record_ids": [],
                "evidence": list(item.get("evidence_refs") or []),
            }
            for item in request.get("source_conditions") or []
        ]
        result["_fail_second_batch"] = any(
            "补充说明" in str(item.get("description") or "")
            for item in request.get("source_conditions") or []
        )
        return result
    result["checks"] = [
        {
            "target_type": "condition",
            "target_id": item["condition_id"],
            "status": "pass",
            "explanation": "OFFLINE verifier. Not a business score.",
        }
        for item in request.get("source_conditions") or []
    ]
    result["checks"].extend(
        {
            "target_type": "claim",
            "target_id": item["claim_id"],
            "status": "pass",
            "explanation": "OFFLINE verifier. Not a business score.",
        }
        for item in request.get("claims") or []
    )
    result["checks"].extend(
        {
            "target_type": "record",
            "target_id": item["record_id"],
            "status": "pass",
            "explanation": "OFFLINE verifier. Not a business score.",
        }
        for item in request.get("records") or []
    )
    return result


def protocol_reply(request: dict) -> dict:
    result = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
    }
    if request.get("source_scope") == "page_map":
        readable = [p for p in request["source_pages"] if p["readable"]]
        # A deterministic transport fixture, not a semantic selection algorithm.
        # Middle-of-report selection exercises arbitrary pages, with original
        # fact context added independently by the real reading owner.
        result["requested_pages"] = [readable[len(readable) // 2]["page"]]
        return result
    if request["purpose"] == "source_inventory":
        pages = [p for p in request["source_pages"] if p["text"].strip()]
        result["requested_pages"] = []
        result["source_conditions"] = [
            {
                "condition_id": f"protocol-{index}",
                "dimension": dimension,
                "description": "OFFLINE protocol load only; not an identified business condition. "
                * 2,
                "evidence": [{"page": page["page"], "quote": page["text"][:100]}],
                "major": True,
                "statement_status": "unclear",
            }
            for index, dimension in enumerate(DIMENSIONS * 2)
            for page in [pages[index % len(pages)]]
        ]
    elif request["purpose"] == "synthesize_views":
        conditions = request.get("source_conditions", [])
        result["claims"] = [
            {
                "claim_id": f"protocol-claim-{index}",
                "dimension": dimension,
                "status": "unclear",
                "answer": "OFFLINE protocol load only; no business interpretation performed. "
                * 3,
                "condition_ids": [
                    c["condition_id"] for c in conditions if c["dimension"] == dimension
                ],
                "supporting_record_ids": [
                    r["record_id"] for r in request.get("records", [])
                ],
                "evidence": conditions[index]["evidence_refs"],
                "limitation": "synthetic_resource_probe_not_research_output",
            }
            for index, dimension in enumerate(DIMENSIONS)
        ]
    else:
        result["checks"] = [
            {
                "target_type": kind,
                "target_id": item[key],
                "status": "pass" if kind == "record" else "unclear",
                "explanation": "Offline protocol shape only; not semantically verified. "
                * 2,
            }
            for kind, values, key in (
                ("condition", request.get("source_conditions", []), "condition_id"),
                ("claim", request.get("claims", []), "claim_id"),
                ("record", request.get("records", []), "record_id"),
            )
            for item in values
        ]
    return result


def _informative_quote(quote: str) -> bool:
    letters = sum(1 for char in quote if char.isalnum() or "\u4e00" <= char <= "\u9fff")
    return letters >= 8


def _compact_quotes(request: dict, count: int) -> list[tuple[int, str]]:
    """Unique compact slices from each sent page. Not a company reading."""
    found: list[tuple[int, str]] = []
    used: set[str] = set()
    page_texts: list[tuple[int, str]] = []
    for page in request.get("source_pages") or []:
        compact = "".join(str(page.get("text") or "").split())
        if len(compact) < 12:
            continue
        page_texts.append((int(page["page"]), compact))
    step = 0
    while page_texts and len(found) < count:
        progressed = False
        for page_no, compact in page_texts:
            if len(found) >= count:
                break
            length = min(80, len(compact))
            start = 0 if len(compact) <= length else min(step * length, len(compact) - length)
            quote = compact[start : start + length]
            if quote not in used and _informative_quote(quote):
                used.add(quote)
                found.append((page_no, quote))
                progressed = True
        if not progressed:
            break
        step += 1
    return found


def direct_chain_reply(request: dict) -> dict:
    """Offline stand-in for the direct chapter chain. Not a reading of the company."""
    result = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
    }
    if request.get("source_scope") == "page_map":
        chosen = [
            int(page["page"])
            for page in request.get("source_pages") or []
            if page.get("readable", True) and str(page.get("text") or "").strip()
        ]
        if len(chosen) >= 2:
            distant = next((page for page in chosen if abs(page - chosen[0]) >= 8), chosen[-1])
            result["requested_pages"] = [chosen[0], distant]
        else:
            result["requested_pages"] = chosen[:1]
        return result
    dimensions = (
        "principal_business",
        "products_services",
        "revenue_model",
        "input_exposure",
        "supply_chain_role",
        "output_exposure",
    )
    quotes = _compact_quotes(request, len(dimensions))
    paired = list(zip(dimensions, quotes, strict=False))
    if request["purpose"] == "source_inventory":
        if request.get("claims"):
            raise ValueError("gap_request_contains_candidates")
        result["requested_pages"] = []
        result["source_conditions"] = [
            {
                "condition_id": f"chain-{index}",
                "dimension": dimension,
                "description": "模拟正文条件，不是年报结论。",
                "evidence": [{"page": page, "quote": quote}],
                "major": True,
                "statement_status": "current",
            }
            for index, (dimension, (page, quote)) in enumerate(paired)
        ]
        return result
    if request["purpose"] == "synthesize_views":
        conditions = [
            item
            for item in request.get("source_conditions") or []
            if item.get("statement_status") == "current"
        ]
        if conditions:
            catalog = request.get("evidence_catalog") or {}
            result["claims"] = []
            for index, item in enumerate(conditions):
                refs = list(item.get("evidence_refs") or [])
                evidence = list(refs)
                if not evidence:
                    for raw in item.get("evidence") or []:
                        if isinstance(raw, str):
                            evidence.append(raw)
                            continue
                        body = catalog.get(raw.get("evidence_id") or "") or raw
                        anchor = body.get("anchor") or {}
                        quote = (
                            anchor.get("bounded_quote")
                            or anchor.get("model_quote")
                            or raw.get("quote")
                        )
                        page = body.get("page", raw.get("page"))
                        if page is not None and quote:
                            evidence.append({"page": int(page), "quote": quote})
                result["claims"].append(
                    {
                        "claim_id": f"chain-claim-{item.get('condition_id') or index}",
                        "dimension": item["dimension"],
                        "status": "observed",
                        "answer": "模拟主张，不是年报结论。",
                        "condition_ids": [item["condition_id"]] if item.get("condition_id") else [],
                        "supporting_record_ids": [],
                        "evidence": evidence,
                    }
                )
            return result
        result["claims"] = [
            {
                "claim_id": f"chain-claim-{index}",
                "dimension": dimension,
                "status": "observed",
                "answer": "模拟主张，不是年报结论。",
                "condition_ids": [],
                "supporting_record_ids": [],
                "evidence": [{"page": page, "quote": quote}],
            }
            for index, (dimension, (page, quote)) in enumerate(paired)
        ]
        return result
    result["checks"] = []
    for kind, values, key in (
        ("condition", request.get("source_conditions") or [], "condition_id"),
        ("claim", request.get("claims") or [], "claim_id"),
        ("record", request.get("records") or [], "record_id"),
    ):
        for item in values:
            check = {
                "target_type": kind,
                "target_id": item[key],
                "status": "pass",
                "explanation": "离线链路核验，不是年报结论。",
            }
            if kind == "condition":
                match = next(
                    (
                        claim["claim_id"]
                        for claim in request.get("claims") or []
                        if claim.get("dimension") == item.get("dimension")
                    ),
                    None,
                )
                if match:
                    check["covers_target_type"] = "claim"
                    check["covers_target_id"] = match
            result["checks"].append(check)
    return result


def gap_loop_reply(request: dict) -> dict:
    """Build answers only from pages this request actually sent. Not a score."""
    result = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
    }
    pages = [
        page
        for page in request.get("source_pages") or []
        if page.get("readable", True) and str(page.get("text") or "").strip()
    ]
    if request["purpose"] == "source_inventory":
        if request.get("claims"):
            raise ValueError("gap_request_contains_candidates")
        result["requested_pages"] = []
        result["source_conditions"] = []
        return result
    if request["purpose"] == "synthesize_views":
        result["claims"] = []
        for item in request.get("source_conditions") or []:
            if str(item.get("condition_id") or "").split(":")[-1] != "operating-role":
                continue
            chosen = next(
                (
                    page
                    for page in pages
                    if any(
                        mark in str(page.get("text") or "")
                        for mark in ("自主生产", "经营模式", "生产线", "加工", "销售")
                    )
                ),
                pages[0] if pages else None,
            )
            if chosen is None:
                continue
            quote = " ".join(str(chosen.get("text") or "").split())[:80]
            result["claims"].append(
                {
                    "claim_id": "operating-role-from-sent-body",
                    "dimension": "supply_chain_role",
                    "status": "observed",
                    "answer": quote,
                    "condition_ids": [item["condition_id"]],
                    "supporting_record_ids": [],
                    "evidence": [{"page": int(chosen["page"]), "quote": quote}],
                }
            )
        return result
    result["checks"] = []
    for kind, values, key in (
        ("condition", request.get("source_conditions") or [], "condition_id"),
        ("claim", request.get("claims") or [], "claim_id"),
    ):
        for item in values:
            check = {
                "target_type": kind,
                "target_id": item[key],
                "status": "pass",
                "explanation": "离线按本请求已送原文核验，不是年报成绩。",
            }
            if kind == "condition":
                match = next(
                    (
                        claim.get("claim_id")
                        for claim in request.get("claims") or []
                        if item["condition_id"] in (claim.get("condition_ids") or [])
                    ),
                    None,
                )
                if match:
                    check["covers_target_type"] = "claim"
                    check["covers_target_id"] = match
            result["checks"].append(check)
    return result


def _unique_quote(text: str, marks: tuple[str, ...]) -> str | None:
    compact = " ".join(str(text or "").split())
    for mark in marks:
        index = compact.find(mark)
        if index < 0:
            continue
        quote = compact[max(0, index - 6) : index + 42]
        if quote and compact.count(quote) == 1:
            return quote
    return None


def core_package_reply(request: dict) -> dict:
    """Answers only from text this request sent. Protocol proof, not a score."""
    result = {
        "schema_version": "company_profile_report_response.v3",
        "request_id": request["request_id"],
        "purpose": request["purpose"],
    }
    pages = [
        page
        for page in request.get("source_pages") or []
        if page.get("readable", True) and str(page.get("text") or "").strip()
    ]
    if request["purpose"] == "source_inventory" and request.get("source_scope") == "page_map":
        def catalog(text: str) -> bool:
            return "目录" in text and ("第一节" in text or text.count(".") >= 4)

        chosen = []
        for page in pages:
            text = str(page.get("text") or "")
            if catalog(text):
                continue
            if any(
                mark in text
                for mark in ("管理层讨论", "经营情况", "经营模式", "财务报告", "主营业务")
            ):
                chosen.append(int(page["page"]))
        if not chosen:
            chosen = [int(page["page"]) for page in pages if not catalog(str(page.get("text") or ""))]
        result["requested_pages"] = chosen[:3]
        return result
    if request["purpose"] == "source_inventory":
        if request.get("claims"):
            raise ValueError("gap_request_contains_candidates")
        catalog = (
            ("principal_business", "core-business", ("主营业务", "纸行业", "浆行业")),
            ("products_services", "core-products", ("主要产品", "浆粕", "纸制品")),
            ("revenue_model", "core-revenue", ("营业收入",)),
            ("input_exposure", "core-input", ("原辅材料", "成本构成", "营业成本")),
            (
                "supply_chain_role",
                "core-question:operating-role",
                ("自主生产", "生产线", "经营模式", "制浆"),
            ),
            ("output_exposure", "core-output", ("销售模式", "直销", "经销")),
        )
        conditions = []
        for dimension, condition_id, marks in catalog:
            for page in pages:
                quote = _unique_quote(str(page.get("text") or ""), marks)
                if quote is None:
                    continue
                conditions.append(
                    {
                        "condition_id": condition_id,
                        "dimension": dimension,
                        "description": "根据本请求原文说明该项，不补原文没有的事实。",
                        "major": True,
                        "statement_status": "current",
                        "evidence": [{"page": int(page["page"]), "quote": quote}],
                    }
                )
                break
        follow = []
        for page in pages:
            text = str(page.get("text") or "")
            if "管理层讨论" in text and _unique_quote(
                text, ("自主生产", "生产线", "经营模式", "制浆")
            ) is None:
                follow.append(int(page["page"]) + 1)
        result["requested_pages"] = follow[:1]
        result["source_conditions"] = conditions
        return result
    if request["purpose"] == "synthesize_views":
        result["claims"] = []
        catalog = request.get("evidence_catalog") or {}
        seen_quotes: set[str] = set()
        for item in request.get("source_conditions") or []:
            evidence = []
            raw_evidence = list(item.get("evidence") or [])
            raw_evidence.extend(item.get("evidence_refs") or [])
            for raw in raw_evidence:
                if isinstance(raw, str):
                    raw = catalog.get(raw) or {}
                if not isinstance(raw, dict):
                    continue
                anchor = raw.get("anchor") or {}
                page_no = raw.get("page")
                quote = str(
                    raw.get("quote")
                    or anchor.get("bounded_quote")
                    or anchor.get("model_quote")
                    or ""
                )
                sent = next(
                    (
                        page
                        for page in pages
                        if page_no is not None and int(page["page"]) == int(page_no)
                    ),
                    None,
                )
                if sent is None or not quote:
                    continue
                compact_page = " ".join(str(sent.get("text") or "").split())
                probe = "".join(quote.split())[:16]
                folded = "".join(compact_page.split())
                if probe and probe in folded:
                    index = folded.find(probe)
                    seen = 0
                    start_at = 0
                    for offset, char in enumerate(compact_page):
                        if char.isspace():
                            continue
                        if seen == index:
                            start_at = offset
                            break
                        seen += 1
                    used = compact_page[start_at : start_at + 48]
                else:
                    marks = {
                        "principal_business": ("主营", "制浆", "造纸"),
                        "products_services": ("主要产品", "产品"),
                        "revenue_model": ("营业收入",),
                        "input_exposure": ("营业成本", "原辅材料", "成本"),
                        "supply_chain_role": ("制浆", "造纸", "生产"),
                        "output_exposure": ("销售模式", "直销", "销售"),
                    }.get(str(item.get("dimension") or ""), ())
                    mark = next((item_mark for item_mark in marks if item_mark in compact_page), "")
                    if not mark:
                        continue
                    index = compact_page.find(mark)
                    used = compact_page[max(0, index - 6) : index + 36]
                if not used:
                    continue
                if used in seen_quotes and item.get("dimension") == "revenue_model":
                    continue
                seen_quotes.add(used)
                evidence.append({"page": int(page_no), "quote": used})
            if not evidence:
                continue
            result["claims"].append(
                {
                    "claim_id": f"core-{item.get('condition_id')}",
                    "dimension": item.get("dimension"),
                    "status": "observed",
                    "answer": evidence[0]["quote"],
                    "condition_ids": [item["condition_id"]] if item.get("condition_id") else [],
                    "supporting_record_ids": [],
                    "evidence": evidence[:1],
                }
            )
        return result
    result["checks"] = []
    for kind, values, key in (
        ("condition", request.get("source_conditions") or [], "condition_id"),
        ("claim", request.get("claims") or [], "claim_id"),
    ):
        for item in values:
            check = {
                "target_type": kind,
                "target_id": item[key],
                "status": "pass",
                "explanation": "离线按本请求已送原文核验，不是年报成绩。",
            }
            if kind == "condition":
                condition_id = str(item.get("condition_id") or "")
                claims = request.get("claims") or []
                match = next(
                    (
                        claim.get("claim_id")
                        for claim in claims
                        if condition_id and condition_id in (claim.get("condition_ids") or [])
                    ),
                    None,
                )
                if match is None and condition_id.endswith("period-operating-revenue"):
                    match = next(
                        (
                            claim.get("claim_id")
                            for claim in claims
                            if claim.get("dimension") == "revenue_model"
                        ),
                        None,
                    )
                if match is None and condition_id.endswith("operating-role"):
                    match = next(
                        (
                            claim.get("claim_id")
                            for claim in claims
                            if claim.get("dimension") == "supply_chain_role"
                        ),
                        None,
                    )
                if match:
                    check["covers_target_type"] = "claim"
                    check["covers_target_id"] = match
            result["checks"].append(check)
    deduped: dict[tuple[str, str], dict] = {}
    for check in result["checks"]:
        deduped[(check["target_type"], str(check["target_id"]))] = check
    result["checks"] = list(deduped.values())
    return result
