"""Nonempty offline protocol stress replies, never a business interpreter.

Only the supervised offline entry imports this transport fixture. It has no
company names, page whitelist or acceptance answers. All synthetic claims and
conditions are explicitly unclear and cannot produce usable research results.
"""

from __future__ import annotations

from .report_interpretation import DIMENSIONS


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
