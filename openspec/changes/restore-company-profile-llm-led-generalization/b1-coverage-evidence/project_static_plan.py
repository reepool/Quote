"""Serialize the coverage-fix reading plan. No transport, model call, or budget admission.

The unit is the existing UTF-8 payload plus 256-byte framing plus maximum output.
It is not a deployment tokenizer and not a fee. Checkpoint text is the iteration-2
parse already persisted for 600037.SH; this script does not re-enter that run.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

from research.company_profile.contracts import ReportSemanticRequestV3
from research.company_profile.report_interpretation import (
    DISCOVERY_CHUNK_CHARS,
    page_map,
    structural_discovery_chunks,
)
from research.company_profile.report_wire import (
    compact_request,
    instructions,
    response_schema,
)
from research.company_profile.resource_probe import protocol_reply
from utils.llm import LlmClient, LlmConfig

CHANGE = Path(__file__).resolve().parent
CHECKPOINT = (
    CHANGE.parent / "b1-resource-evidence" / "iteration2" / "checkpoint.json.gz"
)
LIMIT = 120_000


def reserve(payload: dict, max_output: int) -> int:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return len(encoded) + 256 + max_output


def gateway_payload(config, profile, request: ReportSemanticRequestV3) -> dict:
    wire = compact_request(request)
    schema = response_schema(request)
    return LlmClient._build_payload(
        model=profile.model,
        messages=[
            {"role": "system", "content": instructions(request)},
            {
                "role": "user",
                "content": json.dumps(wire, ensure_ascii=False, separators=(",", ":")),
            },
        ],
        schema=schema,
        schema_name="company_profile_report_response",
        mode="json_object",
        temperature=0,
        max_output_tokens=request.max_output_tokens,
        max_output_tokens_field=profile.max_output_tokens_field,
        stream=profile.stream,
        stream_include_usage=profile.stream_include_usage,
    )


def main() -> None:
    checkpoint = json.loads(gzip.decompress(CHECKPOINT.read_bytes()))
    pages = tuple(
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(page.get("readable", True))
            and bool(str(page.get("text") or "").strip()),
        }
        for page in checkpoint["pages"]
    )
    report = checkpoint["report_interpretation"]["report"]
    identity = {
        "semantic_policy": "company_profile_llm_led.v2",
        "prompt_version": "company_profile_report_reading.2026-10-17.v2",
        "preprocessing_version": "uniform_page_map.v1",
        "model": "offline_fake",
        "execution_mode": "offline_fake",
        "semantic_reuse": False,
        "wire_protocol": "company_profile_report_request.v3",
        "discovery_scope": "page_map",
    }
    readable = [page for page in pages if page["readable"]]
    # Same deterministic middle-page choice as the nonempty protocol fixture.
    selected = {readable[len(readable) // 2]["page"]}
    full_ids = {
        page["page"]
        for page in pages
        if page["readable"]
        and any(abs(page["page"] - number) <= 1 for number in selected)
    }
    preview = page_map(pages)
    chunks = structural_discovery_chunks(pages, full_ids)
    planned = [
        ("source_inventory", "page_map", preview, 1000),
        *(
            (
                "source_inventory",
                "full_context",
                tuple(
                    {
                        "page": fragment["page"],
                        "text": fragment["text"],
                        "readable": True,
                        "offset": fragment["offset"],
                        "scope": fragment["body_scope"],
                    }
                    for fragment in chunk
                ),
                2500,
            )
            for chunk in chunks
        ),
    ]
    config = LlmConfig.from_mapping(
        json.loads(Path("config/13_llm.json").read_text())["llm"]
    )
    profiles = tuple(config.concrete_profiles_for("semantic_extraction"))
    requests = []
    for index, (purpose, scope, source_pages, max_output) in enumerate(
        planned, start=1
    ):
        request = ReportSemanticRequestV3(
            request_id=f"static:{index}",
            purpose=purpose,
            report=report,
            processing_identity=identity,
            source_pages=tuple(source_pages),
            source_scope=scope,
            max_output_tokens=max_output,
            timeout_seconds=30,
        )
        bounds = []
        for profile in profiles:
            payload = gateway_payload(config, profile, request)
            output = payload.get("max_completion_tokens", payload.get("max_tokens"))
            bounds.append(
                {
                    "profile": profile.name,
                    "model": profile.model,
                    "reserved_units": reserve(payload, int(output)),
                }
            )
        requests.append(
            {
                "purpose": purpose,
                "source_scope": scope,
                "pages": [page["page"] for page in request.source_pages],
                "body_scopes": sorted(
                    {page.get("scope") for page in request.source_pages}
                ),
                "text_utf8_bytes": sum(
                    len(page["text"].encode()) for page in request.source_pages
                ),
                "max_output_tokens": max_output,
                "max_reserved_units": max(item["reserved_units"] for item in bounds),
                "bounds": bounds,
            }
        )
    discovery_units = sum(item["max_reserved_units"] for item in requests)
    largest = max(item["max_reserved_units"] for item in requests)
    residue_pages = [
        page for page in pages if page["readable"] and page["page"] not in full_ids
    ]
    supplement_request = ReportSemanticRequestV3(
        request_id="static:supplement",
        purpose="source_inventory",
        report=report,
        processing_identity=identity,
        source_pages=tuple(residue_pages),
        source_scope="full_context",
        max_output_tokens=2500,
        timeout_seconds=30,
    )
    supplement_units = max(
        reserve(gateway_payload(config, profile, supplement_request), 2500)
        for profile in profiles
    )
    result = {
        "schema_version": "b1_coverage_static_plan.v1",
        "classification": "unsent_static_plan_not_a_rehearsal",
        "meter": "utf8_payload_bytes_plus_256_framing_plus_max_output",
        "meter_is_tokenizer": False,
        "real_calls": 0,
        "physical_pages": len(pages),
        "characters": sum(len(page["text"]) for page in pages),
        "text_utf8_bytes": sum(len(page["text"].encode()) for page in pages),
        "protocol_selected_page": next(iter(selected)),
        "full_context_pages": sorted(full_ids),
        "discovery_chunk_chars": DISCOVERY_CHUNK_CHARS,
        "discovery_requests": requests,
        "discovery_request_count": len(requests),
        "discovery_reserved_units": discovery_units,
        "discovery_gap_versus_120000": discovery_units - LIMIT,
        "largest_discovery_request_units": largest,
        "one_retry_extra_units": largest,
        "discovery_plus_one_retry_units": discovery_units + largest,
        "unpacked_residue_supplement_units": supplement_units,
        "synthesis_and_verify": (
            "not serialized; discovery alone exceeds 120000 before synthesis, "
            "verify, Evidence growth, the allowed supplement, or a retry"
        ),
        "request_count_note": (
            "Count above is discovery only. Synthesis, verify, one supplement "
            "and one retry are additional physical requests."
        ),
        "dynamic_growth": (
            "Any larger message is rejected before send by the existing physical "
            "budget. This plan is not an absolute upper bound for model output."
        ),
        "offline_208_run": "not_started",
    }
    destination = CHANGE / "static-plan.json"
    destination.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "discovery_requests": result["discovery_request_count"],
                "discovery_reserved_units": discovery_units,
                "gap": result["discovery_gap_versus_120000"],
                "plus_retry": result["discovery_plus_one_retry_units"],
                "supplement_if_all_residue_pages": supplement_units,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
