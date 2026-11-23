"""Bind captured workpaper bytes to the existing local-only runtime input."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

from .models import ReportIdentity
from .workpaper import WorkpaperReadingScope
from .workpaper_sections import SectionSelection


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _read(reference: Any, root: Path) -> dict:
    data = (root / reference.path).read_bytes()
    if _sha(data) != reference.sha256:
        raise ValueError("saved workpaper input hash differs from its explicit reference")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("saved workpaper input must be an object")
    return value


def _wire(receipt: dict, ledger: dict, sequence: int) -> tuple[dict, str, dict]:
    attempts = [a for a in ledger.get("attempts", ()) if a.get("sequence") == sequence]
    if len(attempts) != 1 or receipt.get("status_code") != 200:
        raise ValueError("saved physical response has no matching original attempt")
    attempt = attempts[0]
    response = receipt["response"]
    choices = response.get("choices") or ()
    if len(choices) != 1 or choices[0].get("finish_reason") != "stop":
        raise ValueError("saved physical response was not a complete stop response")
    content = choices[0].get("message", {}).get("content")
    if (not isinstance(content, str) or not content
        or _sha(content.encode()) != (attempt.get("stream_summary") or {}).get("content_sha256")
        or response.get("model") != attempt.get("model")
        or receipt["request"].get("model") != attempt.get("model")
        or not attempt.get("source_label") or not attempt.get("profile")
        or not attempt.get("logical_request_id")):
        raise ValueError("saved model content/author does not match the original ledger")
    usage = response.get("usage") or {}
    prior_usage = attempt.get("usage") or {}
    if usage.get("total_tokens") != prior_usage.get("total_tokens"):
        raise ValueError("saved supplier usage differs from the original ledger")
    payloads = []
    for message in receipt["request"].get("messages", ()):
        if message.get("role") != "user":
            continue
        try:
            payload = json.loads(message.get("content", ""))
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and "report" in payload:
            payloads.append(payload)
    if len(payloads) != 1:
        raise ValueError("saved model request must have one bound report payload")
    return payloads[0], content, attempt


@dataclass(frozen=True)
class SavedDraftInput:
    report: ReportIdentity
    pages: tuple[dict, ...]
    source_pdf_path: str
    identity: dict
    resume: dict

    def source(self, item):
        if (item.get("instrument_id") != self.report.instrument_id
            or item.get("report_period") != self.report.report_period
            or (item.get("metadata") or {}).get("published_at") != self.report.published_at):
            raise ValueError("queued report differs from the saved workpaper source")
        return {"report": self.report, "pages": self.pages, "source_pdf_path": self.source_pdf_path,
                "workpaper_extraction_resume": self.resume}


def load_saved_draft_input(plan: Any, root: Path) -> SavedDraftInput:
    """Project old wire/ledger attribution, not a new model result or JSON repair."""
    saved = plan.saved_draft
    checkpoint = _read(saved.checkpoint, root)
    selection_wire = _read(saved.selection_receipt, root)
    failed_wire = _read(saved.failed_receipt, root)
    ledger = _read(saved.ledger, root)
    report = ReportIdentity.model_validate(checkpoint["report"])
    if (plan.instrument_ids != (report.instrument_id,)
        or plan.document_versions != {report.instrument_id: report.document_version}
        or not checkpoint.get("work_id") or checkpoint.get("workpaper") is not None):
        raise ValueError("local plan scope differs from the original failed workpaper")
    source_path = root / Path(checkpoint["source_pdf_path"])
    if _sha(source_path.read_bytes()) != report.document_version:
        raise ValueError("source PDF hash differs from the bound report")
    original = dict(checkpoint["execution"]["input_identity"]["processing_identity"])
    schema_only = getattr(plan, "consumption_mode", "local_format_only") == "local_enrichment_schema_only"
    if (original.get("reading_strategy") != "chapter_workpaper_v1"
        or original.get("execution_mode") != "provider"):
        raise ValueError("local saved workpaper requires its real original reading identity")
    selection_request, selection_raw, selection_attempt = _wire(selection_wire, ledger, saved.selection_sequence)
    failed_request, failed_raw, failed_attempt = _wire(failed_wire, ledger, saved.failed_sequence)
    if (selection_request.get("report") != report.model_dump(mode="json")
        or failed_request.get("report") != report.model_dump(mode="json")
        or selection_attempt.get("error_code")
        or (not schema_only and failed_attempt.get("error_code") != "response_parse_error")):
        raise ValueError("saved source/accepted selection/rejected extraction binding differs")
    if schema_only:
        from .workpaper_enrichment import WorkpaperEnrichmentPlan
        from .workpaper_llm import normalize_retained_enrichment_unread
        from .workpaper import WorkpaperDraftResponse, normalize_workpaper_draft

        enrichment = WorkpaperEnrichmentPlan.model_validate(original.get("workpaper_enrichment"))
        if (enrichment.report != report or failed_attempt.get("error_code")
            or failed_request.get("targeted_reading") != {"targets": [t.model_dump(mode="json") for t in enrichment.targets]}):
            raise ValueError("saved schema consumption requires the original explicit enrichment request")
        normalized, _ = normalize_retained_enrichment_unread(failed_raw, [t.element for t in enrichment.targets])
        WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(normalized))
    selected = json.loads(selection_raw)
    SectionSelection.model_validate(selected)
    scope = WorkpaperReadingScope.model_validate(failed_request["reading_scope"])
    # The original packet owner can skip selected sections whose headings cannot
    # be located in PDF layout. Exact retained wire bytes are checked again
    # against that owner's reconstructed packet before consuming any facts.
    if (not {s.section_id for s in scope.sections}.issubset(selected["selected_section_ids"])
        or not isinstance(failed_request.get("source_text"), str)):
        raise ValueError("saved extraction scope differs from accepted selection")
    def author(attempt):
        return {"model": attempt["model"], "source_label": attempt["source_label"],
                "selected_profile": attempt["profile"], "usage": attempt.get("usage") or {}}
    selection = {"purpose": "select_profile_sections", "request_id": selection_attempt["logical_request_id"],
        "contract_status": "accepted", "finish_reason": "stop", "raw_content": selection_raw,
        "response_hash": _sha(selection_raw.encode()), "response_payload": selected, **author(selection_attempt)}
    failed = {"purpose": "extract_profile_workpaper", "request_id": failed_attempt["logical_request_id"],
        "contract_status": "rejected", "finish_reason": "stop", "response_hash": _sha(failed_raw.encode()),
        "scope_kind": "selected_source_sections", "section_ids": [s.section_id for s in scope.sections],
        **author(failed_attempt)}
    resume = {"mode": "local_enrichment_schema_only" if schema_only else "local_format_only", "document_version": report.document_version,
        "selection_receipt": selection, "selection_response": selected,
        "failed_response": {"content": failed_raw, "content_sha256": _sha(failed_raw.encode()), "finish_reason": "stop"},
        "failed_call": failed, "original_processing_identity": original,
        "expected_source_packet_sha256": _sha(json.dumps({
            "reading_scope": failed_request["reading_scope"], "source_text": failed_request["source_text"],
        }, ensure_ascii=False, sort_keys=True).encode())}
    identity = {k: v for k, v in original.items() if k not in {
        "workpaper_review_policy", "workpaper_review_execution", "quality_review_profile", "repair_profile",
        "review_max_output_tokens", "workpaper_review_recovery", "review_existing_draft_hash", "review_resume_hash",
    }}
    identity.update(local_format_only=True, workpaper_extraction_resume_hash=_sha(
        json.dumps(resume, ensure_ascii=False, sort_keys=True).encode()))
    return SavedDraftInput(report, tuple(checkpoint["pages"]), str(source_path), identity, resume)
