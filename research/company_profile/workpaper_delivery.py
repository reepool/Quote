"""Fact-level research delivery from existing source/review records, not release approval."""
from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Mapping

from .workpaper import (
    CompanyProfileWorkpaper, WorkpaperUseReview, workpaper_elements, workpaper_fact_hash,
)

DELIVERY_LABELS = {
    "reviewed": "已核（研究用途，非正式接受）",
    "pending": "待核（无其他限用）",
    "limited": "保真限用",
    "source_only": "仅源参考／解释",
    "withheld": "已隔离",
    "duplicate": "重复未再展示",
}


def bind_workpaper_use_review(paper: CompanyProfileWorkpaper, review: WorkpaperUseReview | Mapping) -> CompanyProfileWorkpaper:
    """Attach an explicit independent decision through the existing typed contract."""
    payload = paper.model_dump(mode="json")
    payload["use_review"] = review.model_dump(mode="json") if isinstance(review, WorkpaperUseReview) else dict(review)
    return CompanyProfileWorkpaper.model_validate(payload)


def bind_saved_model_review_scope(paper: CompanyProfileWorkpaper, original: CompanyProfileWorkpaper) -> CompanyProfileWorkpaper:
    """Recover local per-fact binding only from the exact saved initial input.

    Old calls, errors and findings are unchanged. This performs no new review.
    """
    review = paper.quality_review
    if (review is None or original.quality_review is not None or original.report != paper.report
        or paper.origin != "llm_generated" or original.origin != "llm_generated"
        or hashlib.sha256(original.model_dump_json().encode()).hexdigest() != review.original_draft_hash):
        raise ValueError("saved model review requires its exact original draft")
    if any(c.purpose == "review_profile_workpaper" and c.request_id in review.retained_request_ids for c in review.calls):
        raise ValueError("retained initial reply cannot be rebound to a replacement seed")
    original_facts = {f.fact_id: f for e in workpaper_elements(original).values() for f in e.facts}
    counts = Counter(review.checked_fact_ids)
    hashes = {fid: workpaper_fact_hash(original_facts[fid]) for fid, n in counts.items()
              if n == 1 and fid in original_facts}
    if review.checked_fact_hashes is not None and review.checked_fact_hashes != hashes:
        raise ValueError("saved model scope conflicts with its existing binding")
    payload = paper.model_dump(mode="json")
    payload["quality_review"]["checked_fact_hashes"] = hashes
    return CompanyProfileWorkpaper.model_validate(payload)


def project_workpaper_delivery(paper: CompanyProfileWorkpaper) -> dict:
    """A checked ID is not enough: retain actual call and current-content binding."""
    review = paper.quality_review
    sections = {s.section_id for s in paper.reading_scope.sections}
    pages = {s.page for e in paper.evidence for s in e.source_spans}

    def valid_call(purpose):
        return bool(review and paper.origin == "llm_generated" and any(
            c.purpose == purpose and c.contract_status == "accepted" and c.finish_reason == "stop"
            and c.model and c.source_label and c.response_hash
            and set(c.section_ids) == sections and set(c.physical_pages) == pages
            for c in review.calls))

    initial = valid_call("review_profile_workpaper")
    final = valid_call("verify_profile_workpaper_repair")
    legacy_unchanged = False
    retained_initial = bool(review and any(c.purpose == "review_profile_workpaper"
                            and c.request_id in review.retained_request_ids for c in review.calls))
    if review and initial and review.checked_fact_hashes is None and not retained_initial:
        # An unchanged old draft can still prove its exact reviewed input.
        before_review = paper.model_copy(update={"quality_review": None, "use_review": None})
        legacy_unchanged = hashlib.sha256(before_review.model_dump_json().encode()).hexdigest() == review.original_draft_hash
    checked = Counter(review.checked_fact_ids) if review else {}
    decisions = {d.fact_id: d for d in paper.use_review.decisions} if paper.use_review else {}
    facts = []
    counts = dict.fromkeys(DELIVERY_LABELS, 0)
    for element, group in workpaper_elements(paper).items():
        for fact in group.facts:
            fid = fact.fact_id
            decision = decisions.get(fid)
            state, basis, reasons = "pending", "not_reviewed", []
            model_stage = None
            if decision is not None:
                state = "reviewed" if decision.disposition == "source_checked" else decision.disposition
                basis = "independent_source_check"
                reasons.append(decision.reason)
            elif review:
                unresolved = [f for f in review.findings if f.affected_fact_id == fid
                              and f.original_fact is not None and f.resolution != "corrected_pending_trust"]
                corrected = [f for f in review.findings if f.resolution == "corrected_pending_trust"
                             and f.revised_fact is not None and f.revised_fact.fact_id == fid]
                if unresolved:
                    state = "withheld"
                    reasons.extend(f.explanation for f in unresolved)
                elif corrected:
                    if final and any(workpaper_fact_hash(f.revised_fact) == workpaper_fact_hash(fact) for f in corrected):
                        state, basis, model_stage = "reviewed", "model_source_check", "final"
                    else:
                        reasons.append("修订尚无匹配当前内容的有效最终核验。")
                elif initial and checked.get(fid) == 1 and (
                    (review.checked_fact_hashes or {}).get(fid) == workpaper_fact_hash(fact) or legacy_unchanged
                ):
                    state, basis, model_stage = "reviewed", "model_source_check", "initial"
                else:
                    reasons.append("未有匹配当前内容的有效核验；旧已核编号不自动继承。")
            else:
                reasons.append("尚未源复核。")
            if state in {"reviewed", "pending"} and (
                (decision is not None and decision.limitation) or fact.uncertainty_reason or fact.subject_scope == "unclear"
                or any(m.uncertainty_reason or m.value is None or m.qualifier != "exact" for m in fact.metrics)
            ):
                state = "limited"
                reasons.append((decision.limitation if decision else None) or fact.uncertainty_reason
                               or "保留原文主体、数值或近似限定，不作为确定值使用。")
            counts[state] += 1
            excluded = state in {"source_only", "withheld", "duplicate"}
            basis_label = {"independent_source_check": "独立源核", "model_source_check": "模型源核"}.get(basis)
            if state == "limited" and basis == "not_reviewed":
                basis_label = "待核"
            label = DELIVERY_LABELS[state] + (f"／{basis_label}" if basis_label else "")
            facts.append({
                "fact_id": fid, "element": element, "state": state,
                "label": label, "review_basis": basis, "model_review_stage": model_stage,
                "reasons": reasons, "evidence_ids": list(fact.evidence_ids),
                # Excluded values stay in raw workpaper audit, not normal delivery.
                "fact": None if excluded else fact.model_dump(mode="json"),
            })
    pending_review_count = sum(e["fact"] is not None and e["review_basis"] == "not_reviewed" for e in facts)
    return {"policy_version": "company_profile_research_delivery.v1", "research_only": True,
            "formal_acceptance": "not_assessed", "production_authorization": "not_authorized",
            "counts": counts, "labels": dict(DELIVERY_LABELS), "pending_review_count": pending_review_count, "facts": facts}
