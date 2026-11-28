"""Fact-level research delivery from existing source/review records, not release approval."""
from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Mapping

from .workpaper import (
    CompanyProfileWorkpaper, WorkpaperUseReview, workpaper_elements, workpaper_fact_hash,
    WorkpaperMetricRestriction,
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
    metric_gaps = []
    subject_scope_gaps = []
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
                              and f.original_fact is not None and f.resolution not in {"corrected_pending_trust", "metrics_limited", "subject_scope_limited"}]
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
            restrictions = list(decision.metric_restrictions if decision is not None else ())
            subject_restriction = decision.subject_scope_restriction if decision is not None else None
            subject_limits = [finding for finding in (review.findings if review else ())
                              if finding.resolution == "subject_scope_limited" and finding.affected_fact_id == fid]
            subject_gap_source = {}
            if subject_limits:
                valid_subject_limits = (initial and checked.get(fid) == 1
                    and review.subject_scope_limit_policy == "source_review_exact_subject_limits.v1"
                    and all(f.subject_scope_limit is not None
                            and f.subject_scope_limit.fact_sha256 == workpaper_fact_hash(fact)
                            and f.element == element and set(f.evidence_ids) == set(fact.evidence_ids)
                            for f in subject_limits))
                if valid_subject_limits:
                    subject_gap_source = {
                        "review_basis": "independent_and_model_source_check" if subject_restriction else "model_source_check",
                        "model_finding_ids": [f.finding_id for f in subject_limits],
                        "model_reasons": [f.explanation for f in subject_limits],
                        **({"model_review_origin": "inherited_prior_review"}
                           if all(f.finding_id.startswith("inherited:") for f in subject_limits) else {}),
                    }
                    subject_restriction = "；".join(dict.fromkeys(filter(None, (
                        subject_restriction, *(f.explanation for f in subject_limits)))))
                    if decision is None:
                        basis, model_stage = "model_source_check", "initial"
                elif decision is None or decision.disposition == "source_checked":
                    state = "withheld"
                    reasons.append("主体局部核验缺少匹配当前内容/原源核回执的有效绑定，暂隔离整条。")
            if review and (review.metric_limit_policy == "source_review_exact_metric_limits.v1"
                           or review.subject_scope_limit_policy == "source_review_exact_subject_limits.v1" or subject_restriction):
                whole_defects = [finding for finding in review.findings if finding.affected_fact_id == fid
                                 and finding.original_fact is not None
                                 and finding.resolution not in {"corrected_pending_trust", "metrics_limited", "subject_scope_limited"}]
                if whole_defects and state not in {"withheld", "source_only", "duplicate"}:
                    state = "withheld"
                    reasons.extend(finding.explanation for finding in whole_defects)
            model_limits = [finding for finding in (review.findings if review else ())
                            if finding.resolution == "metrics_limited" and finding.affected_fact_id == fid]
            model_gap_sources = {}
            if model_limits:
                valid_limits = (initial and checked.get(fid) == 1
                    and review.metric_limit_policy == "source_review_exact_metric_limits.v1"
                    and all(finding.metric_limit is not None
                            and finding.metric_limit.fact_sha256 == workpaper_fact_hash(fact)
                            for finding in model_limits))
                if valid_limits:
                    blocked = {item.metric_index for item in restrictions}
                    for finding in model_limits:
                        for index in finding.metric_limit.metric_indices:
                            model_gap_sources.setdefault(index, []).append(finding)
                            if index not in blocked:
                                restrictions.append(WorkpaperMetricRestriction(metric_index=index, reason=finding.explanation))
                                blocked.add(index)
                    if decision is None:
                        basis, model_stage = "model_source_check", "initial"
                elif decision is None or decision.disposition == "source_checked":
                    state = "withheld"
                    reasons.append("指标局部核验缺少匹配当前内容/原源核回执的有效绑定，暂隔离整条。")
            if restrictions and state not in {"source_only", "withheld", "duplicate"}:
                state = "limited"
                reasons.append(("独立源核" if decision else "模型源核") + "仅支持原描述及未受限指标；部分原指标已隔离，不能视为完整数值覆盖。")
            excluded = state in {"source_only", "withheld", "duplicate"}
            payload = fact.model_dump(mode="json")
            component_metadata = {}
            if subject_restriction and not excluded:
                state = "limited"
                usage_restriction = "不得按事实主体作为合并口径加总或比较；指标须按各自主体及现有限制使用。"
                reasons.extend((subject_restriction, usage_restriction))
                payload["subject_scope"] = "unclear"
                payload["uncertainty_reason"] = "；".join(dict.fromkeys(
                    reason for reason in (fact.uncertainty_reason, subject_restriction) if reason))
                gap = {"field": "subject_scope", "state": "unclear", "reason": subject_restriction,
                       "consolidated_aggregation_allowed": False, "usage_restriction": usage_restriction,
                       **subject_gap_source}
                component_metadata["subject_scope_gap"] = gap
                subject_scope_gaps.append({"element": element, "fact_id": fid, "status": "unclear",
                    **gap, "evidence_ids": list(fact.evidence_ids),
                    "reasons": [subject_restriction, usage_restriction]})
            counts[state] += 1
            basis_label = {"independent_source_check": "独立源核", "model_source_check": "模型源核"}.get(basis)
            if state == "limited" and basis == "not_reviewed":
                basis_label = "待核"
            label = DELIVERY_LABELS[state] + (f"／{basis_label}" if basis_label else "")
            if restrictions and not excluded:
                blocked = {item.metric_index for item in restrictions}
                component_metadata.update({
                    "metric_gaps": [{"metric_index": item.metric_index, "name": fact.metrics[item.metric_index].name,
                                     "state": "withheld", "reason": item.reason,
                                     **({"review_basis": ("independent_and_model_source_check"
                                          if decision and any(r.metric_index == item.metric_index for r in decision.metric_restrictions)
                                          else "model_source_check"),
                                         "model_finding_ids": [f.finding_id for f in model_gap_sources[item.metric_index]],
                                         **({"model_review_origin": "inherited_prior_review"}
                                            if all(f.finding_id.startswith("inherited:") for f in model_gap_sources[item.metric_index]) else {}),
                                         "model_reasons": [f.explanation for f in model_gap_sources[item.metric_index]]}
                                        if item.metric_index in model_gap_sources else {})}
                                    for item in restrictions],
                    "metric_source_indices": [i for i in range(len(fact.metrics)) if i not in blocked],
                })
                payload["metrics"] = [item for i, item in enumerate(payload["metrics"]) if i not in blocked]
                metric_gaps.append({"element": element, "fact_id": fid, "status": "unclear",
                    "metric_indices": sorted(blocked),
                    "metric_names": [fact.metrics[i].name for i in sorted(blocked)],
                    "evidence_ids": list(fact.evidence_ids),
                    "reasons": [item.reason for item in restrictions],
                    **({"review_basis": "model_source_check" if not decision or not decision.metric_restrictions else "independent_and_model_source_check",
                        "model_finding_ids": sorted({f.finding_id for findings in model_gap_sources.values() for f in findings})}
                       if model_gap_sources else {}),
                    "suggested_reading": "建议定向精读该事实的原业务章节，核对指标主体、方向、期间和表头；不推断替代值。"})
            facts.append({
                "fact_id": fid, "element": element, "state": state,
                "label": label, "review_basis": basis, "model_review_stage": model_stage,
                "reasons": reasons, "evidence_ids": list(fact.evidence_ids),
                # Excluded values stay in raw workpaper audit, not normal delivery.
                "fact": None if excluded else payload,
                **component_metadata,
            })
    pending_review_count = sum(e["fact"] is not None and e["review_basis"] == "not_reviewed" for e in facts)
    information_gaps = [
        {"element": name, "status": element.status, "reasons": list(element.limitations)}
        for name, element in workpaper_elements(paper).items()
        if element.status != "observed" or element.limitations
    ]
    # Current-draft rechecks retain unresolved historical gaps locally, without
    # sending those opinions to the model or resurrecting their removed facts.
    information_gaps.extend({"element": finding.element, "fact_id": finding.affected_fact_id,
        "status": "unclear", "reasons": [finding.explanation],
        "review_basis": "inherited_model_source_check", "model_finding_ids": [finding.finding_id]}
        for finding in (review.findings if review else ())
        if finding.finding_id.startswith("inherited:") and finding.original_fact is None
        and finding.resolution != "corrected_pending_trust")
    suggested_acquisitions = [
        {"finding_id": finding.finding_id, "element": finding.element,
         "severity": finding.severity, "reason": finding.explanation,
         "evidence_ids": list(finding.evidence_ids)}
        for finding in (review.findings if review else ())
        if finding.original_fact is None and finding.resolution in {
            "omission_candidate", "repair_rejected", "verification_failed"}
    ]
    information_gaps.extend(metric_gaps)
    information_gaps.extend(subject_scope_gaps)
    suggested_acquisitions.extend({"element": gap["element"], "fact_id": gap["fact_id"],
        "metric_indices": gap["metric_indices"], "metric_names": gap["metric_names"], "severity": "material",
        "reason": gap["suggested_reading"] + " " + "；".join(gap["reasons"]),
        "evidence_ids": gap["evidence_ids"]} for gap in metric_gaps)
    suggested_acquisitions.extend({"element": gap["element"], "fact_id": gap["fact_id"],
        "field": "subject_scope", "severity": "material",
        "reason": "建议按需定向精读原业务章节，区分经营主体、统计范围与资产所属；不猜替代主体。 " + "；".join(gap["reasons"]),
        "evidence_ids": gap["evidence_ids"]} for gap in subject_scope_gaps)
    return {"policy_version": "company_profile_research_delivery.v1", "research_only": True,
            "formal_acceptance": "not_assessed", "production_authorization": "not_authorized",
            "information_gaps": information_gaps, "suggested_acquisitions": suggested_acquisitions,
            "counts": counts, "labels": dict(DELIVERY_LABELS), "pending_review_count": pending_review_count, "facts": facts}
