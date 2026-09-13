## 0. Freeze legacy production and establish baseline

- [x] 0.1 Disable the legacy rollout, manual backfill, and semantic-production switches; verify no running legacy business-profile worker remains and record the freeze date/reason.
- [x] 0.2 Preserve official announcement/PDF acquisition, immutable evidence, read-only audit, status, and cooperative-stop paths; verify stage 0 does not delete production rows.
- [x] 0.3 Execute the real Telegram `/run business_profile_backfill ...` entry shape against the active configuration and verify it returns `任务已禁用` before `_execute_task_direct`, LLM access, or any legacy semantic write.

## 1. Publish the authoritative product requirements

- [x] 1.1 Publish `company_profile_product_and_industry_semantic_requirements.md` as the sole current requirements document and update `docs/README.md` and `docs/development/README.md`.
- [x] 1.2 Mark superseded company-profile requirements, benchmark material, and the old semantic-production runbook as historical/frozen with an explicit pointer to the new master document.
- [x] 1.3 Record the seven known `300750.SZ` legacy semantic conflicts as appendix examples, including their correct revenue/sales/inventory/production meanings and the decision not to choose a legacy canonical row.
- [x] 1.4 Add and review the consistency matrix linking product scope, object model, completeness, source/chapter rules, subject/unit semantics, regime packages, LLM contracts, legacy reset, and tests.
- [x] 1.5 Incorporate the final contract clarifications: package-by-chapter checklist scope, `metric_type` to `logical_slot` mapping, conditional capacity, operator-approved first package manifest, isolated new-contract storage, aggregator cross-check-only fallback, frozen legacy wording, and deferred `ValueChainRole`/`CommodityExposure` writers.

## Follow-up change map (outside this change)

The following work is intentionally not an implementation task of this contract-only change. Each item requires a separately reviewed OpenSpec change before code or production data is modified:

1. `research-manufacturing-materials-profile-package`: create the industry-document template, sample/annotation protocol, manufacturing/materials commonality study, chapter map, field checklist, prompts, and benchmark.
2. `implement-company-profile-common-semantic-model`: implement only the common objects and strict extract/repair/verify contract accepted by the industry study; keep role/exposure writers deferred.
3. `slice-manufacturing-materials-profile`: deliver the isolated multi-report manufacturing/materials vertical slice and researcher acceptance.
4. `reset-legacy-business-profile-semantics`: inventory and dry-run an explicit deletion manifest, preserve official documents/raw evidence, then physically reset old-contract semantic data only after the new slice passes.
5. One independent change per additional industry package and business-regime transition set; do not enable finance, healthcare, TMT, mining, utilities, consumer, or services fields from this change.
6. A separate controlled-production-recovery change to reopen bounded backfill only after the approved package, benchmark, vertical slice, cleanup, and reset gates pass.
