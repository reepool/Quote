## Context

The authoritative post-stability replay completed all twenty reports, but its immutable source review found two critical and seven noncritical errors. The existing bounded workflow already forbids contradicted regime closure, top-five aggregate Relationships, non-owner legal-empty Evidence, and cost-component Segment identities; the remaining gap is enforcement on the exact shapes observed in the replay.

The authoritative replay, review package, outcomes, and comparison stay immutable. This change closes the defects provider-free and does not spend another cohort replay or alter provider parameters.

## Goals / Non-Goals

**Goals:**

- Reject a `business_regime` legal-empty result when the same Evidence explicitly reports a consolidation-scope change but omits the corresponding event.
- Reject top-five aggregate counterparty identities without suppressing valid named/anonymous ranked facts or independently disclosed aggregate related-party identities.
- Reject cost-component labels such as `原材料及燃动费` as Segment identities.
- Bind legal-empty coverage to the planner's chapter-owning `candidate_pages` rather than adjacent context pages, and reject explicit context-only citations.
- Make the nine reviewed errors reproducible and closed through provider-free tests and a hash-bound audit.

**Non-Goals:**

- No new semantic fields, confidence platform, provider route, token/timeout/model changes, or twenty-report replay.
- No mutation or reinterpretation of the authoritative replay.
- No Stage 6, approved writer, scheduler/backfill, commodity exposure, value-chain, or DCF enablement.

## Decisions

1. **Enforce semantic safety in existing normalization/application owners.** Business-regime contradiction remains in `stage5_provider` normalization, where an invalid provider response can use the existing bounded repair/failure path. Aggregate counterparty suppression remains in `stage5_service` immediately after verification and before projection. This avoids prompt-only correctness and does not create a second semantic loop.

2. **Use closed source shapes rather than broad identity bans.** A Relationship is blocked when its source-native/object label denotes a top-five customer/supplier aggregate. `identity_class=report_local_aggregate` remains valid for independently disclosed identities such as `集团所属单位`.

3. **Use planner-owned pages for legal-empty Evidence.** `candidate_pages` already identifies pages that passed chapter-owner scoring; adjacent pages exist only for bounded context. Missing coverage Evidence is filled from owning pages, and explicit context-only Evidence is rejected. Manual/non-shadow scopes with no `candidate_pages` retain their existing all-scope behavior.

4. **Treat pure consolidation-no-change text conservatively.** It may establish that the control-scope subquestion had no change, but it cannot by itself close the broader business-regime field as `not_disclosed` or `not_applicable`; the result must remain `unclear` unless the Evidence also contains the governed business/product/service no-change disclosure.

5. **Verify from frozen reviewed cases without provider calls.** Tests use the reviewed source shapes and the closure audit binds the archived review inputs by hash. A passing audit proves only that the observed defects are closed, not that scale readiness or 99% precision has been achieved.

## Risks / Trade-offs

- **[More conservative regime outcomes]** Some reports may remain `unclear` when they disclose only that consolidation scope did not change. → This is preferable to a false broad legal-empty conclusion and remains usable under the existing restricted research policy.
- **[Owner-page enforcement may expose weak plans]** A scope whose only apparent owner is an incidental page will now fail before acceptance. → Preserve the typed failure and fix Evidence planning in a later bounded validation rather than accepting unsupported coverage.
- **[Aggregate-label grammar can overmatch]** Broad text matching could block a legitimate independent aggregate identity. → Limit matching to top-five customer/supplier aggregate wording and keep the existing independent aggregate test.
- **[Provider-free closure is not empirical precision]** The next cohort may expose new shapes. → Do not change readiness or production authorization in this change; a later controlled validation must measure the effect.
