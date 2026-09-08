## Context

The immutable authority for the completed manufacturing/materials slice is `stage55-closure-four-20260907-a`. It completed all 43 extract and 43 verify calls and registered `research_slice_usable`, while its post-run benchmark truthfully retained Gold 14/24, nine failed annotations, and one contract conflict. Review of those non-passes identified a narrow set of normalization and adjudication issues rather than a reason to reopen extraction: annual duration facts can carry the report-end date, an explicit expected-completion year on continuation Evidence can be omitted from the qualifier, one source-supported business-segment subject is stricter than its Gold metadata permits, and one equity-transfer milestone uses a more source-specific event type than the Gold label.

The existing owners remain authoritative. `CompanyProfileSemanticService.run_task` owns validation and dispositions, the Stage 5 provider adapter owns compact model-output expansion, and the post-run benchmark owns Gold comparison. The change must not create another semantic loop, period service, event ontology, or confidence platform.

## Goals / Non-Goals

**Goals:**

- Make `period_type` determine whether `reported_period` represents a duration, instant, event, or expectation instead of treating every report-end date as interchangeable with an annual period.
- Preserve an explicitly disclosed expected completion or commissioning expression in `source_native.qualifier`, including when it is found on an approved continuation page.
- Add only the minimum closed Gold metadata/matcher rules needed to represent a source-supported non-group subject refinement and one explicitly adjudicated effective-milestone event equivalence.
- Re-evaluate the immutable closure bundle offline under a new benchmark identity and disclose all unchanged failures and conflicts.
- Keep the research slice and all accepted records non-production.

**Non-Goals:**

- Re-running any LLM scope or the complete four-report slice.
- Changing the Evidence plan, reading additional PDF pages, or adding PDF/OCR/table infrastructure.
- Rewriting runtime records or Gold source-native values to improve the score.
- Adding fuzzy event matching, broad aliases, probabilistic confidence, a new ontology, or a generalized time-normalization platform.
- Starting stage six, legacy reset/backfill, approved-table writes, CommodityExposure, ValueChainRole, DCF, scheduler, API, or Telegram integration.

## Decisions

### 1. Reuse `period_type` as the closed temporal discriminator

The adapter will normalize only records it constructs for the bounded company-profile workflow:

- `duration`: annual flow or rate facts use the disclosed year such as `2025`; a mechanically supplied report-end date is reduced to that year only when it is the same report year and no narrower source period is disclosed.
- `instant`: stock or balance facts retain the disclosed as-of date, such as an inventory amount or volume at `2025-12-31`.
- `event`: `reported_period` retains the occurrence period while `event_date` and `regime_effective_at` retain the exact evidenced date.
- `expected`: an expected future period is not substituted for the current reported fact's period; it remains a separate expected fact only when the existing object contract calls for one.

This uses the existing enum and avoids a parallel period model. The alternative of globally rewriting `reported_period` in the Pydantic model is rejected because it would mutate imported fixtures and historical records outside the Stage 5 adapter's ownership.

### 2. Keep expected completion in source-native qualifier

When the approved Evidence for a capacity row includes a continuation page or footnote that explicitly states an expected completion or commissioning year, the compact extract instruction and adapter preserve the exact expression in `source_native.qualifier`. The main capacity record keeps the report-period semantics and physical row anchor. No qualifier is created from project timing conventions, Gold, magnitude, or an Evidence page outside the prepared scope.

The alternative of assigning the expected year to `reported_period` is rejected because it conflates when the capacity was reported with when a project is expected to finish.

### 3. Add one bounded subject-strictness mode

Gold metadata may use `allow_supported_non_group_refinement` when Gold intentionally preserves `subject_scope=unclear` but runtime Evidence supports a narrower non-group scope: `business_segment`, `named_subsidiary`, or `issuer`. The matcher may return `accepted_with_uncertainty` only after the normal metric, object, value, period, and physical-anchor checks pass. It must not authorize `consolidated_group`; that scope continues to require the existing explicit or numeric-reconciliation basis.

This avoids changing the Gold expected subject merely to mirror runtime while preventing the mode from becoming a general subject waiver.

### 4. Event equivalence is an explicit directional contract decision

The benchmark will recognize the directional pair `major_asset_restructuring_effective` (Gold) and `equity_transfer` (runtime) only for the same Chengfei physical Evidence anchor when `event_date` and `regime_effective_at` both equal `2025-01-06` and the source states that the equity transfer was completed and entered the consolidation scope. It may then produce `semantic_match` with an auditable reason. The reverse direction, different anchors, project-launch events, commitments, and events without the same effective date remain failures.

A broad event-family alias table is rejected because it could collapse economically distinct milestones.

### 5. Offline evaluation never changes the authoritative bundle

Implementation verification will run unit fixtures and may create a new post-run evaluation artifact derived from `stage55-closure-four-20260907-a`. The authority bundle remains byte-for-byte unchanged. Only directly affected Gold outcomes may change; all missing facts, anchor mismatches, and the Chengfei sales `not_applicable` versus frozen `not_disclosed` conflict remain disclosed.

## Risks / Trade-offs

- **[Period normalization could erase a real as-of date]** → Normalize only `duration` records whose date is a mechanical report-end value for the same report year; keep `instant` records and explicit narrower periods unchanged.
- **[Qualifier extraction could become answer-shaped prompting]** → State only the classification and source-copy rule; never include the expected year or target numeric answer in the prompt.
- **[Subject refinement could waive unsupported promotion]** → Limit it to non-group scopes and require all other semantic and anchor checks to pass; consolidated scope remains governed by the existing subject basis.
- **[Event equivalence could become fuzzy taxonomy matching]** → Encode the single directional decision with exact anchor and effective-date conditions and reject all unlisted pairs.
- **[Benchmark improvement could be mistaken for production approval]** → Retain the original research usability status, publish changed and unchanged Gold outcomes, and keep `production_authorization=not_authorized` in every evaluation summary.
