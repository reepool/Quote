## Context

The September 13 fresh-cohort batch exercised the existing Stage 5 owner on four unseen
manufacturing/materials reports. It retained 362 Evidence-bound accepted facts, but two
logical requests exhausted their route deadline and several chapters stayed incomplete
because of reproducible reconciliation, coverage, verifier, and Evidence-selection false
negatives. The failures occur in existing shared owners and therefore affect later unseen
reports; company-specific review patches would not provide the requested scalable path.

The authoritative execution chain remains the existing shadow operator into
`ManufacturingMaterialsShadowBatchService`, Stage 5 provider/service, common LLM gateway,
and research-only projection. The task-start LLM configuration, historical bundles, Gold,
production stores, and downstream publication paths are outside this change.

## Goals / Non-Goals

**Goals:**

- Give every eligible member of a bounded logical LLM route a meaningful share of the one
  existing request deadline.
- Preserve distinct annual/interim/instant segment rows while merging metric partitions
  that describe the same source row and period.
- Close coverage when accepted facts or owner-valid legal-empty Evidence satisfy the field,
  without synthesizing records or hiding substantive verifier failures.
- Stop `分部间抵销` from being rejected solely because its explicit adjustment label lacks
  a separate `合并` or `集团` token.
- Correct only the Evidence-owner source shapes observed as misses in the frozen cohort.
- Validate the combined fixes once on eight unseen, local-valid annual reports.

**Non-Goals:**

- No new retry loop, timeout extension, model ranking, confidence score, Evidence planner,
  semantic vocabulary, Gold expectation, or production datastore.
- No mutation or replay of historical four-report, OOS, shadow, or fresh-cohort bundles.
- No Stage 6, approved write, scheduler/backfill, CommodityExposure, ValueChainRole, or DCF.

## Decisions

### Share the remaining route deadline among eligible hops

Before each physical attempt the gateway will count the current source plus remaining
eligible route members within the configured hop budget. Non-final attempts receive no more
than an equal share of the remaining logical deadline, subject to the existing useful-attempt
minimum; the final eligible member may use the remainder. This preserves the single absolute
deadline and existing failure classifications while preventing the first source from taking
roughly 80% of a four-model request.

The alternative of increasing the logical timeout was rejected because it hides poor
failover allocation and increases batch latency. Independent per-provider deadlines were
rejected because they would multiply the caller's declared deadline.

### Make normalized period part of segment row identity

Each compact row is normalized before its merge key is formed. The key includes source
dimension, source row label, normalized `reported_period`, and `period_type`. Existing closed
annual aliases for the report year remain equivalent; different years, interim periods, and
instant dates remain distinct. Metadata and cells merge only within that identity.

The alternative of dropping period conflicts was rejected because it can combine different
physical facts. Treating every partition row as distinct was rejected because it would
duplicate the same source row across metric partitions.

### Derive coverage only from already accepted semantics or owner Evidence

The Stage 5 application owner will treat an accepted record for a requested field as observed
coverage even when a rejected supplemental representation exists, provided the rejection
does not expose an independent substantive defect. Legal-empty results remain dependent on
Evidence that owns the exact field. The owner tests will be expanded only for the real source
shapes found in the fresh cohort.

The alternative of making coverage follow provider prose was rejected because it would let
the model approve itself. Synthesizing missing records from coverage was rejected because it
would lose source-native semantics.

### Recognize the narrow Chinese adjustment label

`分部间抵销` and its orthographic `抵消` variant are explicit consolidation-adjustment row
identities when `row_class=consolidation_adjustment`. The adapter may normalize only the
adjustment subject metadata required by the existing verifier exception. Ordinary segment
rows and generic `公司` wording receive no promotion.

The alternative of broadening all subject inference was rejected because it would weaken an
existing critical guard.

### Validate once on a frozen eight-report cohort

Eight identities absent from prior company-profile validation inputs will be selected from
existing local-valid annual-report artifacts. Their six core chapter Evidence plans and
hashes will be frozen before provider access. After a read-only connectivity check, one new
immutable four-model batch will run; there will be no targeted reruns or in-batch tuning.

Eight reports double the latest fresh cohort while keeping source review bounded. A larger
sample was rejected for this fix-validation change because it would delay the business
feedback without changing the five explicit acceptance families.

## Risks / Trade-offs

- [Equal deadline shares may cut off a slow model that would eventually succeed] → The final
  eligible route member keeps the remaining budget, and all attempts retain typed lineage.
- [Period-aware identity may expose previously hidden duplicate rows] → Duplicate cells for
  the same normalized identity remain blocking and focused fixtures cover annual aliases and
  distinct periods.
- [Coverage relaxation could hide a bad candidate] → Only accepted-field satisfaction and
  owner-valid legal-empty results close coverage; independent Evidence, subject, value,
  period, provider, or contract failures remain visible blockers.
- [Adjustment handling could promote normal rows] → The exception requires the explicit
  narrow label plus the existing adjustment row class and applies only to Segment/Measurement.
- [Eight reports cannot prove all-industry readiness] → The audit reports only observed
  recurrence and research usability; production remains `not_authorized`.

## Migration Plan

1. Add provider-free regression fixtures for all five observed failure families.
2. Apply minimal changes in the existing gateway, provider/service, and Evidence selector.
3. Run focused and affected provider-free tests plus static checks.
4. Freeze eight unseen reports and six-chapter Evidence plans.
5. Run one immutable provider-bearing batch and complete source-bound review/audit.
6. If implementation or empirical gates fail, retain the exact typed result and close the
   change as `hold` or `failed`; rollback is the single code commit because no historical or
   production data is mutated.

## Open Questions

None. Empirical report status is intentionally determined only by the frozen validation run.
