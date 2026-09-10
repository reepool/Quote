## Context

The immutable routing/continuation replay executed all twenty reports, but segment
financials produced 30 of 72 report blockers and 135 of 273 unresolved rows. In 25
high-cardinality scopes, every metric partition returned successfully and the local
adapter then rejected the merged draft. The current merge treats Evidence IDs and all
model-repeated metadata as row identity, although revenue, cost, and margin can be
reported on different physical pages for the same segment row. Cross-page tables also
place the dimension heading on an opening page while row values occur on continuation
pages.

The existing `CommonGatewaySemanticProvider`, compact segment-row schema, Stage 5
normalizer, shadow planner, and report-local service remain authoritative. The repair
must be local, provider-free, and compatible with the existing semantic objects.

## Goals / Non-Goals

**Goals:**

- Merge metric partitions by the source-backed `(dimension, label)` row anchor while
  retaining all approved row Evidence needed by the merged cells.
- Remove harmless model repetition from row identity without weakening subject,
  period, adjustment, or Evidence checks.
- Validate the table dimension against the complete governed scope and the label/value
  against the cited row Evidence, including cross-page continuation tables.
- Keep non-owner material out of segment completion and preserve explicit legal-empty
  owner disclosures.
- Retain the precise local merge/normalization failure in the surfaced provider error.
- Prove the behavior with existing-path, provider-free fixtures.

**Non-Goals:**

- No prompt, compact response schema, semantic object, model, output-token, deadline,
  repair, Gold, or readiness-threshold change.
- No LLM call, shadow replay, historical bundle mutation, or result splice.
- No generic owner framework, confidence platform, approved writer, Stage 6,
  scheduler/backfill, commodity exposure, value-chain publication, or DCF.

## Decisions

1. **Use `(dimension, label)` as the merge anchor and union approved Evidence.** Each
   metric partition already has exactly one allowed cell and cites Evidence from the
   same controlled scope. Different Evidence sets are therefore not a row conflict;
   the merged row keeps their stable union. Duplicate metric cells and Evidence outside
   the prepared scope remain errors. This is preferred to adding per-cell Evidence to
   the compact LLM schema because the current semantic model can validate the union and
   no model contract change is needed.

2. **Reconcile metadata by policy, not serialized equality.** Dimension and label are
   source-owned. For ordinary segment rows, the adapter continues to derive
   `business_segment` after source validation. Optional subject fields repeated by the
   model may be omitted or reconciled only when they do not assert a different explicit
   subject. Period fields may use the prepared report period when omitted, but
   conflicting non-empty periods, period types, row classes, or explicit subjects are
   rejected. Consolidation adjustments retain their existing group-wording or numeric-
   reconciliation requirements.

3. **Separate scope-level dimension proof from row-level proof.** A dimension heading
   may occur anywhere in the approved table scope, including the opening page. The row
   label must still occur in its cited Evidence, and the existing expanded Measurement
   validation must still find the source value/header in approved Evidence. This closes
   continuation tables without allowing unrelated context to establish a Segment.

4. **Keep owner rules narrow and source-bound.** Cost-component labels remain
   prohibited as Segment rows. Industry policy, audit references, and ordinary issuer
   income statements cannot establish segment-dimension legal empty. Explicit
   `只有一个报告分部`, governed segment/revenue-cost tables, and explicit not-applicable
   disclosures remain valid owners. Changes are made in the existing planner/provider
   helpers rather than a new routing service.

5. **Preserve the causal exception message.** Local partition merge and normalization
   errors remain classified as `candidate_schema_invalid`, but their bounded message
   includes the exact local reason. This keeps the typed external contract while making
   future audits distinguish identity, Evidence, owner, and schema failures.

## Risks / Trade-offs

- **[Unioned Evidence is broader than one metric cell]** → The union remains limited to
  the same source-backed row and approved scope; row label and metric source validation
  still run before acceptance.
- **[Metadata reconciliation hides a real semantic conflict]** → Only absent or
  policy-owned ordinary-row fields are normalized; conflicting explicit subject,
  period, period type, row class, or adjustment evidence remains blocking.
- **[Scope-level dimension matching admits incidental words]** → Require the governed
  segment/revenue-cost owner scope and preserve full-heading checks; row labels and
  values cannot be sourced from unrelated pages.
- **[Owner exclusions remove a real table]** → Fixtures retain positive controls for
  explicit multi-segment tables, one-segment disclosures, legal empty, and cost tables
  with an explicit top-level segment dimension.
