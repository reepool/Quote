## Context

Wide segment tables are extracted in metric partitions and merged before full semantic
expansion. Ordinary segment rows are eventually resolved to `business_segment` from the
validated table dimension and label, but the current pre-merge normalizer instead keeps
some rows `unclear` while preserving other providers' report-default group draft. The
same physical row can therefore fail identity comparison before the common expansion
rule runs.

## Goals / Non-Goals

**Goals:**

- Apply the existing Evidence-backed `business_segment` resolution before partition row
  identity comparison.
- Treat `unclear`, `business_segment`, and
  `consolidated_group/report_default_group_scope` as equivalent only for normal segment
  rows whose dimension and label are already bound to controlled Evidence.
- Preserve explicit narrower subjects, affirmative group subjects, and consolidation
  adjustments as real identity boundaries.

**Non-Goals:**

- No provider call, canary rerun, historical-bundle mutation, subject-model expansion,
  prompt change, token change, or generalized reconciliation framework.
- No relaxation of dimension, label, Evidence, period, row-class, or duplicate-cell
  conflicts.

## Decisions

1. Normalize in `_normalize_segment_partition_metadata`, before rows enter
   `rows_by_anchor`. This is the narrowest existing owner and makes all partitions use
   the same representation before `_merge_segment_partition_metadata` compares them.
2. Canonicalize non-adjustment absent/unclear/business-segment scopes to
   `business_segment`. During pairwise metadata merge, collapse a report-default group
   draft only when the other partition is already segment-scoped. This preserves the
   default marker long enough for the existing stronger-basis rule to prefer a
   direct-source or numeric-reconciled group result.
3. Keep issuer, named-subsidiary, direct-source and numeric-reconciled consolidated-group
   rows unchanged. A real conflict against a segment-scoped row still fails closed.

## Risks / Trade-offs

- **[A provider incorrectly marks a normal row with the report default]** → Controlled
  dimension and label already prove the narrower segment row, so canonicalization is
  evidence-backed and auditable.
- **[An explicit total row is mistaken for a segment]** → Explicit consolidated basis is
  preserved; the report-default basis collapses only when paired with a segment scope.
- **[The immutable canary remains hold]** → This change proves the failure shape
  provider-free and does not rewrite or rerun the historical result.
