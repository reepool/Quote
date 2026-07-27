## 1. Manual asymmetric override

- [x] 1.1 Add validation and persistence for CNInfo manual asymmetric reviews, including supersession, total-share-capital terms, beneficiary-only terms, and factor-effect lineage.
- [x] 1.2 Expose factor-effect metadata through resolved-term reads and apply authoritative manual overrides without modifying raw observations.
- [x] 1.3 Allow an analysis-free review/date-evidence bundle only for unchanged current CNInfo terms with `factor_effect=normal`.

## 2. Factor isolation

- [x] 2.1 Exclude `factor_effect=none` events from CNInfo factor aggregation while reporting them as resolved no-effect events.
- [x] 2.2 Add regression tests for corrected non-null terms and recorded zero-factor events.

## 3. Announcement prefilter

- [x] 3.1 Add conservative deterministic title-prefilter helpers with implementation-title protection.
- [x] 3.2 Integrate prefiltering before title LLM classification and document resolution, persist rejected lineage, and expose report counts.
- [x] 3.3 Add tests for excluded document types, protected implementation notices, and no-LLM filtered titles.

## 4. Operator decisions and verification

- [x] 4.1 Apply the four explicit operator decisions from persisted CNInfo data and verify review supersession, corrected terms, dates, and factor effects.
- [x] 4.2 Run focused tests, OpenSpec validation, targeted database/factor checks, and review the complete uncommitted diff.
- [x] 4.3 Refresh governance state after a manual override and ensure the
  resolved operator decision supersedes stale date conflicts.
- [x] 4.4 Add and apply the `000623.SZ` CNInfo-only decision with the
  non-tradable-share contraction retained as descriptive non-factor lineage.
- [x] 4.5 Verify no raw CNInfo, TDX audit, or production-factor rows are changed
  by the review write, then run focused tests and OpenSpec validation.

## 5. Operator-approved CNInfo/TDX date alignment

- [x] 5.1 Add an exact event/TDX-row classifier that accepts operator-approved
  asymmetric economic differences only when the TDX date is a compatible
  exchange trading session.
- [x] 5.2 Persist a review/date-evidence bundle that keeps raw CNInfo economics,
  records TDX as date-only evidence, and writes no resolved-term overlay.
- [x] 5.3 Add a local preview/write script for the 15 reviewed conflicts and
  regression tests for source isolation, identity validation, and review
  supersession.
- [x] 5.4 Apply all 15 decisions and verify governance resolution plus zero
  mutation of raw CNInfo, TDX audit, and production factor rows.
- [x] 5.5 Run focused tests, OpenSpec validation, and uncommitted review.
