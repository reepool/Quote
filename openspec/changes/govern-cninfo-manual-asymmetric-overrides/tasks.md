## 1. Manual asymmetric override

- [x] 1.1 Add validation and persistence for CNInfo manual asymmetric reviews, including supersession, total-share-capital terms, beneficiary-only terms, and factor-effect lineage.
- [x] 1.2 Expose factor-effect metadata through resolved-term reads and apply authoritative manual overrides without modifying raw observations.

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
