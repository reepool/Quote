## 1. Predecessor Watermark Closure

- [x] 1.1 Trace the normal A-share quote/composite update completion path and persist successful per-exchange `a_share_quote_baostock_sina` watermarks.
- [x] 1.2 Add bounded compatibility verification for missing pre-existing watermarks using local quote and BaoStock/Sina composite coverage.
- [x] 1.3 Add unit tests for fresh, missing-but-verifiable, missing-unverifiable, and stale predecessor states.

## 2. Canonical Retry Governance

- [x] 2.1 Separate workflow-level canonical predecessor deferral from actionable instrument factor retry state.
- [x] 2.2 Ensure successful targeted canonical merge clears stale retry markers in scope and genuine factor/selection/write failures remain queued.
- [x] 2.3 Add regression coverage for the observed 115-instrument false retry expansion.

## 3. Announcement Classification

- [x] 3.1 Add deterministic exclusions for private-placement assistance/compensation disclaimers and ordinary repurchase or restricted-share cancellation notices.
- [x] 3.2 Preserve positive classification for genuine equity/profit distribution implementation notices.
- [x] 3.3 Add regression tests for `301588.SZ`, `688303.SH`, and representative genuine implementation titles.

## 4. BSE and Reporting Semantics

- [x] 4.1 Verify and test that a complete zero-record BSE recent window remains successful and non-blocking.
- [x] 4.2 Add canonical blocker, predecessor readiness, actionable retry count, and bounded unmatched-announcement samples to the daily result and scheduler report.
- [x] 4.3 Add report-format tests for success-empty BSE, predecessor deferral, semantic deferral, and successful canonical merge.

## 5. Verification and Delivery

- [x] 5.1 Run focused corporate-action daily, factor rebuild, BSE, scheduler report, and database-operation unit tests.
- [x] 5.2 Validate the OpenSpec change and mark all completed tasks.
- [x] 5.3 Review all uncommitted changes, fix confirmed defects, and re-run focused tests.
- [x] 5.4 Commit and push only the files created or modified for this change.

## 6. Persisted Announcement Policy Upgrade

- [x] 6.1 Reclassify persisted special-announcement queues with the active title policy and remove obsolete non-XDXR carryovers without clearing unrelated deferred reasons.
- [x] 6.2 Persist the active title-policy version and expose bounded carryover revalidation counts.
- [x] 6.3 Add regression coverage for legacy private-placement disclaimers, retained exceptional notices, missing-title fail-closed behavior, and current policy metadata.
- [x] 6.4 Run focused tests, validate the amended OpenSpec, review the diff, and commit and push only this follow-up change.
