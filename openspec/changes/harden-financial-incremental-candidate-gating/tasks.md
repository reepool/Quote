## 1. Candidate And Pending Lifecycle

- [x] 1.1 Exclude persisted accepted disclosure gaps from incremental candidate construction while preserving reconciliation coverage.
- [x] 1.2 Partition active and expired pending financial disclosure states using the fixed Shanghai-time retry deadline.
- [x] 1.3 Persist expired pending rows as `pending_recheck_expired` without resetting their original audit timestamps or retry horizon.
- [x] 1.4 Expose expired pending and eligible candidate-source counts in the incremental result.

## 2. Announcement Eligibility

- [x] 2.1 Extend non-primary financial title filtering for performance forecasts, pre-increase/pre-decrease notices, earnings previews, and equivalent result announcements.
- [x] 2.2 Preserve delayed-disclosure and delisting-risk exception handling and cover formal report titles outside the rolling display window.

## 3. Operational Health And Reporting

- [x] 3.1 Include repair source-routing diagnostics and unresolved pending work in incremental status derivation.
- [x] 3.2 Render expired pending counts and degraded official-source guidance in the Telegram financial maintenance report.

## 4. Verification And Documentation

- [x] 4.1 Add unit tests for accepted-gap exclusion, active/expired pending behavior, deadline preservation, and bounded candidate selection.
- [x] 4.2 Add announcement-filter tests for performance-result noise and delayed/risk exceptions.
- [x] 4.3 Add scheduler/service tests for degraded CNInfo routing, successful fallback writes, pending status, and report content.
- [x] 4.4 Update financial maintenance documentation with daily/reconciliation ownership, pending terminal status, and degraded source semantics.
- [x] 4.5 Run focused pytest suites and OpenSpec validation, then review the complete task diff and resolve confirmed findings.
