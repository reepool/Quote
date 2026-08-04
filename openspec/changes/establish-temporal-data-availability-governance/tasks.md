## 1. Shared Temporal Contract

- [x] 1.1 Add typed release-plan, evidence, exception, lifecycle-status, and point-in-time eligibility primitives in a reusable research module
- [x] 1.2 Add focused unit tests for timezone validation, due/grace/delay transitions, evidenced exceptions, source failures, and PIT eligibility

## 2. Commodity Calendar Persistence

- [x] 2.1 Extend `commodity_publication_calendar` with additive temporal availability columns and indexes
- [x] 2.2 Implement idempotent migration and calendar read/write compatibility for existing databases and legacy rows
- [x] 2.3 Add storage tests proving migration preserves rows and does not invent `available_at`

## 3. NBS Release Governance

- [x] 3.1 Add configuration and validation for the NBS 4th/14th/24th Asia/Shanghai release rule, grace window, and evidenced exceptions
- [x] 3.2 Replace the NBS fixed-lag eligibility rule with release plans and due-period selection
- [x] 3.3 Reconcile discovered observations, missing due periods, exceptions, and provider failures into governed lifecycle statuses and task diagnostics
- [x] 3.4 Add focused NBS tests covering lower-period next-month release, not-due/grace behavior, delayed availability, cancellations, rescheduling, and source failures

## 4. Point-in-Time Commodity Consumption

- [x] 4.1 Add an optional governed availability cutoff to commodity observation storage reads
- [x] 4.2 Update DCF commodity context selection to use valuation-time availability and report a dependency gap when governed data is unavailable
- [x] 4.3 Add storage and DCF tests proving post-cutoff and unknown-availability rows are excluded without remote fetching

## 5. Validation and Documentation

- [x] 5.1 Document the shared availability contract, NBS release policy, migration behavior, and FX/futures compatibility boundary
- [x] 5.2 Run targeted temporal, commodity, DCF, scheduler, and OpenSpec validation
