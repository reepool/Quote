## 1. Storage And Selection Policy

- [x] 1.1 Add additive durable business-profile work-item storage, indexes, leases, retries, supersession, and queue-health queries
- [x] 1.2 Implement idempotent latest-active-annual enqueue and explicit scoped document enqueue for manual backfill
- [x] 1.3 Change automatic disclosure planning to latest-annual-only while preserving corrections and historical immutable artifacts

## 2. Discovery And Async Workers

- [x] 2.1 Add resumable split date-window state for page-bound market discovery and incomplete-window reporting
- [x] 2.2 Implement bounded asynchronous stage workers with independent concurrency, time/item budgets, checkpoint reuse, retry, and backpressure
- [x] 2.3 Implement the daily discovery-first orchestration and manual scoped backfill service on the same durable queues

## 3. Scheduler And Configuration

- [x] 3.1 Add DataManager entry points for daily incremental production and manual backfill without blocking the event loop
- [x] 3.2 Replace legacy automatic business-profile scheduler task methods/configuration with one daily task and one manual-only backfill task
- [x] 3.3 Update research production-operation configuration and runbook for latest-annual scope, queue budgets, peak season, rollback, and manual exceptions

## 4. Verification

- [x] 4.1 Add storage and service tests for deduplication, correction supersession, leases, retries, latest-annual filtering, specialist backfill, and queue health
- [x] 4.2 Add discovery tests for partial-window splitting and tests proving backlog/failure does not block later discovery or independent work
- [x] 4.3 Add scheduler/DataManager tests for the consolidated daily and manual-only tasks and removal of legacy schedules
- [x] 4.4 Run focused business-profile and scheduler tests, strict OpenSpec validation, static checks, rollout/read-only validation, and review the complete uncommitted diff
