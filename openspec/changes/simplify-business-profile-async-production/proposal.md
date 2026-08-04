## Why

Business-profile production currently spreads discovery, semantic maintenance, and reconciliation across several periodic jobs while slow PDF and LLM work remains coupled to orchestration. This makes annual-report season difficult to drain without delaying fresh discovery, and it performs more disclosure analysis than the latest-annual operating policy requires.

## What Changes

- Replace the separate automatic daily, weekly, monthly, semiannual, and annual business-profile maintenance jobs with one bounded daily incremental job and one manual-only backfill job.
- Add durable, idempotent stage work for announcement discovery, latest-annual selection, PDF acquisition, deterministic parsing, LLM resolution, candidate persistence, and reconciliation.
- Make discovery checkpointed and independently resumable so download, parsing, or LLM backlog never blocks later discovery runs.
- Change the default automatic disclosure policy to each issuer's latest available annual report and its corrections or replacements; keep prior annual artifacts immutable for point-in-time use.
- Keep specialist disclosures, prospectuses, explicit historical periods, and forced reprocessing behind the manual backfill entry point.
- Add bounded worker budgets, leases, retries, coalescing, supersession, queue health, and peak-season backpressure controls.
- Keep parse and semantic computation concurrent while routing every business-profile SQLite transaction through one observable, cooperative writer gate.
- Expose downloaded annual-report PDFs as a reusable asset catalog backed by the existing immutable source-file manifest, and reuse a verified catalog hit before downloading.

## Capabilities

### New Capabilities

- `business-profile-async-production`: Defines the latest-annual evidence policy, durable stage queues, non-blocking daily orchestration, manual backfill, idempotency, retries, and production observability.

### Modified Capabilities

- `financial-operations-scheduler`: Replaces multiple automatic business-profile schedules with one daily incremental task and one manual-only backfill task.

## Impact

- Affected code: business-profile disclosure planning, production operations, semantic runtime orchestration, scheduler task registration, configuration, and focused tests.
- Affected storage: additive durable work-item and discovery-cursor state in `research.db`; existing immutable PDF, manifest, evidence, fact, role, exposure, and LLM audit tables remain authoritative. The annual-report catalog is a query projection over `financial_source_files`, not a second source of truth.
- Affected operations: old business-profile automatic reconciliation schedules are removed; their checks run incrementally inside the daily workflow, while historical and specialist scope is explicit manual work.
- No new provider, model SDK, or manual-review dependency is introduced. Existing feature switches remain disabled by default until production rollout gates are met.
