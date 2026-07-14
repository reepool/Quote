## 1. Baseline Audit And Field Contracts

- [x] 1.0 Review `docs/development/incremental_sync_change_watermarks_requirements.md` and confirm OpenSpec proposal/design/specs remain aligned before implementation.
- [x] 1.1 Inventory all enabled daily/scheduled write jobs in `config/05_scheduler.json` and classify each as quote, factor, futures, FX, commodity, research, governance, policy, or read-only.
- [x] 1.2 Define canonical semantic hash fields for `daily_quotes`, `adjustment_factors`, futures bars, FX observations, commodity observations, financial facts, industry memberships, valuation outputs, and policy events.
- [x] 1.3 Document which operational fields are excluded from hashes, including `updated_at`, batch ids, ingestion run ids, retry metadata, and report-only diagnostics.
- [x] 1.4 Identify existing hash/version/ingestion metadata in research, futures, FX, and commodity storage paths to reuse before adding new columns.
- [x] 1.5 Produce a P0/P1/P2/P3 rollout matrix that maps each domain to owner module, database, business key, hash source, write path, API surface, and test coverage.

## 2. Storage Foundation

- [x] 2.1 Add non-destructive migrations for quote-database changelog storage and missing row hash or row version fields.
- [x] 2.2 Add reusable changelog record models or payload helpers with domain, dataset, business key, sequence id, old/new hash, row version, source, run id, and changed timestamp.
- [x] 2.3 Add storage indexes for changelog lookup by sequence id, domain, dataset, instrument or series id, and observation date.
- [x] 2.4 Add unit tests proving migrations preserve existing historical rows and support empty databases.

## 3. Quote And Adjustment Factor Write Paths

- [x] 3.1 Update `save_daily_data` / `save_daily_quotes` to compare semantic hashes before upsert and classify inserted, changed, and unchanged rows.
- [x] 3.2 Ensure unchanged overlap-window quote rows do not refresh semantic row version or append changelog records.
- [x] 3.3 Update adjustment-factor upserts to compare factor semantics and emit separate factor-domain change records.
- [x] 3.4 Return structured write counters from quote and factor save operations without breaking current callers that only need success/failure.
- [x] 3.5 Add focused tests for quote insert, unchanged duplicate, material OHLCV change, metadata-only change, and factor restatement.

## 4. Quote API Watermark Surfaces

- [x] 4.1 Add response models for changelog records, pagination metadata, and latest watermark metadata.
- [x] 4.2 Add read-only quote/factor change query endpoints or route helpers with `since_sequence`, domain/dataset filters, limit, and stable ascending order.
- [x] 4.3 Ensure `/api/v1/quotes/daily` default behavior and response schema remain unchanged.
- [x] 4.4 Document that raw quote changes and adjustment-factor changes have different effects on `adjust=none` versus qfq/hfq consumers.
- [x] 4.5 Add API tests for empty changelog, paginated changes, domain filtering, and no default quote API regression.
- [x] 4.6 Add latest-watermark tests for no rows, one domain, multiple domains, and invalid `since_sequence` inputs.

## 5. Scheduler And Reporting

- [x] 5.1 Thread inserted, changed, unchanged, skipped, and changelog-written counters through daily quote update reports.
- [x] 5.2 Preserve existing task lifecycle, dependency execution, active-task cleanup, and report-delivery timeout behavior.
- [x] 5.3 Add scheduler tests showing overlap-window unchanged rows appear in reports without advancing watermarks.
- [x] 5.4 Update Telegram/report formatters to show changelog counters compactly and omit zero-noise sections where appropriate.

## 6. Backfill, Repair, Calendar, And Master Governance Integration

- [x] 6.1 Ensure range backfill and gap repair use the same quote/factor changelog-enabled write path.
- [x] 6.2 Verify repair universe lifecycle filtering and operator override semantics remain unchanged.
- [x] 6.3 Define whether trading-calendar and master-governance changes emit governance-domain records in phase one or are documented as deferred.
- [x] 6.4 Add tests that historical repair of a changed row emits a changelog record while lifecycle-skipped targets do not appear as source failures.

## 7. Futures, FX, And Commodity Domain Integration

- [x] 7.1 Adapt futures bar and continuous-series hash-aware write paths to emit shared changelog records for inserted and changed rows.
- [x] 7.2 Adapt FX direct and derived observation writes to emit changelog records with lineage or revision metadata.
- [x] 7.3 Adapt special commodity daily/monthly observations to emit changelog records and keep dry runs non-persistent.
- [x] 7.4 Keep commodity policy discovery and promotion changes isolated from price-observation changes.
- [x] 7.5 Add domain tests for changed, unchanged, dry-run, and policy-isolation scenarios.

## 8. Research Domain Integration

- [x] 8.1 Map shareholder incremental/reconciliation writes to shareholder-domain changelog records using existing payload hashes and ingestion runs.
- [x] 8.2 Map financial disclosure/fact writes to financial-domain changelog records using report period, fact identity, source profile, and mapping/parser version.
- [x] 8.3 Map industry taxonomy and membership writes to industry-domain changelog records with taxonomy version and effective date metadata.
- [x] 8.4 Map valuation input/history, technical, risk, and risk-free-rate writes to derived or observation-domain changelog records with calculation/input lineage.
- [x] 8.5 Mark read-only, diagnostics-only, or disabled research tasks with explicit no-changelog diagnostics.
- [x] 8.6 Add tests that existing `/api/v1/research/*` read APIs are unchanged when no changelog parameters are used.

## 9. Operational Backfill And Rollout Controls

- [x] 9.1 Add optional bounded hash backfill tooling for existing rows, with dry-run counts and no forced full-history rewrite.
- [x] 9.2 Add configuration flags to enable/disable changelog emission per domain during rollout.
- [x] 9.3 Add validation or health checks for latest watermark, changelog growth, and domain emission status.
- [x] 9.4 Add rollback notes showing how to disable noisy domain emission without affecting source tables.
- [x] 9.5 Define retention and compaction policy placeholders, with explicit warning that detailed changelog rows must not be pruned before consumer checkpoint policy is documented.

## 10. Documentation And Validation

- [x] 10.1 Update API documentation with watermark semantics, pagination, domain filters, and local-observed CDC limitations.
- [x] 10.2 Update scheduler/operator docs explaining why overlap windows and reconciliation remain required.
- [x] 10.3 Update development docs with semantic hash field definitions and derived-data lineage policy.
- [x] 10.4 Run focused unit tests for quote/factor changelog, API routes, scheduler reports, futures, FX, commodity, and research integrations.
- [x] 10.5 Run OpenSpec validation/status checks and record any unresolved risks or deferred phase-two items.
- [x] 10.6 Re-review the standalone requirements document against implemented behavior and update it with final route names, config keys, and rollout status before marking the change complete.
