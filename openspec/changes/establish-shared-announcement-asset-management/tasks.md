## 1. Shared Catalog And Storage

- [x] 1.1 Persist source-qualified announcements, attachments, versions, blobs,
  effective annual reports, cursors, retries, and operations in SQLite.
- [x] 1.2 Store verified PDFs once by SHA-256 under
  `data/filings/announcements/blobs` and reuse identical bytes.
- [x] 1.3 Register verifiable existing annual-report files for local reuse.

## 2. Annual-Report Workflows

- [x] 2.1 Classify complete annual reports, exclude summaries/notices, and select
  the newest valid correction as the sole current asset per fiscal year.
- [x] 2.2 Implement latest-only active A-share bootstrap with resumable state.
- [x] 2.3 Implement independent cursor/overlap-based daily discovery and bounded
  attachment retry.

## 3. API Integration

- [x] 3.1 Provide local-first ensure/get/list/content APIs with optional authorized
  acquisition for missing reports.
- [x] 3.2 Keep business-profile and broker parsing outside the shared asset module
  and do not gate scheduler operation on consumer completion.

## 4. Scope Reduction And Acceptance

- [x] 4.1 Remove backup/restore jobs, configuration, evidence gates, and generated
  backup/test artifacts from this change.
- [x] 4.2 Run focused tests for local reuse, on-demand acquisition, correction
  selection, latest-only bootstrap, and daily discovery persistence.
- [x] 4.3 Remove legacy migration, consumer parser orchestration, backup/capacity
  code, obsolete documentation, and their dedicated tests.
- [x] 4.4 Reuse unchanged universe/census snapshots and coverage instead of
  writing full-market JSON and rows on every daily run.
- [x] 4.5 Clean generated backup/test data and redundant live snapshot rows, then
  complete one real bounded daily update without a failed or blocked operation.
