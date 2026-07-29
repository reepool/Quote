## 1. Incremental Discovery Boundaries

- [x] 1.1 Add pure schedule-aware announcement-window resolution for calendar-daily and trading-day modes
- [x] 1.2 Add deterministic corporate-action title trigger classification and discovery diagnostics
- [x] 1.3 Wire complete cursor catch-up bounds and relevant-only deferred queue persistence into daily discovery

## 2. Completed Market Cutoff

- [x] 2.1 Add database support for fully covered local quote cutoffs by A-share exchange
- [x] 2.2 Cap daily factor rebuilding at the latest common completed quote date, persist complete retry sets, and expose the resolved cutoffs
- [x] 2.3 Exclude BSE observations from the CNInfo-derived factor path while retaining TDX BSE diagnostics

## 3. Readiness Semantics

- [x] 3.1 Add source-separated CNInfo, TDX reference, and reconciliation completeness summaries
- [x] 3.2 Update the daily result and Telegram report to use source-separated labels and counts
- [x] 3.3 Preserve legacy combined completeness and canonical quality gates for manual full-market evaluation

## 4. Configuration And Verification

- [x] 4.1 Configure the scheduled task for trading-day announcement coverage and a bounded complete catch-up
- [x] 4.2 Add unit tests for overnight and long-holiday windows, title filtering, cursor backlog, pre-market cutoff, BSE isolation, and readiness status
- [x] 4.3 Run focused tests and OpenSpec validation, then review the complete uncommitted change
