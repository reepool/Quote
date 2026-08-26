## 1. Integrity Contracts and Test Fixtures

- [x] 1.1 Add focused DataManager daily-update fixtures for valid target-date rows, incomplete/invalid existing rows, empty source responses, and successful writes.
- [x] 1.2 Add SourceFactory tests proving A-share stock short-window empty primary results try eligible backups and that all-empty results are not reported as covered.
- [x] 1.3 Add scheduler/calendar fixtures distinguishing an explicit closed calendar row, a missing calendar row, and DateUtils fallback success/failure.
- [x] 1.4 Extend daily update report assertions with unresolved-empty, quality-failure, stale-source, calendar-unknown, re-fetch, and legitimate-no-quote counters.

## 2. Daily Quote Coverage and Quality

- [x] 2.1 Add the minimum daily quote validation/preparation at the DataManager daily write boundary, rejecting missing fields, non-positive prices, negative values, and invalid OHLC relationships before `save_daily_quotes`.
- [x] 2.2 Preserve existing semantic upsert/changelog behavior while ensuring rejected or incomplete rows cannot default to `is_complete=True`.
- [x] 2.3 Replace date-only `should_update` skipping with a quality-aware target-date row check; re-fetch incomplete/invalid rows and keep valid complete rows idempotently skipped.
- [x] 2.4 Classify empty or zero-write instrument outcomes as unresolved/legitimate-no-quote instead of success, and prevent unresolved required coverage from advancing successful-through watermarks.
- [x] 2.5 Add bounded per-exchange diagnostics and scheduler success/warning/failure gating for the new daily outcome classes.

## 3. Calendar and Source Freshness

- [x] 3.1 Add an explicit calendar-coverage check at the scheduler/DB boundary so missing rows are distinguishable from confirmed non-trading rows.
- [x] 3.2 Apply DateUtils fallback only when it yields a reliable answer; otherwise report `calendar_unknown` and prevent a successful daily result for that exchange.
- [x] 3.3 Enable A-share stock end-date coverage validation in routing configuration and apply the existing stale-source circuit breaker to stock routes with an explicit threshold.
- [x] 3.4 Add tests for stale stock primary fallback, all-source stale/unavailable outcomes, and stock coverage configuration merging.

## 4. Corporate-Action Discovery and Regression Verification

- [x] 4.1 Expand ex-dividend discovery period derivation to cover the required quarterly report anchors and cross-year annual anchors with deduplication.
- [x] 4.2 Preserve partial/retry diagnostics when any required factor-discovery period fails or returns an unusable schema, and add tests for Q1/Q2/Q3/annual coverage.
- [x] 4.3 Run targeted unit tests for DataManager daily updates, SourceFactory routing, scheduler calendar/report behavior, database quote persistence, and daily factor discovery.
- [x] 4.4 Run an end-to-end mocked daily-update case covering valid update, valid existing skip, bad-row re-fetch, empty unresolved result, stale-source fallback, and calendar-unknown reporting; review the diff against the pre-existing worktree changes.
