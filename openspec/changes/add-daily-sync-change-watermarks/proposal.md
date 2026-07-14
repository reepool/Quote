## Why

Current daily sync jobs rely on bounded lookback windows and periodic reconciliation to avoid missing late source updates, but callers have no precise signal for which local rows changed after their last sync. This creates unnecessary re-downloads and still cannot reliably communicate historical corrections, adjustment-factor restatements, or changed derived inputs across stocks, indexes, futures, FX, commodities, financial, industry, and valuation datasets.

Standalone requirements baseline: `docs/development/incremental_sync_change_watermarks_requirements.md`. This OpenSpec change is the implementation contract for that document.

## What Changes

- Add a local, append-only daily-sync change log with monotonically increasing watermarks for rows or logical observations changed by platform ingestion jobs.
- Add content-based change detection so idempotent overlap-window re-fetches do not create false positive changes.
- Expose read-only API/query contracts that let callers ask for changes after a prior watermark and then re-fetch only affected business keys.
- Cover all scheduled daily update domains in scope:
  - equity/index/ETF daily quotes and adjustment factors in the quote database
  - HK/US/A-share quote daily update variants
  - commodity futures daily bars and continuous-series observations
  - FX observations and derived FX observations
  - special commodity daily/monthly observations and policy/event source discoveries
  - research-domain financial disclosure, shareholder, industry, valuation input/history, risk-free-rate, technical/risk snapshots where daily or scheduled refreshes write local data
- Preserve current default APIs, scheduler behavior, gap repair, backfill, master governance, trading-calendar governance, policy discovery, and research read APIs unless callers explicitly use the new change-watermark surfaces.
- Keep existing lookback, catch-up, reconciliation, and repair jobs as correctness backstops; the change log reports what the local platform detected, not a guarantee that upstream free sources provide complete CDC.
- Treat adjustment-factor changes as first-class changes because they can invalidate qfq/hfq adjusted quote outputs even when raw quote rows are unchanged.
- Add structured reporting counters for inserted, changed, unchanged, skipped, and changelog-written rows so operators can verify that overlap windows are no longer noisy.
- Implement in phases:
  - P0: quote database daily quotes and adjustment factors, plus quote/factor read-only watermark API.
  - P1: futures, FX, and special commodity price observations using existing hash-aware paths.
  - P2: research-domain financial, shareholder, industry, valuation, technical, risk, and interest-rate writes.
  - P3: governance/policy domains and optional cross-database aggregation.

## Capabilities

### New Capabilities

- `daily-sync-change-watermarks`: Cross-domain local change-log and watermark contract for scheduled daily sync, backfill, reconciliation, and read-only API consumers.

### Modified Capabilities

- `quote-api-query-semantics`: Add read-only quote and adjustment-factor change-watermark query semantics without changing existing `/quotes/daily` defaults.
- `scheduler`: Require scheduled daily and reconciliation jobs to report changelog/write counters and preserve existing task lifecycle behavior.
- `data-storage-layout`: Add non-destructive storage metadata for row hashes, row versions, and append-only change records across relevant local databases.
- `research-data-engine`: Require research-domain daily writes and derived refreshes to either emit change records or explicitly declare why they are read-only/unchanged-only.
- `futures-market-data`: Align existing inserted/changed/unchanged hash-based write paths with the shared changelog and watermark contract.
- `fx-market-data`: Align FX observation and derived-observation writes with shared changelog and watermark semantics.
- `special-commodity-market-data`: Align commodity observations, monthly benchmark refreshes, and policy/event discovery writes with shared changelog semantics.

## Impact

- **API**: New read-only endpoints or query helpers for change listing and latest watermark; no change to existing endpoint defaults or response shapes.
- **Storage**: Non-destructive migrations adding row hash/version fields where missing and append-only changelog tables per database or a shared registry; no deletion or rewrite of historical observations during rollout.
- **Daily ingestion**: Quote writes need content comparison before upsert; existing futures/FX/commodity hash paths can be adapted with less churn.
- **Adjustment factors**: Factor upserts must emit separate changes and document their effect on adjusted quote consumers.
- **Backfill/repair**: Historical backfills and gap repairs must emit the same change records when they insert or materially change rows, but retain current operator-scoped date ranges and lifecycle filters.
- **Trading calendar and master governance**: Governance outputs may emit changelog records for lifecycle/calendar changes, but these records must not force quote re-fetch unless a downstream task explicitly consumes them.
- **Financial/industry/valuation**: Existing ingestion runs, hashes, `data_as_of`, and derived calculation metadata should be reused; derived outputs must record source watermarks or input hashes so downstream consumers can distinguish raw data changes from recalculation-only changes.
- **Policy discovery**: Policy candidate discovery and promotion can publish dataset/policy changes, but API consumers of market data should not be affected unless they subscribe to those domains.
- **Operations**: Reports and docs must make clear that free upstream sources still require periodic reconciliation because the platform can only report changes it has observed locally.
- **Documentation**: The standalone requirements document is the review baseline; proposal, design, specs, and tasks must remain aligned with it before implementation starts.
