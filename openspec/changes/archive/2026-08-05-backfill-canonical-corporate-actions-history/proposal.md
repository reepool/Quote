## Why

The repository already retains 56,802 historical corporate-action observations and the governed evidence needed to project consumer-safe canonical events, but the canonical read tables are empty. Historical backtests therefore cannot consume events even though acquisition and evidence governance are already in place.

## What Changes

- Execute a bounded, dry-run-first historical projection from existing CNInfo/TDX observations into the append-only canonical corporate-action revision table and current compatibility projection.
- Preserve raw observations, resolved terms, effective-date evidence, coverage states, and existing factor decisions without mutation.
- Add explicit historical projection scope, deterministic ordering, idempotent rerun behavior, and operator-visible counts for ready and blocked events.
- Validate the projection in a temporary database before production writes, then execute production writes in resumable batches with a database-scoped watermark.
- Keep events with missing effective dates, incomplete terms, source conflicts, or incomplete acquisition coverage blocked instead of fabricating backtest facts.
- Expose the projection through the existing canonical corporate-action read contract after readiness and PIT checks pass.

## Capabilities

### New Capabilities

- `canonical-corporate-action-history-backfill`: Historical projection, readiness gates, lineage, idempotency, and resumable operator execution over existing corporate-action evidence.

### Modified Capabilities

None. Scheduler and watermark behavior required by this change is specified as
part of the new capability; existing scheduler and change-record contracts are
not otherwise changed.

## Impact

- Affected code: `research/backtest_data/corporate_action_projection.py`, `research/backtest_data/quote_store.py`, scheduler task registration and reporting, and related tests.
- Affected storage: `quotes.db` canonical corporate-action revision/current tables and their existing watermark domain.
- Affected APIs: existing canonical corporate-action pagination, readiness filtering, and `known_at` selection become populated for accepted historical events.
- No new provider, network download, credential, or BaoStock request is introduced.
