## Why

The financial disclosure incremental task currently reports `degraded` whenever the official CNInfo routing step is incomplete, even when the configured Sina/THS fallback successfully writes complete local financial data. This makes an operational source downgrade look like a failed data collection run and obscures the actual source used by the resulting facts.

## What Changes

- Treat an incremental repair as successful when all selected targets are either written successfully or confirmed unchanged, regardless of whether CNInfo or a configured fallback supplied the facts.
- Keep official-source health and fallback usage as separate diagnostics instead of using them as the top-level completion status.
- Add an explicit source summary to the operator report identifying whether the run used CNInfo, fallback sources, or a mixture.
- Preserve degraded/failed status for unresolved blockers, write failures, pending unresolved work, or incomplete fallback collection.
- Add tests covering successful fallback completion, mixed official/fallback completion, and unresolved source failures.

## Capabilities

### New Capabilities

- `financial-sync-source-aware-reporting`: Source-aware completion status and operator reporting for financial disclosure synchronization.

### Modified Capabilities

- `financial-operations-scheduler`: Change incremental sync status semantics so successful fallback collection is reported as success while retaining source-health diagnostics.

## Impact

- `research/financial_disclosure_incremental_sync.py` status and source-routing metadata.
- `scheduler/tasks.py` Telegram/operator report rendering.
- Unit tests for incremental sync and scheduler financial reports.
- No database schema migration; existing run metadata remains backward compatible.
