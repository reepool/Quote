## Why

Futures daily maintenance can currently treat an empty, not-yet-published exchange response as proof of a closed day, then select an older "latest" trading date and report success even though recent price bars remain missing. The DCE failures on 2026-08-12 and 2026-08-13 exposed that calendar evidence, publication timing, repair-window selection, and completion status must be governed together.

## What Changes

- Stop treating an empty official daily-market payload by itself as evidence that a weekday is closed; preserve it as unresolved unless weekend rules or official closure evidence confirm the date.
- Add configurable exchange publication cutoffs so intraday manual runs target the latest completed publication date and post-cutoff empty results become data-quality failures rather than closed days.
- Make scheduled futures maintenance re-probe a bounded rolling calendar window and include recent missing or stale trading dates in the sync target set, allowing later positive evidence to repair earlier calendar mistakes.
- Add an exchange-level freshness/completeness gate that compares expected governed trading dates with persisted price coverage before a run can report success.
- Report each exchange's requested range, governed target dates, expected latest date, actual latest price date, repaired gaps, and remaining blockers; dry-runs and partial exchange completion must retain truthful non-success status.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `futures-official-trading-calendar-backfill`: Tighten empty-response evidence semantics and add publication-aware classification and bounded recent-date repair.
- `futures-trading-day-governance`: Generate target dates from publication-aware calendars plus recent uncovered gaps instead of selecting only one latest stored trading day.
- `futures-market-data`: Require exchange-level price freshness and gap completeness before reporting a successful run.
- `scheduler`: Run bounded rolling calendar repair before daily futures sync and expose per-exchange target/freshness diagnostics with truthful task status.

## Impact

- Affects the official futures provider probe, calendar backfill service, trading-day governance, futures sync completion logic, scheduler/manual task orchestration, and Telegram reports.
- Adds narrowly scoped configuration for exchange publication cutoffs and rolling repair lookback; existing explicit `start/end` backfills remain compatible.
- Requires correction of previously stored weak `official_empty_payload` closed-day rows when later official rows prove the date traded.
- Adds focused unit and integration coverage around intraday empty payloads, DCE recovery, recent-gap repair, dry-run behavior, and partial/success reporting.
