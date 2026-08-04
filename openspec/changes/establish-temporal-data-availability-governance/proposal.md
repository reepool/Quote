## Why

Source-driven market and research data currently mix observation dates, planned release dates, actual publication dates, and local availability dates. This caused the NBS ten-day commodity task to report a false blocker before the official release time and leaves historical commodity consumers vulnerable to point-in-time leakage when they filter only by `observation_date`.

## What Changes

- Introduce a shared temporal data-availability contract that models observation periods, expected release timestamps, grace windows, actual publication timestamps, local first-seen timestamps, availability timestamps, evidence quality, and lifecycle status.
- Separate release planning from provider fetching so source adapters receive due observation periods instead of independently deciding when data should exist.
- Add an official schedule-aware NBS ten-day release rule using the 4th, 14th, and 24th publication convention, timezone-aware cutoffs, and configured cancellation or rescheduling evidence.
- Extend special-commodity publication-calendar persistence to store planned and actual availability semantics while preserving existing rows and APIs.
- Require special-commodity point-in-time reads and DCF inputs to filter by governed `available_at`, not only by `observation_date`.
- Distinguish legal not-due windows, due-but-waiting grace periods, delayed publications, cancellations, unresolved gaps, and source failures in governance and task reporting.

## Capabilities

### New Capabilities

- `temporal-data-availability-governance`: Shared contracts, status transitions, release planning, evidence quality, and point-in-time eligibility for source-published observations.

### Modified Capabilities

- `special-commodity-market-data`: Special-commodity publication calendars and consumers must use schedule-aware release planning and governed availability cutoffs.

## Impact

- Affects `research/special_commodity_market_data.py`, special-commodity configuration, DCF commodity context assembly in `data_manager.py`, and focused unit tests.
- Adds a reusable research-layer temporal-governance module without changing existing stock or futures trading-calendar storage.
- Extends the local `commodity_publication_calendar` schema additively; existing observation and calendar rows remain readable.
- No new third-party dependency is required.
