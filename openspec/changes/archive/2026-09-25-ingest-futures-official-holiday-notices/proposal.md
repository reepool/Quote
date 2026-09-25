## Why

Domestic futures daily sync treats a weekday with no official daily file as unresolved. Scheduled exchange holidays such as the 2026 Mid-Autumn closure on 2026-09-25 therefore block the calendar preflight for every exchange and stop price sync for real trading days still inside the repair window. The holiday notices already exist; the daily path never ingests them.

## What Changes

- Refresh each configured exchange's official annual holiday notice inside the existing official calendar backfill, before any daily-file probe.
- Parse full-day closure ranges into governed closed calendar rows, and record night-session suspensions without marking that date closed.
- Skip daily probes and price fetches for dates already closed by an official holiday notice. Weekend closure stays, but an official notice that names a weekend as a trading day overrides it.
- Keep post-cutoff empty or 404 responses unresolved when no official notice covers that date. A later daily file that contradicts a notice goes to review and does not silently rewrite the calendar.
- Leave AkShare/Sina calendars and the GFEX contract-event calendar out of this decision.

## Capabilities

### New Capabilities

- None. Holiday evidence is part of the existing trading-day governance and calendar backfill path.

### Modified Capabilities

- `futures-trading-day-governance`: official holiday notices become the forward source for full-exchange closures and night-session suspensions, and govern which dates are target trading dates.
- `futures-official-trading-calendar-backfill`: the existing backfill refreshes those notices before probing daily files, skips notice-closed dates, and preserves the current unresolved behavior for uncovered weekdays.
- `futures-market-data`: production sync uses the governed dates from that preflight and does not request prices for notice-closed dates.

## Impact

- Owner stays `FuturesOfficialCalendarBackfillService` plus `FuturesTradingDayGovernanceService` and `OfficialFuturesCalendarProvider`. No new scheduler job, no parallel calendar writer.
- Config: `config/11_futures.json` `trading_day_governance` notice sources for SHFE, INE, DCE, CZCE, and GFEX.
- Storage: existing `futures_calendar_notices` and `futures_trading_calendar` in `data/futures.db`.
- Daily job `futures_market_data_sync` keeps its current preflight call. `skip_trading_calendar_backfill` still skips the refresh and uses already stored notice rows.
- Tests and the current futures calendar requirement notes. Production job id, schedule, and price-bar schema do not change.
