## Context

`futures_market_data_sync` already runs `FuturesOfficialCalendarBackfillService` before trading-day governance and price sync. That backfill classifies weekdays by probing official daily files. Weekends are closed locally. A 404 or empty file stays unresolved, and after the 18:00 publication cutoff it blocks the exchange. The 2026-09-25 Mid-Autumn closure blocked all five exchanges even though each exchange had published an annual 休市安排 notice on 2025-12-17.

`FuturesTradingDayGovernanceService.upsert_official_notice` and `futures_calendar_notices` already own notice evidence. `OfficialFuturesCalendarProvider` can fetch and parse structured or nearby-window text, but the daily path never calls it, and the text window cannot read a range such as “9月25日至9月27日休市”. Strong verified rows already include `classification_rule=official_holiday_notice`.

## Goals / Non-Goals

**Goals:**

- Make each exchange's own holiday notice the forward source for full-day closures.
- Run that refresh inside the existing calendar backfill, before daily probes.
- Keep daily files as the confirmation for dates the notice does not close.
- Keep one calendar writer and the current daily job id.

**Non-Goals:**

- Inferring a holiday from 404 or an empty payload.
- Using the AkShare/Sina calendar or GFEX `/u/interfacesWebTpTradingCalendar/loadList` as the closure source.
- Rebuilding historical calendars that official daily rows already verified.
- Modeling product-level halts, or treating a night-session suspension as a closed day.
- Adding a scheduler job or a second calendar database.

## Decisions

1. **Notice refresh is the first step of the existing official calendar backfill.** `FuturesOfficialCalendarBackfillService.run` calls the governance service to refresh notices for the exchanges in that run, then probes only dates that are not already closed by a notice. The scheduler and `data_manager` entry stay the current preflight call. `skip_trading_calendar_backfill` does not refresh notices; stored rows still drive target dates.

2. **One notice per exchange, selected from that exchange's official listing.** Config under `trading_day_governance` holds a listing URL and a selector for the current year's 休市安排, plus an optional pinned URL for tests. SHFE, INE, DCE, CZCE, and GFEX are stored separately even when the dates match. A notice fetch that needs an existing anti-bot client reuses that exchange's bounded client. One exchange's fetch or parse failure does not invent closures and does not block the other exchanges' notice sync.

3. **The parser reads closure ranges and night-session sentences.** “X日至Y日休市” expands to inclusive closed dates with `quality_flag=backfilled_verified`, `source_profile=exchange_official_holiday_notice`, and `classification_rule=official_holiday_notice`. A bare month-day uses the notice's stated year; an explicit other year in the same sentence is kept. “晚上不进行夜盘交易” is notice metadata and, when that date is a trading day, calendar metadata. It does not set `is_trading_day=false`. An explicit weekend trading day is written open and the weekend rule must not close it afterward. A date the notice closes that is already a deterministic weekend stays closed and may be upgraded to the notice classification. A date whose existing verified status disagrees with the notice is left unchanged and sent to review; other confident ranges in that notice are still written. Unparsed text creates `review_required` and writes no guessed rows.

4. **Daily probe remains the oracle for uncovered weekdays.** Notice-closed dates are not probed by the normal backfill. If a probe is nevertheless run and returns contract rows or fails, the notice-closed row is preserved, a contradiction is recorded, and review is opened. A post-cutoff 404 or empty file on a date with no covering notice stays unresolved and can still block that exchange. DCE route exhaustion and timeouts stay on the current unresolved path.

5. **Idempotent apply.** An unchanged notice hash refreshes derived closed dates without a second evidence row. A newer notice changes only dates it explicitly states. Closures from an earlier notice that the newer notice does not mention stay in place. Daily-file verified rows outside a notice's stated dates are left in place.

## Risks / Trade-offs

- [Notice HTML changes and the parser misses a range] → That exchange returns `review_required`, writes no guessed closures, and the daily probe keeps today's unresolved behavior for uncovered dates.
- [A notice is withdrawn or a makeup session is added] → A newer official notice updates only the dates it explicitly states; a contradicting daily file opens review instead of a silent overwrite.
- [DCE or CZCE notice pages need the browser client] → Reuse the bounded client already used for that exchange. Failure isolates to that exchange's notice sync.
- [Night-session metadata arrives before the trading-day row exists] → Store it on the notice and copy it onto the calendar row when the daily probe later verifies that trading day.

## Migration Plan

- Ship the parser and backfill step behind the existing daily preflight. No job-id or schedule change.
- The first successful production preflight writes the current-year closures, including 2026-09-25 through 2026-09-27 and 2026-10-01 through 2026-10-07, per exchange whose notice parses.
- Rollback is the previous backfill behavior: stop applying notice rows and leave existing daily-file calendar rows. Notice rows can be identified by `source_profile=exchange_official_holiday_notice`.

## Open Questions

- None for the acceptance path. Exact listing URLs are confirmed during implementation from each exchange's current notice index and pinned in config with the fetched evidence URL stored on the notice row.
