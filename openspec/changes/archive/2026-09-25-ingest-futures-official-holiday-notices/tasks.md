## 1. Notice source and parser

- [x] 1.1 Add per-exchange holiday-notice listing and selector config under `trading_day_governance` for SHFE, INE, DCE, CZCE, and GFEX, including optional pinned URLs for tests.
- [x] 1.2 Extend `OfficialFuturesCalendarProvider` to select each exchange's current-year 休市安排 and parse inclusive “X日至Y日休市” ranges plus night-session-only sentences. A bare month-day uses the notice year; an explicit other year stays on that date.
- [x] 1.3 Send ambiguous notice text to the existing review-required path and write no guessed calendar rows. A single date that disagrees with a verified row does not discard the notice's other confident ranges. A later notice changes only dates it explicitly states.

## 2. Governance and backfill

- [x] 2.1 Persist notice evidence and derived closed rows through `FuturesTradingDayGovernanceService.upsert_official_notice`, using `official_holiday_notice` and `exchange_official_holiday_notice`.
- [x] 2.2 Keep night-session suspensions off `is_trading_day`, and stop the weekend rule from closing a weekend the notice marks open.
- [x] 2.3 Run notice refresh as the first step of `FuturesOfficialCalendarBackfillService.run`, skip probes for notice-closed dates, and leave post-cutoff 404 or empty files unresolved when no notice covers the date.
- [x] 2.4 Preserve a notice-closed row when a later daily probe returns contract rows or fails, and open review instead of silently flipping the date to trading.
- [x] 2.5 Exclude notice-closed dates from target trading dates so `futures_market_data_sync` does not fetch them. `skip_trading_calendar_backfill` must not refresh notices, but must still honor stored closures.

## 3. Verification

- [x] 3.1 Add unit tests for the 2026 Mid-Autumn range 2026-09-25..2026-09-27, National Day 2026-10-01..2026-10-07, a night-session suspension on 2026-09-24, a weekend made a trading day, an unchanged notice hash, one exchange's parse failure, and a post-cutoff 404 that stays unresolved.
- [x] 3.2 Add a sync test where the repair window contains both a notice holiday and a later trading day, and assert the holiday is not fetched and does not block the exchange.
- [x] 3.3 Update the current futures calendar requirement note so daily preflight order and holiday-notice ownership match this change.

## 4. Production check

- [x] 4.1 Dry-run the existing calendar backfill for the five exchanges over 2026-09-21..2026-10-08 and confirm notice-closed dates are skipped while uncovered weekdays still probe.
