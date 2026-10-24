## 1. Classification Implementation

- [x] 1.1 Add `DataManager._is_latest_quote_suspended(instrument_id, latest_quote_date)` that returns True only when `db_ops.get_daily_quote` returns a row whose `tradestatus` equals 0 (compare without lossy int coercion; treat non-integer values as not suspended); return False when the date is missing, the getter is unavailable, the lookup raises, or no row is found (fail-closed).
- [x] 1.2 In `update_daily_data`'s empty-result branch, extend the `legitimate_no_quote` condition to also cover A-share stocks (`is_a_stock`, type `stock`) where `rejected_count == 0` and `_is_latest_quote_suspended` is True; keep the existing master-`trading_status` condition untouched for all exchanges.
- [x] 1.3 Ensure exempted results increment only the `legitimate_no_quote` integrity counters, skip `_adjust_daily_instrument_failure_counts`, and skip the `empty_unresolved_retries` append; do NOT append exempted instruments to the shared integrity `samples` list (the report renders that list as anomalies) — emit an info-level log line naming each exempted instrument instead.

## 2. Operator Report

- [x] 2.1 Extend the daily-update integrity line in `utils/report/engine.py` with a `停牌无行情: <n>` segment sourced from `legitimate_no_quote`, shown only when the count is non-zero; adjust the section's early-return condition so the section still renders when `legitimate_no_quote > 0` even if `empty_unresolved`, samples, and retry attempts are all zero.

## 3. Tests

Place 3.1–3.3 in a new `tests/unit/test_daily_update_suspension_exemption.py`; extend the existing `tests/unit/test_daily_update_report.py` for 3.4 (repo convention: focused per-behavior unit files, mocked db_ops/sources, no network).

- [x] 3.1 Unit tests for `_is_latest_quote_suspended`: suspended row → True; trading row → False; missing date / no row / getter raises / getter absent / non-integer tradestatus → False.
- [x] 3.2 Classification branch tests through `update_daily_data` with mocked sources and db_ops: suspended-latest A-share stock returning empty → `legitimate_no_quote` incremented, `stock_failure_count` unchanged, no retry candidate; trading-latest stock returning empty → failure counted; empty result with `rejected_count > 0` → failure counted; HKEX exchange path unchanged.
- [x] 3.3 Watermark predicate test: an exchange result whose only empties are exempted yields `stock_failure_count == 0` and `_quote_exchange_blocks_stock_watermark` returns False; one trading-stock empty still returns True.
- [x] 3.4 Report formatter test: integrity line renders `停牌无行情` when `legitimate_no_quote > 0` and omits it when zero.

## 4. Verification

- [x] 4.1 Run the affected pytest subset (new tests plus `tests/unit/test_daily_update_report.py`, `tests/unit/test_data_manager.py`, `tests/unit/test_scheduler_daily_update_integrity.py`) and record results.
- [ ] 4.2 Live repair runbook after merge: `/run daily_data_update 2026-10-23` (expect SZSE watermark `success` through 2026-10-23 and the integrity line to show `停牌无行情: 1`), then `/run a_share_cninfo_corporate_action_daily_sync` (expect the canonical merge to run and the report to show the production adjustment-factor table updated). Record both report outputs as acceptance evidence.
- [ ] 4.3 Next scheduled nightly run (first trading day after merge) confirms SZSE watermark advances and the 03:30 corporate-action job no longer reports `predecessor_watermark_stale`.
