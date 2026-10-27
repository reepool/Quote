## Why

On 2026-10-23 the SZSE stock operational watermark froze at 2026-10-22 because a single
stock, `000016.SZ` (*ST康佳A, suspended since 2026-09-04 and announced for delisting on
2026-10-23), returned no data from every quote source. TDX hosts permanently stop serving
securities they have internally retired, so waiting or retrying cannot recover the row.
The failed fetch incremented `stock_failure_count`, which marks the whole exchange
watermark `partial` and blocks the corporate-action canonical merge every night
(`predecessor_watermark_stale`).

The watermark's own coverage computation already excludes instruments whose latest
persisted quote shows `tradestatus=0` (suspended) from per-exchange coverage
(`database/operations.py:get_latest_stock_quote_dates_by_exchange`). The failure
classification in `update_daily_data` contradicts that design: it only exempts
instruments whose instrument-master `trading_status` is not 1, and the official-list
master source hardcodes `trading_status=1` for every listed stock. The result is a
semantic split: coverage says a suspended stock's missing day does not matter, while
failure counting says the same missing day is an exchange-blocking failure.

## What Changes

- Classify an empty daily-quote result as `legitimate_no_quote` (not a failure, not
  retry-eligible, not watermark-blocking) when all of the following hold:
  - the instrument is an A-share stock (`exchange in SSE/SZSE/BSE`);
  - no rows were prepared and no rows were quality-rejected (sources returned nothing
    usable, as opposed to returning bad rows);
  - the instrument's latest persisted quote row exists and shows `tradestatus=0`
    (last observed state is suspended).
- Keep the existing master-`trading_status` exemption unchanged for all exchanges.
- Keep every other empty result classified exactly as today: instruments with trading
  evidence (`tradestatus=1`), with no quote history, with quality-rejected rows, with
  evidence lookup failures, and all non-A-share exchanges remain failures.
- Exempted instruments get no fabricated rows: suspended days stay absent from
  `daily_quotes` and remain repairable by later catch-up when a source serves them
  again (for example after the stock resumes trading in a delisting review period).
- Show the exemption in the operator report's integrity line (`停牌无行情` count) so
  operators can see why an instrument disappeared from `未覆盖`.
- With the only nightly failures being suspended-stock empties, the per-exchange stock
  watermark can reach `success` and the corporate-action canonical merge proceeds.

## Capabilities

### New Capabilities

- `a-share-daily-quote-failure-classification`: Classification contract for empty
  daily-quote results on A-share stocks, the suspension evidence rule, its effect on
  per-exchange failure counts and the stock readiness watermark, and operator-visible
  reporting of the exemption.

## Impact

- Affected code:
  - `data_manager.py` `update_daily_data` empty-result branch (around the
    `legitimate_no_quote` / `empty_unresolved` split) and a small suspended-evidence
    helper.
  - `utils/report/engine.py` daily-update integrity line.
- Affected storage: none. No schema, watermark-key, or master-data changes.
- Affected configuration: none.
- In-place fix inside the existing authoritative daily-update path; no new entry
  points, services, or owners (per `docs/development/project_development_governance.md`).
- Explicitly out of scope (tracked as separate future work, not this change):
  - consuming delisting announcements into instrument-master lifecycle state;
  - enabling the `security_state_forward` backtest rollout stage;
  - any change to TDX host selection or delisted-security handling in sources;
  - HKEX/US empty-result semantics.
