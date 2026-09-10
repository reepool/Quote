# tencent-daily-quote-source Specification

## Purpose

Defines the contract for the Tencent backup daily-quote source for A-share stock daily updates: the single newfqkline URL with unadjusted basis, exact 11-column field mapping with prefix-based volume units, holiday-tolerant previous-close lookback, window coverage via bounded paging, failure semantics that plug into the existing daily failover and circuit breakers (including HTTP 403/429 recognition), route placement as the first backup after `pytdx`, and shared transport / rate-limit constraints.

## ADDED Requirements

### Requirement: Unadjusted daily bars from a single newfqkline URL with exact field mapping

The source SHALL fetch daily klines from exactly `https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get` with an empty adjust flag (raw unadjusted prices, `day` node), and SHALL map the 11-column row as: idx0 date, idx1 open, idx2 close, idx3 high, idx4 low, idx5 volume; idx6 SHALL be skipped; idx7 SHALL NOT be used (turnover reported as `0.0`); amount SHALL be idx8 × 10000 (万元 to yuan); idx9/idx10 SHALL be ignored. Volume units SHALL follow the code-prefix rule: `sh688*` and `sh689*` are already in shares (×1); all other covered A-share stocks are in lots (×100 to shares). The runtime SHALL NOT switch between endpoint hosts.

#### Scenario: Close/high/low order is not confused with OHLC order

- **WHEN** a raw day row is `["2026-09-10", "9.22", "9.30", "9.33", "9.19", "356060.00", {}, "0.27", "33040.12", "0.00", "0.00"]`
- **THEN** the parsed bar SHALL have open=9.22, close=9.30, high=9.33, low=9.19
- **AND** the stored close SHALL always lie within `[low, high]`

#### Scenario: Lots-based markets are converted by ×100

- **WHEN** a raw day row reports volume `898172.00` for `600000.SH` on 2026-09-03
- **THEN** the returned bar volume SHALL be `89817100` shares, matching the pytdx-written row for the same trading day
- **AND** volume `11810.00` for `920000.BJ` on 2026-09-03 SHALL become `1181000` shares

#### Scenario: STAR board and CDR codes are already in shares

- **WHEN** a raw day row reports volume `22153826.00` for `688981.SH` or `7250217.00` for `689009.SH`
- **THEN** the returned bar volume SHALL remain as-is (×1), matching the pytdx-written rows for the same trading days

#### Scenario: Amount is converted from 万元 to yuan

- **WHEN** a raw day row reports amount `84197.29` (万元) for `600000.SH` on 2026-09-03
- **THEN** the returned bar amount SHALL be `841972900` yuan at the 万元 precision of the endpoint

### Requirement: Daily window coverage uses bounded paging with count+1 deduplication

The source SHALL cover the requested `[start_date, end_date]` window by requesting at most `batch_size` (800) bars per call — the endpoint may return one extra row (800+1), which SHALL be deduplicated by date — and paging toward older dates (next page end boundary = earliest bar date of the current page minus one day). Paging SHALL stop when the window is covered, a page returns no rows (including an end boundary earlier than the listing date, which is a healthy empty), or a bounded safety iteration limit (100) is reached. Paged results SHALL be concatenated, date-deduplicated, and sorted ascending.

#### Scenario: Multi-page history reaches the listing date

- **WHEN** a fetch window starts before the oldest single page of an instrument listed on 1999-11-10
- **THEN** the source SHALL page backward until `start_date` is covered without duplicate dates
- **AND** the total number of requests SHALL be bounded by the safety iteration limit

#### Scenario: End boundary before the listing date is a healthy empty

- **WHEN** a page's end boundary precedes the instrument's listing date and the endpoint returns no rows
- **THEN** the source SHALL treat it as the normal termination of paging, not an error

### Requirement: pre_close uses a holiday-tolerant lookback from start_date

The daily update window `[T-1, T]` often contains a single trading day, and adjacent trading days can be up to 11 calendar days apart (Spring Festival, National Day). To derive `pre_close`/`change`/`pct_change`, the source SHALL extend the fetch start at least 15 calendar days before `start_date` (or until the previous valid bar is found, capped at 20 calendar days), derive the fields from the previous bar, and filter the output back to `[start_date, end_date]`. For an instrument's IPO first day the source SHALL report `pre_close=None` rather than fabricating a value. `turnover` SHALL be `0.0`.

#### Scenario: Spring Festival gap is bridged

- **WHEN** `start_date` is 2026-02-24, the first trading day after the 11-calendar-day gap from 2026-02-13
- **THEN** the 15-calendar-day lookback SHALL include the 2026-02-13 bar
- **AND** the output row for 2026-02-24 SHALL carry `pre_close` from that bar while the output itself contains only rows within `[start_date, end_date]`

#### Scenario: IPO first day has no fabricated previous close

- **WHEN** the fetch window contains an instrument's first-ever trading bar and no earlier bar exists in the lookback
- **THEN** the bar SHALL be returned with `pre_close=None`

### Requirement: Failure semantics plug into the existing daily failover

The source SHALL return `[]` with `last_fetch_diagnostic` for data-level failures (empty node, unparseable rows, dead codes) and SHALL raise exceptions for transport-level failures: connection refused/reset/timeouts as `ConnectionError`, and HTTP 403/429 as an exception carrying a `code` or `status` attribute in `{403, 429}` or a lowercased message containing one of the literals matched by the factory (`http error 403`, `http error 429`, `403` with `forbidden`, `too many requests`, `status code: 403`, `status_code=403`) so the existing throttle breaker counts them. The source SHALL NOT swallow HTTP 403/429 into empty results, SHALL NOT sleep between requests inside the daily update loop (rate-limiter pacing excluded), and SHALL NOT mark healthy empty results as connection-unhealthy.

#### Scenario: Transport failure counts toward the circuit breaker

- **WHEN** requests to the Tencent endpoint fail with connection reset or timeout for an instrument
- **THEN** the source SHALL raise `ConnectionError`
- **AND** the factory SHALL count it toward the existing transport-error circuit breaker and continue the configured fallback chain

#### Scenario: HTTP 403/429 is recognized by the throttle breaker

- **WHEN** the endpoint responds with HTTP 403 or 429
- **THEN** the source SHALL raise an exception whose `code`/`status` attribute is 403 or 429, or whose message contains one of the factory-matched literals
- **AND** the factory SHALL count it toward the existing HTTP throttle circuit breaker instead of treating it as empty data

#### Scenario: Malformed rows are data-level failures

- **WHEN** a page contains rows that cannot be parsed into valid bars
- **THEN** the source SHALL skip those rows, return the remaining valid bars (or `[]` if none), and record the reason and count in `last_fetch_diagnostic`
- **AND** the factory SHALL treat the result as a normal insufficient result that falls through to the next source without breaker penalties

### Requirement: Intraday partial bar passes through unchanged

During trading hours the endpoint returns today's not-yet-closed bar. The source SHALL return it as-is, consistent with the pytdx source, and completeness handling SHALL remain with the scheduler market-close wait and downstream integrity validation.

#### Scenario: Request during trading hours includes today's bar

- **WHEN** `get_daily_data` is called intraday with `end_date` set to today
- **THEN** the returned rows SHALL include today's partial bar without filtering or special flagging by the source

### Requirement: Latest daily snapshot comes from the batch quote endpoint with the same volume rule

`get_latest_daily_data` SHALL query `qt.gtimg.cn` with GBK decoding (no Referer required) and map the fixed field positions: index 3 last price, 4 previous close, 5 open, 6 volume under the same code-prefix unit rule as daily bars, 30 quote time (`yyyyMMddHHmmss`), 33 high, 34 low, and 37 amount in 万 (×10000 to yuan). The snapshot is a live quote and SHALL NOT be used as historical daily data by the daily update path. Codes without a parsable payload SHALL return an empty dict per the base contract.

#### Scenario: Field mapping for a live instrument

- **WHEN** `get_latest_daily_data` is called for `600000.SH` during a trading session
- **THEN** the returned dict SHALL contain time/open/high/low/close/volume/pre_close/amount mapped from the documented positions with units converted

#### Scenario: Dead code returns empty dict

- **WHEN** the quote endpoint returns no parsable payload for the requested code
- **THEN** `get_latest_daily_data` SHALL return `{}`

### Requirement: Route placement is the first backup after pytdx without touching other routes

The daily routing configuration SHALL insert `tencent` immediately after `pytdx` for the SSE, SZSE and BSE stock chains only. Instrument-list, trading-calendar and adjustment-factor routing SHALL NOT reference `tencent`, and index and HKEX chains SHALL remain unchanged.

#### Scenario: Fallback order for A-share stock daily updates

- **WHEN** the factory resolves the daily route for SSE/SZSE stock
- **THEN** the chain SHALL be `pytdx → tencent → baostock → akshare`
- **AND** for BSE stock the chain SHALL be `pytdx → tencent → akshare`
- **AND** when `tencent` fails or returns insufficient data, the next source SHALL take over per the existing failover semantics

#### Scenario: Other routes remain unchanged

- **WHEN** routing is validated at startup
- **THEN** `tencent` SHALL NOT appear in `routing.instrument_list`, `routing.calendar` or `routing.factor`
- **AND** index and HKEX chains SHALL be unchanged from their pre-change values

### Requirement: Requests use the pinned URL, the shared transport, and configured rate limits

All HTTP calls SHALL go to the single pinned newfqkline URL (or the `qt.gtimg.cn` snapshot endpoint) through the project's shared HTTP transport layer with TLS certificate verification enabled, and request pacing SHALL go through the source `RateLimiter` driven by `data_sources_config.tencent` (default ≤3600 requests/minute, ≤60000/hour, ≤1000000/day — about 5× the measured serial daily-update rate of ~11 requests/second).

#### Scenario: Rate limiter paces full-market fallback

- **WHEN** the source fetches daily bars for the whole market after a primary-source failure
- **THEN** request pacing SHALL respect the configured per-minute/hour/day limits without adding failure-backoff sleep calls inside the daily update loop

#### Scenario: TLS verification is never disabled

- **WHEN** any Tencent endpoint is called
- **THEN** the request SHALL use the shared transport with certificate verification enabled
