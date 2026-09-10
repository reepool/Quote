# tencent-daily-quote-source Specification

## Purpose

Defines the contract for the Tencent backup daily-quote source for A-share stock daily updates: unadjusted basis with exact field mapping and units, window coverage via bounded paging, failure semantics that plug into the existing daily failover and circuit breakers, route placement as the first backup after `pytdx`, and shared transport / rate-limit constraints.

## ADDED Requirements

### Requirement: Tencent daily bars are unadjusted with exact field mapping

The source SHALL fetch daily klines from `web.ifzq.gtimg.cn/appstock/app/fqkline/get` with an empty adjust flag (raw unadjusted prices, `day` node) and SHALL map each row's fields in the endpoint's order `[date, open, close, high, low, volume]`. Volume SHALL be converted from lots (手) to shares by ×100. Fields the endpoint does not provide (historical amount) SHALL be reported as `0.0` rather than omitted or fabricated.

#### Scenario: Close/high/low order is not confused with OHLC order

- **WHEN** a raw day row is `["2026-09-10", "9.22", "9.30", "9.33", "9.19", "356060"]`
- **THEN** the parsed bar SHALL have open=9.22, close=9.30, high=9.33, low=9.19
- **AND** the stored close SHALL always lie within `[low, high]`

#### Scenario: Volume is converted from lots to shares

- **WHEN** a raw day row reports volume `898172` (手) for `600000.SH` on 2026-09-03
- **THEN** the returned bar volume SHALL be `89817100` (股), matching the pytdx-written row for the same trading day

### Requirement: Daily window coverage uses bounded paging

The source SHALL cover the requested `[start_date, end_date]` window by requesting at most `batch_size` (800) bars per call and paging toward older dates (next page end boundary = earliest bar date of the current page minus one day). Paging SHALL stop when the window is covered, a page returns no rows or no rows beyond coverage, or a bounded safety iteration limit (100) is reached. Paged results SHALL be concatenated, date-deduplicated, and sorted ascending.

#### Scenario: Multi-page history reaches the requested start

- **WHEN** a fetch window starts before the oldest single page of an instrument listed in 1999
- **THEN** the source SHALL page backward until `start_date` is covered without duplicate dates
- **AND** the total number of requests SHALL be bounded by the safety iteration limit

#### Scenario: Dead or recoded code returns empty, not an error

- **WHEN** the endpoint returns a response whose `day` node is empty (e.g. a recoded BSE code)
- **THEN** the source SHALL return an empty result without raising a transport error

### Requirement: Failure semantics plug into the existing daily failover

The source SHALL return `[]` with `last_fetch_diagnostic` for data-level failures (empty node, unparseable rows, dead codes) and SHALL raise `ConnectionError` only for transport-level failures (connection refused/reset, timeouts) so the existing factory transport-error circuit breaker counts them. The source SHALL NOT sleep between requests inside the daily update loop and SHALL NOT mark healthy empty results as connection-unhealthy.

#### Scenario: Transport failure counts toward the circuit breaker

- **WHEN** requests to the Tencent endpoints fail with connection reset or timeout for an instrument
- **THEN** the source SHALL raise `ConnectionError`
- **AND** the factory SHALL count it toward the existing transport-error circuit breaker and continue the configured fallback chain

#### Scenario: Malformed rows are data-level failures

- **WHEN** a page contains rows that cannot be parsed into valid bars
- **THEN** the source SHALL skip those rows, return the remaining valid bars (or `[]` if none), and record the reason and count in `last_fetch_diagnostic`
- **AND** the factory SHALL treat the result as a normal insufficient result that falls through to the next source without breaker penalties

### Requirement: Intraday partial bar passes through unchanged

During trading hours the endpoint returns today's not-yet-closed bar. The source SHALL return it as-is, consistent with the pytdx source, and completeness handling SHALL remain with the scheduler market-close wait and downstream integrity validation.

#### Scenario: Request during trading hours includes today's bar

- **WHEN** `get_daily_data` is called intraday with `end_date` set to today
- **THEN** the returned rows SHALL include today's partial bar without filtering or special flagging by the source

### Requirement: Latest daily snapshot comes from the batch quote endpoint

`get_latest_daily_data` SHALL query `qt.gtimg.cn` with GBK decoding (no Referer required) and map the fixed field positions: index 3 last price, 4 previous close, 5 open, 6 volume in lots (×100 to shares), 30 quote time (`yyyyMMddHHmmss`), 33 high, 34 low, and 37 amount in 万 (×10000 to yuan). Codes without a parsable payload SHALL return an empty dict per the base contract.

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

### Requirement: Requests use the shared transport with configured rate limits

All HTTP calls SHALL go through the project's shared HTTP transport layer with TLS certificate verification enabled, and request pacing SHALL go through the source `RateLimiter` driven by `data_sources_config.tencent` (default ≤3600 requests/minute, ≤60000/hour, ≤1000000/day).

#### Scenario: Rate limiter paces full-market fallback

- **WHEN** the source fetches daily bars for the whole market after a primary-source failure
- **THEN** request pacing SHALL respect the configured per-minute/hour/day limits without adding sleep calls inside the daily update loop

#### Scenario: TLS verification is never disabled

- **WHEN** any Tencent endpoint is called
- **THEN** the request SHALL use the shared transport with certificate verification enabled
