# a-share-daily-update-integrity Specification

## Purpose

Defines coverage, quality, calendar, source-freshness, and reporting rules for the normal A-share daily quote update.

## ADDED Requirements

### Requirement: Daily quote success requires coverage evidence

The A-share daily update SHALL NOT count an instrument as successful solely because the source call returned without raising an exception.

#### Scenario: Source returns empty on a required trading day

- **WHEN** the target date is an explicitly confirmed trading day and all eligible daily sources return no quote rows for an instrument that requires coverage
- **THEN** the instrument SHALL be recorded as unresolved or failed with a machine-readable reason
- **AND** it SHALL NOT increment the successful instrument count or advance a successful-through quote watermark

#### Scenario: Legitimate suspended or no-quote instrument is evidenced

- **WHEN** the target date is a confirmed trading day but instrument metadata/source evidence explicitly identifies the instrument as legitimately non-quoting or suspended
- **THEN** the result SHALL record a distinct legitimate-no-quote classification
- **AND** it SHALL NOT be presented as a normal quote coverage success

#### Scenario: Short-window primary source is empty

- **WHEN** an A-share stock primary source returns an empty result for a short daily window without an exception
- **THEN** the configured eligible backup sources SHALL be attempted unless a legitimate-no-quote classification is already established

### Requirement: Existing target-date bars are quality-gated before skip

The daily update SHALL skip an existing target-date bar only when the row is complete and passes the applicable daily OHLC quality checks.

#### Scenario: Existing target-date bar is complete and valid

- **WHEN** the latest local row covers the target date and has valid positive OHLC values, consistent high/low relationships, required fields, and complete status
- **THEN** the instrument SHALL remain idempotently skipped
- **AND** the result SHALL identify it as already covered rather than as a newly fetched success

#### Scenario: Existing target-date bar is incomplete or invalid

- **WHEN** the latest local row covers the target date but is incomplete, has missing required fields, contains zero/negative prices, or violates OHLC ordering
- **THEN** the daily update SHALL request the instrument again through the normal source chain
- **AND** the invalid row SHALL NOT prevent a valid replacement from being persisted

### Requirement: Daily writes enforce minimum quote quality

The daily quote write path SHALL reject invalid A-share OHLCV rows before persistence and SHALL NOT default rejected or incomplete rows to `is_complete=True`.

#### Scenario: Invalid OHLC row is returned

- **WHEN** a fetched row has missing required fields, non-positive prices, negative values, or `high < low` / open-or-close outside the high-low range
- **THEN** the row SHALL be rejected from the daily upsert
- **AND** the instrument result SHALL expose a bounded quality failure count/sample

#### Scenario: Valid row lacks derived metadata

- **WHEN** a valid fetched row lacks derived fields or explicit completeness metadata
- **THEN** the daily path SHALL populate the minimum derived/quality fields using existing project semantics
- **AND** completeness SHALL reflect validation rather than an unconditional true default

### Requirement: Trading-calendar absence is not a holiday decision

The scheduler SHALL distinguish a confirmed closed day from missing calendar evidence.

#### Scenario: Calendar row explicitly says closed

- **WHEN** the calendar contains a row for the exchange and target date with `is_trading_day=false`
- **THEN** the exchange SHALL be treated as closed and skipped normally

#### Scenario: Calendar row is missing

- **WHEN** no calendar row exists for the exchange and target date
- **THEN** the scheduler SHALL use the existing DateUtils fallback when it can provide an explicit answer
- **AND** if no reliable answer is available, the exchange SHALL be marked calendar-unknown and SHALL NOT contribute to a successful daily-update result

#### Scenario: Calendar refresh returns no rows

- **WHEN** a calendar refresh completes with zero updated/covered rows for a requested current date
- **THEN** the scheduler SHALL retain the calendar-unknown diagnostic instead of silently treating the exchange as a normal non-trading day

### Requirement: A-share stock data must cover the requested end trading day

The source factory SHALL apply end-date trading-day coverage validation and stale-source protection to A-share stock daily routes.

#### Scenario: Stock source returns an older window

- **WHEN** a stock source returns rows whose latest date is earlier than the latest expected trading day in the requested window
- **THEN** the source result SHALL be rejected as stale
- **AND** the next eligible source SHALL be tried

#### Scenario: All stock sources are stale or unavailable

- **WHEN** no eligible A-share stock source provides the expected end-date coverage
- **THEN** the instrument SHALL be unresolved/failed with source freshness diagnostics
- **AND** it SHALL NOT count as a successful daily update

### Requirement: Daily integrity outcomes are reported

The structured daily update result and scheduler report SHALL expose bounded counters and samples for unresolved empty results, invalid rows, re-fetches of bad existing bars, stale source responses, calendar-unknown exchanges, and legitimate no-quote classifications.

#### Scenario: Mixed instrument outcomes complete

- **WHEN** one daily run contains valid updates, already-covered valid rows, unresolved sources, and quality failures
- **THEN** the report SHALL preserve the per-exchange outcome counts and representative samples
- **AND** overall success SHALL require that no required unresolved/quality/calendar failure remains
