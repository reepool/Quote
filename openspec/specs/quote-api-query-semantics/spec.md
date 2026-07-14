# quote-api-query-semantics

## Purpose
This capability defines quote API query semantics for latest bars and symbol resolution.
## Requirements
### Requirement: Latest Quotes Return Newest Stored Bar
The Quotes API SHALL return the newest available daily quote row for each requested instrument in `/api/v1/quotes/latest`.

#### Scenario: Descending database rows
- **WHEN** the stored rows for a requested instrument include multiple dates in the latest query window
- **THEN** the API response SHALL contain the row with the maximum `time` value for that instrument
- **AND** it SHALL NOT return an older row because of DataFrame row position

#### Scenario: Standard exchange suffix input
- **WHEN** a caller requests `/api/v1/quotes/latest` with an instrument id using a standard exchange suffix such as `000001.SZSE` or `600000.SSE`
- **THEN** the query SHALL use the normalized database instrument id such as `000001.SZ` or `600000.SH`
- **AND** the response SHALL identify the instrument consistently with the queried database instrument

### Requirement: Daily Symbol Queries Resolve To One Instrument
The Quotes API SHALL resolve a bare `symbol` query for `/api/v1/quotes/daily` to a single instrument before reading quote rows.

#### Scenario: Stock and index share the same symbol
- **WHEN** a caller requests `/api/v1/quotes/daily?symbol=000001` and the instrument catalog contains both `000001.SZ` stock and `000001.SH` index instruments
- **THEN** the API SHALL return quote rows only for the resolved stock instrument `000001.SZ`
- **AND** every row in `data` SHALL have the same `instrument_id` as the response-level instrument id

#### Scenario: Caller requests exact index instrument
- **WHEN** a caller requests `/api/v1/quotes/daily?instrument_id=000001.SH`
- **THEN** the API SHALL return quote rows for `000001.SH`
- **AND** the symbol-resolution preference for stock instruments SHALL NOT override the explicit instrument id

### Requirement: Quote Query Documentation Describes Ambiguity Handling
The user-facing API documentation SHALL describe deterministic quote query behavior for latest quotes and ambiguous bare symbols.

#### Scenario: Documentation distinguishes symbol and instrument id
- **WHEN** a maintainer reads the Quotes API documentation
- **THEN** the documentation SHALL explain that `symbol` resolves to one preferred instrument
- **AND** it SHALL instruct callers to use `instrument_id` when they need an exact stock, index, ETF, or other instrument

### Requirement: Quote Changes Are Queryable Without Changing Daily Quote Defaults
The Quotes API SHALL expose read-only change-watermark queries for quote rows while preserving the existing default behavior of `/api/v1/quotes/daily`.

#### Scenario: Existing daily quote query remains unchanged
- **WHEN** a caller requests `/api/v1/quotes/daily` without change-watermark parameters
- **THEN** the API SHALL return the same data contract as before this change
- **AND** it SHALL NOT require callers to provide a watermark

#### Scenario: Caller lists changed quote keys
- **WHEN** a caller requests quote changes after a prior watermark
- **THEN** the API SHALL return changed quote business keys and change metadata
- **AND** the caller SHALL be able to re-fetch the full rows through existing quote endpoints

### Requirement: Adjustment Factor Changes Are Exposed Separately From Raw Quotes
The Quotes API SHALL distinguish raw daily quote changes from adjustment-factor changes that can invalidate adjusted quote results.

#### Scenario: Raw quote unchanged but factor changed
- **WHEN** a factor sync changes an instrument's adjustment factor and no raw OHLCV row changes
- **THEN** the quote change surface SHALL expose an adjustment-factor change
- **AND** it SHALL NOT mislabel the raw quote row itself as changed

### Requirement: Change Pagination Is Stable
The Quotes API SHALL provide stable pagination for quote and factor change lists.

#### Scenario: Paginated quote changes
- **WHEN** more quote changes exist than the requested page limit
- **THEN** the response SHALL return changes in ascending watermark order
- **AND** it SHALL include a next watermark or `has_more` indicator for continuation

### Requirement: Latest Quote Watermark Is Queryable
The Quotes API SHALL expose the latest quote or adjustment-factor watermark without requiring callers to fetch a change page.

#### Scenario: Caller checks latest quote watermark
- **WHEN** a caller requests the latest quote-domain watermark
- **THEN** the API SHALL return the latest quote change sequence
- **AND** it SHALL include enough domain metadata for the caller to persist the checkpoint safely

