## ADDED Requirements

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
