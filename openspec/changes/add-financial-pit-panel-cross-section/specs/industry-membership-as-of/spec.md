## ADDED Requirements

### Requirement: Bulk point-in-time industry membership query
The system SHALL provide a bulk industry membership query that, for a required `as_of` date, returns for each instrument the industry classification in effect on that date, derived from `industry_classification_history` official effective dates, with paging and optional taxonomy and exchange filters.

#### Scenario: Each instrument returns its classification in effect at as_of
- **WHEN** the query runs with `as_of = T`
- **THEN** every returned row SHALL be the classification whose effective start date is on or before T and, where recorded, whose expiry or successor start is after T
- **AND** an instrument with no classification effective at T SHALL be absent from the result rather than filled with its latest classification

#### Scenario: Response fields match the consumer contract
- **WHEN** the endpoint returns items
- **THEN** each item SHALL carry `instrument_id`, `industry_code`, `industry_name`, `taxonomy_system`, `effective_from`, and `effective_to`
- **AND** dates SHALL use `YYYYMMDD` formatting consistent with the PIT panel API layer

#### Scenario: Filters and paging apply
- **WHEN** the query includes `taxonomy_system`, `exchange`, `limit`, or `offset`
- **THEN** the system SHALL apply them to the result set
- **AND** single-page size SHALL NOT exceed 2000 rows

#### Scenario: Bulk query reuses the as-of semantics of the existing single-instrument path
- **WHEN** the bulk query is implemented
- **THEN** its per-instrument selection SHALL be consistent with the existing single-instrument `get_industry_membership_as_of` semantics
- **AND** the implementation SHALL NOT duplicate a per-instrument request loop in the API entry layer
