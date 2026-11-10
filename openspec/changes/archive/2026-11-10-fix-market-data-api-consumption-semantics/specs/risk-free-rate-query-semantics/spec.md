## ADDED Requirements

### Requirement: Risk-Free-Rate HTTP Reads Are Bounded And Explicitly Ordered
The risk-free-rate HTTP endpoint SHALL support order=desc|asc with default desc, limit default 100 and range 1–2000, and offset default zero. It SHALL apply the existing latest-revision-slot selection and requested inclusive date interval before sorting by observation_date in the requested direction with source_profile and revision_id ascending as deterministic tie breakers. Rows SHALL expose source_profile so same-date source identities remain distinguishable. Existing total SHALL remain the number of returned observations; additional total_available, returned_records, first_date, last_date, truncated and next_offset SHALL describe the filtered interval and page.

#### Scenario: Caller requests the latest two observations
- **WHEN** limit=2 is supplied without dates to a series containing more than two observations
- **THEN** exactly its two newest observations SHALL be returned in descending observation-date order
- **AND** total_available and interval date boundaries SHALL describe all eligible observations
- **AND** truncated SHALL be true with next_offset=2

#### Scenario: Caller traverses a date interval
- **WHEN** explicit date bounds and successive offsets are requested with stable stored inputs
- **THEN** the pages SHALL cover that inclusive interval without duplicates or omissions
- **AND** the final page SHALL have truncated=false and next_offset=null

#### Scenario: Multiple source profiles share an observation date
- **WHEN** stable stored inputs contain two observations with the same date and different source_profile keys
- **THEN** pagination SHALL use the full declared ordering so each business key appears exactly once across pages
- **AND** the response SHALL NOT collapse those identities or claim observation_date is unique

#### Scenario: Internal valuation reads consume historical rates
- **WHEN** DCF or another existing internal consumer queries rates on or before its as-of date
- **THEN** the HTTP default limit and ordering SHALL NOT truncate or alter that consumer's existing storage-query contract

### Requirement: Rate Observation Dates Are Distinct From Successful Fetch Times
Rate metadata SHALL expose series.last_observation_date as the latest stored observation date independently of requested date bounds and SHALL distinguish it from data_as_of successful source fetch time and the filtered interval boundaries. A successful non-empty sync SHALL persist a timezone-aware fetch timestamp through the existing write owner; failed or empty fetches SHALL NOT advance or clear the prior successful timestamp. Reads SHALL preserve unknown historical timestamps as null with data_as_of_quality=unknown_historical unless trustworthy persisted timestamp evidence is available with a declared basis. Series, observation-quality, source-identity and pagination fields SHALL be declared in actual response models/OpenAPI and SHALL survive HTTP serialization.

#### Scenario: Legacy observations lack timestamp evidence
- **WHEN** a historical observation has no trustworthy fetch timestamp
- **THEN** data_as_of SHALL remain null and its quality SHALL identify unknown historical timing
- **AND** the response SHALL still expose observation-date interval boundaries
- **AND** request time or observation date SHALL NOT be invented as a historical fetch timestamp

#### Scenario: A source sync succeeds and a later one fails
- **WHEN** a non-empty fetch succeeds at T and a subsequent fetch fails
- **THEN** the successfully written observations and series SHALL record T
- **AND** a failed or empty later run SHALL neither replace T with an attempted fetch time nor clear it by upserting static metadata without data_as_of

#### Scenario: Observation persistence fails after a successful fetch
- **WHEN** one chunk persists and a later chunk fails
- **THEN** the series successful-sync timestamp SHALL remain at the previous successful run, with its update ordered after successful completion of the intended observation writes
- **AND** already persisted rows SHALL only retain their actual fetch timestamp rather than an invented complete-run assertion

### Requirement: Rate Revision Slots Do Not Claim Historical Vintages
The API SHALL retain legacy revision_id=latest where used as an overwrite slot and SHALL disclose revision_kind=overwrite_slot rather than representing that constant as a unique historical data revision.

#### Scenario: An overwrite-slot observation is returned
- **WHEN** an observation uses the legacy latest revision slot
- **THEN** the response and OpenAPI SHALL explicitly identify its slot semantics and lack of a historical-vintage guarantee

#### Scenario: First sync creates a static series parent
- **WHEN** the series does not exist and a non-empty fetch succeeds
- **THEN** the sync SHALL create a static series with data_as_of=null before inserting observations, satisfying the existing foreign key
- **AND** successful fetch time SHALL be set on the series only after all observations are written
