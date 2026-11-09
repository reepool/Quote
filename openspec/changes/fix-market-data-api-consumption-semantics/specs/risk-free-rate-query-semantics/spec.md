## ADDED Requirements

### Requirement: Risk-Free-Rate HTTP Reads Are Bounded And Explicitly Ordered
The risk-free-rate HTTP endpoint SHALL support order=desc|asc with default desc, limit default 100 and range 1–2000, and offset default zero. It SHALL filter the requested inclusive date interval before stable observation-date sorting and pagination. Existing total SHALL remain the number of returned observations; additional total_available, returned_records, first_date, last_date, truncated and next_offset SHALL describe the filtered interval and page.

#### Scenario: Caller requests the latest two observations
- **WHEN** limit=2 is supplied without dates to a series containing more than two observations
- **THEN** exactly its two newest observations SHALL be returned in descending observation-date order
- **AND** total_available and interval date boundaries SHALL describe all eligible observations
- **AND** truncated SHALL be true with next_offset=2

#### Scenario: Caller traverses a date interval
- **WHEN** explicit date bounds and successive offsets are requested with stable stored inputs
- **THEN** the pages SHALL cover that inclusive interval without duplicates or omissions
- **AND** the final page SHALL have truncated=false and next_offset=null

#### Scenario: Internal valuation reads consume historical rates
- **WHEN** DCF or another existing internal consumer queries rates on or before its as-of date
- **THEN** the HTTP default limit and ordering SHALL NOT truncate or alter that consumer's existing storage-query contract

### Requirement: Rate Observation Dates Are Distinct From Successful Fetch Times
Rate metadata SHALL distinguish last observed date from data_as_of successful source fetch time. A successful non-empty sync SHALL persist a timezone-aware fetch timestamp through the existing write owner; failed or empty fetches SHALL NOT advance that timestamp. Reads SHALL preserve unknown historical timestamps as null with data_as_of_quality=unknown_historical unless trustworthy persisted timestamp evidence is available with a declared basis.

#### Scenario: Legacy observations lack timestamp evidence
- **WHEN** a historical observation has no trustworthy fetch timestamp
- **THEN** data_as_of SHALL remain null and its quality SHALL identify unknown historical timing
- **AND** the response SHALL still expose observation-date interval boundaries
- **AND** request time or observation date SHALL NOT be invented as a historical fetch timestamp

#### Scenario: A source sync succeeds and a later one fails
- **WHEN** a non-empty fetch succeeds at T and a subsequent fetch fails
- **THEN** the successfully written observations and series SHALL record T
- **AND** the failed run SHALL NOT replace T with its attempted fetch time

### Requirement: Rate Revision Slots Do Not Claim Historical Vintages
The API SHALL retain legacy revision_id=latest where used as an overwrite slot and SHALL disclose revision_kind=overwrite_slot rather than representing that constant as a unique historical data revision.

#### Scenario: An overwrite-slot observation is returned
- **WHEN** an observation uses the legacy latest revision slot
- **THEN** the response and OpenAPI SHALL explicitly identify its slot semantics and lack of a historical-vintage guarantee
