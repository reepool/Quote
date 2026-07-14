# daily-sync-change-watermarks Specification

## Purpose
TBD - created by archiving change add-daily-sync-change-watermarks. Update Purpose after archive.
## Requirements
### Requirement: Local Change Records Are Append-Only
The platform SHALL persist local-observed changes from daily sync, backfill, repair, reconciliation, and scheduled refresh jobs in append-only change records with monotonically increasing watermarks.

#### Scenario: Changed row creates one watermark
- **WHEN** an ingestion job inserts or materially changes a normalized business row
- **THEN** the platform SHALL append a change record with a new `sequence_id`
- **AND** the record SHALL include domain, dataset, business key, change type, old hash, new hash, row version, source, run or batch id, and changed timestamp

#### Scenario: Unchanged overlap fetch is ignored
- **WHEN** an overlap-window daily sync fetches a row whose semantic hash matches the stored row
- **THEN** the platform SHALL count the row as unchanged
- **AND** it SHALL NOT append a change record

### Requirement: Watermarks Represent Local Observed Changes
The platform SHALL define change-watermark semantics as changes detected by local ingestion and reconciliation jobs, not as a guarantee that upstream public sources publish complete CDC.

#### Scenario: Upstream revision is not fetched yet
- **WHEN** an upstream source silently revises a historical observation but no platform job has fetched and compared that observation
- **THEN** the platform SHALL NOT claim that the changelog contains that upstream revision
- **AND** reconciliation, overlap, and repair jobs SHALL remain the mechanism for discovering the revision locally

### Requirement: Business Keys Are Domain-Specific
The changelog SHALL use a shared envelope while preserving domain-specific keys for observations, facts, memberships, derived rows, and policy events.

#### Scenario: Futures contract bar change
- **WHEN** a futures contract bar changes
- **THEN** the change record SHALL identify the contract or series, trade date, source, and source mode
- **AND** it SHALL NOT force the row into a stock-style `(instrument_id, trade_date)` key

#### Scenario: Financial fact change
- **WHEN** a financial numeric fact changes
- **THEN** the change record SHALL identify the instrument, report period, fact identity, source profile, and parser or mapping version where available

### Requirement: Change Readers Can Resume From Watermarks
The platform SHALL expose read-only change queries that return changes after a caller-supplied watermark with stable ordering and pagination.

#### Scenario: Client resumes after checkpoint
- **WHEN** a caller requests changes after `sequence_id=N`
- **THEN** the platform SHALL return records with `sequence_id > N` ordered by ascending `sequence_id`
- **AND** the response SHALL include the highest returned sequence and whether more records remain

#### Scenario: Caller filters by domain
- **WHEN** a caller requests only `quotes` changes
- **THEN** the platform SHALL NOT return financial, policy, industry, FX, futures, or commodity changes

#### Scenario: Latest watermark is available
- **WHEN** a caller requests the latest watermark for one supported domain
- **THEN** the platform SHALL return the latest available `sequence_id` for that domain
- **AND** it SHALL return an explicit empty or zero-watermark response when no changes exist

### Requirement: Adjustment Factor Changes Are First-Class
The platform SHALL emit separate change records for adjustment-factor inserts and material changes because adjusted quote outputs can change even when raw daily quote rows do not.

#### Scenario: Factor restatement changes adjusted quotes
- **WHEN** an adjustment factor for an instrument changes
- **THEN** the platform SHALL append an adjustment-factor change record
- **AND** consumers of qfq or hfq quote data SHALL be able to detect that adjusted quote results may need re-fetching

### Requirement: Backfill And Repair Emit The Same Change Contract
Historical backfill, gap repair, and reconciliation jobs SHALL emit the same changelog contract when they insert or materially change rows.

#### Scenario: Range backfill repairs historical quote
- **WHEN** a range backfill updates a historical quote row whose semantic hash differs from the stored hash
- **THEN** the platform SHALL append a quote change record for that historical business key
- **AND** it SHALL preserve the operator-requested date range and lifecycle filtering behavior

### Requirement: Policy And Governance Changes Are Isolated By Domain
Policy discovery, master governance, and trading-calendar governance changes SHALL be publishable as domain-specific changes without affecting market-data consumers unless explicitly requested.

#### Scenario: Policy discovery promotes an event
- **WHEN** a policy-discovery workflow promotes or changes a policy event
- **THEN** the platform SHALL record the change under the policy or evidence domain
- **AND** a quote-only change query SHALL NOT include that policy event

### Requirement: Dry Runs Do Not Advance Watermarks
Dry-run, preflight, diagnostic, and read-only workflows SHALL NOT persist changelog records or advance watermarks.

#### Scenario: Dry run detects would-change rows
- **WHEN** a dry-run task detects rows that would be inserted or changed in write mode
- **THEN** the task result SHALL report would-write counters
- **AND** no persistent changelog sequence SHALL be created
