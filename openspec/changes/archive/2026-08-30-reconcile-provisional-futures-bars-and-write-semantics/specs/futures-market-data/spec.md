## ADDED Requirements

### Requirement: Publication-eligible futures bars must be finalized after cutoff
The futures sync SHALL distinguish provisional same-day observations from finalized daily observations using the target exchange's publication timezone and cutoff. Persisted row presence alone MUST NOT satisfy post-cutoff completeness when the required date is represented only by provisional evidence.

#### Scenario: Same-day bar is fetched before cutoff
- **WHEN** an official or fallback provider returns a bar for the exchange-local current trading date before the configured publication cutoff
- **THEN** the stored row SHALL be marked provisional with acquisition time and cutoff context
- **AND** the row MAY remain available for diagnostics
- **AND** it SHALL NOT be represented as finalized settlement coverage

#### Scenario: Nightly run receives changed final data
- **WHEN** the publication-eligible target date has a provisional row and a post-cutoff provider fetch returns changed final values
- **THEN** the sync SHALL update or source-replace the row through the existing priority rules
- **AND** it SHALL record final verification time and ingestion run id
- **AND** the finalized date SHALL satisfy completeness when no other blocker remains

#### Scenario: Nightly run verifies identical final data
- **WHEN** a post-cutoff official fetch returns values semantically identical to an existing provisional row
- **THEN** the sync SHALL persist that final verification advanced
- **AND** it SHALL count the price values as unchanged
- **AND** it SHALL NOT report a same-source price correction solely for the finality metadata update

#### Scenario: Nightly final fetch fails
- **WHEN** publication is due and the provider cannot verify or finalize a required date that has only provisional stored rows
- **THEN** the exchange SHALL remain partial or blocked
- **AND** the result SHALL list the stale provisional date and provider blocker
- **AND** the provisional row SHALL be retained rather than deleted

#### Scenario: Current date is not publication-eligible
- **WHEN** a run occurs before the configured cutoff
- **THEN** the exchange-local current date SHALL NOT be required for final completeness
- **AND** any provisional current-date row SHALL NOT make an earlier completed-date run fail

### Requirement: Futures write summaries must distinguish business transitions
Futures series and contract write results SHALL classify business-semantic outcomes independently from physical insert/update/delete operations while preserving existing aggregate counters.

#### Scenario: First observation for a series and date is written
- **WHEN** no observation exists for a series, trade date, and source mode before the write
- **THEN** the result SHALL count a `new_business_date` row
- **AND** it SHALL include the newly covered trade date in the business-date summary

#### Scenario: Official row supersedes aggregator fallback
- **WHEN** an official row replaces an existing lower-priority fallback row for the same series, trade date, and source mode
- **THEN** the result SHALL count a `source_upgrade`
- **AND** it SHALL NOT count that transition as new business-date coverage
- **AND** existing delete and insert change-log evidence SHALL remain auditable

#### Scenario: Same source publishes corrected values
- **WHEN** an existing source key receives changed normalized values or a changed semantic hash
- **THEN** the result SHALL count a `same_source_correction`
- **AND** it SHALL preserve the existing `changed` aggregate counter

#### Scenario: Rerun requires no value or finality change
- **WHEN** the stored row already matches the incoming values and has sufficient final verification evidence
- **THEN** the result SHALL count it as unchanged
- **AND** it SHALL NOT emit a business transition or semantic change record

### Requirement: Futures completeness must expose provisional reconciliation
Each exchange completeness result SHALL expose finalized latest coverage, stale provisional dates, dates finalized by the current run, and post-cutoff verification blockers in addition to actual persisted latest date.

#### Scenario: Persisted latest date is provisional
- **WHEN** `actual_latest_price_date` reaches the expected latest date but that date has only provisional evidence after cutoff
- **THEN** the exchange SHALL NOT report success
- **AND** the completeness result SHALL distinguish persisted latest date from finalized latest date

#### Scenario: Rolling run finalizes a provisional date
- **WHEN** the daily rolling run successfully finalizes a recent provisional date
- **THEN** the completeness result SHALL include that date under finalized or reconciled dates
- **AND** it SHALL remove the date from remaining stale provisional dates
