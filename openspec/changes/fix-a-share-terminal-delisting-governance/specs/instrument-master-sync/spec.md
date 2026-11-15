## MODIFIED Requirements

### Requirement: Master data source priority
The system SHALL use exchange-official sources as the primary A-share instrument master authority, BaoStock as first fallback or cross-check, AkShare as second fallback or supplement, and pytdx only as an optional current-list validator. Current-list absence alone SHALL NOT establish delisting.

#### Scenario: BaoStock provides listing status
- **WHEN** BaoStock returns `ipoDate`, `outDate`, and status for an instrument
- **THEN** the system SHALL use those fields only within the configured fallback or cross-check role
- **AND** official lifecycle evidence SHALL take precedence, with conflicting fallback evidence reported as a discrepancy
- **AND** ordinary production list-cache/upsert SHALL NOT establish or alter terminal lifecycle state from BaoStock-only fields before required official confirmation

#### Scenario: AkShare fallback is used
- **WHEN** official and BaoStock routes fail or require permitted metadata supplementation and AkShare provides current-list evidence
- **THEN** the system SHALL allow configured fallback or non-terminal metadata supplementation with degraded authority reported as applicable
- **AND** it SHALL NOT erase any existing `delisted_date`, override confirmed terminal evidence, or union official-absent fallback rows into an official-success current universe

#### Scenario: pytdx list evidence is available
- **WHEN** pytdx returns a current security list for an exchange
- **THEN** the system SHALL use pytdx evidence only for diagnostics or discrepancy reporting and MUST NOT use it to set `listed_date`, `delisted_date`, or final active status

#### Scenario: Official terminal lists supply missing lifecycle evidence
- **WHEN** an actual SSE or SZSE stock master refresh runs
- **THEN** it SHALL reconcile official terminal-list evidence for local in-scope securities through the existing governance chain
- **AND** it SHALL NOT depend on a security name containing a delisting marker

### Requirement: Delisted instruments are deactivated
The system SHALL mark officially confirmed completed delistings as inactive and delisted, using the actual effective date when known. Future dates SHALL NOT trigger immediate terminal writes; confirmed terminal state SHALL remain protected when its exact date is unknown.

#### Scenario: BaoStock reports a past outDate
- **WHEN** BaoStock reports `outDate=2026-04-27` for `600355.SH` after that date
- **THEN** the system SHALL reconcile it under the configured source-authority and evidence policy
- **AND** if official evidence confirms that effective date, the row SHALL have `delisted_date=2026-04-27`, `is_active=false`, `status=delisted`, and `trading_status=0`
- **AND** BaoStock-only cross-check evidence SHALL NOT override official lifecycle state or substitute for required official confirmation

#### Scenario: Fallback source lacks delisting date
- **WHEN** a fallback source returns the same confirmed-delisted instrument with null `delisted_date`
- **THEN** the system SHALL NOT clear its existing date or reactivate it
- **AND** all three lifecycle fields SHALL remain consistent

#### Scenario: Repeated official terminal write omits the date
- **WHEN** the terminal writer receives confirmed completed evidence without a date for an already-dated delisting
- **THEN** it SHALL preserve the existing date and SHALL NOT count another new deactivation

#### Scenario: Effective date is in the future
- **WHEN** the terminal writer is asked to mark an instrument delisted using a future effective date
- **THEN** it SHALL reject premature terminal mutation
- **AND** governance SHALL retain the future decision for reevaluation without falsely reporting a successful deactivation

#### Scenario: Confirmed date-unknown terminal state receives current-list data
- **WHEN** a formally delisted instrument has null `delisted_date` and a later official or fallback current-list row reports active
- **THEN** the system SHALL preserve terminal evidence and `status=delisted / is_active=false / trading_status=0`
- **AND** it SHALL report the conflicting current evidence instead of automatically reactivating the instrument

### Requirement: A-share stock sync is exposed as a governance policy
The A-share stock instrument master sync SHALL be available through the shared master-governance policy registry as `a_share_stock`.

#### Scenario: A-share stock policy refreshes all stock exchanges
- **WHEN** the `a_share_stock` policy receives exchanges `SSE`, `SZSE`, and `BSE`
- **THEN** it SHALL run existing A-share stock synchronization through exchange-official-first authority, BaoStock first fallback, and AkShare second fallback
- **AND** it SHALL include SSE/SZSE terminal reconciliation while retaining BSE source-specific confirmation and visible fallback warnings

#### Scenario: BSE is included in stock policy
- **WHEN** a business job needs A-share stock master data for all A-share exchanges
- **THEN** `BSE` SHALL be included in the `a_share_stock` requirement
- **AND** the system SHALL NOT require a separate BSE scheduler task solely to make BSE current before quote updates

#### Scenario: Stock policy result is normalized
- **WHEN** the `a_share_stock` policy completes
- **THEN** the policy result SHALL expose per-exchange added, updated, deactivated, active, warning, error, and source usage counters in the shared governance result format
- **AND** it SHALL retain terminal-confirmation detail without changing existing result field meanings
