# instrument-master-sync

## Purpose
This capability defines A-share instrument master synchronization before daily market-data maintenance.
## Requirements
### Requirement: A-share instrument master sync
The system SHALL provide a structured instrument master synchronization capability for A-share exchanges `SSE`, `SZSE`, and `BSE`.

#### Scenario: Sync returns structured results
- **WHEN** instrument master sync runs for one or more A-share exchanges
- **THEN** it SHALL return per-exchange counts for active instruments before sync, active instruments after sync, added instruments, updated instruments, deactivated instruments, unchanged instruments, warnings, errors, source used, and elapsed time

#### Scenario: Sync updates local instruments
- **WHEN** the primary or fallback source returns a valid instrument record
- **THEN** the system SHALL upsert the record into `instruments` without deleting historical quotes

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

### Requirement: New listed instruments are added
The system SHALL add newly listed A-share instruments discovered by the master-data sync before quote updates use the active universe.

#### Scenario: BSE current list contains new instruments
- **WHEN** the source list contains BSE instruments that are absent from local `instruments`
- **THEN** the system SHALL insert those instruments with normalized `.BJ` instrument IDs and include them in the active universe for subsequent normal daily quote updates

#### Scenario: Added instruments are reported
- **WHEN** a sync inserts new instruments
- **THEN** the daily update report SHALL include the inserted count and representative instrument IDs per exchange

### Requirement: Normal daily update refreshes master data first
The system SHALL run A-share instrument master sync before a normal daily quote update and then re-read the active instrument universe for quote fetching.

#### Scenario: Normal daily update runs
- **WHEN** `daily_data_update` runs without an explicit past `target_date`
- **THEN** the scheduler SHALL run instrument master sync before quote fetching
- **AND** quote fetching SHALL use the post-sync active instrument list

#### Scenario: Master sync fails before daily update
- **WHEN** pre-run instrument master sync fails or degrades
- **THEN** the daily update report SHALL include the failure or degraded status and the configured continuation behavior

### Requirement: Historical backfill does not refresh current master data by default
The system SHALL keep explicit historical backfill runs isolated from automatic current-universe refresh unless a caller explicitly requests it.

#### Scenario: Past target date backfill runs
- **WHEN** `daily_data_update` is invoked with a `target_date` before the current local date
- **THEN** the scheduler SHALL skip automatic instrument master sync by default
- **AND** the report SHALL include `instrument_master_sync.status=skipped` with a backfill skip reason

### Requirement: Daily report includes master freshness
The daily update report SHALL include instrument master synchronization and freshness diagnostics.

#### Scenario: Report is generated after normal daily update
- **WHEN** a normal daily update report is generated
- **THEN** the report SHALL include `instrument_master_sync` with source usage, per-exchange counts, warnings, errors, and freshness timestamps derived from `instruments.updated_at`

#### Scenario: Master data is stale
- **WHEN** one or more A-share exchange instrument rows are older than the configured freshness threshold
- **THEN** the report SHALL surface a freshness warning for those exchanges

### Requirement: Delisting audit script current-list comparison covers STAR Market
The delisting audit script SHALL compare SSE current listed stocks using both main-board A-shares and STAR Market current-list data.

#### Scenario: AkShare SSE current-list audit runs
- **WHEN** `scripts/fix_delisted_status.py --dry-run` runs Step 3
- **THEN** the SSE current-list set SHALL include both `stock_info_sh_name_code(symbol="主板A股")` and `stock_info_sh_name_code(symbol="科创板")`
- **AND** normal current STAR Market instruments SHALL NOT be reported as disappeared solely because the audit omitted the STAR Market list

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

### Requirement: A-share stock policy preserves current backfill behavior
The A-share stock policy SHALL preserve the existing distinction between normal current runs and historical backfills.

#### Scenario: Normal daily update invokes stock policy
- **WHEN** `daily_data_update` runs for the current or future local date and declares `a_share_stock`
- **THEN** the policy SHALL refresh or reuse A-share stock master data according to the configured mode
- **AND** quote fetching SHALL use the post-policy active stock universe

#### Scenario: Historical daily update skips stock policy
- **WHEN** `daily_data_update` runs for a past target date without explicit current-refresh override
- **THEN** the `a_share_stock` policy SHALL be skipped with a historical-backfill reason
- **AND** quote fetching SHALL keep the explicit historical update semantics
