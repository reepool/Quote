## ADDED Requirements

### Requirement: Reviewed code-lineage catalog
The system SHALL load A-share security-code lineage only from a versioned,
validated catalog containing a canonical instrument id, security-code history
start, issuer regimes, transition policies, source evidence, and reviewed row
decisions.

#### Scenario: Valid reviewed entry is loaded
- **WHEN** a catalog entry has ordered issuer regimes, valid dates, supported continuity policies, and complete reviewed decisions
- **THEN** the system SHALL expose a typed lineage entry without changing the canonical instrument id

#### Scenario: Invalid entry is loaded
- **WHEN** a catalog entry has overlapping regimes, an invalid date, an unsupported continuity policy, or an incomplete reviewed decision
- **THEN** the system SHALL fail before fetching sources or writing quotes

### Requirement: Leading history gaps are reported
The system SHALL detect a leading quote gap by comparing the governed
security-code history start with the earliest stored quote and SHALL distinguish
that range from interior quote gaps.

#### Scenario: Stored history starts after governed code history
- **WHEN** the earliest local quote is later than the catalogued code-history start
- **THEN** the audit SHALL report a leading-history gap with both boundary dates
- **AND** it SHALL NOT silently treat the first stored quote as complete-history evidence

#### Scenario: Stored history reaches governed code history
- **WHEN** the earliest local quote equals the catalogued code-history start
- **THEN** the audit SHALL report no leading-history gap

### Requirement: Multi-source repair is reviewed and fail closed
The repair workflow SHALL compare normalized pytdx history with an independent
AkShare/Tencent history and SHALL resolve missing dates or conflicting OHLC
values only through catalogued review decisions.

#### Scenario: Approved independent-source-only date is encountered
- **WHEN** `600018.SH` date `2001-08-16` is absent from pytdx and present in the independent source with the reviewed values
- **THEN** the repair plan SHALL include that independent-source row

#### Scenario: Approved pytdx close is encountered
- **WHEN** sources disagree on `600018.SH` date `2003-11-17`
- **THEN** the repair plan SHALL select the pytdx row with close `12.95`
- **AND** the report SHALL preserve the conflicting source values

#### Scenario: Newly observed pytdx conflict is independently resolved
- **WHEN** pytdx reports close `13.70` and AkShare/Tencent reports close `13.71` on `600018.SH` date `2003-07-16`
- **THEN** the repair plan SHALL select the AkShare/Tencent row with close `13.71`
- **AND** the reviewed evidence SHALL identify the matching Sohu history row

#### Scenario: Unreviewed conflict is encountered
- **WHEN** normalized sources disagree on OHLC for a date without a matching reviewed decision
- **THEN** the workflow SHALL fail closed for writes
- **AND** the report SHALL identify the unresolved conflict

### Requirement: Repair is dry-run-first and missing-only
The repair command SHALL default to dry-run, SHALL accept only catalogued
instruments, and SHALL insert only dates absent from local `daily_quotes`.

#### Scenario: Command runs without apply authorization
- **WHEN** an operator runs the repair command without the explicit apply flag
- **THEN** it SHALL report the proposed rows and diagnostics without modifying quotes or metadata

#### Scenario: Existing local date is in source history
- **WHEN** a reviewed source row has a date already present in local storage
- **THEN** the workflow SHALL exclude it from the write set
- **AND** it SHALL NOT update or overwrite the existing row

#### Scenario: Repair is rerun after success
- **WHEN** the same reviewed repair is applied again
- **THEN** zero duplicate quote rows SHALL be inserted
- **AND** the report SHALL identify the run as already complete or idempotent

### Requirement: Transition continuity is explicit
The system SHALL report issuer transition boundaries and SHALL prevent a
catalogued non-continuous boundary from being represented as an ordinary
continuous raw-price interval.

#### Scenario: Absorption-merger boundary is audited
- **WHEN** the `600018.SH` lineage crosses `2006-10-26`
- **THEN** the audit SHALL mark the boundary as non-continuous
- **AND** it SHALL report the last predecessor quote and first current-issuer quote

#### Scenario: No authoritative merger factor exists
- **WHEN** a transition is catalogued as non-continuous without authoritative conversion evidence
- **THEN** the workflow SHALL NOT synthesize or persist an adjustment factor

### Requirement: Applied lineage evidence is persisted
After a successful quote repair, the system SHALL persist the catalog version,
issuer regimes, source evidence, reviewed decisions, applied row summary, and
transition policy in instrument master metadata.

#### Scenario: Quote apply succeeds
- **WHEN** all approved missing rows are inserted and persisted coverage passes validation
- **THEN** the system SHALL save the reviewed lineage evidence in `instrument_master_metadata.metadata_json`

#### Scenario: Quote apply fails
- **WHEN** quote insertion or persisted coverage validation fails
- **THEN** the system SHALL NOT report the lineage repair as successfully applied
- **AND** it SHALL NOT replace the previous successful lineage metadata
