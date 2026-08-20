## ADDED Requirements

### Requirement: Product-page browser fallback is exchange scoped
The system SHALL keep browser-assisted product enrichment scoped to the exchange whose official page or business endpoint is being accessed.

#### Scenario: DCE contract information is enriched
- **WHEN** DCE `contractInfo` or a DCE product page is requested during master governance
- **THEN** the system SHALL use the same validated DCE browser route and session used by the authoritative DCE provider

#### Scenario: Another exchange product page needs fallback
- **WHEN** a CZCE, INE, SHFE, or GFEX product page needs browser-assisted fallback
- **THEN** the fallback SHALL NOT initialize, wait for, or circuit-break on the DCE browser challenge
- **AND** a DCE access failure SHALL NOT delay that exchange's master governance

### Requirement: DCE browser request lifecycle is not orphaned by caller timeout
The market-data sync SHALL let the bounded DCE browser client own timeout and cleanup for official DCE exchange payload requests.

#### Scenario: A DCE route takes longer than the generic source timeout
- **WHEN** a DCE exchange payload request is still rotating bounded browser routes after the generic official-source timeout elapses
- **THEN** the sync SHALL continue awaiting that request until the DCE client succeeds or reaches its own bounded terminal result
- **AND** it SHALL NOT start fallback or queue another DCE date while the prior browser request remains active
