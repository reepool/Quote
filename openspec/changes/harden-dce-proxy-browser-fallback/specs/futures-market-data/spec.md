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
