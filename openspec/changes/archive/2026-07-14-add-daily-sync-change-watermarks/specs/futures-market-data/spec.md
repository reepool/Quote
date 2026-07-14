## ADDED Requirements

### Requirement: Futures Market Data Writes Emit Shared Change Records
Futures market-data sync SHALL map existing inserted, changed, and unchanged bar classifications into the shared daily-sync changelog contract.

#### Scenario: Contract bar hash changes
- **WHEN** a futures contract price bar exists and the incoming bar has a different semantic hash
- **THEN** the futures storage path SHALL update the bar
- **AND** it SHALL append a futures-domain change record with the contract id, trade date, source, and source mode

#### Scenario: Contract bar hash matches
- **WHEN** a futures contract price bar exists and the incoming bar hash matches the stored hash
- **THEN** the futures storage path SHALL count the row as unchanged
- **AND** it SHALL NOT append a change record

### Requirement: Futures Continuous Series Changes Are Separately Identified
Futures continuous-series observations SHALL emit change records using series-level keys instead of only contract-level keys.

#### Scenario: Continuous series row changes
- **WHEN** a continuous futures series observation changes after roll or source repair
- **THEN** the changelog SHALL identify the series id and trade date affected
