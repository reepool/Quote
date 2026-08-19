## ADDED Requirements

### Requirement: Annual-Report Scheduling Must Use Shared Asset Jobs Only
Scheduled and manual annual-report discovery, download, repair, integrity, and latest-report backfill SHALL execute only through shared announcement asset application services and task adapters.

#### Scenario: Daily annual-report maintenance runs
- **WHEN** the configured annual-report daily job starts
- **THEN** it SHALL invoke the shared announcement asset daily workflow
- **AND** no legacy business-profile archive synchronization job SHALL run

#### Scenario: A consumer requires missing coverage
- **WHEN** business-profile or broker processing reports a missing shared asset
- **THEN** an operator or dependency SHALL invoke the shared ensure/backfill operation
- **AND** the consumer SHALL resume from the shared asset after it becomes ready

#### Scenario: Legacy job id is requested
- **WHEN** a caller requests a retired annual-report archive sync job or command
- **THEN** command resolution SHALL reject it as unavailable rather than silently executing duplicate acquisition logic
