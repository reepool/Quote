## ADDED Requirements

### Requirement: Independent Daily Annual-Report Update

The scheduler SHALL run annual-report discovery independently of all consumer
modules at least once per project calendar day.

#### Scenario: Daily run discovers new filings

- **WHEN** the configured daily job runs
- **THEN** it scans supported SSE/SZSE/BSE routes using persisted cursors and a
  bounded overlap window
- **AND** persists new metadata before downloading eligible attachments

#### Scenario: Discovery succeeds but attachment download fails

- **WHEN** a discovery window completes and one attachment fails
- **THEN** the discovery cursor may advance
- **AND** the failed attachment remains in a retryable state

### Requirement: Idempotent Scheduling

The scheduler SHALL prevent overlapping equivalent runs and SHALL make repeated
discovery of the same source announcement idempotent.

#### Scenario: Equivalent run is already active

- **WHEN** cron or an operator starts the same daily scope concurrently
- **THEN** the existing operation is reused or the duplicate start is rejected
