## ADDED Requirements

### Requirement: Validation evidence remains source-specific
The system SHALL distinguish TDX raw company-action events, Eastmoney structured distribution records accessed through AkShare, CNInfo official announcement metadata, and provider cumulative-factor evidence.

#### Scenario: AkShare wrapper is used
- **WHEN** the system loads `stock_fhps_em` records
- **THEN** it SHALL identify the upstream source as Eastmoney and the adapter as AkShare

#### Scenario: CNInfo metadata matches an instrument
- **WHEN** a CNInfo implementation announcement is found for an instrument
- **THEN** the system SHALL report official-announcement existence evidence without claiming that metadata validates the distribution amounts

### Requirement: Event fields are reconciled independently of factors
The system SHALL reconcile implemented Eastmoney cash-dividend and bonus-share records against TDX events by normalized instrument and ex-date.

#### Scenario: Event date and fields agree
- **WHEN** TDX and Eastmoney have the same ex-date and their per-10-share cash and bonus values are within field tolerance
- **THEN** the event SHALL be classified as an exact event-field match

#### Scenario: Event date agrees but fields differ
- **WHEN** TDX and Eastmoney have the same ex-date but cash or bonus values exceed field tolerance
- **THEN** the event SHALL be classified as a field conflict with both source values

#### Scenario: Rights-only TDX event has no Eastmoney row
- **WHEN** a TDX event contains only rights-issue fields and no Eastmoney distribution row exists
- **THEN** it SHALL be classified as unsupported by that reference contract rather than a missing TDX or missing Eastmoney event

### Requirement: Cumulative factor paths are compared at stable anchors
The system SHALL rebuild unit-baseline cumulative paths from event-day factors and compare TDX with each reference provider at year-end and latest anchors.

#### Scenario: Latest normalized error is small
- **WHEN** the latest cumulative factor error is at or below the configured acceptable threshold
- **THEN** the cumulative result SHALL be classified as acceptable

#### Scenario: Intermediate anchors diverge but latest converges
- **WHEN** year-end anchors contain material divergence even though the latest factors converge
- **THEN** the report SHALL retain the historical anchor conflicts and SHALL NOT infer event completeness from latest convergence

### Requirement: Validation is read-only and bounded
The system SHALL produce bounded summaries and follow-up samples without updating production factors or TDX event rows.

#### Scenario: Validation completes with conflicts
- **WHEN** event-field, source-coverage, or cumulative conflicts remain
- **THEN** the result SHALL be partial and SHALL include bounded samples and explicit reason codes
