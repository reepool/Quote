## ADDED Requirements

### Requirement: Scheduler SHALL separate business-profile discovery from semantic processing
The scheduler SHALL provide an index-only discovery job independently from PDF acquisition and semantic production.

#### Scenario: Discovery job execution
- **WHEN** the business-profile discovery task runs
- **THEN** it performs no PDF download, LLM call, candidate promotion, or valuation mutation

### Requirement: Business-profile jobs SHALL be disabled until production identities are complete
Scheduled semantic promotion SHALL remain disabled when field families, runtime identities, or required promotion manifests are empty or inconsistent.

#### Scenario: Empty production configuration
- **WHEN** a scheduled semantic task has no field families or promotion identities
- **THEN** it fails configuration validation with an explicit operational report instead of completing as a no-op
