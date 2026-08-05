## ADDED Requirements

### Requirement: Source-aware completion status
The financial disclosure incremental sync SHALL report successful completion when final local collection is complete, even if the official CNInfo attempt was incomplete and a configured fallback supplied the final facts.

#### Scenario: Fallback completes all targets
- **WHEN** CNInfo is unavailable or does not provide ready facts
- **AND** Sina/THS fallback writes or confirms every selected target
- **AND** there are no failed writes, blockers, mapping gaps, pending unresolved repairs, or scan errors
- **THEN** the run status SHALL be `success`
- **AND** the run SHALL retain official-source diagnostics as a warning or source-health field

#### Scenario: Fallback leaves an unresolved target
- **WHEN** the official source and all configured fallback sources fail to make one or more targets ready
- **THEN** the run status SHALL remain `degraded` or `failed` according to existing blocker rules
- **AND** the report SHALL identify the unresolved targets or blocker count

### Requirement: Final source classification
The incremental sync SHALL expose the source that supplied final collection results independently from completion status.

#### Scenario: Official source supplies all changed targets
- **WHEN** all changed targets are completed by CNInfo data20 and no fallback target succeeds
- **THEN** the source classification SHALL be `cninfo`

#### Scenario: Fallback supplies all changed targets
- **WHEN** fallback sources complete all changed targets after official-source misses
- **THEN** the source classification SHALL be `fallback`
- **AND** the operator report SHALL state that the final data source was not CNInfo

#### Scenario: Official and fallback both contribute
- **WHEN** some changed targets are completed by CNInfo and other changed targets are completed by fallback sources
- **THEN** the source classification SHALL be `mixed`

### Requirement: Operator report separates source health from completion
The scheduler report SHALL display final completion status and source classification as separate fields.

#### Scenario: Successful fallback report
- **WHEN** a run succeeds using fallback data
- **THEN** the report SHALL display a success conclusion
- **AND** SHALL include a source label identifying the fallback source family
- **AND** SHALL retain CNInfo attempt, ready, and warning counts for operator diagnosis
