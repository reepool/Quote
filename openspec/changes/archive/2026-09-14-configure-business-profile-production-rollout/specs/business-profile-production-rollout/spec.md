## ADDED Requirements

### Requirement: Production rollout SHALL use a versioned staged configuration
The system SHALL load a versioned business-profile rollout plan that identifies one active phase, its field families, selection policy, promotion mode, stage budgets, prerequisites, and readiness thresholds.

#### Scenario: Initial deployment
- **WHEN** the production rollout configuration is first deployed
- **THEN** `structured_shadow` is the active phase and daily scheduling and automatic promotion remain disabled

#### Scenario: Unknown or disabled phase
- **WHEN** a rollout task requests a phase that is absent or disabled
- **THEN** the task fails before discovery, network access, queue creation, or fact writes

### Requirement: Full-market bootstrap SHALL enqueue only the latest active annual report
The system SHALL support a bounded manual bootstrap that scans a historical official-announcement frontier but enqueues at most one latest eligible annual-report work item per issuer and processing identity.

#### Scenario: Multiple annual periods in the discovery range
- **WHEN** the frontier contains multiple annual-report periods for one issuer before the knowledge cutoff
- **THEN** bootstrap enqueues only the newest eligible period and does not enqueue older annual periods

#### Scenario: Annual-report correction exists
- **WHEN** an active correction or replacement exists for the issuer's latest annual period
- **THEN** bootstrap selects the newest active correction while retaining the original manifest and file history

#### Scenario: Repeated bounded bootstrap
- **WHEN** the same phase is run repeatedly with unchanged source and processing identities
- **THEN** completed work and verified annual-report assets are reused and remaining durable work continues without duplicates

### Requirement: Runtime identities SHALL describe the actual production implementation
The system SHALL derive runtime identities from the active document, section, selector, parser, schema, catalog, model, verifier, rule, and policy implementations and SHALL fail closed when configured or promotion-manifest identities differ.

#### Scenario: Model configuration changes
- **WHEN** the semantic extraction provider, profile, or model changes
- **THEN** the derived processing identity changes and prior completed semantic work is not mislabeled or silently reused

#### Scenario: Catalog version changes
- **WHEN** the fact, product, unit, or disclosure-template catalog version changes
- **THEN** the derived catalog identity changes and approved promotion requires a new matching manifest

### Requirement: Shadow phases SHALL persist work without approved publication
The system SHALL allow discovery, acquisition, parsing, semantic extraction, verification, candidate writes, exceptions, and machine rework while promotion is disabled.

#### Scenario: Structured shadow result
- **WHEN** a structured shadow task extracts valid segments or operating facts
- **THEN** the records are stored as candidates with evidence and lineage and no record is automatically approved

#### Scenario: Shadow exception
- **WHEN** an extraction fails a deterministic or semantic gate
- **THEN** the reason is persisted as machine rework or a bounded review exception instead of being discarded

### Requirement: Promotion phases SHALL require real passed manifests
The system SHALL reject an approved-write phase unless every enabled field family has a complete promotion manifest with `enabled=true`, `benchmark_passed=true`, and identities equal to the derived runtime identities.

#### Scenario: Manifest is only a template
- **WHEN** a promotion phase references a manifest whose benchmark has not passed
- **THEN** the task returns not-ready and performs no approved writes

#### Scenario: All promotion gates match
- **WHEN** the field-family manifest is passed, complete, current, and all candidate gates succeed
- **THEN** the system may auto-promote the record and append an immutable system review audit

### Requirement: Daily scheduling SHALL remain closed until bootstrap readiness
The system SHALL keep daily cron disabled until historical frontier discovery is complete, required latest-annual coverage meets configured thresholds, blocking queue failures are resolved or classified, required field families are approved, and the daily phase is explicitly activated.

#### Scenario: Bootstrap queue still has claimable work
- **WHEN** any required rollout phase has pending or retry-due work
- **THEN** readiness reports daily activation as false

#### Scenario: Explicit daily transition
- **WHEN** all configured readiness gates pass and the operator activates the daily phase and scheduler switches
- **THEN** the daily task performs discovery-first latest-annual incremental production using the same durable queues and identities

### Requirement: Operational commands SHALL accept rollout scope types correctly
The task-manager runtime parser SHALL treat `field_families`, `document_types`, exchanges, and instrument ids as list-valued parameters and SHALL preserve explicit latest-annual versus expanded selection policy.

#### Scenario: Scoped latest-annual command
- **WHEN** an operator starts manual backfill with comma-separated field families, document types, or instruments and `selection_policy=latest_annual_only`
- **THEN** the scheduler receives typed lists and uses latest-annual coalescing rather than iterating characters or expanding historical periods
