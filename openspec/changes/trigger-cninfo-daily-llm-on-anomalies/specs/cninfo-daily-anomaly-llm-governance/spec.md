## ADDED Requirements

### Requirement: Ordinary structured events bypass semantic analysis
The daily CNInfo workflow SHALL keep complete ordinary distributions on the
deterministic structured path without invoking an LLM.

#### Scenario: Ordinary structured dividend is complete
- **WHEN** a refreshed event is `structured_complete`, has no exceptional title marker, and has no material current-run reconciliation conflict
- **THEN** the event SHALL NOT be submitted to title classification, document extraction, or semantic analysis

### Requirement: Daily anomaly selection is deterministic and auditable
The system SHALL assign stable reason codes to every event selected for semantic
governance.

#### Scenario: Structured event is incomplete
- **WHEN** a current refreshed CNInfo event has a `partial_*` quality status
- **THEN** its source event key SHALL be selected with an incomplete-structured-event reason

#### Scenario: Exceptional implementation title is observed
- **WHEN** a daily announcement title contains an implementation-grade
  restructuring, compensation, share-reform, debt-conversion, shrinkage, or
  asymmetric-distribution marker
- **THEN** an associable current CNInfo event SHALL be selected with the matched
  exceptional markers in its audit reasons

#### Scenario: Current event conflicts with TDX
- **WHEN** a newly inserted or changed CNInfo event has a material economic
  reconciliation conflict with the TDX reference path
- **THEN** its CNInfo source event key SHALL be selected without copying TDX
  economic terms into CNInfo

### Requirement: Selected anomalies reuse governed semantic resolution
Selected daily anomaly events SHALL use the existing official-document archive,
LLM title classification, semantic schema, deterministic validation, resume,
and auto-promotion policies.

#### Scenario: High-confidence anomaly passes every gate
- **WHEN** the semantic result has official quoted evidence and satisfies the
  existing deterministic auto-promotion policy
- **THEN** the result MAY be promoted through the governed overlay and SHALL NOT
  modify the raw CNInfo observation

#### Scenario: Semantic result remains ambiguous
- **WHEN** an anomaly fails an evidence, context, consistency, or asymmetric
  economic gate
- **THEN** it SHALL remain manual review workload and SHALL NOT change the
  factor path

### Requirement: Unmatched special announcements remain deferred
The system SHALL preserve exceptional implementation announcements that do not
yet have an associable structured CNInfo event.

#### Scenario: Structured endpoint has not published the event
- **WHEN** an exceptional daily announcement selects an instrument but the
  refreshed structured response contains no bounded matching event
- **THEN** the instrument SHALL remain in the deferred refresh queue with reason
  `unmatched_special_announcement` and no synthetic CNInfo event SHALL be created

### Requirement: Daily semantic work is bounded and resumable
The daily workflow SHALL enforce a configurable anomaly event cap and SHALL
reuse existing validated or manual analysis when its input lineage is unchanged.

#### Scenario: Anomaly volume exceeds the cap
- **WHEN** selected event keys exceed the configured daily semantic limit
- **THEN** the bounded prefix SHALL run and remaining event keys SHALL be
  reported as deferred for a later run

#### Scenario: Identical analysis already exists
- **WHEN** an anomaly has a resumable analysis with the same document and input
  hashes
- **THEN** the workflow SHALL reuse and revalidate it without a new model call

### Requirement: Semantic readiness is separate from execution health
The daily result SHALL report semantic execution status and review readiness
separately from structured source freshness.

#### Scenario: Analysis succeeds but review is required
- **WHEN** all selected anomaly work executes successfully but one or more
  results require human review
- **THEN** semantic readiness SHALL be `partial` while operational source status
  MAY remain `success`

#### Scenario: Semantic execution fails
- **WHEN** an enabled anomaly has an LLM, document, or persistence failure
- **THEN** daily operational status SHALL be `partial` and the last validated
  source and factor observations SHALL be preserved

### Requirement: Promoted anomalies are reflected in the same daily run
The task SHALL rebuild source-isolated factors for anomaly instruments after a
governed overlay is promoted.

#### Scenario: Auto-promotion writes a governed overlay
- **WHEN** at least one daily semantic result is promoted
- **THEN** the final daily factor result SHALL be rebuilt for the affected
  anomaly instruments before the task completes
