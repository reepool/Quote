## ADDED Requirements

### Requirement: Structured ambiguity invokes bounded semantic extraction
The system SHALL invoke the configured semantic gateway for `structured_segments` or `tabular_operating_facts` only when bounded selected annual-report sections contain governed structured signals and deterministic parsing produces no usable records.

#### Scenario: Deterministic rows are usable
- **WHEN** selected sections produce evidence-backed deterministic segment or operating-fact rows
- **THEN** the system persists those deterministic candidates
- **AND** does not invoke structured semantic fallback for that document and field family

#### Scenario: Governed table layout is ambiguous
- **WHEN** selected sections contain a governed table signature or parser diagnostic but deterministic parsing produces no usable rows
- **THEN** the system sends only bounded selected snippets to the structured semantic gateway
- **AND** records the fallback reason, page count, call count, and outcome

#### Scenario: No structured disclosure exists
- **WHEN** selected sections contain neither a governed structured signal nor explicit structured facts
- **THEN** the system records `expected_non_disclosure`
- **AND** does not create zero-valued facts or repeatedly retry the document

### Requirement: Structured semantic output is evidence constrained
The system MUST accept structured semantic rows only when local validation proves that each row is supported by an allowed selected page and exact quote and satisfies the closed structured schema.

#### Scenario: Valid segment row
- **WHEN** the model returns a segment dimension, label, supported numeric values, unit information, and an exact allowed evidence reference
- **THEN** the system converts the row into an evidence-backed segment candidate
- **AND** records semantic derivation lineage separately from deterministic parsing

#### Scenario: Valid operating-fact row
- **WHEN** the model returns a governed operating metric, supported numeric value, unit, period, and exact allowed evidence reference
- **THEN** the system converts the row into an evidence-backed operating-fact candidate
- **AND** retains the source page, quote hash, and selected-artifact identity

#### Scenario: Unsupported or partially invalid output
- **WHEN** any model row cites an unselected page, mismatched quote, unsupported type, invalid number, or inferred undisclosed value
- **THEN** the system rejects the response atomically
- **AND** persists no facts from that response

### Requirement: Disabled semantic networking does not consume content retries
The system SHALL distinguish a deliberately disabled or unavailable semantic gateway from a content or model failure that can benefit from retry.

#### Scenario: Network kill switch is enabled
- **WHEN** structured semantic fallback is required while semantic network calls are disabled
- **THEN** the work item remains resumable at the semantic stage
- **AND** its content-attempt count is not incremented
- **AND** operator telemetry reports `blocked_configuration`

#### Scenario: Gateway or schema failure occurs
- **WHEN** the enabled gateway times out, fails, or returns invalid structured output
- **THEN** the work item follows the bounded retry policy
- **AND** the failure reason remains distinct from expected non-disclosure

### Requirement: Existing ambiguous structured retries are recoverable
The system SHALL provide an idempotent recovery path for structured semantic retries proven to have selected evidence pages but zero records because of ambiguous layout or deterministic parser failure.

#### Scenario: Recover affected retry
- **WHEN** a retrying or terminal semantic work item has a valid manifest, selected pages, zero evidence, and an affected structured zero-output classification
- **THEN** recovery resets it to semantic without redownloading or reparsing the annual report
- **AND** preserves checkpoint and recovery history

#### Scenario: Preserve unrelated work
- **WHEN** a work item has evidence-backed output, expected non-disclosure, a different failure, or no valid bound manifest
- **THEN** recovery leaves it unchanged

#### Scenario: Repeat recovery
- **WHEN** recovery is run again after an affected item has already been reset
- **THEN** the item is not reset or counted again

### Requirement: Operator telemetry reports semantic fallback progress
The system SHALL report structured semantic fallback requirements, calls, accepted and rejected records, configuration blockers, expected non-disclosures, and retry reasons separately from deterministic records and effective publication.

#### Scenario: Bounded batch uses structured fallback
- **WHEN** a batch processes ambiguous structured tables
- **THEN** the report exposes fallback-required documents, LLM calls, accepted evidence records, rejected outputs, and remaining blockers
- **AND** effective publication increases only for accepted evidence-backed output or governed non-disclosure completion
