## MODIFIED Requirements

### Requirement: Each semantic request is bounded to one chapter task
The workflow MUST define separate versioned `extract`, `repair`, and `verify` request and response models. Every request MUST identify one report, one active package manifest, one chapter task, its checklist, allowed object and enum values, prohibited inferences, and a continuous Evidence bundle containing required headers, units, footnotes, and continuation pages. The provider-facing Evidence catalog MUST classify each Evidence item as `field_owner`, `context_only`, or `mixed`, and MUST list the exact checklist field IDs that own it. An unknown task is rejected before provider invocation. Missing required preparation inputs MUST fail before an LLM provider is called.

#### Scenario: Context-only Evidence is present
- **WHEN** a request includes an Evidence item with no prepared field binding
- **THEN** the provider catalog marks it `context_only` with an empty owning-field list
- **AND** the item remains available for context but cannot be treated as field-owning proof

#### Scenario: One Evidence item supports multiple fields
- **WHEN** the same Evidence item is prepared with bindings for two checklist fields
- **THEN** the provider catalog marks it `mixed` and lists both exact field IDs
- **AND** it does not authorize the Evidence for any unlisted field

### Requirement: Verification is independent and cannot mutate facts
Verify MUST evaluate each candidate and each active checklist item against the original Evidence, returning `pass`, `block`, or `unclear` checks and reason codes. It MUST NOT add candidates, edit source values, choose package assignment, grant approval, perform canonical conversion, or waive a blocking check through an aggregate score. Legal-empty coverage MUST be accepted only when the cited Evidence owns the requested field or the source explicitly contains the requested field’s governed not-applicable disclosure; context-only Evidence alone MUST remain insufficient.

#### Scenario: Context-only Evidence is used for legal-empty coverage
- **WHEN** a model cites contextual page text but no Evidence item owns the requested checklist field
- **THEN** verification blocks or leaves the coverage unresolved with the existing typed coverage reason
- **AND** it does not treat the contextual text as proof that the field is undisclosed or not applicable

#### Scenario: Control-scope wording is narrower than business regime
- **WHEN** Evidence says only that consolidation scope or control did not change
- **THEN** verification does not accept that statement as broader principal-business, product, service, or statistical-regime no-change coverage
- **AND** an owning broader regime disclosure is still required

### Requirement: Candidate responses are schema and semantic constrained
Every candidate response MUST preserve request identity, allowed object/metric/action values, source-native data, physical Evidence, subject and period semantics, uncertainty, and prohibited-inference status. It MUST preserve `processing_direction`, `identity_class`, `row_class=consolidation_adjustment`, `activity_actor`, and `comparison_basis` when applicable; missing comparison basis on a restated comparative is a blocker, not a repairable guess. A cost-component or generic operating-cost row MUST NOT be emitted as a business Segment unless the supplied field-owning Evidence explicitly identifies a segment dimension and segment row. The validator MUST reject JSON-external prose, unknown enums, canonicalized source values, unrequested fields, missing required capacity/comparison semantics, and Activity/Measurement mixing.

#### Scenario: Cost composition is mistaken for a segment
- **WHEN** a model cites a cost-component table as the basis for a business Segment without a field-owning segment dimension
- **THEN** the response is rejected or remains unresolved through the existing schema/semantic path
- **AND** the cost fact is not relabeled as a business segment
