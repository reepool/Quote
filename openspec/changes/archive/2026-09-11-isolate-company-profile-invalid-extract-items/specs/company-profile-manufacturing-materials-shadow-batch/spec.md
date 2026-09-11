## ADDED Requirements

### Requirement: Invalid extract items are isolated without accepting unsupported facts
When a provider returns a schema-parseable extract response, the Stage 5 adapter MUST validate candidate and coverage items independently at the existing local semantic boundaries. An item that violates Evidence ownership, source-text binding, business-regime scope, counterparty direction, or another existing item contract MUST be excluded without discarding independently valid sibling items. Every excluded item MUST produce a bounded typed `candidate_schema_invalid` diagnostic. The adapter MUST NOT rewrite the item, rebind Evidence, infer a direction, or relax subject, metric, unit, period, actor, or source-native requirements.

#### Scenario: One invalid item accompanies a valid sibling
- **WHEN** a schema-parseable extract response contains one independently valid item and one item that fails a local semantic or Pydantic contract
- **THEN** the valid item proceeds through the unchanged Stage 5 workflow
- **AND** the invalid item is excluded and recorded as a bounded typed rejected-item diagnostic

#### Scenario: No valid result remains for a required field
- **WHEN** every returned item for an active required field is excluded by item-level validation
- **THEN** the system leaves that field unresolved through the existing coverage workflow
- **AND** it does not synthesize observed, not-disclosed, not-applicable, or production-approved data

#### Scenario: Response envelope or identity is invalid
- **WHEN** the provider response is not a parseable object, violates the response envelope, or returns a mismatched request identity
- **THEN** the extract call remains a typed call-level failure
- **AND** item isolation does not hide or downgrade the envelope failure

#### Scenario: Historical failure shapes are replayed without a provider
- **WHEN** the frozen 29-row external replay failure inventory and the five equivalent mixed-response fixture families are evaluated
- **THEN** every historical trace is classified and every fixture proves valid-sibling retention plus invalid-item exclusion with zero provider calls
- **AND** the audit states that unavailable raw historical payloads prevent claiming an actual historical candidate-salvage count

#### Scenario: Research and production boundaries remain closed
- **WHEN** item isolation and its provider-free proof pass
- **THEN** the frozen external bundle remains unchanged and `production_authorization=not_authorized`
- **AND** the change does not enable Stage 6, approved writes, scheduler/backfill, commodity exposure, value-chain publication, DCF, a new shadow replay, or adaptive token budgets
