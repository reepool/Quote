## ADDED Requirements

### Requirement: Shadow provider requests expose Evidence ownership roles
The shadow batch provider adapter MUST serialize the existing prepared Evidence bindings as explicit `field_owner`, `context_only`, or `mixed` roles in extract, repair, and verify request payloads. The role metadata MUST be derived deterministically from prepared field IDs, preserve the existing `field_ids` compatibility key, and MUST NOT change Evidence IDs, source text, field contracts, or acceptance behavior.

#### Scenario: Unbound page context is sent with a request
- **WHEN** a prepared Evidence item has no `field_id`
- **THEN** the request catalog marks it `context_only` and lists no owning field IDs
- **AND** the item remains traceable by its original Evidence ID

#### Scenario: Field-bound Evidence is sent with a request
- **WHEN** a prepared Evidence item has one or more field bindings
- **THEN** the request catalog marks it `field_owner` or `mixed` and lists those exact field IDs
- **AND** the adapter does not add unrelated fields or rewrite the underlying Evidence

### Requirement: Shadow semantic instructions constrain Evidence role misuse
Shadow extract and verify instructions MUST state that legal-empty coverage requires field-owning Evidence, contextual Evidence cannot close an unrelated checklist item, control-scope no-change is not broader business-regime no-change, and cost-component rows are not business segments. These instructions MUST preserve the existing fail-closed validator and verifier behavior.

#### Scenario: Model attempts a context-only legal-empty result
- **WHEN** a model uses context-only Evidence to justify `not_disclosed` or `not_applicable` coverage
- **THEN** the request contract instructs the model not to do so
- **AND** local verification remains authoritative if the model still returns it
