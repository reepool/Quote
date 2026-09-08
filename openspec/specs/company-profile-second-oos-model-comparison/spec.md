# company-profile-second-oos-model-comparison Specification

## Purpose
Define a bounded second out-of-sample annual-report validation with an independent three-model comparison, one selected-model formal run, and research-only failure/caveat handling.

## Requirements

### Requirement: The second OOS sample is frozen before model comparison
The validation MUST select exactly one official, locally valid annual report absent from the four-report authority, Gold annotations, targeted runs, adjudication ledger, and BaoSteel OOS records. Before any model request, the manifest MUST record report identity, exchange, report period, business model, disclosure form, selection rationale, PDF path and hash, known limitations, and `production_authorization=not_authorized`.

#### Scenario: A new sample is admitted
- **WHEN** the sample is not present in any excluded authority or prior OOS record and its PDF is locally valid
- **THEN** the operator freezes it as the sole second-sample input
- **AND** later model output cannot replace or broaden the sample

### Requirement: Three models receive identical comparison requests
The comparison MUST send the same frozen Evidence, request scope, prompt, schema, temperature, output budget, and deadline independently to `grok-4.6`, `glm-5.3-flash`, and `gemini-3.8-flash`. Each response MUST be stored under its own model identity and request hash.

#### Scenario: Overview and table requests are compared
- **WHEN** the two representative frozen scopes are sent to all three models
- **THEN** each model has an independent result for each scope
- **AND** no candidate or disposition from one model is used as input to another model's request

### Requirement: Model comparison measures semantic usefulness, not only transport success
For every completed comparison call, the audit MUST record structured parse status, latency, input/output token usage when available, candidate count and shape, Evidence binding, source-native value/unit/header preservation, verifier agreement when run, and typed provider/execution failures. Model selection MUST rank structured semantic reliability before latency or cost.

#### Scenario: One model returns quickly but loses Evidence
- **WHEN** a faster model has weaker Evidence binding or source support than a slower model
- **THEN** the slower model ranks higher for primary selection
- **AND** the comparison report records the trade-off rather than selecting on latency alone

### Requirement: Formal OOS output uses one selected model
After offline checks and the three-model comparison, the formal second-sample run MUST use one selected primary model consistently for its extract, repair, and verify calls. The formal bundle MUST NOT combine records from comparison outputs or from multiple primary models.

#### Scenario: Comparison and formal runs remain isolated
- **WHEN** comparison artifacts exist for all three models
- **THEN** the formal bundle contains only the selected model's run records and Evidence
- **AND** all non-selected results remain audit-only comparison evidence

### Requirement: The formal run is bounded and research-only
The change MUST permit exactly one complete formal OOS run under a fresh run ID after offline checks pass. A timeout, schema failure, provider failure, or semantic hold MUST be retained as a typed result without targeted reruns or historical splicing. All accepted records MUST remain `accepted_for_review`, and `production_authorization` MUST remain `not_authorized`.

#### Scenario: Formal run completes with caveats
- **WHEN** the selected model completes the six core chapters but some facts remain unclear or restricted
- **THEN** the report records `usable_with_caveats` or the applicable state with explicit Evidence and usage limits
- **AND** no production or Stage 6 state changes
