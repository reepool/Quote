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

### Requirement: A pre-bundle Evidence-contract failure has one bounded completion path
When a formal OOS run terminates without a bundle because its frozen Evidence plan contains a field outside the existing semantic contract, a later completion change MAY freeze one corrected plan revision and MUST preserve the original plan and failure as immutable audit evidence. The corrected plan MUST remove or replace only invalid fields with an already supported field and MUST NOT introduce a new semantic field, change Evidence pages or source anchors, or import candidates from the failed or comparison runs.

#### Scenario: The invalid checklist aliases are corrected
- **WHEN** the original plan contains `energy_input`, `relationship`, and `business_event` but the existing contract uses `material_input`, `counterparty_relationship`, and `business_regime`
- **THEN** the corrected revision removes both unsupported aliases and retains only their existing contract fields
- **AND** all report identity, PDF hash, pages, anchors, headers, units, and unrelated scopes remain unchanged

### Requirement: The completion run validates locally before using the selected model
The completion workflow MUST execute preparation with the prepared-field closed-set check across every scope before any provider request. After those checks pass, it MUST perform an explicit connectivity observation outside a restricted sandbox and MUST execute exactly one complete formal run with the previously selected model, a new run ID, and the existing bounded parameters.

#### Scenario: The corrected plan reaches formal execution
- **WHEN** the corrected plan passes preparation and contains only supported Stage 5 field IDs
- **THEN** the operator records the Scorpio connectivity result and starts one `gemini-3.8-flash` formal run
- **AND** no second complete run, targeted retry, cross-model splice, prompt change, timeout change, or semantic-rule change occurs in the completion change

### Requirement: Completion output remains research-only regardless of state
The completion run MUST preserve either one immutable formal bundle or one typed terminal failure. Its report state MAY be `usable`, `usable_with_caveats`, `hold`, or `failed`, but all accepted records MUST remain `accepted_for_review`, `production_authorization` MUST remain `not_authorized`, and Stage 6 and all production consumers MUST remain closed.

#### Scenario: The completion run finishes
- **WHEN** the run reaches any terminal report state
- **THEN** the audit records six-chapter completion, accepted facts or legal-empty results, Evidence references, caveats, blockers, model parameters, and provider traces
- **AND** the result is not written to approved tables, scheduler/backfill, commodity exposure, value chain, DCF, or Stage 6

### Requirement: A held second-OOS Activity may be adjudicated offline from immutable Evidence
After the single formal second-OOS run, a reviewer MAY accept a held Activity only through a decision bound to the committed run and report hashes, exact review and runtime target IDs, original Evidence, source actors, source wording, and prior actor basis. The offline application MUST accept only an existing Activity human-review candidate and MUST NOT invoke extract, repair, verify, PDF processing, or another provider request.

#### Scenario: Parallel verbs have one direct grammatical actor
- **WHEN** the source sentence uses “公司” as the grammatical subject governing 制造、加工、销售 and the held candidates preserve `activity_actor=source_actor=公司` but incorrectly use `actor_basis=explicit_economic_relationship`
- **THEN** approved decisions may change only those candidates to `actor_basis=direct_grammatical_actor` and `accepted_for_review`
- **AND** `subject_scope` remains `unclear` without promotion to `issuer` or `consolidated_group`

### Requirement: Offline adjudication derives a separate research-only result
The adjudication MUST leave the committed source bundle byte-for-byte unchanged and MUST write its result outside the source run. After valid decisions are applied, it MUST recompute the affected coverage, research projection, contract benchmark, and report state through the existing Stage 5 policy. The derived missing-coverage review MAY close only when the reviewed Activity records are accepted from the bound Evidence.

#### Scenario: The only report blocker is resolved
- **WHEN** all three approved Activity decisions apply successfully and no other required chapter blocker remains
- **THEN** `explicit_activity` coverage becomes `observed`, the business-overview task becomes complete, and the report state is derived as `usable_with_caveats` because unclear subjects remain
- **AND** the result records zero provider calls and retains `production_authorization=not_authorized`

### Requirement: Invalid or over-broad review decisions fail closed
The offline adjudication MUST reject a missing or duplicate review target, a source-hash mismatch, an Evidence or source-text mismatch, a candidate outside Activity, an actor mismatch, a prior basis other than `explicit_economic_relationship`, a requested basis other than `direct_grammatical_actor`, or any attempt to alter subject scope or production authorization.

#### Scenario: A decision attempts an unsupported promotion
- **WHEN** a decision does not exactly match the held Activity candidate or attempts to promote “公司” beyond `subject_scope=unclear`
- **THEN** the adjudication fails before writing output
- **AND** the committed source bundle and all production consumers remain unchanged
