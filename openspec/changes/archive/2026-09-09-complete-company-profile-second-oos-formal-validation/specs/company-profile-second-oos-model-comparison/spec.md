## ADDED Requirements

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
