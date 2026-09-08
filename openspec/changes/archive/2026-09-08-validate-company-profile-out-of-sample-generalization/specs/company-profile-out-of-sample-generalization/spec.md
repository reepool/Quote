## ADDED Requirements

### Requirement: The validation sample is independent and frozen
The out-of-sample validation MUST use exactly one official, locally valid manufacturing or materials annual report whose issuer/report identity did not participate in the approved four-report manifest, Gold annotations, targeted runs, or manual adjudication ledger. Before semantic execution, the validation manifest MUST record the sample identity, instrument, exchange, report period, business model, disclosure form, selection reason, PDF path and hash, and known limitations. The sample MUST NOT be selected or replaced in response to model output.

#### Scenario: A new annual report is selected
- **WHEN** an official local report is within the existing manufacturing/materials boundary and absent from all in-sample evidence and adjudication artifacts
- **THEN** the report may be frozen as the sole out-of-sample validation sample
- **AND** its identity and PDF hash are recorded before extract or verify is invoked

#### Scenario: A prior or replacement sample is requested
- **WHEN** the requested report participated in the prior four-report work or the operator attempts to replace the frozen sample after seeing runtime output
- **THEN** validation preparation fails
- **AND** no substitute report or mixed bundle is created

### Requirement: The unseen report uses the existing six-chapter Evidence contract
Before provider invocation, the validation MUST prepare versioned Evidence for the existing six core chapter families: business overview plus Activity; segment or legal empty; operating quantity/capacity or legal empty; material/energy input or legal empty; counterparty/concentration or legal empty; and business regime checked as an event or explicit `not_applicable`. Each request scope MUST preserve the PDF hash, continuous physical pages, section identity, headers, source units, footnotes, continuation context, checklist, and known preparation gaps. The validation MUST use existing PDF output and MUST NOT add an automatic selector, parser, OCR engine, or Gold-shaped Evidence.

#### Scenario: All chapter contexts are prepared
- **WHEN** the frozen report supplies readable, bounded Evidence or a source-supported legal empty result for each core chapter
- **THEN** one versioned sample-specific Evidence plan is committed before semantic execution
- **AND** the plan does not contain runtime answers or Gold expectations

#### Scenario: Required context is unreadable or incomplete
- **WHEN** a required page, owning header, unit, footnote, or continuation link cannot be prepared reliably
- **THEN** that scope records the existing typed preparation failure
- **AND** the provider is not asked to infer the missing context

### Requirement: One semantic owner executes an isolated validation mode
The validation MUST reuse the existing company-profile preparation, `CompanyProfileSemanticService.run_task`, task-specific provider, research projection, acceptance policy, and atomic bundle store. An explicit out-of-sample manifest kind MAY admit the one frozen sample without changing the default four-report allowlist. The operator entry MUST remain a thin caller and MUST NOT implement sample iteration, extract/repair/verify, disposition, projection, or persistence loops independently.

#### Scenario: The frozen validation manifest is supplied
- **WHEN** the operator supplies the valid one-sample out-of-sample manifest and Evidence plan
- **THEN** the existing owners execute the same bounded semantic contracts for that report
- **AND** the completed four-report mode continues to reject reports outside its original allowlist

#### Scenario: A mixed or multi-sample validation is requested
- **WHEN** the validation manifest contains an in-sample report, more than one report, or an unknown manifest kind
- **THEN** the request fails before provider invocation
- **AND** no alternate semantic path is used

### Requirement: Live validation is one complete non-composite run
After offline contract checks pass, the change MUST permit exactly one operator-level complete run for the frozen report under a new run ID. The run MAY use only the existing bounded provider attempts and failover. It MUST NOT launch targeted semantic runs, a second complete run, or merge records from the four-report authority, Gold fixtures, diagnostics, interrupted output, or another sample. The committed result MUST be an immutable isolated bundle, or a bounded non-reusable failure manifest when atomic commit cannot complete.

#### Scenario: The complete report run finishes
- **WHEN** all prepared request scopes reach completed, held, or failed dispositions in the one authorized run
- **THEN** the bundle atomically records the manifest, Evidence, calls, candidates, dispositions, coverage, projection, report status, and review material
- **AND** no historical record is copied into it

#### Scenario: The run has a transport or execution failure
- **WHEN** a required scope ends with a typed provider or execution failure after existing bounded attempts
- **THEN** the report retains `hold` or `failed` according to the current policy
- **AND** this change does not add timeout, schema, provider, targeted-run, or fallback modifications to chase a passing result

### Requirement: Existing policy determines report usability without post-hoc Gold
The validation MUST calculate `usable`, `usable_with_caveats`, `hold`, or `failed` from the current six-chapter completion, frozen blockers, independent verification, coverage, subject-use restrictions, and applicable real negative cases. It MUST NOT create Gold expectations after viewing runtime output or require a Gold score to finish the study. `usable` or `usable_with_caveats` demonstrates bounded generalization for this sample only; `hold` or `failed` MUST remain an honest completed validation result with its blockers disclosed.

#### Scenario: The unseen report satisfies current research policy
- **WHEN** all six chapters are complete, no frozen blocker or execution failure remains, and every triggered negative case passes
- **THEN** the report is classified as `usable` or `usable_with_caveats` under the existing policy
- **AND** the audit states that the result supports reuse only for the frozen sample and contract version

#### Scenario: The unseen report exposes a blocker
- **WHEN** a required chapter is incomplete, a frozen blocker fires, or an execution failure remains
- **THEN** the report remains `hold` or `failed`
- **AND** the validation completes without rewriting the policy or hiding the failure as a caveat

### Requirement: Review material exposes evidence and genuinely new findings
The validation MUST produce one researcher-readable report listing chapter coverage, accepted facts, legal empty outcomes, caveats, blockers, and usage restrictions. Before execution, it MUST freeze a deterministic human evidence-audit rule that selects every blocker, caveat, and proposed adjudication plus the first accepted or legal-empty result in stable request order for each core chapter. Every audited or proposed-adjudication item MUST include the exact original text or table cells, one-based physical page, Evidence identity, runtime target when present, applicable rule, and review conclusion or recommended decision. Findings MUST distinguish an existing rule working as designed from a genuinely new disclosure form, preparation gap, adapter/verifier mismatch, execution failure, or unresolved policy decision. The change MUST NOT implement a new semantic rule in response to those findings.

#### Scenario: Human adjudication is required
- **WHEN** Evidence supports more than one plausible semantic treatment and the existing closed policy does not decide between them
- **THEN** the review package lists the alternatives, recommendation, original text, page, Evidence, and runtime target
- **AND** the report remains appropriately caveated or held pending a later authorized decision

#### Scenario: No adjudication is proposed for a chapter
- **WHEN** a core chapter has accepted or legally empty output and no blocker, caveat, or unresolved decision
- **THEN** the deterministic audit still checks the first such result against its original Evidence
- **AND** the review report records whether the source, semantic fields, and usage restrictions are supported

#### Scenario: An existing guard blocks prohibited inference
- **WHEN** the unseen report triggers an already specified negative rule and the workflow blocks it correctly
- **THEN** the audit records the guard as existing behavior rather than a new failure class
- **AND** no follow-up change is proposed solely because the guard fired

### Requirement: Out-of-sample validation remains research-only
Every manifest, bundle, projection, review report, and final audit MUST retain `accepted_for_review` semantics and `production_authorization=not_authorized`. The validation MUST NOT write production databases or approved tables, enable legacy backfill, create CommodityExposure or ValueChainRole, feed DCF, or alter scheduler, API, Telegram, or stage-six state.

#### Scenario: The unseen report is research usable
- **WHEN** the final report is `usable` or `usable_with_caveats`
- **THEN** its accepted facts remain available only in the isolated research bundle and restricted research projection
- **AND** no production consumer or stage-six action is authorized
