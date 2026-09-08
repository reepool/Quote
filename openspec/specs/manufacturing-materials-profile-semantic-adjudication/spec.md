# manufacturing-materials-profile-semantic-adjudication Specification

## Purpose
TBD - created by archiving change resolve-manufacturing-materials-profile-semantic-holds. Update Purpose after archive.
## Requirements
### Requirement: Stage-five holds are adjudicated from immutable runtime evidence
The adjudication MUST use `run-stage5-final-four-luna-20260905-f` as the immutable problem baseline and MUST record each decision against the original sample, request scope, candidate or coverage result, Evidence anchor, blocker, and frozen contract rule. It MUST NOT edit the baseline bundle, copy Gold values into runtime results, or promote targeted diagnostic runs into the authoritative four-report result.

#### Scenario: A held runtime record is reviewed
- **WHEN** a reviewer decides whether a held record is valid, invalid, legally empty, or still ambiguous
- **THEN** the adjudication record identifies the exact runtime target and Evidence supporting the decision
- **AND** the baseline run remains byte-for-byte unchanged

### Requirement: The four blocking semantic families have explicit outcomes
The adjudication MUST separately resolve subject scope, reported business change, external-service processing volume, and same-control comparative semantics. Subject scope MUST require affirmative source wording or documented same-report reconciliation; a no-major-business-change disclosure MUST produce supported `not_applicable` coverage rather than an invented event; a combined processing/sales source label at one physical anchor MUST produce only the source-supported primary metric; and a restated comparative MUST retain `comparison_basis` without overwriting earlier knowledge-time facts.

#### Scenario: No major business change is disclosed beside a consolidation change
- **WHEN** one Evidence scope states that consolidation scope changed and separately states that products, services, or principal business had no applicable major change
- **THEN** the consolidation change may form its own event and the no-change item forms `not_applicable` coverage
- **AND** the two statements are not merged into one BusinessEvent

#### Scenario: One physical fact uses a processing label with a sales alias
- **WHEN** the source-native name describes one external processing volume and includes a parenthetical sales alias
- **THEN** the accepted result contains one `processing_volume` with the complete source-native name
- **AND** neither extract nor verify requires a second `sales_volume` from that anchor

#### Scenario: A same-control table includes adjusted and pre-adjustment comparatives
- **WHEN** the report presents current, same-control-restated, and pre-adjustment comparative columns
- **THEN** each accepted fact retains its reported period, knowledge time, subject evidence, and comparison basis
- **AND** no later restated fact deletes or overwrites an original-as-published fact

### Requirement: Corrected scopes are rerun under new immutable run identities
Any semantic correction MUST be verified first through new targeted run IDs for the affected scopes. A new authoritative four-report decision MUST come from one complete new four-report run bundle after targeted checks pass. Historical baseline, failed, preflight, or targeted bundles MUST NOT be merged to fill missing records in that authoritative run.

#### Scenario: A corrected verifier rule is ready for validation
- **WHEN** offline tests prove the correction and the approved Evidence plan is unchanged
- **THEN** the operator executes the affected scope under a new run ID before a complete four-report rerun
- **AND** the prior held bundle remains independently reviewable

### Requirement: Research slice completion follows the approved acceptance policy and remains non-production
The adjudication MUST preserve every frozen blocker, incomplete required scope, Gold mismatch, contract conflict, and unevaluated real-report negative case in the audit output. Report and slice usability MUST be derived by the approved research acceptance policy: untriggered real-report negatives and allowed `unclear` subjects are not failures by themselves, while required execution failures, actually failed negative cases, and frozen semantic blockers remain blocking. Every outcome MUST retain `production_authorization=not_authorized` and MUST NOT start legacy reset, backfill, publication, CommodityExposure, ValueChainRole, or DCF writes.

#### Scenario: Three reports are usable and one report remains semantically unresolved
- **WHEN** any one report retains a required execution failure or frozen blocker after adjudication and rerun
- **THEN** the four-report result cannot be `research_slice_usable`
- **AND** no production or reset authorization is created

### Requirement: Residual benchmark discrepancies are adjudicated offline
The residual manufacturing/materials period, qualifier, subject-strictness, and event-label discrepancies MUST be evaluated from the immutable `stage55-closure-four-20260907-a` bundle and its original Evidence. The change MUST NOT invoke an LLM, alter an Evidence plan, edit that bundle, merge a historical or targeted run, or require another four-report run. Any new benchmark output MUST use a new evaluation identity and identify the immutable bundle as its sole runtime source.

#### Scenario: Offline rules are applied to the closure bundle
- **WHEN** the period, qualifier, subject, and event rules pass focused unit and fixture tests
- **THEN** the benchmark may be rerun offline against `stage55-closure-four-20260907-a`
- **AND** no extract, repair, or verify request is issued

#### Scenario: An unaffected Gold case remains unresolved
- **WHEN** a Gold failure is caused by a missing record, different physical anchor, or semantic contract conflict outside this change
- **THEN** the new evaluation retains that failure or conflict
- **AND** it does not synthesize a match from another record

### Requirement: Manufacturing and materials residual decisions remain explicit
The adjudication MUST distinguish period normalization from factual absence and MUST record the specific Gold metadata or directional event decision used for any changed outcome. The Putailai consolidation-adjustment duration values MAY use annual period equivalence without merging anchors. The Jinhua product-margin subject MAY use only the approved non-group refinement rule. The Jinhua expected-completion case MUST require the source qualifier on its approved Evidence. The Chengfei transfer event MAY use only the explicit directional event equivalence. The Chengfei sales coverage conflict MUST remain `gold_contract_conflict`, and a missing consolidation-adjustment margin or Segment structure MUST remain failed until a future source-backed runtime record exists.

#### Scenario: Directly affected cases improve without score chasing
- **WHEN** a directly affected Gold annotation satisfies its new closed period, qualifier, subject, or event rule
- **THEN** only that annotation's auditable match outcome changes
- **AND** the evaluator reports the rule and runtime target that caused the change

#### Scenario: Production remains unauthorized
- **WHEN** the offline adjudication and benchmark complete
- **THEN** the research slice remains research-only with `production_authorization=not_authorized`
- **AND** stage six, legacy backfill, approved tables, CommodityExposure, ValueChainRole, and DCF remain closed
