## ADDED Requirements

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
