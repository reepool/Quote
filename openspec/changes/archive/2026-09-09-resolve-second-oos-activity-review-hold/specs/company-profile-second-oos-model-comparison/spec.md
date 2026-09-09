## ADDED Requirements

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
