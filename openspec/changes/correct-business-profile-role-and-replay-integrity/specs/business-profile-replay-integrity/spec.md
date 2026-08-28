## ADDED Requirements

### Requirement: Result policy governs the complete business-profile lifecycle
The selected `result_policy` MUST be propagated consistently through semantic artifact selection, atomic record persistence, role derivation, exposure publication, promotion, and historical repair.

#### Scenario: Reuse finds a compatible completed result
- **WHEN** `result_policy=reuse` finds a source-compatible semantic result and all required governed records or locally reconstructable inputs are present
- **THEN** the system MUST reuse or locally replay them without an unnecessary LLM call and MUST apply the same policy to derived roles and publications

#### Scenario: Reuse result uses a legacy identity contract
- **WHEN** the semantic content is reusable but its persisted fact or role identity predates the corrected contract
- **THEN** the system MUST attempt a deterministic local upgrade and MUST NOT blindly promote the legacy conflicting identity

#### Scenario: Replace is requested
- **WHEN** `result_policy=replace` produces a fresh observation
- **THEN** the system MUST persist it through normal successor, review, temporal, and publication rules without deleting source evidence or using last-write-wins

### Requirement: Approved history is protected during reuse and repair
Reuse and repair MUST preserve valid approved history, held decisions, immutable evidence, and review audit. A conflicting candidate MUST NOT overwrite an approved record, and absence of a reconstructable successor MUST NOT authorize deletion of the approved record.

#### Scenario: Candidate conflicts with approved legacy fact
- **WHEN** a reused candidate still has an incompatible broad identity with an approved fact
- **THEN** the candidate MUST be skipped or held with an explicit reason while the approved record remains available until a valid successor transition is proven

#### Scenario: Invalid machine-derived role has no valid source semantics
- **WHEN** a role was generated only by an obsolete deterministic rule and has no external-service support
- **THEN** repair MUST remove it from the current projection and MUST preserve the supporting atomic disclosures and evidence

### Requirement: Deterministic failures are isolated and classified correctly
A deterministic evidence, identity, or temporal failure MUST be isolated to the affected governed fact or role group, MUST NOT abort unrelated valid groups, and MUST NOT be classified as LLM provider congestion.

#### Scenario: One derived role group conflicts
- **WHEN** one role group cannot be reconciled but other roles or exposure facts from the report are valid
- **THEN** the system MUST persist a typed held/conflict diagnostic for that group and continue processing unrelated groups

#### Scenario: Gateway returns a retryable failure
- **WHEN** an actual LLM gateway request fails with a retryable provider response
- **THEN** the existing bounded retry, backoff, and gateway classification MUST apply without conflating it with local data-contract failures

### Requirement: Historical repair is bounded, local, and idempotent
The system MUST provide one business-profile integrity repair service with zero-write audit mode by default and explicit scoped apply mode. Repair MUST use local evidence, semantic artifacts, governed records, and existing domain owners only; it MUST perform no network or LLM call and repeated apply MUST converge without creating new records or transitions.

#### Scenario: Audit is run for affected instruments
- **WHEN** an operator audits a bounded instrument set
- **THEN** the report MUST identify invalid inventory-derived roles, role identity duplicates, row-identity conflicts, and incompatible reuse artifacts with stable ids and proposed actions while performing zero writes

#### Scenario: Apply repairs reconstructable records
- **WHEN** apply is explicitly requested and local evidence proves the correction
- **THEN** the service MUST transact through existing role/fact/publication owners, report before/after state, and leave a second apply unchanged

#### Scenario: Machine-derived garbage is unreferenced
- **WHEN** an invalid derived record was never valid under corrected semantics and has no inbound lineage, publication, or review dependency
- **THEN** apply MAY physically delete that record with an explicit reason while source evidence and valid atomic/history records remain immutable

#### Scenario: Repair cannot reconstruct a record
- **WHEN** local persisted data is insufficient for a safe correction
- **THEN** the service MUST hold and report the item without guessing, remote reacquisition, or LLM use

### Requirement: Run reports expose origin, completeness, and token use
For every requested field family, run reporting MUST distinguish `llm_extracted`, `semantic_reused`, `local_replayed`, and `program_derived` work, MUST report held/skipped/conflicted group counts and reasons, and MUST count tokens only for actual LLM gateway calls.

#### Scenario: Historical data is repaired and replayed locally
- **WHEN** a field family completes using persisted artifacts and program derivation only
- **THEN** the report MUST show local replay/derivation origin and zero prompt, completion, and total LLM tokens

#### Scenario: One group remains held
- **WHEN** valid groups publish but one required group remains unresolved
- **THEN** the field family MUST NOT be reported as fully complete and the held reason MUST be visible in the run result
