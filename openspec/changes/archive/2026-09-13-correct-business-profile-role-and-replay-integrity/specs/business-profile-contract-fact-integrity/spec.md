## ADDED Requirements

### Requirement: Contract facts use evidence-derived occurrence identity
Every newly produced contract or table-row operating fact MUST have a program-derived occurrence key based on immutable source identity, physical page/table location, row or occurrence ordinal, ordered raw cells, and any explicit contract reference available from the evidence. A model-generated id or value-only hash MUST NOT be the durable occurrence identity.

#### Scenario: Repeated product label represents two contracts
- **WHEN** two source rows use the product label `多晶硅料` but represent different contracts or table occurrences
- **THEN** the system MUST generate distinct occurrence keys even if the rows share report period, fact type, segment, and evidence span

#### Scenario: Contract number is not disclosed
- **WHEN** distinct table rows have no contract number or counterparty
- **THEN** physical row identity and immutable raw-cell context MUST keep the facts distinct without inventing an entity or contract identifier

#### Scenario: Parser omits row ordinal
- **WHEN** deterministic table parsing provides an immutable evidence span and ordered occurrences but no row ordinal
- **THEN** the system MUST use a deterministic occurrence ordinal within that span and mark the identity quality, or hold the group as `unresolved` if no stable ordering exists

### Requirement: Occurrence identity is consistent across persistence and time
The occurrence key MUST participate consistently in fact record identity, fact scope, temporal stable identity, metadata, ambiguity grouping, reuse validation, and publication lineage.

#### Scenario: Distinct contracts disclose 4.18 and 0 billion yuan
- **WHEN** exact evidence supports `4.18 亿元` and `0 亿元` as fulfilled amounts for two distinct contracts
- **THEN** both raw facts MUST coexist without a temporal conflict and neither value MAY overwrite or invalidate the other solely because the product labels match

#### Scenario: Same occurrence is replayed
- **WHEN** the same source occurrence is replayed with the same processing contract
- **THEN** the system MUST reproduce or reuse the same fact identity and MUST NOT create a duplicate current fact

### Requirement: Raw values remain source facts and aggregates are programmatic
The system MUST retain each LLM/source-provided raw value and raw unit unchanged. Unit normalization, totals, ratios, and company-level aggregation MUST be performed by versioned program rules and MUST produce separately typed derived facts with explicit inputs.

#### Scenario: One contract has zero fulfilled amount
- **WHEN** the source row explicitly discloses a zero fulfilled amount
- **THEN** the system MUST preserve zero as a valid contract-level raw fact and MUST NOT discard it as implausible without contradictory evidence

#### Scenario: Consumer requests a company purchase total
- **WHEN** multiple contract-level purchase facts could contribute to a company-level total
- **THEN** the system MUST apply an explicit program aggregation rule and MUST NOT ask the LLM to select one row or silently sum incomparable measures

### Requirement: Company-level aggregates are separate derived facts
Any company-level total, ratio, or other aggregate derived from contract/table facts MUST have its own typed record, explicit input occurrence keys, aggregation rule version, and calculation provenance; it MUST NOT replace or alter the underlying contract-level facts.

#### Scenario: Two contract rows contribute to a total
- **WHEN** a caller requests a company-level purchase total from multiple contract occurrences
- **THEN** the program MUST either produce a separately typed aggregate with all included occurrence keys or report that no valid aggregation rule exists

### Requirement: Legacy facts are upgraded without rewriting valid history
The system MUST reconstruct occurrence identity for legacy facts from local persisted evidence and semantic artifacts when reliable. It MUST create governed row-aware successors while preserving legacy approved records as history; it MUST hold rather than guess when reconstruction is unreliable.

#### Scenario: Approved and candidate legacy rows are locally reconstructable
- **WHEN** an approved `4.18 亿元` fact and candidate `0 亿元` fact can be tied to distinct source occurrences using persisted local data
- **THEN** repair MUST create or reuse distinct row-aware successors, transition the broad legacy current identity through normal temporal ownership, and preserve both source facts

#### Scenario: Legacy row boundary cannot be reconstructed
- **WHEN** local evidence cannot distinguish whether two records are different occurrences or duplicates
- **THEN** the approved history MUST remain intact, the conflicting group MUST be held with a typed reason, and unrelated facts MUST continue

### Requirement: Ambiguity review cannot change source measurements
Any configured semantic ambiguity review MUST be limited to occurrence boundaries and MUST reference supplied occurrence candidates. It MUST NOT modify raw values, raw units, evidence ids, or program normalization, and absence of a stronger-model route MUST NOT block this capability.

#### Scenario: Current single model is configured
- **WHEN** no separate ambiguity-review model exists
- **THEN** the system MUST use the configured single semantic route if review is required or preserve the evidence-backed rows as held candidates without claiming stronger-model review

#### Scenario: Review is inconclusive
- **WHEN** a review cannot establish whether rows are distinct
- **THEN** all source-supported measurements MUST be preserved and only the affected occurrence group MUST remain held
