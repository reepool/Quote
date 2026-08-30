## ADDED Requirements

### Requirement: Governance identity parity
The identity fields used by semantic conversion MUST be identical to the fields used by temporal governance and approved-as-of reads. Missing required occurrence fields MUST fail closed with an explicit identity-incomplete diagnostic.

#### Scenario: Conversion and governance agree
- **WHEN** a fact is converted and then promoted
- **THEN** both stages compute the same stable identity and promotion does not create a duplicate temporal row

#### Scenario: Incomplete identity
- **WHEN** a model result lacks source-row or contract identity for a row-sensitive fact
- **THEN** the fact remains candidate or machine rework and is not silently assigned a random replacement identity

### Requirement: Temporal separation by report flow
The system MUST treat different report periods and source revisions as separate report flows while rejecting overlapping duplicates within the same occurrence identity.

#### Scenario: Different report periods
- **WHEN** two otherwise similar facts belong to different annual report periods
- **THEN** both can be approved as separate historical observations

#### Scenario: Same occurrence overlap
- **WHEN** two records have identical occurrence material and overlapping validity
- **THEN** the system reuses or replaces according to explicit policy and never approves both as independent facts

### Requirement: Report-flow as-of visibility
For `segments` and `operating_facts`, the system MUST use the knowledge window, report period, and configured freshness policy to determine approved-as-of visibility. The observation interval (`valid_from`/`valid_to`) MUST remain report metadata and MUST NOT make a report-flow fact invisible solely because the knowledge cutoff is after the report period end.

#### Scenario: Annual report remains visible after publication
- **WHEN** an annual fact has `report_period=2025-12-31`, `valid_to=2025-12-31`, and `knowledge_from=2026-04-17`
- **THEN** an approved-as-of query at `2026-04-30` returns the fact when its freshness policy is satisfied

### Requirement: Deterministic conversion failure isolation
The deterministic structured extraction path MUST catch unknown or ambiguous unit resolutions and invalid numeric conversions at the document/row boundary. It MUST emit a typed machine-rework diagnostic containing source document, page/table, row and unit context, and MUST NOT abort the entire semantic scope or leave a resumable checkpoint that deterministically crashes again.

#### Scenario: Unknown table unit
- **WHEN** a structured table contains a value with an unresolved unit such as `万元/吨`
- **THEN** the row becomes `unit_normalization_failed` or `unit_resolution_pending`, the remaining rows continue, and the worker report does not classify it as gateway congestion

### Requirement: Pending facts block completion
An operating fact with unresolved unit, invalid numeric reconciliation, incomplete occurrence identity, or missing evidence MUST prevent its field family from being marked complete and MUST remain unavailable for automatic promotion until repaired.

#### Scenario: Atomic fact has pending unit
- **WHEN** semantic extraction creates a candidate with `unit_resolution_pending`
- **THEN** the family quality is not ready, a machine-rework target is persisted, and the candidate cannot be promoted or reused as a complete family

### Requirement: Case-sensitive SI unit conversion
Deterministic unit parsing MUST preserve SI prefix case and resolve the complete unit token before applying any compound-unit conversion. Bare `m` MUST mean metre and bare `g` MUST mean gram; bare `M`, `G`, or `k` are incomplete prefix tokens and MUST produce a typed unit error. Compound units `mm`, `mg`, `Mt`, and `kt` MUST resolve to millimetre, milligram, megatonne, and kilotonne respectively, without substring or case-folding substitutions.

#### Scenario: Bare base units are not prefixes
- **WHEN** a source row contains bare tokens `m` or `g`
- **THEN** they normalize as metre and gram respectively, while bare `M`, `G`, or `k` are rejected as incomplete prefix tokens

#### Scenario: Compound units use their complete token
- **WHEN** a source row contains `mm`, `mg`, `Mt`, or `kt`
- **THEN** the parser resolves the complete token to the correct physical dimension and multiplier, and an incompatible dimension is rejected as a typed unit error
