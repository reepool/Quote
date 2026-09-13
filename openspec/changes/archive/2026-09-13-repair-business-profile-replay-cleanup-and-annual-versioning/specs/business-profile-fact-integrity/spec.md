## ADDED Requirements

### Requirement: Governance identity parity
Semantic conversion, temporal governance, and approved-as-of reads MUST use the same immutable source occurrence key. They MUST keep the source occurrence key separate from the semantic-content fingerprint. Generated evidence IDs, selected-artifact hashes, semantic run IDs, model interpretations, and raw narrative character offsets MUST NOT alter the source key. Narrative occurrences MUST use a versioned normalized quote/context locator with a same-page match ordinal. Missing required physical occurrence fields MUST fail closed with an explicit identity-incomplete diagnostic.

#### Scenario: Conversion and governance agree
- **WHEN** a fact is converted and then promoted
- **THEN** both stages compute the same stable identity and promotion does not create a duplicate temporal row

#### Scenario: Incomplete identity
- **WHEN** a model result lacks source-row or contract identity for a row-sensitive fact
- **THEN** the fact remains candidate or machine rework and is not silently assigned a random replacement identity

#### Scenario: Stable source row survives replay infrastructure changes
- **WHEN** a parser/selection replay regenerates evidence records for the same source document page, table, row and metric slot
- **THEN** conversion and governance compute the same source occurrence key as the prior run, while the new evidence remains additional provenance rather than record identity

#### Scenario: Stable narrative locator survives offset drift
- **WHEN** the same normalized source quote and bounded context are found at a different raw character offset under the same normalization-policy version and same-page match ordinal
- **THEN** conversion and governance compute the prior source occurrence key and retain the new offset only as evidence-validation provenance

#### Scenario: Canonical approved identity is re-keyed without changing the fact
- **WHEN** an exact migration manifest proves that a retained approved row's business content and physical source occurrence are unchanged but its persisted occurrence identity was derived from a legacy evidence ID
- **THEN** migration updates only the source-occurrence metadata and dependent lineage hash, preserves the governed record ID and all business/review/temporal fields, appends immutable old/new identity audit, and a later replay returns that retained governed ID through `reused(actual_governed_id)`

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

### Requirement: Transformation lineage uses source-native objects
Semantic transformation extraction MUST preserve source-native input and output object labels without model-generated governed IDs. Runtime binding MUST resolve those labels to compatible activities/facts within the same report occurrence when the binding is unambiguous. When no separate compatible component records exist but one exact-evidence assertion explicitly and unambiguously contains both input and output labels, runtime MUST persist complete assertion-backed component lineage without requiring separately extracted component activity IDs, and the complete lineage MUST enter the existing deterministic processor-role mapping. Raw labels, exact evidence, and any bound IDs MUST remain traceable; missing sides, inconsistent evidence, or ambiguous binding MUST produce typed machine rework.

#### Scenario: Recycling transformation binds both sides
- **WHEN** a report states that recovered metals are processed into lithium salts, precursors, or cathode materials and the same bundle contains compatible source activities/facts
- **THEN** runtime binds the input and output objects, persists lineage, and derives one processor role supported by that explicit transformation

#### Scenario: Standalone process action has no output
- **WHEN** a `processes` activity has no explicit output object or no compatible output occurrence can be bound
- **THEN** no processor role is automatically derived and the missing lineage is reported as machine rework

#### Scenario: One source assertion explicitly states both transformation sides
- **WHEN** the `300750.SZ` recycling disclosure states in one exact-evidence span that recovered metals/materials are processed into cathode materials, precursors, or lithium salts and both source-native sides are unambiguous
- **THEN** runtime persists complete evidence-backed component lineage and derives one processor role without requiring another LLM call or model-generated governed IDs

### Requirement: Case-sensitive SI unit conversion
Deterministic unit parsing MUST preserve SI prefix case and resolve the complete unit token before applying any compound-unit conversion. Bare `m` MUST mean metre and bare `g` MUST mean gram; bare `M`, `G`, or `k` are incomplete prefix tokens and MUST produce a typed unit error. Compound units `mm`, `mg`, `Mt`, and `kt` MUST resolve to millimetre, milligram, megatonne, and kilotonne respectively, without substring or case-folding substitutions. The governed power-unit catalog is an explicit compatibility exception: `MW`, `mw`, and mixed-case `mW` in a power context MUST resolve to megawatt (10^6 watt), because annual-report power disclosures and upstream PDF text extraction use these spellings interchangeably. This exception MUST NOT be generalized to other dimensions or prefixes.

#### Scenario: Bare base units are not prefixes
- **WHEN** a source row contains bare tokens `m` or `g`
- **THEN** they normalize as metre and gram respectively, while bare `M`, `G`, or `k` are rejected as incomplete prefix tokens

#### Scenario: Compound units use their complete token
- **WHEN** a source row contains `mm`, `mg`, `Mt`, or `kt`
- **THEN** the parser resolves the complete token to the correct physical dimension and multiplier, and an incompatible dimension is rejected as a typed unit error

#### Scenario: Power aliases locate megawatt
- **WHEN** a power value contains `MW`, `mw`, or mixed-case `mW`
- **THEN** the governed power catalog resolves each spelling to megawatt with multiplier `1000000`, and the result records the power-catalog compatibility rule rather than treating `mW` as milliwatt
