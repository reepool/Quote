## ADDED Requirements

### Requirement: Counterparty disclosure and legal-entity resolution are distinct
The system MUST preserve a clearly disclosed named customer or supplier without requiring a global enterprise catalog, while explicitly distinguishing a disclosed name from a governed resolved legal entity.

#### Scenario: Exact governed legal identity
- **WHEN** a counterparty matches a local governed legal name, official identifier, or explicitly approved alias at the cutoff
- **THEN** the relationship may use `resolved_entity` status and the governed entity ID.

#### Scenario: Named external counterparty is not in the local entity set
- **WHEN** official evidence clearly names an external counterparty but no governed local entity is available
- **THEN** the relationship is retained as `disclosed_name_only` with raw name and evidence and does not claim cross-company entity resolution.

#### Scenario: Unique ticker short name
- **WHEN** a disclosed name matches only a unique securities short name or historical ticker name
- **THEN** uniqueness alone does not create an approved alias or resolved legal entity.

### Requirement: Relationship approval preserves identity meaning
Approval MUST either bind a governed entity or explicitly approve the evidence-backed disclosed-name-only status; a generic approval MUST NOT silently convert an unresolved name into a resolved entity.

#### Scenario: Human approves an unresolved relationship
- **WHEN** a reviewer approves a relationship with no governed entity ID
- **THEN** the decision must explicitly confirm `disclosed_name_only`, and the API continues to expose that limitation.

#### Scenario: Later governed resolution
- **WHEN** a disclosed-name-only relationship later resolves to an approved governed entity
- **THEN** the new occurrence supersedes the prior current occurrence while preserving the earlier audit record and evidence.

### Requirement: Relationship current state is report-aware
The system MUST separate evidence occurrence identity from stable relationship lineage and MUST select the newest eligible occurrence per lineage at the knowledge cutoff.

#### Scenario: Same customer appears in consecutive annual reports
- **WHEN** two approved reports disclose the same relationship lineage with different evidence IDs
- **THEN** only the newest eligible occurrence is current and the older occurrence remains in history.

#### Scenario: Same counterparty has two disclosed contracts
- **WHEN** the same report discloses two relationships distinguished by contract reference or source-row occurrence
- **THEN** both occurrences remain separately queryable and neither overwrites the other.

#### Scenario: Relationship ends explicitly
- **WHEN** evidence or review supplies a valid end date
- **THEN** the relationship is not current on or after that half-open interval boundary.

### Requirement: Concentration semantics are deterministic
Anonymous concentration facts MUST reconcile the disclosed label, relationship direction, raw value, and raw unit in program code before approval.

#### Scenario: Customer label conflicts with supplier direction
- **WHEN** a label denotes top customers but the relationship direction is `buys_from`
- **THEN** production fails closed with a semantic-direction conflict instead of writing supplier concentration.

#### Scenario: Percentage is disclosed
- **WHEN** evidence supplies raw value `30` with raw unit `%`
- **THEN** program code normalizes it to fraction `0.30`, records the conversion rule, and validates the finite `[0, 1]` result.

#### Scenario: Unit-ambiguous share is outside fraction range
- **WHEN** a named or anonymous relationship supplies `30` without an explicit percent unit or a valid normalized fraction
- **THEN** the relationship share is held for correction and is not approved as `30`.

### Requirement: Relationship freshness participates in profile readiness
Business-profile readiness MUST report current relationship and activity coverage at the requested cutoff and MUST NOT treat every historical point-in-time activity or relationship as indefinitely current.

#### Scenario: Only stale relationship evidence exists
- **WHEN** all approved relationship occurrences fall outside the configured report-aware current window or have been superseded
- **THEN** relationship temporal coverage is reported as missing or stale rather than current.

#### Scenario: Approved profile API omits candidates
- **WHEN** an approved profile or DCF context is requested without diagnostic candidates
- **THEN** unresolved candidates do not enter the approved relationship set, executable input hash, or model input.

### Requirement: Existing relationship state is repaired without new LLM extraction
The repair flow MUST derive corrected lineages, current selections, and resolution statuses from persisted evidence and semantic records whenever those records are sufficient.

#### Scenario: Multiple annual occurrences are current
- **WHEN** audit finds approved occurrences from multiple report years in one stable lineage
- **THEN** apply mode retains the newest eligible occurrence as current and supersedes or closes prior occurrences without deleting their evidence.

#### Scenario: Short-name auto-resolution was previously approved
- **WHEN** a relationship was approved solely through an automatically approved securities short-name alias
- **THEN** audit identifies it and apply mode either binds supported governed identity evidence or moves it to disclosed-name-only/held status without recalling the extraction LLM.
