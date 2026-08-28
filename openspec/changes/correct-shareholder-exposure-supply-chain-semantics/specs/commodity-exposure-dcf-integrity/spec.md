## ADDED Requirements

### Requirement: Commodity exposure identities preserve distinct business legs
Exposure publication identity and predecessor selection MUST include the source fact/action class, exposure role, scope, commodity mapping, assumption lineage, and consumer identity needed to prevent semantically distinct legs from superseding one another.

#### Scenario: Purchase and consumption of the same commodity
- **WHEN** approved purchase and consumption facts exist for the same company, period, scope, and commodity
- **THEN** publishing one does not supersede the other, and both lineages remain available or are deterministically aggregated with both fact IDs preserved.

#### Scenario: Two model consumers share assumptions
- **WHEN** the same approved fact and assumptions are published for two consumer IDs
- **THEN** the publications have distinct consumer-bound identities and cannot overwrite one another.

#### Scenario: Assumption aliases are requested together
- **WHEN** synonymous assumption names such as `pass_through` and `pass_through_score` are supplied
- **THEN** they are canonicalized before selection and conflicting duplicates fail closed rather than using last-write-wins.

#### Scenario: Correct purchase and consumption publications coexist
- **WHEN** current publications already preserve separate purchase and consumption action lineage for the same commodity
- **THEN** repair treats that state as valid and does not report or hold it merely because two action classes coexist.

### Requirement: Exposure role and direction are program-governed
The system MUST derive publication role and financial direction from deterministic action and approved product/catalog rules, not from an unconstrained LLM calculation or a default revenue role.

#### Scenario: Energy commodity is consumed
- **WHEN** an approved consumption fact maps uniquely to a product catalog entry classified as energy input
- **THEN** publication uses `energy_cost` and the governed negative cost direction.

#### Scenario: Hedge activity has no supported publication rule
- **WHEN** an approved hedge activity produces an exposure fact but no deterministic hedge direction rule exists
- **THEN** the fact remains available as fact-only and no executable publication is created.

#### Scenario: Exposure role is absent or unknown
- **WHEN** a publication or mapping has no recognized role
- **THEN** it is excluded from executable mappings and is not defaulted to revenue.

#### Scenario: Legacy or external DCF context omits role
- **WHEN** an otherwise shaped mapping reaches a DCF consumer without `economic_role` or a recognized exposure role
- **THEN** the consumer rejects the automatic cycle adjustment and reports an input gap instead of assuming a revenue leg.

### Requirement: Fact-only is an explicit publication gap
A fact-only outcome MUST be terminal for the current processing attempt but MUST remain an explicit non-published gap until a valid mapping or deterministic rule becomes available.

#### Scenario: Product has no approved market mapping
- **WHEN** an approved fact cannot resolve a unique current commodity mapping
- **THEN** the run does not repeatedly call the extraction LLM, the publication is not counted as complete, and the gap remains visible without consuming normal retry attempts.

#### Scenario: Publication later becomes possible
- **WHEN** a valid local mapping or rule is approved later
- **THEN** local replay publishes from the persisted approved fact and closes the gap without re-extracting the annual report.

### Requirement: Executable DCF mappings require approved company evidence
The automatic company DCF path MUST accept only current mappings sourced from approved company business-profile publications and MUST keep industry-default mappings as non-executable research context unless a separate explicit valuation policy opts in.

#### Scenario: Only an industry default exists
- **WHEN** a company has an industry commodity mapping but no current approved company exposure publication
- **THEN** the default remains visible in industry context but does not change margin assumptions, does not enter executable DCF lineage, and is not labeled governed company evidence.

#### Scenario: Approved company mapping exists
- **WHEN** a current approved company publication has an active market series at the valuation cutoff
- **THEN** it may enter executable DCF inputs with its role, direction, source exposure ID, and lineage.

#### Scenario: Candidate or fact-only exposure exists
- **WHEN** an exposure is candidate, held, stale, superseded, or fact-only
- **THEN** it cannot enter executable DCF mappings even if its commodity has an industry price series.

#### Scenario: Exposure or assumption validity has ended
- **WHEN** an approved exposure or assumption has an effective end at or before the valuation cutoff
- **THEN** repository as-of selection and every executable consumer exclude it using the same half-open validity rule.

#### Scenario: Diagnostic candidates change
- **WHEN** candidate facts, relationships, or exposures change but approved executable business-profile inputs do not
- **THEN** DCF model inputs, `profile_version`, executable input hash, and valuation lineage remain unchanged.

#### Scenario: Diagnostic profile is requested explicitly
- **WHEN** an API caller requests candidate diagnostics
- **THEN** candidates may appear in diagnostic output but remain excluded from executable mappings and model fingerprints.

### Requirement: DCF cycle adjustment respects revenue and cost direction
DCF cycle selection MUST be deterministic by approved mapping role and MUST apply opposite economic direction for revenue and cost price legs; iteration order MUST NOT choose the series.

#### Scenario: Revenue commodity is at a high percentile
- **WHEN** a governed revenue series is selected and its price is at a high cycle percentile
- **THEN** normalization treats the condition as a potential revenue-side profit peak under the configured cycle policy.

#### Scenario: Cost commodity is at a high percentile
- **WHEN** a governed feedstock or energy cost series is selected and its price is at a high cycle percentile
- **THEN** normalization treats the condition as cost pressure rather than a revenue-side profit peak.

#### Scenario: Multiple mappings are ambiguous
- **WHEN** multiple revenue/cost series are eligible and no governed spread, materiality, or selection rule chooses one
- **THEN** automatic cycle adjustment fails closed and reports the ambiguity instead of selecting the first diagnostic.

### Requirement: Existing exposure and DCF state is auditable and replayable locally
The system MUST provide a dry-run-first idempotent audit and repair flow for old exposure collisions, false fact-only closure, and industry-only governed DCF context using persisted approved facts, mappings, and local market metadata.

#### Scenario: Legacy componentization proposes an approved successor
- **WHEN** a locally proven legacy exposure can be decomposed into components
- **THEN** its successor passes the current publication classifier and promotion gates; a generic system promotion cannot bypass those gates.

#### Scenario: Cost exposure was superseded by a different action
- **WHEN** audit finds purchase and consumption publications linked by the old predecessor key
- **THEN** apply mode reconstructs distinct current lineages from approved facts and preserves their evidence IDs.

#### Scenario: Audit distinguishes collision from valid coexistence
- **WHEN** repair compares approved facts, predecessor/supersession pointers, and current publication lineage
- **THEN** it reports a collision only when a distinct approved leg was lost, overwritten, or linked by the old identity, not when all expected legs are already current.

#### Scenario: Industry-only context was labeled governed
- **WHEN** an existing DCF context contains no approved company mapping but is labeled governed business profile
- **THEN** audit reports the case and subsequent DCF assembly excludes it from executable company mappings without requiring remote data or an LLM call.
