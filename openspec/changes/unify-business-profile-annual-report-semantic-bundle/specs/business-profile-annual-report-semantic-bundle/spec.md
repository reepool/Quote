## ADDED Requirements

### Requirement: Annual-report evidence follows business disclosure structure
The system SHALL limit company-profile semantic input to bounded, deduplicated sections inside the resolved management-discussion chapter and SHALL recognize industry context, principal business, products and applications, operating model, main-business analysis, production and sales, procurement and costs, orders, and major customer or supplier disclosures.

#### Scenario: Standard annual-report subsections are present
- **WHEN** an annual report contains standard or configured aliases for relevant business subsections inside management discussion and analysis
- **THEN** the system ranks those pages as candidates for the semantic bundle
- **AND** records the matched subsection keys and immutable page evidence

#### Scenario: Repeated terms occur outside the business chapter
- **WHEN** the table of contents, governance chapter, or financial notes repeat a business alias
- **THEN** the system excludes those pages when a reliable management-discussion scope exists

#### Scenario: Several field families select the same page
- **WHEN** a page supports both activities and relationships
- **THEN** the semantic bundle contains that page once
- **AND** its characters count once against the request budget

### Requirement: One semantic request returns reusable atomic facts
For each company, report period, document version, and selected evidence scope, production semantic analysis MUST issue at most one LLM extraction request that returns both atomic activities and named relationships in one closed JSON response.

#### Scenario: Both semantic field families are pending
- **WHEN** `atomic_activities` and `named_relationships` require processing for the same report and evidence scope
- **THEN** the system sends one LLM request containing the joint evidence bundle
- **AND** makes the validated response available to both field-family consumers

#### Scenario: Model output is accepted
- **WHEN** the model returns schema-valid activities and relationships
- **THEN** each item has a Chinese semantic summary and cites supplied evidence span identifiers
- **AND** source labels, source values, source units, acronyms, and proper nouns remain source-native

#### Scenario: A model attempts governed derivation
- **WHEN** model output includes canonical IDs, normalized units, calculated values, value-chain roles, commodity direction, confidence, or other program-owned decisions
- **THEN** those values are not authoritative inputs to publication
- **AND** local programs perform the governed normalization and derivation

### Requirement: Joint semantic responses survive retries and restarts
The system SHALL persist each validated joint response before field-family conversion and SHALL replay an exact persisted response without another LLM call when document, evidence scope, request, prompt, and schema identities are unchanged.

#### Scenario: Sibling field family consumes a response in the same run
- **WHEN** the first semantic field-family item has obtained the joint response
- **THEN** the sibling field-family item reuses the in-run response
- **AND** the LLM call count for that document remains one

#### Scenario: Processing restarts after a partial field-family failure
- **WHEN** a joint response is persisted but one field-family conversion or write must retry
- **THEN** the retry loads and validates the persisted response
- **AND** does not repeat extraction or discard the successful sibling result

#### Scenario: Evidence or prompt changes
- **WHEN** the document hash, selected evidence, request payload, prompt version, or schema version changes
- **THEN** the prior response is not replayed as an exact match
- **AND** the new request and response receive a distinct content identity

### Requirement: Field-family governance remains independent
The system MUST validate, persist, promote, and report `atomic_activities` and `named_relationships` independently after joint extraction, and value-chain roles and commodity exposure MUST remain deterministic downstream products of approved atomic facts.

#### Scenario: Relationships are not disclosed
- **WHEN** the joint response has supported activities and a schema-valid empty relationship list
- **THEN** the relationship field family records expected non-disclosure independently
- **AND** activity processing may complete

#### Scenario: Activities are missing
- **WHEN** an annual report joint response contains no supported issuer activity
- **THEN** the activity field family enters the existing bounded missing-context recovery path
- **AND** an unrelated relationship result is not converted into an activity

#### Scenario: Derived publication runs later
- **WHEN** approved activities become inputs to value-chain and commodity processing
- **THEN** program rules resolve products, roles, units, exposure direction, and publication eligibility
- **AND** the joint LLM response does not directly populate derived publication tables

### Requirement: Operator metrics expose joint extraction efficiency
The semantic stage SHALL report joint requests, durable replays, in-run sibling reuse, and saved LLM calls in addition to existing per-field-family completion and error metrics.

#### Scenario: One report completes both semantic families
- **WHEN** both field families consume one joint response successfully
- **THEN** metrics report one joint LLM request, one sibling reuse, and two independently completed field families
