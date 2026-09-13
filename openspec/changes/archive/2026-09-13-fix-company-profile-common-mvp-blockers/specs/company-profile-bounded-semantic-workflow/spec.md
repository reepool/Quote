## ADDED Requirements

### Requirement: Business overview candidates are substantively source-bound
The bounded workflow MUST accept a BusinessOverview only when its exact cited source text
states a substantive principal business, product/service offering, business model, or direct
business activity. A metric-only sentence, regulatory disclosure boilerplate, generic tenure
or positioning slogan, or risk commentary without such a declaration MUST be rejected through
the existing isolated invalid-item path. When an exact leading business clause is followed by
risk commentary, the adapter MAY retain only that contiguous source substring and MUST NOT
invent or paraphrase wording.

#### Scenario: Revenue sentence is returned as overview
- **WHEN** a model returns only a main-business revenue amount as BusinessOverview
- **THEN** the adapter rejects that item without rejecting unrelated valid items

#### Scenario: Business clause precedes risk commentary
- **WHEN** the cited source begins with an explicit principal-business declaration and then describes cyclical or macroeconomic risk
- **THEN** the adapter may retain the exact leading business clause and excludes the risk explanation

### Requirement: Legal-empty results require disclosure-owner Evidence
For material-input and segment fields, legal-empty coverage MUST cite both an owner-valid
disclosure class and source text that can establish the requested field's absence or
inapplicability. Incidental raw-material words in a risk/management page, company-total revenue,
industry classification, or a `细分行业` phrase MUST NOT establish material or segment
legal-empty coverage.

#### Scenario: Risk page mentions raw-material prices
- **WHEN** a material-input scope cites a risk or management page that mentions raw-material price volatility but is not a procurement, material, cost-composition, or principal-business owner
- **THEN** legal-empty material coverage is rejected as non-owning Evidence

#### Scenario: Industry narrative contains a company total
- **WHEN** a segment scope cites company-total revenue and industry classification without a segment or revenue-cost disclosure owner
- **THEN** legal-empty segment coverage is rejected and the required field remains unresolved

### Requirement: Source-native event labels remain source-native
A BusinessEvent MUST preserve its canonical event type independently of source-native metadata.
Its `source_native.name` MUST be null or an exact whitespace-normalized substring of cited
Evidence. The adapter MUST remove an unsupported canonical English label and MUST NOT replace it
with invented source wording.

#### Scenario: Canonical English label is returned as source-native
- **WHEN** a model returns `consolidation_scope_increase` as `source_native.name` but that text does not occur in the Chinese Evidence
- **THEN** the adapter clears only `source_native.name` and retains the evidenced event semantics and source-native value

### Requirement: Stage-five traces retain routed attempt lineage
Stage 5 provider traces MUST retain the existing gateway selected profile, source label,
failover count, and safe physical attempt lineage for successful and failed routed calls.
Failure traces MUST NOT lose the model identities already present in `LlmError.lineage`, and
MUST NOT include credentials or full prompt/response content.

#### Scenario: All routed sources fail
- **WHEN** a routed Stage 5 call ends in a typed gateway error after one or more physical attempts
- **THEN** its failed trace records the attempted source labels/models and final failover count while preserving the typed contract error
