## ADDED Requirements

### Requirement: Industry packages derive from common evidence, not one company
Each industry package MUST be defined by an independent requirements document based on deep review of multiple representative annual reports and disclosure forms. The first vertical slice MUST use at least three reports covering at least two companies, with two reports from another company or another year than the first focus report; a single company, including `300750.SZ`, MUST NOT define the industry’s fields, chapter map, prompt, or acceptance contract.

#### Scenario: Manufacturing package research begins
- **WHEN** the manufacturing/materials package is researched
- **THEN** the sample set covers multiple business models, exchanges, table forms, subject scopes, and stable/transformation cases
- **AND** CATL is recorded as one sample rather than the anchor definition

#### Scenario: First vertical slice uses a bounded manifest
- **WHEN** the first manufacturing/materials slice is enabled before an automatic resolver is approved
- **THEN** an operator-approved manifest names the packages, instruments, report periods, and evidence for the slice
- **AND** no developer or model may add an extension package outside that manifest

### Requirement: Industry documents have a mandatory structure
An industry document MUST define boundaries, sub-industries, representative samples, researcher questions, annual-report chapter maps, common and industry-specific fields, obligation levels, units, deterministic rules, extract/repair/verify contracts, positive/negative/empty/conflict examples, package-combination rules, benchmark criteria, and explicit non-goals.

#### Scenario: Proposed package lacks negative examples
- **WHEN** an industry package document contains fields and prompts but no negative, legal-empty, or ambiguous cases
- **THEN** the package is not eligible for implementation

### Requirement: Package assignment is report-period regime data
Industry package assignment MUST be bound to instrument, report period, business regime, package version, knowledge time, and evidence. The first bounded slice MUST use an operator-approved manifest; after at least a second industry package is independently accepted and package automation is approved, a current static industry label MUST NOT be applied permanently across historical reports.

#### Scenario: Company changes from manufacturing to services
- **WHEN** official disclosures establish a principal-business change between report periods
- **THEN** reports before and after the change receive different regime-bound package assignments
- **AND** historical facts are not reclassified using the current package

### Requirement: Transformations and shell listings trigger regime review
Major asset restructurings, shell listings, principal-business disposals/acquisitions, and disclosed principal-business changes MUST trigger a new regime assessment. The system MUST close the prior regime and open the new regime only at an evidence-backed effective date; uncertain timing or dominance MUST produce `package_assignment_unclear`.

#### Scenario: Transition report contains old and new businesses
- **WHEN** a report period contains material old and newly acquired businesses and no single stable package represents the whole period
- **THEN** the assignment records a transition regime with the minimum justified package combination
- **AND** measurements remain bound to their disclosed segment and subject scope

### Requirement: Primary and extension package composition is governed
The common base package MUST always be active. Each regime MUST have one primary package and MAY have only evidence-triggered extension packages. Common fields MUST have one definition and extraction owner; package conflicts MUST fail closed instead of selecting the easier rule.

#### Scenario: Manufacturer owns a material mining segment
- **WHEN** the report explicitly discloses a mining segment that meets the mining extension trigger
- **THEN** the manufacturing primary package and the minimal mining extension may be composed
- **AND** shared revenue fields are extracted once while mining-only fields retain package provenance

### Requirement: Unresearched packages cannot be guessed
If the correct industry package is not yet researched, approved, or unambiguously assignable, the system MUST use only the common base package and report `package_assignment_unclear`; it MUST NOT substitute the nearest existing package or allow a developer or LLM to improvise a field set.

#### Scenario: Newly transformed business has no approved package
- **WHEN** a company enters a business not covered by any approved package
- **THEN** common BusinessOverview and base facts may be collected
- **AND** industry-specific extraction remains blocked pending package research

### Requirement: Initial industry families are planning categories only
Manufacturing/materials, resources/mining, energy/utilities, consumer/retail/restaurant, general services, finance, healthcare, and TMT/platform SHALL be treated as an initial research backlog, not as enabled production packages. A package becomes enabled only after its independent document, benchmark, and vertical-slice acceptance pass.

#### Scenario: Finance fields exist in the planning taxonomy
- **WHEN** the common requirements list the finance family
- **THEN** no banking or insurance field is added to production schemas until the finance package is independently researched and approved
