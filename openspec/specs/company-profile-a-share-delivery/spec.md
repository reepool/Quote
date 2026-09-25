# company-profile-a-share-delivery Specification

## Purpose
Target all-A-share core profile, commodity association and task delivery contract. Approved planning revision 2026-09-13; implementation remains tracked in the corresponding change.

## Requirements

### Requirement: All A-share companies receive the common core path
The system MUST discover the SSE, SZSE and BSE A-share universe at a declared date and expose a status for every selected company. Every industry MUST be eligible for common principal-business, product/service and revenue-model extraction. Industry packages MUST enhance only their supported metrics; absent classification or unsupported enhancement MUST NOT exclude the company from the common path. Missing official assets MUST remain explicit and use existing acquisition services.

#### Scenario: Bank has no approved banking enhancement
- **WHEN** a bank has a valid annual report but no implemented banking metrics package
- **THEN** the common path collects evidenced business lines and revenue model
- **AND** manufacturing quantities are not required or invented.

#### Scenario: Report asset is missing
- **WHEN** a company has no readable effective annual report
- **THEN** it remains in the universe with an asset-pending or failed status
- **AND** available companies continue processing.

### Requirement: Core usefulness measures substantive business answers
The system MUST assess principal business, major products/services or business lines, and revenue-generation model separately. The core MUST be complete only when all three have substantive evidence-backed answers. Disclosed important business composition MUST be retained, but absent numerical segmentation MUST NOT prevent an evidenced qualitative revenue model. Optional operational metrics and counterparties MUST expose their own gaps. Empty statuses and raw record counts MUST NOT be treated as a complete core.

#### Scenario: Single business has no segment amounts
- **WHEN** the report evidences a principal service and how it earns revenue but no segment amounts
- **THEN** the system can deliver the qualitative core with explicit numerical gaps.

#### Scenario: Many numeric rows but no principal business
- **WHEN** a result contains many accepted measurements but lacks principal-business evidence
- **THEN** accepted facts remain deliverable and core completeness remains false.

### Requirement: Subject default is uniform across extraction and consumers
Every active extractor, verifier, projection and evaluator MUST use consolidated_group with report_default_group_scope when no explicit narrower scope or unresolved subject conflict exists. Explicit issuer, named subsidiary and business segment evidence MUST take precedence. Direct wording and numerical reconciliation MUST retain their own basis. Original actor wording MUST remain unchanged; a default group MUST NOT transfer a third-party action to the listed company.

#### Scenario: Source only says company
- **WHEN** a company-owned annual-report fact says 公司 with no narrower scope or conflict
- **THEN** it uses consolidated_group and report_default_group_scope without requiring extra reconciliation.

#### Scenario: Parent-only table
- **WHEN** the local table explicitly belongs to the parent company
- **THEN** it remains issuer-scoped even when the report cover names a group.

### Requirement: Durable tasks deliver partial results continuously
The existing queue and company-profile application owner MUST implement preview, run, status, pause and resume with bounded resource usage. Execution MUST automatically prepare Evidence, checkpoint completed scopes, isolate record-local semantic errors and resume unfinished scopes without repeating accepted identical work. Changed report or policy identity MUST create a scoped successor. Ordinary production MUST NOT use fixed research sample counts or OOS exclusion lists. Approved official structured facts MUST be reused where available before LLM fallback.

#### Scenario: Execution stops after two scopes
- **WHEN** a task resumes with unchanged source and policy identity
- **THEN** completed scopes are reused and only unfinished work is executed.

#### Scenario: One company has an invalid candidate
- **WHEN** a candidate fails value or Evidence checks
- **THEN** it is isolated with a reason while unrelated accepted records and other companies continue.

### Requirement: Basic commodity exposure is independent of price sensitivity
The system MUST publish evidence-backed commodity identity or pending source-native mapping, business role, source record references, report/knowledge time and uncertainty through the new-contract research path. Roles MUST be derived from supported products, inputs, energy or hedge disclosures using versioned mappings. Missing market-series binding, quantities or net-profit sensitivity MUST NOT suppress an established association. Ambiguous mappings MUST remain pending. No observed association MUST NOT imply zero exposure.

#### Scenario: Copper input has no disclosed quantity or market series
- **WHEN** accepted source facts identify copper as an input and the commodity mapping is unambiguous
- **THEN** the research view shows the copper input association with unknown quantity and unbound market series
- **AND** it makes no unsupported net-profit direction claim.

#### Scenario: Company has both commodity sales and purchases
- **WHEN** both roles have source evidence
- **THEN** both are retained without invented netting or sensitivity.

### Requirement: Research release and legacy retirement have explicit scope
The implementation MUST connect a single new-contract writer and reader for company facts and basic commodity associations, with automatic rule-based acceptance and retained review evidence. It MUST keep legacy semantic writers disabled, prove isolated persistence and recovery, and expose the exact operator entry before activation. Legacy semantic deletion MUST follow an explicit dependency-aware dry run and verified read cutover; it MUST NOT be a prerequisite for independent new-space research delivery. This planning approval MUST NOT authorize database deletion, activate configuration, or enable DCF or trading.

#### Scenario: New research task is activated
- **WHEN** task integration, accepted-record persistence, consumer boundaries and recovery pass their implementation acceptance
- **THEN** only the declared new research writer and readers are enabled
- **AND** old backfill semantics and unrelated consumers remain disabled.

### Requirement: Quality review measures source semantics rather than structural existence
The system MUST report full-universe core coverage, independently sampled important-disclosure recall and fact accuracy, critical semantic errors, freshness, latency, token cost and review workload. Sampling rules, denominator and expansion thresholds MUST be declared before live observations. Evidence ID existence MUST NOT count as semantic correctness. Fixed Gold scores, untriggered real negatives and human-review item counts MUST NOT veto unrelated accepted research records. Fixture guards and actual source reviews MUST be reported separately.

#### Scenario: Structural links pass but source was not reviewed
- **WHEN** only IDs, hashes or page references were checked
- **THEN** semantic precision remains unassessed and no zero-recurrence claim is made.

### Requirement: First expansion after 4.1 remains a reviewed change
Meeting the 4.1 numeric gates on the current two-report v4 sample MUST NOT be treated as scale-quality success or production authorization. A first expansion MUST be a separately reviewed change that activates a mode on the existing published owner, with an immutable a-priori plan that binds `knowledge_cutoff`, registry identity, and official report versions before enqueue. The current two-company live-run and source-review v1 files MUST remain on disk as the retained baseline of 2 independently reviewed reports and 2 occupied strata. New observations MUST use independent snapshots. Ordinary `run` and `resume` MUST remain available when first-expansion mode is not `active`. `scale_quality_claim_allowed` MUST remain false until a later explicit authorization.

#### Scenario: Current 4.1 result is not a production claim
- **WHEN** source review records recall 7/7, accuracy 7/7, critical numeric 0, 2 reports, and 2 occupied strata with `expansion_gates_met=true`
- **THEN** production authorization remains `not_authorized`
- **AND** scale-quality claims remain forbidden
- **AND** first expansion cannot start from this result alone without the reviewed M4 change
- **AND** the existing two-company v1 report files remain readable after a later expansion snapshot is written
- **AND** an ordinary `run` without an expansion snapshot remains allowed while first-expansion mode is not `active`
