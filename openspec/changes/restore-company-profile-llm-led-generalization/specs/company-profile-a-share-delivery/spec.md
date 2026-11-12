## MODIFIED Requirements

### Requirement: Core usefulness measures substantive business answers
For new profiles after the 2026-11-12 user instruction, the system MUST treat supply-chain position, inputs and downstream fields as optional enrichment, not a company pass/fail prerequisite. Missing enrichment remains explicit and does not invalidate the substantive business/products/revenue profile. Existing frozen scores and their historical view requirements are unchanged.
The system MUST distinguish the v9 eight-element workpaper's readable usefulness from formal company completeness. A structured, sourced partial workpaper may answer business structure, products/services, monetization, scale/mix, operating inputs, customers/channels/markets, drivers and changes with explicit gaps; successful JSON keys or raw fact counts do not prove useful content. Formal core completeness still requires separate substantive evidence-backed principal business, products/services and revenue model, with product/input exposure and roles evaluated under the acceptance program. Absent optional numeric segmentation must not prevent an evidenced qualitative model. Existing fact/Evidence types and the sole writer/query owner MUST serve the workpaper projection and rendering, without a parallel database or semantic rules replacing the LLM.

#### Scenario: Single business has no segment amounts
- **WHEN** the report evidences a principal service and how it earns revenue but no segment amounts
- **THEN** the system can deliver the qualitative core with explicit numerical gaps.

#### Scenario: Many numeric rows but no principal business
- **WHEN** a result contains many accepted measurements but lacks principal-business evidence
- **THEN** accepted facts remain deliverable and core completeness remains false.

### Requirement: Planned collection vetoes the revenue sentence
Semantic review MUST distinguish actual, planned, negated and historical actions with their own subject, period and clause context. A planned collection MUST NOT be promoted to current revenue, but an independently evidenced actual fee mechanism in the same sentence MUST remain usable. A whole-sentence regex veto MUST NOT replace contextual judgment under the new policy. Source wording and relevant qualifiers MUST remain traceable; old v13-v16 frozen verdicts MUST remain historical.

#### Scenario: Only planned collection is described
- **WHEN** a service passage supports only planned future fees
- **THEN** it does not establish current revenue generation

#### Scenario: Actual and planned actions coexist
- **WHEN** a passage reports current collected fees and a distinct proposed new fee
- **THEN** the current source-supported mechanism remains available and the proposal stays planned rather than vetoing the entire sentence

### Requirement: Durable tasks deliver partial results continuously
The existing queue and company-profile application owner MUST implement preview, run, status, pause and resume with bounded resource usage. Execution MUST automatically prepare Evidence, checkpoint completed scopes, isolate record-local semantic errors and resume unfinished scopes without repeating accepted identical work. Changed report or policy identity MUST create a scoped successor. Ordinary production MUST NOT use fixed research sample counts or OOS exclusion lists. Reliable official structured facts MUST be reused where available, but mandatory new-report LLM interpretation and coverage checking MUST not be skipped. Run identity MUST include the source, processing, model and prompt versions. Resources and continuation limits MUST follow the current acceptance plan rather than legacy pair-run budgets.

#### Scenario: Execution stops after two scopes
- **WHEN** a task resumes with unchanged source and policy identity
- **THEN** completed scopes are reused and only unfinished work is executed.

#### Scenario: One company has an invalid candidate
- **WHEN** a candidate fails value or Evidence checks
- **THEN** it is isolated with a reason while unrelated accepted records and other companies continue.

### Requirement: Basic commodity exposure is independent of price sensitivity
The system MUST first publish major product/service output and input exposure and evidence-supported supply-chain positioning, including non-commodity services. It MUST additionally publish evidence-backed commodity identity or pending source-native mapping, business role, source record references, report/knowledge time and uncertainty through the new-contract research path. Roles MUST be supported by contextual source evidence. Deterministic mappings and model interpretations MUST be distinguishable, retaining their versions, citations and uncertainty. Missing market-series binding, quantities or net-profit sensitivity MUST NOT suppress an established association. Ambiguous mappings MUST remain pending. No observed association MUST NOT imply zero exposure.

#### Scenario: Copper input has no disclosed quantity or market series
- **WHEN** accepted source facts identify copper as an input and the commodity mapping is unambiguous
- **THEN** the research view shows the copper input association with unknown quantity and unbound market series
- **AND** it makes no unsupported net-profit direction claim.

#### Scenario: Company has both commodity sales and purchases
- **WHEN** both roles have source evidence
- **THEN** both are retained without invented netting or sensitivity.

### Requirement: Research release and legacy retirement have explicit scope
The implementation MUST connect a single new-contract writer and reader for company facts and basic commodity associations, with automatic hard-constraint enforcement, contextual semantic verification and retained review evidence. Model output MUST NOT approve itself; no-provider deterministic semantics MUST NOT serve as a successful fallback for the new policy. It MUST keep legacy semantic writers disabled, prove isolated persistence and recovery, and expose the exact operator entry before activation. Legacy semantic deletion MUST follow an explicit dependency-aware dry run and verified read cutover; it MUST NOT be a prerequisite for independent new-space research delivery. This planning approval MUST NOT authorize database deletion, activate configuration, or enable DCF or trading.

#### Scenario: New research task is activated
- **WHEN** task integration, accepted-record persistence, consumer boundaries and recovery pass their implementation acceptance
- **THEN** only the declared new research writer and readers are enabled
- **AND** old backfill semantics and unrelated consumers remain disabled.
