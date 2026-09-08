## ADDED Requirements

### Requirement: Residual benchmark discrepancies are adjudicated offline
The residual manufacturing/materials period, qualifier, subject-strictness, and event-label discrepancies MUST be evaluated from the immutable `stage55-closure-four-20260907-a` bundle and its original Evidence. The change MUST NOT invoke an LLM, alter an Evidence plan, edit that bundle, merge a historical or targeted run, or require another four-report run. Any new benchmark output MUST use a new evaluation identity and identify the immutable bundle as its sole runtime source.

#### Scenario: Offline rules are applied to the closure bundle
- **WHEN** the period, qualifier, subject, and event rules pass focused unit and fixture tests
- **THEN** the benchmark may be rerun offline against `stage55-closure-four-20260907-a`
- **AND** no extract, repair, or verify request is issued

#### Scenario: An unaffected Gold case remains unresolved
- **WHEN** a Gold failure is caused by a missing record, different physical anchor, or semantic contract conflict outside this change
- **THEN** the new evaluation retains that failure or conflict
- **AND** it does not synthesize a match from another record

### Requirement: Manufacturing and materials residual decisions remain explicit
The adjudication MUST distinguish period normalization from factual absence and MUST record the specific Gold metadata or directional event decision used for any changed outcome. The Putailai consolidation-adjustment duration values MAY use annual period equivalence without merging anchors. The Jinhua product-margin subject MAY use only the approved non-group refinement rule. The Jinhua expected-completion case MUST require the source qualifier on its approved Evidence. The Chengfei transfer event MAY use only the explicit directional event equivalence. The Chengfei sales coverage conflict MUST remain `gold_contract_conflict`, and a missing consolidation-adjustment margin or Segment structure MUST remain failed until a future source-backed runtime record exists.

#### Scenario: Directly affected cases improve without score chasing
- **WHEN** a directly affected Gold annotation satisfies its new closed period, qualifier, subject, or event rule
- **THEN** only that annotation's auditable match outcome changes
- **AND** the evaluator reports the rule and runtime target that caused the change

#### Scenario: Production remains unauthorized
- **WHEN** the offline adjudication and benchmark complete
- **THEN** the research slice remains research-only with `production_authorization=not_authorized`
- **AND** stage six, legacy backfill, approved tables, CommodityExposure, ValueChainRole, and DCF remain closed
