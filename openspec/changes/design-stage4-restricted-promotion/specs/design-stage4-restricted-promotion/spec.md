## ADDED Requirements

### Requirement: The design stays closed until review
Before the design review passes, the change MUST only contain its design documents. It MUST NOT change Python. It MUST NOT replay, enqueue, or edit a historical artifact, ledger, or control-plane file. It MUST NOT enable publication or grant production, scale quality, DCF, trading, or price-sensitivity use.

#### Scenario: Design review does not start implementation
- **WHEN** the design review has not yet accepted this contract
- **THEN** no query or export code is changed
- **AND** the frozen replay artifacts remain unchanged

### Requirement: Research query uses the existing read owner
A later implementation MUST expose the four accepted chapters through `company_profile_common_core` query and export. `CompanyProfileTaskService` MUST remain the application owner, and `CompanyProfileReadService` MUST remain the read owner. CLI, Scheduler, and Telegram MUST only forward to that service. The implementation MUST NOT add a second query path or make `CompanyProfileResearchWriter` write these chapter results.

#### Scenario: Query does not create a runtime record
- **WHEN** a researcher queries one of the four frozen companies
- **THEN** the response is rendered by the existing read service
- **AND** no new common-core runtime record is written

### Requirement: The input set is the frozen four-report contract
The read projection MUST use only `300750.SZ`, `603659.SH`, `920015.BJ`, and `302132.SZ` for report period 2025-12-31, with the report identities and PDF hashes frozen by the archived aggregate contract. It MUST use only material-input `.2026-09-30.3`, operating-quantity `.2026-10-01.4`, segment-financial `.2026-10-01.4`, and counterparties `.2026-09-29.1`, each bound to that contract's enqueue, run, result, and source-review hashes. A hash mismatch MUST omit that chapter. Historical observations MUST keep their original values and MUST NOT replace a current chapter.

#### Scenario: A historical result does not fill a gap
- **WHEN** a current chapter artifact hash does not match the aggregate contract
- **THEN** that chapter is not delivered
- **AND** the historical failure rows remain unchanged

### Requirement: Each company returns a partial chapter view
For each of the four companies, query and export MUST return material inputs, operating quantities, segment financials, and counterparties from the matching frozen result. Page, reported period, unit, and chapter semantics MUST remain the stored values. Facts, source evidence, and coverage MUST be exportable. The four chapter scores MUST NOT be added together.

#### Scenario: A researcher exports one company
- **WHEN** a researcher exports `300750.SZ` for the frozen 2025 report
- **THEN** the export contains that company's four chapter facts, evidence, and coverage
- **AND** the export does not include another company's facts as substitutes

### Requirement: Chengfei material inputs stay legally empty
The `302132.SZ` material-input section MUST show the stored `legal_empty` scope outcomes and MUST contain zero material-input facts. A `legal_empty` outcome MUST NOT be rendered as a named material fact or as an unchecked gap.

#### Scenario: Legal empty is visible
- **WHEN** a researcher queries `302132.SZ` material inputs
- **THEN** the fact list is empty
- **AND** the stored legal-empty coverage remains visible

### Requirement: Repeated access does not duplicate or overwrite history
Query MUST NOT write files. Export MUST write only inside the caller-supplied export directory. Repeated query MUST return the same facts without appending a second copy into research storage. Export MUST NOT write into a replay root, including a later sibling of a dated replay directory, or into a symlink to that root. It MUST NOT write into the common-core namespace, a checkpoint, or a publication-control file. Before writing any output byte, export MUST acquire exclusive use of the target directory. A conflicting export MUST fail and MUST leave the first completed export unchanged. Existing common-core profiles MUST NOT be overwritten by this chapter projection.

#### Scenario: A second query adds no stored facts
- **WHEN** the same company and report are queried twice
- **THEN** the returned facts are the same set
- **AND** the frozen result files and historical observations keep their original bytes

#### Scenario: A new replay subdirectory is rejected
- **WHEN** an export targets a new directory under a protected replay root or a symlink to that root
- **THEN** the export fails before writing
- **AND** the replay root stays unchanged

#### Scenario: Concurrent exports do not mix companies
- **WHEN** two company exports target the same directory at the same time
- **THEN** one export fails without writing
- **AND** the completed export contains only its own company records

### Requirement: Four chapters do not complete the core profile
The restricted view MUST set its own core-profile completeness to false. Missing `extract_business_overview` and `extract_business_regime` MUST be shown as gaps not included in this delivery. This contract MUST NOT require those chapters, a six-chapter package, or a regime date before the four accepted chapters can be queried.

#### Scenario: Overview remains a displayed gap
- **WHEN** the four accepted chapters are returned
- **THEN** the restricted view says the core profile is not complete
- **AND** business overview and business regime remain visible gaps

### Requirement: Research consumers stop before publication
The authorized consumer MUST be research query and export only. Responses MUST keep `production_authorization` as `not_authorized` and `scale_quality_claim_allowed` as false. DCF, trading, and price sensitivity MUST remain unauthorized. The read path MUST NOT call publication enable, pause, resume, or rollback. While publication is paused or rolled back, new profile writes MUST remain refused, and this read path MUST still only read the frozen artifacts.

#### Scenario: A successful query does not enable publication
- **WHEN** query returns a restricted research view
- **THEN** publication control is unchanged
- **AND** production, scale quality, DCF, and trading stay unauthorized
