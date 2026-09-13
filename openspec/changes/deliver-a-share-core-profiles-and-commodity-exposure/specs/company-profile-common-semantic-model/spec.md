## MODIFIED Requirements

### Requirement: Subject period and business regime are explicit
Every governed fact MUST carry a supported subject scope and period semantics. Subject scope MUST be one of `consolidated_group`, `issuer`, `named_subsidiary`, `business_segment`, or `unclear`. When the source does not explicitly identify a narrower issuer, named subsidiary, or business segment and no unresolved subject conflict exists, the research adapter MUST use `subject_scope=consolidated_group` with `subject_basis=report_default_group_scope`. Explicit issuer, subsidiary, segment, and source-supported numeric-reconciliation scopes remain authoritative. Regime-sensitive facts MUST preserve report period, knowledge time, regime effective time, and comparison basis without rewriting historical knowledge. An explicitly restated comparative MUST carry `comparison_basis`; a missing basis is a blocker, not an inferred default.

#### Scenario: Ordinary report wording uses the default group scope
- **WHEN** a report fact is written with `公司` or otherwise has no explicit narrower subject and the Evidence supports the fact
- **THEN** the adapter records `subject_scope=consolidated_group` and `subject_basis=report_default_group_scope`
- **AND** it preserves the original wording and Evidence without claiming issuer-specific wording

#### Scenario: Explicit standalone wording remains issuer-scoped
- **WHEN** local Evidence explicitly identifies a parent-only statement, 母公司 or 本公司单体 scope rather than ordinary company wording
- **THEN** the fact retains `subject_scope=issuer` and its source-supported basis
- **AND** the default group rule does not widen the subject

#### Scenario: Named subsidiary or segment remains narrow
- **WHEN** Evidence identifies a named subsidiary or a disclosed segment row
- **THEN** the fact retains `named_subsidiary` or `business_segment`
- **AND** it is not promoted to consolidated group solely by the report-level default

#### Scenario: Same-control comparative is disclosed later
- **WHEN** a post-restructuring report restates an earlier period on a same-control basis
- **THEN** the restated fact coexists with the predecessor's original-as-published fact using separate knowledge times and comparison bases
- **AND** the later record does not overwrite the earlier known fact
