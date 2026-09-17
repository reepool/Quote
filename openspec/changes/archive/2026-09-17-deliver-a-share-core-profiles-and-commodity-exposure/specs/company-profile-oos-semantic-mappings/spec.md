## MODIFIED Requirements

### Requirement: Adjustment rows do not imply consolidated subject scope
An adjustment marker alone MUST NOT overwrite explicit narrower scope. Subject assignment MUST follow the current default-group policy: the narrow source label 分部间抵销/分部间抵消 with consolidation_adjustment is an explicit adjustment basis; other company-owned rows without narrower scope or conflict use report_default_group_scope. Adjustment values MUST remain separate from ordinary products and MUST NOT be double-counted as group totals.

#### Scenario: Evidence labels a row as inter-segment elimination
- **WHEN** a source preserves 分部间抵消 and consolidation_adjustment without conflicting narrower scope
- **THEN** the existing explicit adjustment rule can use consolidated_group with direct_source_wording
- **AND** other semantic errors remain isolated.
