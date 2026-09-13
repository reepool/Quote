## MODIFIED Requirements

### Requirement: Subject scope requires affirmative evidence
Subject assignment MUST follow the current report-default policy. Explicit parent-only, named-subsidiary or business-segment Evidence MUST retain the narrower scope. Explicit group wording uses direct_source_wording; a completed same-report numeric reconciliation retains its own basis and Evidence. Otherwise a company-owned fact without contradictory scope MUST use consolidated_group with report_default_group_scope. Affirmative Evidence remains required for a narrower scope or a claim of direct wording, not for the declared default convention.

#### Scenario: Product table reconciles to consolidated revenue
- **WHEN** same-report reconciliation is performed
- **THEN** the result retains numeric_reconciliation_to_consolidated_statement and its cited pages.

#### Scenario: Company wording has no corroboration
- **WHEN** a company-owned fact only says 公司 without a narrower scope or conflict
- **THEN** it uses consolidated_group with report_default_group_scope.

#### Scenario: Scope conflict remains
- **WHEN** explicit local scope statements contradict one another
- **THEN** the candidate remains unclear rather than using the default to hide the conflict.

### Requirement: Consolidation adjustments require independent subject support
A `consolidation_adjustment` row MUST preserve source-native values and row class. New subject assignment MUST follow the report-default policy with explicit narrower scopes taking precedence. The narrow source label 分部间抵销/分部间抵消 qualifies for the explicit adjustment rule; other company-owned rows without narrower Evidence or conflict use report_default_group_scope. The marker MUST NOT cause duplicate group totals or overwrite explicit subsidiary scope.

#### Scenario: Inter-segment elimination lacks group wording
- **WHEN** a source labels a row `分部间抵消` without affirmative group wording or reconciliation Evidence
- **THEN** the explicit adjustment rule may preserve consolidated_group with direct_source_wording if no contradictory narrower scope exists
- **AND** the adjustment remains distinct from ordinary group-total facts
