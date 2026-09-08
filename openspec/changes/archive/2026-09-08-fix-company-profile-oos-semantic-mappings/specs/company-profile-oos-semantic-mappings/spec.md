## ADDED Requirements

### Requirement: Distinct source rows remain distinct occurrences

The Stage 5 semantic adapter MUST include the source row or measured object in occurrence conflict identity whenever two candidates share logical slot, period, and physical Evidence but represent different source rows. Equal numeric values MUST NOT cause distinct source objects to be merged or marked as the same occurrence.

#### Scenario: Equal utilization values belong to different steel classes

- **WHEN** Evidence reports `铁 ... 96%` and `坯材 ... 96%` under the same utilization header
- **THEN** the adapter retains two independently anchored Measurements with measured objects `铁` and `坯材`
- **AND** it does not emit `occurrence_semantic_conflict` solely because the values are equal

### Requirement: Supplier disclosures use supplier field semantics

When Evidence explicitly describes supplier procurement amount or supplier concentration, the adapter MUST bind the candidate to the supplier field family and preserve `supplier_purchase_amount` or the source-supported share metric. It MUST NOT bind a supplier fact to `customer_concentration` merely because both appear in one counterparty scope.

#### Scenario: Top-five supplier totals are adjacent to customer totals

- **WHEN** the source reports customer sales totals followed by supplier purchase totals
- **THEN** the customer and supplier Measurements retain separate field IDs and logical slots
- **AND** neither value is relabelled as the other counterparty direction

### Requirement: Related-party transactions remain separate from concentration

The Stage 5 scope contract MUST distinguish named related-party transaction amounts, purchase/service ratios, and top-five customer or supplier concentration. A related-party amount outside the active concentration checklist MUST remain an explicit blocked or deferred candidate with its source metric, rather than being accepted as concentration or silently discarded.

#### Scenario: Related-party purchase and service rows occur in a concentration chapter

- **WHEN** Evidence contains related-party sales, purchases, or service totals not covered by the concentration checklist
- **THEN** the workflow records the scope mismatch explicitly
- **AND** it does not map those amounts into customer or supplier concentration fields

### Requirement: Adjustment rows do not imply consolidated subject scope

An adjustment row MAY retain `row_class=consolidation_adjustment` and its source-native revenue/cost/margin Measurements, but the adapter MUST keep `subject_scope=unclear` unless the cited Evidence contains affirmative group wording or an allowed same-report reconciliation. The row class alone MUST NOT promote the subject.

#### Scenario: Evidence labels a row as inter-segment elimination

- **WHEN** the source row is `分部间抵消` without `合并`, `本集团`, `集团`, or reconciliation Evidence
- **THEN** the row and values remain available with `subject_scope=unclear`
- **AND** unsupported `consolidated_group` promotion is blocked.

### Requirement: Semantic corrections are verified before a new run

The change MUST prove each correction with offline fixtures and focused regression tests before invoking the gateway. If a new out-of-sample run is executed, it MUST use one fresh run ID, preserve immutable historical bundles, and retain `production_authorization=not_authorized`.

#### Scenario: Local fixtures pass

- **WHEN** all four semantic mapping fixtures and existing Stage 5 regressions pass
- **THEN** one complete validation run MAY be started
- **AND** no targeted loop or historical bundle merge is permitted.
