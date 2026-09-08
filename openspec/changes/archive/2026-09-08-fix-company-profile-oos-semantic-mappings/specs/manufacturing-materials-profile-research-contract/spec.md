## MODIFIED Requirements

### Requirement: Customers, suppliers, concentration, and related-party transactions are separate concepts

The industry contract MUST distinguish customer concentration, supplier concentration, named or anonymous counterparty relationships, and related-party transaction amounts or service ratios. Supplier procurement facts MUST use supplier field semantics even when adjacent to customer disclosures. Related-party rows outside the active concentration checklist MUST remain explicitly deferred or blocked rather than being relabelled as concentration.

#### Scenario: One section reports both customer and supplier totals

- **WHEN** Evidence contains customer sales totals and supplier purchase totals
- **THEN** each direction retains its own field family and metric meaning
- **AND** no supplier value is accepted as a customer fact.

#### Scenario: Related-party transaction disclosure exceeds the concentration checklist

- **WHEN** a related-party table reports sales, purchases, or services not covered by the current concentration task
- **THEN** the source facts remain traceable as deferred or blocked candidates
- **AND** the task does not widen its concentration contract implicitly.

### Requirement: Consolidation adjustments require independent subject support

A `consolidation_adjustment` row MUST preserve its source-native values and row class, but it MUST NOT be assigned `consolidated_group` solely from the adjustment label. Subject promotion requires affirmative group wording or the existing allowed same-report reconciliation.

#### Scenario: Inter-segment elimination lacks group wording

- **WHEN** a source labels a row `分部间抵消` without affirmative group wording or reconciliation Evidence
- **THEN** its subject remains `unclear`
- **AND** the row is not treated as a consolidated-group fact.
