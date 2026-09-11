## ADDED Requirements

### Requirement: A unique complete segment heading is bound from controlled Evidence

For `extract_segment_financials`, when no explicit `source_row_dimensions` mapping exists and controlled Evidence yields exactly one supported complete segment table heading, the Stage 5 adapter MUST own that heading locally. The compact provider row schema MUST omit and forbid `dimension`; normal rows MUST be expanded and partition-merged using the exact Evidence heading. The provider MUST continue to own the row label, metric cells, period, subject decision, and Evidence IDs. An explicit `source_row_dimensions` mapping MUST retain precedence.

#### Scenario: One complete report-segment heading is locally bound

- **WHEN** controlled Evidence contains exactly one supported complete heading `报告分部的财务信息` and a compact normal row omits `dimension`
- **THEN** the adapter expands the row with `segment_dimension=报告分部的财务信息`
- **AND** the resulting Segment and Measurements preserve the model-supplied row label, cells, period, subject decision, and Evidence IDs.

#### Scenario: Explicit row mapping remains authoritative

- **WHEN** the prepared scope provides `source_row_dimensions` for its allowed labels
- **THEN** the existing per-row local mapping remains authoritative
- **AND** the unique-scope-heading rule does not replace or broaden that mapping.

### Requirement: Ambiguous segment headings remain provider-owned and fail closed

The Stage 5 adapter MUST NOT locally bind a segment heading when controlled Evidence contains zero or more than one supported complete heading. It MUST NOT derive a complete heading from a prefix, translated value, neighboring narrative heading, or inferred business meaning. Existing source-label, full-heading, Evidence, period, subject, duplicate-cell, coverage, and row-identity validation MUST remain enforced.

#### Scenario: Multiple complete headings are not automatically selected

- **WHEN** one controlled scope contains two supported complete headings such as `分产品` and `分地区`
- **THEN** the provider schema continues to require a source dimension
- **AND** the adapter does not choose either heading locally.

#### Scenario: Short prefix is not repaired when local binding is unavailable

- **WHEN** local binding is unavailable because controlled Evidence contains multiple supported headings including `报告分部的财务信息`, and a provider returns `报告分部`
- **THEN** existing validation rejects the ambiguous prefix
- **AND** the adapter does not rewrite it to the complete heading.

### Requirement: Consolidation adjustments retain their special dimension

A compact row marked `row_class=consolidation_adjustment` MUST resolve to `dimension=adjustment` after its label passes the existing explicit adjustment checks, even when the surrounding scope has one unique locally bound table heading. This rule MUST NOT relax the existing subject or reconciliation requirements for consolidation adjustments.

#### Scenario: Adjustment row does not inherit the normal table heading

- **WHEN** a uniquely bound segment scope contains a valid `consolidation_adjustment` row
- **THEN** the expanded Segment and Measurements use `segment_dimension=adjustment`
- **AND** normal rows in the same scope use the unique complete Evidence heading.
