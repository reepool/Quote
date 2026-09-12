## MODIFIED Requirements

### Requirement: Partition results merge fail-closed before semantic expansion
Partition responses MUST be merged at the compact segment-row layer under the original request identity. Before row identity comparison, an ordinary row whose controlled Evidence binds its source dimension and label MUST normalize absent, `unclear`, and `business_segment` scope to the existing `business_segment` representation. A `consolidated_group` draft with `subject_basis=report_default_group_scope` MUST collapse to `business_segment` when paired with that segment representation, but the existing stronger-basis rule MUST retain a direct-source or numeric-reconciled consolidated-group result when it is paired only with the report-default draft. Explicit issuer, named-subsidiary, affirmative consolidated-group subjects, and consolidation-adjustment rows MUST remain distinct. Rows may merge only when their source dimension, label, normalized period, normalized subject fields, and Evidence IDs match; metric cell keys MUST be disjoint. Coverage MUST remain field-local. The combined result MUST pass the original minimal schema, full `ExtractResponse` validation, existing occurrence reconciliation, independent verification, and report projection without lowering any requirement.

#### Scenario: Revenue and cost partitions describe the same row
- **WHEN** two successful partitions return the same Evidence-bound source row with disjoint revenue and cost cells, and one uses the report-default group draft while the other uses `unclear` or `business_segment`
- **THEN** the adapter normalizes both ordinary rows to `business_segment`, combines the cells into one compact row, and expands it once under the original request ID
- **AND** the workflow does not create duplicate Segment records or a false subject identity conflict

#### Scenario: Stronger group basis refines the report default
- **WHEN** one partition uses `report_default_group_scope` and another partition for the same row carries a direct-source or numeric-reconciled consolidated-group basis
- **THEN** the existing stronger group basis remains authoritative
- **AND** the report-default draft is not prematurely collapsed to `business_segment`

#### Scenario: Explicit group subject conflicts with a segment row
- **WHEN** one partition has an affirmative direct-source or numeric-reconciled consolidated-group subject and another partition resolves the same anchor only as a business segment
- **THEN** the extract ends with a typed schema/contract failure
- **AND** the report-default normalization does not erase the substantive subject conflict

#### Scenario: Consolidation adjustment retains special subject semantics
- **WHEN** a partition row is a `consolidation_adjustment`
- **THEN** its existing explicit wording or reconciliation subject rules remain authoritative
- **AND** it is not normalized to `business_segment`

#### Scenario: Other row identities conflict
- **WHEN** partitions disagree on source dimension, label, Evidence IDs, normalized period, preserved subject semantics, row class, or return the same metric cell twice
- **THEN** the extract ends with a typed schema/contract failure
- **AND** no partial partition result enters verification or the research projection
