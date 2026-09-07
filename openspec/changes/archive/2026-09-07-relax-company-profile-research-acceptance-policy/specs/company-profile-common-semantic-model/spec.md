## MODIFIED Requirements

### Requirement: Researcher projection exposes facts uncertainty evidence and downstream boundaries
The system MUST provide a stable research-facing projection grouped into business overview, regime/package, segments, activities, operating measurements, disclosed inputs and counterparties, business events, coverage, and evidence references. The projection MUST distinguish reported facts, deterministic derivations, and research assumptions. It MUST derive discrete confidence as `high`, `medium`, `low`, or `rejected` from existing Evidence completeness, verification/disposition, subject basis, and contradiction state; the LLM MUST NOT provide a probability. A program-owned closed usage policy MUST restrict consolidated aggregation, ranking, and cross-company comparison according to `object_type`, metric family, measured object, and `subject_scope`/`subject_basis`. Commodity exposure and value-chain sections MUST explicitly report `not_authorized`, `not_assessed`, `insufficient_evidence`, or approved bounded results rather than infer direction or complete chain position.

#### Scenario: Manufacturing profile is rendered with subject caveats
- **WHEN** validated manufacturing/materials records for one report are projected and some accepted records have `subject_scope=unclear`
- **THEN** a researcher can read the source-native business overview, product revenue/cost/margin, applicable capacity/production/sales/inventory, disclosed inputs/relationships, regime, and coverage state
- **AND** each displayed fact links to source-native value and Evidence, while consolidated-sensitive uses are withheld according to the closed policy

#### Scenario: Unclear consolidated-sensitive fact is restricted
- **WHEN** an accepted revenue, margin, or concentration Measurement has `subject_scope=unclear`
- **THEN** the projection may retain it as a source fact with bounded confidence
- **AND** it MUST NOT expose the Measurement as a consolidated aggregation, ranking, or cross-company comparison input

#### Scenario: Business-segment fact remains distinct
- **WHEN** a Measurement has `subject_scope=business_segment`
- **THEN** the projection may display it as a segment fact with its derived confidence
- **AND** it MUST NOT reinterpret it as a group total without explicit or reconciled group basis

#### Scenario: Raw-material price risk is mentioned without an approved exposure contract
- **WHEN** the report mentions lithium, nickel, or cobalt price effects but no later commodity mapping and direction decision has been approved
- **THEN** the projection may show the disclosed material/risk fact and `commodity_exposure.status=not_assessed`
- **AND** it does not state that a price rise is positive or negative for profit
