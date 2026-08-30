## ADDED Requirements

### Requirement: Candidate isolation after cleanup
The publication layer MUST publish only records that pass current identity, evidence, and semantic gates. Deleted or non-reusable candidate artifacts MUST not be eligible for publication or replay.

#### Scenario: Failed candidate is removed
- **WHEN** cleanup deletes a failed semantic receipt and its candidate descendants
- **THEN** publication and replay queries exclude those records

#### Scenario: Valid candidate remains reviewable
- **WHEN** a candidate has complete evidence and a current identity but is not yet approved
- **THEN** it remains available to the normal review path without being exposed as approved output

### Requirement: Publication failure transparency
The system MUST expose publication gaps and their deterministic reasons without converting them into successful publication or hiding them as empty data.

#### Scenario: Publication gate fails
- **WHEN** a derived exposure or role lacks a required approved component
- **THEN** publication remains unapproved and reports the missing gate reason

#### Scenario: Complete publication
- **WHEN** all required components and identity gates pass
- **THEN** the system publishes the derived record once and a replay does not duplicate it

### Requirement: Action-preserving exposure lineage
The exposure fact, publication payload, predecessor lookup, and repair audit MUST use the same non-empty `source_activity_action` for action-sensitive lineage. Actions that share a market role, such as `sells` and `produces`, MUST remain distinguishable in lineage and MUST NOT be linked as predecessor/successor solely because their derived role is equal. Collision repair MUST recover the action from referenced exposure facts when publication metadata is absent; if neither side can be resolved, it MUST report `lineage_incomplete` rather than silently skipping the audit.

#### Scenario: Same commodity with different actions
- **WHEN** one approved exposure is derived from `sells` and another from `produces` for the same commodity and scope
- **THEN** predecessor lookup does not cross-link the two actions, and the collision audit can report an action mismatch if an old record already contains such a link

#### Scenario: Legacy lineage is audited from facts
- **WHEN** an old exposure publication lacks `source_activity_action` but its referenced exposure fact contains the action
- **THEN** collision repair uses the fact action for comparison and reports a mismatch when the predecessor action differs; if the referenced fact has been removed, the audit reports `lineage_incomplete`

### Requirement: Reported numeric precision is part of publication validation
Gross-margin reconciliation MUST account for the precision and unit of the reported margin itself, in addition to revenue and cost rounding. Missing cost MUST NOT cause an out-of-range or unit-inconsistent reported margin to be treated as valid.

#### Scenario: Rounded integer percentage
- **WHEN** the calculated margin is `35.249%` and the report discloses `35%`
- **THEN** the result is evaluated using the disclosed precision and is not rejected solely because the normalized difference exceeds a fixed `0.01%` tolerance

#### Scenario: Missing cost cannot validate an out-of-range margin
- **WHEN** segment cost is missing or not applicable and a reported margin is outside the canonical fraction range or conflicts with its declared/header unit
- **THEN** reconciliation records a publication blocker and the row cannot be automatically promoted merely because cost reconciliation is not applicable

### Requirement: Gross-margin normalization has one owner
Deterministic segment extraction MUST carry the table-header percent unit (or an explicit row unit) into the margin normalizer. Gross margins MUST be converted to the canonical fraction exactly once before reconciliation and publication; downstream validation MUST NOT divide an already normalized fraction again or infer a percent unit from the numeric magnitude alone.

#### Scenario: Header percent is normalized once
- **WHEN** a segment table declares `%` in its header and reports a gross margin of `35%`
- **THEN** the extracted and published value is the fraction `0.35`, with provenance showing the header unit, and a second conversion is not applied

#### Scenario: Explicit fraction is preserved
- **WHEN** a deterministic source already supplies a canonical fraction such as `0.35249`
- **THEN** reconciliation consumes that fraction unchanged and records the source unit as fraction rather than treating it as a percent value
