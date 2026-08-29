## ADDED Requirements

### Requirement: Governance identity parity
The identity fields used by semantic conversion MUST be identical to the fields used by temporal governance and approved-as-of reads. Missing required occurrence fields MUST fail closed with an explicit identity-incomplete diagnostic.

#### Scenario: Conversion and governance agree
- **WHEN** a fact is converted and then promoted
- **THEN** both stages compute the same stable identity and promotion does not create a duplicate temporal row

#### Scenario: Incomplete identity
- **WHEN** a model result lacks source-row or contract identity for a row-sensitive fact
- **THEN** the fact remains candidate or machine rework and is not silently assigned a random replacement identity

### Requirement: Temporal separation by report flow
The system MUST treat different report periods and source revisions as separate report flows while rejecting overlapping duplicates within the same occurrence identity.

#### Scenario: Different report periods
- **WHEN** two otherwise similar facts belong to different annual report periods
- **THEN** both can be approved as separate historical observations

#### Scenario: Same occurrence overlap
- **WHEN** two records have identical occurrence material and overlapping validity
- **THEN** the system reuses or replaces according to explicit policy and never approves both as independent facts
