## ADDED Requirements

### Requirement: Security-code and issuer listing dates are distinguished
The instrument master governance layer SHALL distinguish the official
security-code first-listing date from issuer-regime listing dates when reviewed
lineage evidence shows that the current issuer entered through a code-preserving
reorganization.

#### Scenario: Official code date predates current issuer
- **WHEN** SSE official master evidence gives `600018.SH` a first-listing date of `2000-07-19` and reviewed issuer evidence gives the current issuer a listing date of `2006-10-26`
- **THEN** governance SHALL preserve `2000-07-19` as the canonical security-code listing date
- **AND** it SHALL record `2006-10-26` as the current issuer-regime start in lineage metadata

#### Scenario: Current-universe selection reads the instrument
- **WHEN** a current-universe job resolves `600018.SH`
- **THEN** the additional issuer-regime metadata SHALL NOT replace its canonical instrument id, exchange, current name, or active status

### Requirement: Reviewed lineage evidence extends master provenance
The instrument master governance layer SHALL persist reviewed code-lineage
evidence as additive provenance without changing existing source-authority
rules for ordinary listing and delisting fields.

#### Scenario: Reviewed lineage metadata is saved
- **WHEN** a lineage repair is successfully applied
- **THEN** master metadata SHALL identify the catalog version, evidence sources, issuer regimes, review decisions, and continuity policy

#### Scenario: Routine master refresh runs later
- **WHEN** a normal A-share master refresh updates current instrument fields
- **THEN** it SHALL preserve previously reviewed lineage metadata unless a newer reviewed lineage version explicitly replaces it
