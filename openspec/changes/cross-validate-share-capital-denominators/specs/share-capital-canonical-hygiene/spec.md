## ADDED Requirements

### Requirement: Share-capital canonical unit declarations
The system SHALL declare source units for every mapping into the share-capital canonical family, distinguishing share counts from CNY paid-in-capital amounts, and SHALL reject mappings whose declared unit is incompatible with the canonical's unit class.

#### Scenario: Share-count and CNY sources are declared distinctly
- **WHEN** mappings are loaded into the source field mapping registry
- **THEN** the THS share-capital source SHALL declare `source_unit=shares`
- **AND** the paid-in-capital source SHALL declare `source_unit=CNY` and SHALL NOT be approved as a core share-count source

#### Scenario: Incompatible unit declarations are rejected
- **WHEN** an alias or mapping declares a unit that contradicts its canonical's unit class (share-count canonical with `CNY`, or amount canonical with `shares`)
- **THEN** the loader SHALL raise an error instead of accepting the mapping

### Requirement: Share-capital bad-row quarantine
The system SHALL identify source-side bad share-capital rows by multi-anchor scan across all report periods and quarantine them under a dedicated canonical with full lineage, without altering their values.

#### Scenario: Bad rows are detected by multi-anchor disagreement
- **WHEN** a share-capital fact disagrees with at least two other available anchors by more than 50 percent
- **THEN** the row SHALL be flagged as a quarantine candidate and a sample of candidates SHALL be manually verified before migration

#### Scenario: Quarantine is reversible and audited
- **WHEN** quarantine is executed
- **THEN** the row's canonical name SHALL change to the quarantine canonical with `row_version` incremented and a change-log entry written
- **AND** the original value and raw lineage SHALL remain untouched
- **AND** a re-run of the scan after migration SHALL report no new candidates

#### Scenario: Anchor-quality reporting settles reference-series reliability
- **WHEN** the cross-validation outputs are aggregated
- **THEN** the disagreement rate of each anchor (including valuation-implied shares) SHALL be reportable so the reliability of every reference series is evidence-based
