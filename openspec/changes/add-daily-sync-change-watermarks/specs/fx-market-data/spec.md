## ADDED Requirements

### Requirement: FX Observations Emit Change Records
FX rate sync SHALL emit changelog records for inserted or materially changed direct and derived FX observations.

#### Scenario: Direct FX observation value changes
- **WHEN** a stored FX observation is refetched with a changed value or semantic hash
- **THEN** the FX storage path SHALL append an FX-domain change record
- **AND** the record SHALL include series id, observation date, source, and revision id where available

### Requirement: Derived FX Changes Preserve Lineage
Derived FX observations SHALL record source lineage so callers can understand why a derived cross-rate changed.

#### Scenario: Derived CNH cross-rate changes
- **WHEN** a derived FX observation changes because one source leg changed
- **THEN** the change record or row metadata SHALL expose lineage or input hash information for the derivation

### Requirement: FX Changelog Does Not Replace Calendar Governance
FX changelog emission SHALL NOT replace FX publication calendar governance or quality checks.

#### Scenario: No observation expected
- **WHEN** FX calendar governance marks a date as an expected non-publication date
- **THEN** FX sync SHALL NOT emit a missing-data change record solely because no observation was written
