## ADDED Requirements

### Requirement: Adjustment Factor APIs Resolve The Active Series Efficiently
Adjustment-factor API defaults SHALL resolve the active canonical series and SHALL keep ordinary responses bounded.

#### Scenario: Caller omits canonical series version
- **WHEN** a caller queries canonical factors or factor quality without a series version
- **THEN** the API SHALL resolve the currently active canonical series
- **AND** it SHALL NOT query an obsolete hard-coded version

#### Scenario: Caller requests factor quality
- **WHEN** a caller requests factor quality without a detail flag
- **THEN** the API SHALL return compact series metrics and bounded samples
- **AND** full-market decisions SHALL be available only through a paged detail query
