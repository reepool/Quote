## ADDED Requirements

### Requirement: CNInfo Canonical Maintenance Requires Fresh Factor Inputs
The scheduled CNInfo corporate-action maintenance workflow SHALL verify the successful freshness watermark of its quote and BaoStock/Sina factor predecessor before updating the promoted canonical series.

#### Scenario: Predecessor completed successfully
- **WHEN** the prior market-data update persisted every quote row and completed factor refresh through the exchange's actual completed trading-session cutoff
- **THEN** CNInfo maintenance MAY build and merge the affected canonical subset

#### Scenario: Trading calendar is current but persisted quotes are stale
- **WHEN** the expected trading-session cutoff is newer than the complete quote coverage stored for eligible tradable stocks
- **THEN** the predecessor watermark SHALL remain partial at the persisted coverage date
- **AND** CNInfo canonical merge SHALL remain deferred

#### Scenario: Predecessor is missing or stale
- **WHEN** the required quote or BaoStock/Sina factor watermark is missing, has persistence failures, or is older than the factor cutoff
- **THEN** CNInfo observations MAY still be refreshed
- **AND** canonical selection and merge SHALL be deferred with a visible partial reason and resumable scope
