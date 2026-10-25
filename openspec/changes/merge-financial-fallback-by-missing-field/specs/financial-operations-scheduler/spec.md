## ADDED Requirements

### Requirement: Missing Official Fields Report Distinguishes Systematic Gaps

Financial disclosure task reports SHALL distinguish canonical facts systematically missing from the official source (missing across most officially parsed targets in the run) from per-instrument missing facts.

#### Scenario: One field is systematically missing

- **WHEN** a canonical fact such as `equity_parent` is missing from nearly all officially parsed targets in a run
- **THEN** the report SHALL label that fact as a systematic official-source gap
- **AND** SHALL list per-instrument missing facts separately with sample instruments

#### Scenario: Only per-instrument gaps exist

- **WHEN** no canonical fact is systematically missing but individual targets lack official facts
- **THEN** the report SHALL show no systematic gap label and SHALL list the affected instruments

### Requirement: Source Comparison Summary In Financial Reports

Financial disclosure incremental and reconciliation reports SHALL include a summary of the same-fetch official/fallback source comparison when it runs.

#### Scenario: Comparison runs with discrepancies

- **WHEN** the reconciliation stage compared overlapping canonical facts and found discrepancies
- **THEN** the report SHALL include compared field count, consistent count, discrepancy count
- **AND** SHALL list a bounded number of discrepancy samples with instrument, report period, canonical fact, and both values

#### Scenario: Comparison runs with no discrepancies

- **WHEN** all compared facts are within tolerance
- **THEN** the report SHALL state the compared and consistent counts without listing samples

#### Scenario: Comparison disabled

- **WHEN** source-fact reconciliation is disabled by configuration
- **THEN** the report SHALL omit the comparison summary without warning
