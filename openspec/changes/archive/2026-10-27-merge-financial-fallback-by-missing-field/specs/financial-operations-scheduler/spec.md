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
- **AND** SHALL list discrepancy samples with instrument, report period, canonical fact, and both values, bounded end-to-end by the configured `report_top_n`

#### Scenario: Comparison runs with no discrepancies

- **WHEN** all compared facts are within tolerance
- **THEN** the report SHALL state the compared and consistent counts without listing samples

#### Scenario: Sanity anomalies from single-sourced fill fields

- **WHEN** definitional sanity checks on fallback-filled fields without an official counterpart produce review-only marks
- **THEN** the report SHALL list them alongside comparison discrepancies, labeled as review-only and not as source differences

#### Scenario: Comparison disabled

- **WHEN** source-fact reconciliation is disabled by configuration
- **THEN** the report SHALL omit the comparison summary without warning

### Requirement: Degraded Reports State The Reason In Plain Language

When a financial disclosure task reports a degraded or partial status, the report SHALL state the concrete causes in operator-readable wording and list the pending targets with their retry windows.

#### Scenario: Degraded solely by pending disclosure

- **WHEN** the run's unresolved targets are all pending recheck within their retry windows
- **THEN** the report SHALL show a reason line such as "N 个标的等待披露方数据，处于重试窗口内"
- **AND** SHALL list up to three pending samples with instrument, report period, missing fields, and retry deadline
- **AND** the source-warning note SHALL attribute the routing errors to pending disclosure (empty official responses are expected for unpublished data) rather than to official-source degradation

#### Scenario: Degraded by real collection failures

- **WHEN** unresolved targets exceed the pending-recheck count
- **THEN** the warning note SHALL keep the official-source degradation wording
