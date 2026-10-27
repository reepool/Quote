## ADDED Requirements

### Requirement: Source Fact Comparison Reuses The Fill Fetch

The official-versus-fallback fact comparison SHALL operate on the fallback data fetched for filling in the same run and SHALL NOT issue additional network requests for comparison.

#### Scenario: Fallback bundle is fetched for a target

- **WHEN** the fallback bundle for an instrument-period is fetched during the repair run
- **THEN** the comparison SHALL use that in-run bundle data against the official facts
- **AND** the run's request count SHALL not increase due to comparison

### Requirement: Discrepancies Are Marked With Both Values

Canonical facts within the configured comparison scope that are available from both official and fallback sources SHALL be compared under a configurable tolerance; facts beyond tolerance SHALL be marked as discrepancies recording both values and the relative difference. The initial comparison scope SHALL cover the required core canonical facts and SHALL be extensible by configuration without code changes.

#### Scenario: Values match within tolerance

- **WHEN** official and fallback values for a canonical fact are equal or within the configured relative tolerance and absolute floor
- **THEN** the fact SHALL be counted as consistent and SHALL not be flagged

#### Scenario: Values differ beyond tolerance

- **WHEN** the difference exceeds the tolerance
- **THEN** a discrepancy record SHALL persist the instrument, report period, canonical fact, source labels, both values, and the relative difference

#### Scenario: One side lacks the fact

- **WHEN** only one of the two sources provides a canonical fact
- **THEN** the fact SHALL be skipped for comparison and SHALL not be flagged as a discrepancy

### Requirement: Single-Sourced Fill Fields Get Definitional Sanity Checks

Canonical facts filled from fallback with no official counterpart available for comparison SHALL receive a definitional plausibility check against same-row official facts when a definitional relationship exists; implausible results SHALL be marked as review-only anomalies and SHALL NOT block the write.

#### Scenario: Implied minority interest is implausible

- **WHEN** `equity_parent` is fallback-filled and official `equity_total` is present
- **AND** the implied minority interest (`equity_total` minus `equity_parent`) is negative beyond the configured absolute floor or exceeds the configured loose share threshold of `equity_total`
- **THEN** the run SHALL mark a review-only anomaly recording both values and the implied minority share
- **AND** the filled value SHALL remain written and reported for review

#### Scenario: Rounding noise passes the sanity check

- **WHEN** the implied minority interest is negative but within the configured absolute floor
- **THEN** the run SHALL NOT mark a review-only anomaly, because the difference is the reference source's rounding granularity rather than a definitional violation

### Requirement: Comparison Results Are Transient Process Data

Comparison result storage SHALL hold at most the latest run's records and comparison-only fallback process data SHALL be removed at run end.

#### Scenario: A new run starts

- **WHEN** a reconciliation run begins
- **THEN** comparison result rows from prior runs SHALL be cleared before new results are written

#### Scenario: Run completes

- **WHEN** the repair run finishes
- **THEN** fallback numeric facts that were fetched but not used for field-level fill SHALL be deleted
- **AND** fallback facts used for fill SHALL remain

### Requirement: Comparison Is Independently Invokable

The comparison logic SHALL be a standalone capability over provided fact sets, callable without fetching.

#### Scenario: Standalone audit invocation

- **WHEN** the comparison is invoked with caller-provided official and fallback fact sets
- **THEN** it SHALL produce the same comparison records and counts without any network access of its own
