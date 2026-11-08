## ADDED Requirements

### Requirement: Readiness queries must fetch only the fields they evaluate
The financial readiness path SHALL issue narrow queries that project only the columns used by readiness evaluation and push canonical-fact filtering into SQL, and SHALL NOT decode full per-instrument fact rows on that path. The existing wide fact-read method SHALL remain unchanged for its other callers.

#### Scenario: Reconciliation readiness evaluates a candidate
- **WHEN** the reconciliation scan evaluates one (instrument, report_period) candidate
- **THEN** the storage layer SHALL return only readiness-relevant columns for the requested canonical facts
- **AND** rows outside the required canonical fact set SHALL be filtered by the database, not by post-fetch Python code

#### Scenario: Another consumer reads full facts
- **WHEN** any non-readiness caller requests financial numeric facts
- **THEN** the existing wide read method SHALL keep its current signature and behavior

### Requirement: The readiness query must be index-served
The narrow readiness query SHALL be served by an existing index filtering on (instrument_id, report_period, canonical_fact_name) for both the hot and history branches; the plan SHALL NOT degrade to a full table scan. An upgrade to a covering index is a CONDITIONAL lever: it is applied only if measured per-candidate cost still exceeds targets after the narrow path ships (raw_fact_json requires a table lookup regardless, so a fully covering index-only scan is not achievable for this query shape).

#### Scenario: Readiness query plan is verified
- **WHEN** EXPLAIN QUERY PLAN is run for the narrow readiness query on production data
- **THEN** both branch plans SHALL show an index SEARCH on (instrument_id, report_period, canonical_fact_name) with no full table scan

#### Scenario: Covering upgrade is conditional
- **WHEN** post-narrow measurements still exceed the performance targets
- **THEN** the covering index SHALL be created via the operator script in a verified maintenance window, and dropped without impact if it does not move the numbers

#### Scenario: History rows still contribute to readiness
- **WHEN** a candidate's facts exist only in the history table (or differ from hot)
- **THEN** the narrow path SHALL merge history rows with the same recency semantics as the wide implementation

#### Scenario: Index is created in a maintenance window
- **WHEN** the covering index is created on the production financial database
- **THEN** the creation SHALL run in a verified no-financial-write window via an explicit operator script
- **AND** WAL readers SHALL remain available during the build
- **AND** a rollback SHALL be possible by dropping the index without data impact

### Requirement: Readiness evaluation must meet measured performance targets
The readiness path SHALL meet end-to-end performance targets verifiable on production-scale data.

#### Scenario: Fat-instrument candidate timing
- **WHEN** readiness evaluates a candidate for an instrument with a full fact history (hundreds of rows per period)
- **THEN** the end-to-end per-candidate cost SHALL NOT exceed 150ms on production data

#### Scenario: Full reconciliation scan completes within budget
- **WHEN** the weekly reconciliation runs its scan phase over the full universe
- **THEN** the scan phase SHALL complete within 75 minutes
- **AND** the total run SHALL complete within the standard 18000s max_runtime, after which the temporary 28800s cap is reverted

### Requirement: Narrow results must be equivalent to the wide implementation
The narrow readiness path SHALL produce evaluation results identical to the current implementation for every candidate, proven by a frozen-sample dual run before rollout.

#### Scenario: Dual run over a frozen sample
- **WHEN** the old and new readiness implementations are executed over a frozen sample covering bank and non-bank profiles, zero-data instruments, BSE, and lifecycle-exempt classifications
- **THEN** ready flags, missing-field reasons and selected facts SHALL match for 100% of candidates

#### Scenario: Same-period duplicate facts keep recency semantics
- **WHEN** multiple fact versions exist for the same period and canonical fact
- **THEN** the narrow path SHALL select the same row (latest updated_at) as the wide implementation

### Requirement: Readiness performance must be observable
The reconciliation run SHALL expose per-candidate scan rate and phase timings so regressions are visible without log archaeology.

#### Scenario: Heartbeat shows scan rate
- **WHEN** the reconciliation scan is running
- **THEN** heartbeats and the run report SHALL include scanned-per-minute and average per-candidate cost

### Requirement: The readiness path SHALL reuse connections within a scan scope
Readiness evaluation loops SHALL NOT open a fresh SQLite connection per storage call within one candidate evaluation; connections SHALL be reused within the enclosing database scope.

#### Scenario: Candidate evaluation issues multiple storage reads
- **WHEN** one candidate evaluation performs several storage reads
- **THEN** those reads SHALL share a connection within the evaluation scope
