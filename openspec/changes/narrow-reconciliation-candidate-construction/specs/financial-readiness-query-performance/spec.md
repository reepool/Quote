## MODIFIED Requirements

### Requirement: Readiness queries must fetch only the fields they evaluate
The financial readiness path SHALL issue narrow queries that project only the columns used by readiness evaluation and push canonical-fact and source filtering into SQL, and SHALL NOT decode full per-instrument fact rows on that path. This applies to the local-core read AND the official-source merge read. The existing wide fact-read method SHALL remain unchanged for its other callers.

#### Scenario: Reconciliation readiness evaluates a candidate
- **WHEN** the reconciliation scan evaluates one (instrument, report_period) candidate
- **THEN** the storage layer SHALL return only readiness-relevant columns for the requested canonical facts
- **AND** rows outside the required canonical fact set SHALL be filtered by the database, not by post-fetch Python code

#### Scenario: Official-source merge resolves missing facts
- **WHEN** a candidate has missing local facts and the readiness merge consults official CNInfo-sourced facts
- **THEN** the merge fetch SHALL project only consumed columns and reduce rows via SQL-side source prefiltering, followed by the exact existing predicate on the narrowed rows
- **AND** it SHALL NOT fetch the instrument's full fact payload to filter in Python
- **AND** the prefilter SHALL be a superset of the exact predicate (no false negatives)

#### Scenario: Another consumer reads full facts
- **WHEN** any non-readiness caller requests financial numeric facts
- **THEN** the existing wide read method SHALL keep its current signature and behavior

## ADDED Requirements

### Requirement: Profile resolution runs once per instrument per scan
The reconciliation scan SHALL resolve each instrument's statement profile at most once and share the result across all of that instrument's report-period candidates. Implementation SHALL reuse the existing batch resolution facility (or an equivalent per-scan memoization over the same lookups) instead of duplicating per-candidate resolution.

#### Scenario: One instrument with eleven period candidates
- **WHEN** the scan builds candidates for one instrument across multiple report periods
- **THEN** industry-membership, company-profile loading and profile resolution SHALL execute at most once for that instrument in that scan

### Requirement: The scan phase must fit the standard run window
The reconciliation scan phase SHALL complete within 60 minutes on the full universe, and the full weekly run SHALL return to the standard 18000s cap after two consecutive qualifying Sundays.

#### Scenario: Scan rate is observable and on target
- **WHEN** the reconciliation scan is running
- **THEN** heartbeat scan_rate_per_sec SHALL be at least 17 (0.06s per candidate) on production data
- **AND** the scan phase SHALL complete within 60 minutes for the full universe

#### Scenario: Cap reverts after stable Sundays
- **WHEN** two consecutive scheduled Sunday runs each complete within 2.5 hours
- **THEN** max_runtime_seconds SHALL be reverted from the temporary 43200 to the standard 18000
