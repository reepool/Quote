## ADDED Requirements

### Requirement: Runtime identity is part of reuse
Semantic reuse MUST compare the persisted run's runtime, schema, prompt, catalog, document, and selected-section identities with the current scope. A completed run with a stale identity MUST be treated as non-reusable and MUST be reprocessed or explicitly reported as stale.

#### Scenario: Runtime contract changes
- **WHEN** the current runtime identity differs from a completed run for the same document and family
- **THEN** the old run is not returned by `result_policy=reuse`, and the next run records the stale-identity reason

### Requirement: Lease renewal and stage error containment
Long-running work MUST renew its lease before expiry. Claim, acknowledge, failure-recording, and stage-level task exceptions MUST be converted into typed worker results and included in the operation report. One stage failure MUST NOT silently abandon sibling tasks.

#### Scenario: LLM exceeds initial lease
- **WHEN** a semantic item runs longer than its initial lease duration
- **THEN** its lease remains owned by the active worker, no second worker claims it, and the final result is acknowledged or retried exactly once

#### Scenario: Storage failure while recording failure
- **WHEN** `fail()` or `claim()` raises a storage exception
- **THEN** the stage reports a typed storage/worker failure and the operation does not discard the status of unrelated stages

### Requirement: Human-held records are protected
Automatic contract recovery MUST NOT reject or otherwise override a record in `held` status when its latest hold decision was created by a human reviewer. Only an automation-owned hold with explicit provenance may be changed automatically. The owner MUST be determined from the most recent hold audit ordered by `reviewed_at` and `audit_id`; an older automation audit MUST NOT override a later human hold.

#### Scenario: Human hold on obsolete-looking record
- **WHEN** contract recovery scans a human-held candidate
- **THEN** the record remains held and the report identifies it as requiring human action

#### Scenario: Latest human hold wins
- **WHEN** a record has an earlier system hold followed by a later human hold
- **THEN** recovery treats the current hold as human-owned, leaves it held, and does not reclassify it as automation-owned because of the older audit

### Requirement: Page-budget anchor preservation
Page selection MUST treat page budgets as semantic-context limits, not PDF parser limits. Explicit pages and pages matching governed table signatures or section anchors MUST be retained before adding context pages. If the budget cannot retain all anchors, the selector MUST emit a dropped-anchor diagnostic and allow a targeted expansion path.

#### Scenario: Dense chapter exceeds context budget
- **WHEN** a chapter contains a high-scoring table anchor after earlier context pages have consumed the budget
- **THEN** the table anchor is retained or its omission is explicitly reported; it MUST NOT be silently removed by final page-number truncation

### Requirement: Quarter and half-year basis gate
Quarterly or half-year report processing MUST supply an explicit `period_basis` before observation intervals are derived. Until that input is available, the periodic ingestion path MUST remain disabled or return a typed configuration blocker rather than assuming a monthly interval.

#### Scenario: Quarterly report without basis
- **WHEN** a Q1 or Q3 document reaches structured ingestion without `period_basis`
- **THEN** processing is blocked with a deterministic configuration/data-shape reason and no monthly observation interval is persisted
