## ADDED Requirements

### Requirement: Stable occurrence replay
The system MUST derive activity, operating-fact, and relationship identity from the same normalized occurrence material containing report period, source document revision, physical page/evidence identity, source row or contract reference, subject scope, and object. A rerun with identical material MUST reuse the existing occurrence under `result_policy=reuse`.

#### Scenario: Same report fact is replayed
- **WHEN** the same annual report and source row are processed again with `result_policy=reuse`
- **THEN** the system writes or returns the existing occurrence and does not create a second overlapping record

#### Scenario: Independent rows remain independent
- **WHEN** two contracts or table rows have different row or contract references but share a page and object label
- **THEN** the system creates two distinct occurrences and does not collapse either row

### Requirement: Annual and correction versioning
The system MUST append a new report-period occurrence for a new annual report and MUST require an explicit replacement policy for a corrected version of the same report flow. Approved historical records MUST remain queryable and immutable.

#### Scenario: New annual report
- **WHEN** a later annual report period is discovered for an instrument
- **THEN** its facts are processed as new occurrences without temporal rejection caused solely by the prior year's approved facts

#### Scenario: Corrected annual report
- **WHEN** a correction has a new source revision for an existing report period
- **THEN** `result_policy=replace` creates a linked successor version and `result_policy=reuse` does not silently overwrite the approved predecessor

### Requirement: Unusable result cleanup
The system MUST physically remove rejected or otherwise non-reusable semantic receipts, their failed work items, and candidate outputs explicitly owned by those receipts. Approved records, source evidence, and audit records MUST NOT be deleted.

#### Scenario: Failed receipt cleanup
- **WHEN** repair marks a receipt as rejected or non-reusable conversion pending
- **THEN** the receipt and its owned candidate outputs are deleted and subsequent replay lookup returns no reusable result

#### Scenario: Approved history protection
- **WHEN** a failed receipt references an occurrence with an approved historical record
- **THEN** cleanup preserves the approved record, evidence, and audit while deleting only the failed receipt and its candidate descendants

### Requirement: Pre-batch lifecycle gate
The system MUST block broad LLM backfill when identity collisions or non-reusable receipts remain in the selected scope, and MUST report the blocking instruments and reasons.

#### Scenario: Blocking audit finding
- **WHEN** a pre-batch scan finds an unresolved occurrence collision or reusable legacy receipt
- **THEN** the batch is not started and the report contains a deterministic repair target list

#### Scenario: Clean scope
- **WHEN** the selected scope passes collision and receipt scans
- **THEN** the batch may proceed to semantic extraction
