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
The system MUST physically remove rejected or otherwise non-reusable semantic receipts, obsolete shadow runs, superseded or failed work items, and candidate outputs explicitly owned by those receipts or runs. Work is considered retired shadow work only when its persisted processing mode is `rollout_phase=structured_shadow` or it carries an explicit, immutable `retirement_marker` with a reason and timestamp; a field-family name alone MUST NOT make an otherwise active work item eligible for deletion. New semantic runs and durable receipts MUST persist the processing identity needed for this decision; the retirement migration MUST write an immutable marker to selected legacy runs/receipts that lack it before cleanup. Approved records, source evidence, and audit records MUST NOT be deleted.

#### Scenario: Failed receipt cleanup
- **WHEN** repair marks a receipt as rejected or non-reusable conversion pending
- **THEN** the receipt and its owned candidate outputs are deleted and subsequent replay lookup returns no reusable result

#### Scenario: Approved history protection
- **WHEN** a failed receipt references an occurrence with an approved historical record
- **THEN** cleanup preserves the approved record, evidence, and audit while deleting only the failed receipt and its candidate descendants

#### Scenario: Retired shadow lifecycle is removed
- **WHEN** a work item or semantic run has the persisted retired processing mode `rollout_phase=structured_shadow` or an explicit `retirement_marker` with a reason and timestamp
- **THEN** repair physically deletes its execution row and owned candidate descendants even if the work status is pending, retry due, or completed

#### Scenario: Active field family is protected
- **WHEN** an otherwise active work item uses `structured_segments` or `tabular_operating_facts` under a non-retired rollout phase
- **THEN** repair MUST NOT delete it solely because of its field-family name

#### Scenario: Retired run metadata is removable
- **WHEN** a semantic run or durable receipt carries `rollout_phase=structured_shadow` or an immutable `retirement_marker` with a reason and timestamp
- **THEN** cleanup removes the run/receipt and owned candidates, and replay lookup cannot return it even when the work-item row is already absent

### Requirement: Pre-batch lifecycle gate
The system MUST block broad LLM backfill when identity collisions or non-reusable receipts remain in the selected scope, and MUST report the blocking instruments and reasons.

#### Scenario: Blocking audit finding
- **WHEN** a pre-batch scan finds an unresolved occurrence collision or reusable legacy receipt
- **THEN** the batch is not started and the report contains a deterministic repair target list

#### Scenario: Clean scope
- **WHEN** the selected scope passes collision and receipt scans
- **THEN** the batch may proceed to semantic extraction

### Requirement: Replay execution-state isolation
The system MUST bind a backfill only to checkpoint state that belongs to the
current durable work row and logical scope. A targeted backfill MUST consume
only the work IDs selected by that invocation, while reusable semantic receipts
remain independent from transient pipeline checkpoints.

#### Scenario: Cleanup leaves an orphan checkpoint file
- **WHEN** lifecycle cleanup removes a failed work row but its deterministic checkpoint path still exists
- **THEN** repair physically deletes the orphan file and recreating the work row starts from an empty checkpoint path

#### Scenario: Replay rotates a stale checkpoint
- **WHEN** enqueue replaces a stale-scope, force-replay, or orphan checkpoint path
- **THEN** the database first points to the new empty path and the old file is physically deleted after it is no longer referenced

#### Scenario: Checkpoint path is outside the owned root
- **WHEN** cleanup encounters a checkpoint path outside the configured root or without the `bp-work-*.json` ownership shape
- **THEN** it refuses deletion and reports a typed cleanup failure without deleting the external file

#### Scenario: Targeted backfill shares a processing identity with queued work
- **WHEN** a single-instrument backfill runs while other instruments have claimable work under the same processing identity
- **THEN** its workers claim only the work IDs selected by the current backfill and leave unrelated queued work unchanged
