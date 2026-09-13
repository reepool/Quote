## ADDED Requirements

### Requirement: Stable occurrence replay
The system MUST derive activity, operating-fact, and relationship source occurrence identity from immutable source coordinates: instrument, report period, source document revision, physical page, table/contract identity, row/column or metric slot, or a stable source-span locator. A narrative source-span locator MUST use the normalization-policy version, normalized source quote hash, bounded normalized context anchors, and same-page repeated-match ordinal; raw extraction character offsets MAY verify a match inside one artifact but MUST NOT enter occurrence identity. A generated evidence record ID, selected-artifact hash, semantic run ID, model-supplied subject scope, or normalized object interpretation MUST NOT change that source occurrence identity. Subject scope, action/relationship, source-native object, value, and unit MUST be tracked in a separate semantic-content fingerprint. A rerun of the same source occurrence with the same content MUST reuse the existing governed record under `result_policy=reuse`; changed semantic content MUST be reported as drift or handled by explicit replacement, not auto-approved as a second occurrence.

#### Scenario: Same report fact is replayed
- **WHEN** the same annual report and source row are processed again with `result_policy=reuse`
- **THEN** the system writes or returns the existing occurrence and does not create a second overlapping record

#### Scenario: Independent rows remain independent
- **WHEN** two contracts or table rows have different row or contract references but share a page and object label
- **THEN** the system creates two distinct occurrences and does not collapse either row

#### Scenario: Independent metric slots on one row remain independent
- **WHEN** one physical table row contains two separately governed metrics with different column or metric-slot coordinates
- **THEN** the system creates two distinct source occurrences even when the metrics share the same page, table, row, subject, and object labels

#### Scenario: Generated evidence identity changes
- **WHEN** the same physical annual-report row is selected again under a regenerated evidence ID or selected-section artifact while its source document, page, table, row and metric slot are unchanged
- **THEN** the source occurrence key remains unchanged and `reuse` returns the existing governed occurrence

#### Scenario: Narrative extraction offsets change
- **WHEN** a parser replay places the same normalized narrative quote and bounded context at different character offsets while the source document revision, page, normalization policy and same-page match ordinal are unchanged
- **THEN** the source occurrence key remains unchanged because the offsets are validation-only provenance

#### Scenario: Model interpretation changes on the same source row
- **WHEN** a replay changes `subject_scope` or another semantic interpretation for the same physical source occurrence
- **THEN** the system reports `occurrence_semantic_drift` or requires `replace`; it MUST NOT approve the changed interpretation as an independent occurrence solely because the semantic field changed

### Requirement: Committed governed targets are authoritative
The repository MUST finalize record identity only after all write-state, temporal, and reuse checks complete. For every requested record it MUST return a deterministic disposition of `written(actual_id)`, `reused(actual_governed_id)`, or `blocked(reason_code)`. Durable semantic-run metadata, extract-stage output, verification targets, counts, and family-completion state MUST be generated from those committed dispositions. A requested ID that was skipped or never written MUST NOT remain in any governed target list.

#### Scenario: Reuse skips a newly generated ID
- **WHEN** temporal reuse determines that a newly generated activity is equivalent to an existing approved activity with a different record ID
- **THEN** persistence returns `reused(existing_approved_id)`, the requested ID is absent from durable target lists, and verify treats the existing approved record as unchanged rather than looking up the missing requested ID

#### Scenario: Reuse finds non-equivalent governed content
- **WHEN** a requested occurrence conflicts with an approved record but its semantic content is not equivalent
- **THEN** persistence returns a typed blocked disposition, family completion is false, and the run MUST NOT silently complete by retaining only the raw artifact

#### Scenario: Legacy failed run has no disposition manifest
- **WHEN** repair audits a failed legacy run that retained a raw requested-ID list but never persisted committed dispositions
- **THEN** it reconstructs an audit-only mapping from the raw requests and actual persisted rows, marks every inferred result as `reconstructed`, distinguishes unknown decisions from observed absence, and MUST NOT persist an inference as the original run's execution truth

### Requirement: Annual and correction versioning
The system MUST append a new report-period occurrence for a new annual report and MUST require an explicit replacement policy for a corrected version of the same report flow. Approved historical business content, governed record IDs, review status, version, and temporal fields MUST remain queryable and immutable. An exact-manifest identity migration MAY update only a retained canonical record's source-occurrence metadata and deterministically dependent lineage hash when the business content is unchanged and the immutable review audit records the manifest plus complete old/new identity material.

#### Scenario: New annual report
- **WHEN** a later annual report period is discovered for an instrument
- **THEN** its facts are processed as new occurrences without temporal rejection caused solely by the prior year's approved facts

#### Scenario: Corrected annual report
- **WHEN** a correction has a new source revision for an existing report period
- **THEN** `result_policy=replace` creates a linked successor version and `result_policy=reuse` does not silently overwrite the approved predecessor

### Requirement: Unusable result cleanup
The system MUST physically remove rejected or otherwise non-reusable semantic receipts, obsolete shadow runs, superseded or failed work items, and candidate outputs explicitly owned by those receipts, runs, or a durable stage-owned publication manifest. Work is considered retired shadow work only when its persisted processing mode is `rollout_phase=structured_shadow` or it carries an explicit, immutable `retirement_marker` with a reason and timestamp; a field-family name alone MUST NOT make an otherwise active work item eligible for deletion. New semantic runs, durable receipts, and derived publication stages MUST persist the processing identity and owner information needed for this decision; the retirement migration MUST write an immutable marker to selected legacy runs/receipts that lack it before cleanup. Approved records, source evidence, and audit records MUST NOT be deleted.

#### Scenario: Failed receipt cleanup
- **WHEN** repair marks a receipt as rejected or non-reusable conversion pending
- **THEN** the receipt and its owned candidate outputs are deleted and subsequent replay lookup returns no reusable result

#### Scenario: Received response fails downstream conversion
- **WHEN** a provider response passes outer schema receipt but fails business conversion, unit, occurrence identity, or persistence validation
- **THEN** its artifact transitions from `received` to a non-reusable conversion-pending/rejected state with the original reason code, and `find_replay` does not return it

#### Scenario: Approved history protection
- **WHEN** a failed receipt references an occurrence with an approved historical record
- **THEN** cleanup preserves the approved record, evidence, and audit while deleting only the failed receipt and its candidate descendants

#### Scenario: Verify fails after semantic persistence
- **WHEN** semantic persistence writes candidates but verify terminates because a governed target is missing, ambiguous, or otherwise deterministically invalid
- **THEN** the semantic run and stage artifact become non-reusable, all candidates owned by that failed semantic/verify execution are deleted before terminal acknowledgement, and reused approved records, evidence, and review audit are preserved

### Requirement: Derived publication ownership and failure cleanup
Derived exposure facts, published exposures, and value-chain roles MUST bind their lifecycle owner to the current derivation/publication execution, using a durable stage-owned run or publication manifest. An ancestor semantic `run_id` MAY be retained as source provenance, but MUST NOT be used as the sole lifecycle owner for a newly derived candidate. The stage-owned manifest MUST record candidate descendants and the current processing identity before publication can complete. If derivation or publication fails after any candidate descendant is written, the stage MUST either roll back those writes or, before entering a terminal/non-reusable state, physically delete the listed candidate descendants and mark its own artifact/manifest non-reusable. Cleanup MUST be able to remove descendants from the manifest even when the ancestor semantic run has already been removed.

#### Scenario: Reused activity receives a new publication owner
- **WHEN** an exposure fact is derived from an approved or reused activity whose ancestor semantic run is no longer retained
- **THEN** the new exposure candidate is owned by the current derivation/publication manifest, keeps the ancestor run only as provenance, and is not classified as an orphan solely because the ancestor run is absent

#### Scenario: Partial publication is cleaned before terminal failure
- **WHEN** a publish stage writes candidate descendants and then fails a governance, persistence, or publication gate
- **THEN** the stage records a deterministic non-reusable reason and deletes all candidate descendants listed in its own manifest before the work becomes terminal, while preserving approved history, evidence, and review audit

#### Scenario: Ancestor run is deleted before descendant cleanup
- **WHEN** an obsolete semantic run is removed before a previously created derived publication is audited
- **THEN** repair uses the publication manifest or equivalent durable owner lineage to delete only its non-approved candidate descendants and does not rely on the missing semantic run row

### Requirement: Semantic-to-verification ownership and failure cleanup
Semantic persistence and verification MUST share a durable owner manifest containing the committed candidate targets, reused governed targets, processing identity, and stage artifact identities. A terminal verify failure MUST invalidate the manifest and delete its owned candidates. A retryable provider failure MAY retain a bounded verification checkpoint only when the owner, processing identity, committed target set, and artifacts remain current and complete; retained state MUST NOT become eligible for general semantic reuse or publication before verification succeeds.

#### Scenario: Joint semantic output fails on one governed target
- **WHEN** a joint activity/relationship bundle persists candidates and verify encounters a missing governed activity target
- **THEN** the owner manifest invalidates the whole unverified joint output, deletes every candidate descendant across its covered families, and records the original deterministic reason without issuing another LLM call

#### Scenario: Retryable verifier transport failure
- **WHEN** all committed governed targets exist but the verifier provider returns a retryable transport error
- **THEN** the current owner may retain a bounded checkpoint and candidate set for that same verification retry, while `find_replay` and publication continue to exclude the unverified candidates

### Requirement: Confirmed incompatible approved occurrence can be rebuilt
An approved record MAY be physically removed only when a migration identifies it by exact instrument, source document/evidence, record ID, and a proven obsolete structural shape. A retained canonical approved record MAY have only its source-occurrence metadata and deterministically dependent lineage hash re-keyed when an exact manifest proves unchanged business content; its record ID, business fields, review status, version, and temporal fields MUST remain unchanged, and immutable review audit MUST record the manifest and complete old/new identity. The migration MUST preserve evidence and review audit, remove only dependent erroneous facts/candidates, emit a dry-run/apply manifest, and force current-schema semantic re-extraction where the business shape itself is obsolete. Missing `source_row_key` alone MUST NOT authorize deletion or re-keying.

#### Scenario: Multi-metric hedge row was projected onto an activity
- **WHEN** the approved legacy activity for `002496.SZ` stores one table metric as the value of the entire `hedges + 套期工具` activity and the exact record/evidence matches the migration manifest
- **THEN** repair removes that erroneous activity and its erroneous derived fact, preserves evidence/audit, and replay rebuilds a valueless hedge activity plus metric-specific facts without a temporal-conflict fallback

#### Scenario: Unrelated approved row lacks a source key
- **WHEN** another approved historical record lacks `source_row_key` but is not in the exact incompatible-record manifest
- **THEN** repair preserves it and reports no deletion eligibility

#### Scenario: Confirmed replay duplicates are rebuilt
- **WHEN** a dry-run manifest proves that multiple approved activities for `002415.SZ` or `300750.SZ` came from the same physical annual-report occurrence and repeated replay, and lists their dependent exposure facts/publications
- **THEN** repair preserves one source-verified canonical approved row and its governed ID/business/review/temporal state, re-keys only its source-occurrence metadata and dependent lineage hash in the manifest transaction, appends immutable audit with complete old/new identity, then removes only the manifest-listed duplicate approved rows and derived records while preserving canonical evidence and review-audit history

#### Scenario: Duplicate candidates disagree semantically
- **WHEN** records that appear similar by action/object have different physical rows or materially different semantic-content fingerprints
- **THEN** repair does not choose a winner or delete them through the duplicate migration; it holds the group for explicit correction

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
The system MUST classify lifecycle findings as instrument-scoped or global before applying the pre-batch gate. An instrument-scoped collision, stale receipt, orphan candidate, or failed work item MUST block only the affected instrument and MUST report its deterministic reason; unrelated instruments in the same invocation MAY proceed. A global database-integrity, shared catalog, shared source-asset, or runtime-schema failure MUST block the whole selected batch. The gate MUST never allow a blocked instrument to enter LLM extraction until its non-reusable state is cleaned or explicitly repaired.

#### Scenario: Blocking audit finding
- **WHEN** a pre-batch scan finds an unresolved occurrence collision or reusable legacy receipt
- **THEN** each affected instrument is held and reported with a deterministic repair target list, while unaffected instruments may proceed unless the finding is classified as global

#### Scenario: Clean scope
- **WHEN** the selected scope passes collision and receipt scans
- **THEN** the batch may proceed to semantic extraction

#### Scenario: One instrument has a scoped lifecycle defect
- **WHEN** only one instrument has an orphan candidate or stale receipt and all shared runtime, catalog, and source-asset checks pass
- **THEN** that instrument is held for cleanup while unaffected instruments proceed, and the operation report lists both the blocked instrument and the instruments that were allowed to continue

#### Scenario: Shared integrity failure remains a batch blocker
- **WHEN** a shared catalog, database integrity, runtime schema, or source asset is invalid for the selected invocation
- **THEN** the entire batch is blocked regardless of per-instrument cleanup status

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

### Requirement: Recoverable machine-rework checkpoint retention
Machine-rework checkpoints MUST be classified by recoverability rather than deleted solely because the work status is `machine_rework`. A checkpoint MAY be retained only when its owned work or manifest still exists, its processing identity matches the current replay contract, and its contents are structurally recoverable. Checkpoints belonging to deleted owners, stale identities, corrupt state, or non-replayable artifacts MUST be physically deleted. Retained checkpoints MUST never make a failed candidate or non-reusable artifact eligible for publication or semantic reuse.

#### Scenario: Recoverable machine-rework state is retained
- **WHEN** a machine-rework work item has a valid owner, current processing identity, and a structurally valid checkpoint with independent progress
- **THEN** cleanup preserves the checkpoint for a bounded retry while candidate publication remains blocked and the retry is reported as recoverable

#### Scenario: Non-recoverable checkpoint is deleted
- **WHEN** a machine-rework checkpoint references a deleted owner, stale scope, corrupt state, or a non-reusable artifact
- **THEN** cleanup physically deletes the checkpoint and its non-approved candidate descendants and reports the deterministic deletion reason
