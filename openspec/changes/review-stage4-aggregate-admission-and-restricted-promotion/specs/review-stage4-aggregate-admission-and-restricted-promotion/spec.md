## ADDED Requirements

### Requirement: The admission review is read-only until scope approval
Before the 1.1 review passes, the change MUST only contain its scope documents. It MUST NOT change Python, enqueue, replay, add a report, resample the approved list, or edit an archived dossier, task, replay, or source-review. It MUST NOT recompute a historical metric or write a later result back into an earlier row.

#### Scenario: Scope review does not open the ledger work
- **WHEN** the 1.1 review has not yet accepted this change
- **THEN** no observation ledger is written from the archives
- **AND** no historical artifact bytes change

### Requirement: The ledger has three immutable layers
After scope approval, the ledger MUST be built only by reading archived dossiers, tasks, replays, and source-reviews. Each row MUST bind one chapter, one plan version, and one report set. The observation layer MUST keep that row's recall, accuracy, critical numeric errors, and gate scope. The identity layer MUST copy that row's frozen instrument, report identifier, document version, report period, and processing identity. The artifact layer MUST copy the archive path and the enqueue, run, result, and source-review SHA-256 values from those files. The ledger MUST keep material-input 19/23 and 23/23, operating-quantity 26/36, 36/38, and 38/38, segment-financial 122/132, 102/132, and 132/132, and counterparty 45/45 as separate rows. A local true MUST stay limited to its own plan, report set, and chapter.

#### Scenario: A later pass does not replace an earlier failure
- **WHEN** the ledger records both a failed observation and a later pass for one chapter
- **THEN** both rows remain visible with their own plans and gates
- **AND** the later pass is not described as a recount of the earlier replay

### Requirement: Missing aggregate conditions produce a hold
An aggregate gate MUST require a written contract that names the included chapters, a source-review bound to each included plan, one consistent report set, the exchanges required by that contract, retention of failed observations and critical numeric errors, and no backfill of a local true. If that contract does not exist, or any requirement fails, the admission result MUST be hold. The review MUST NOT invent an aggregate `expansion_gates_met=true` or a cross-chapter recall score. Hold MUST NOT authorize a restricted-promotion design card. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`. Publication, closure, mode, and processing identity MUST remain unchanged. The six-chapter package, scale quality, and production MUST stay inactive.

#### Scenario: No aggregate contract is in force
- **WHEN** the archived slices have only their own gates and no written Stage 4 aggregate contract
- **THEN** the admission result is hold
- **AND** no restricted-promotion implementation starts
- **AND** no successor replay is created for an archived observation
