## ADDED Requirements

### Requirement: Legacy semantic production is frozen
The legacy company-profile rollout, manual backfill entry, and semantic-production switch MUST remain disabled after stage 0. Legacy report/schema/prompt identities MUST NOT create new production facts or resume the old 10.3 replay.

#### Scenario: Operator invokes legacy backfill
- **WHEN** an operator attempts the old `business_profile_backfill` command while the redesign is active
- **THEN** the task is rejected as disabled/not ready before semantic or LLM work begins
- **AND** no new legacy fact, run, receipt, or checkpoint is written

### Requirement: Raw official assets remain available
The freeze MUST preserve official announcement discovery, original annual-report/PDF acquisition, immutable document assets, reusable raw evidence, read-only audit, status query, and cooperative stop capabilities.

#### Scenario: New annual report is discovered during freeze
- **WHEN** an official annual report becomes available while semantic production is frozen
- **THEN** the governed raw document may be acquired and archived
- **AND** no legacy semantic extraction is automatically started

### Requirement: Authority transition is explicit
The new master requirements document MUST be listed in `docs/README.md` as the sole current company-profile product/industry requirements. Superseded requirements, benchmark documents, and the legacy production runbook MUST visibly state that they are historical or frozen and cannot enable new production.

#### Scenario: Developer follows documentation index
- **WHEN** a developer navigates current company-profile requirements
- **THEN** the index leads to the new master requirements
- **AND** legacy documents cannot reasonably be mistaken for concurrent authority

### Requirement: Existing semantic data does not constrain the new model
The new product, industry, and semantic contracts MUST NOT preserve old semantic rows or tests merely for compatibility. A later reset MUST retain official documents and reusable raw evidence while physically deleting legacy semantic data that cannot satisfy the new contract.

#### Scenario: Old activity contains a sales-volume measurement
- **WHEN** a legacy approved activity encodes a numeric sales-volume fact in the activity structure
- **THEN** the new schema is designed according to Activity/Measurement separation
- **AND** the legacy row is scheduled for audited reset/re-extraction rather than forcing a compatibility field into the new model

### Requirement: New vertical slices write to isolated contract storage
Before legacy reset, every new-contract vertical slice MUST write only to a separate storage space or explicitly isolated namespace. It MUST NOT mix new-contract facts with legacy approved tables, replay indexes, or publication paths.

#### Scenario: New manufacturing slice runs before reset
- **WHEN** the new manufacturing/materials vertical slice persists candidates or approved test facts
- **THEN** all writes use the isolated new-contract namespace
- **AND** no legacy approved row or legacy replay lookup is changed

### Requirement: Reset is a separate audited change
Physical deletion of legacy activities, measurements, relationships, roles, exposures, runs, receipts, work, and checkpoints MUST occur only in a separate reviewed reset change after a new vertical slice passes. The reset MUST produce a dry-run manifest, preserve official documents/raw evidence, disable legacy reads/writes, and avoid a permanent legacy branch.

#### Scenario: Manufacturing vertical slice passes
- **WHEN** the new manufacturing/materials slice has passed business acceptance
- **THEN** an independently reviewed reset plan enumerates legacy deletion targets before apply
- **AND** this contract change alone does not delete production data

### Requirement: Production resumes by approved vertical slices
Backfill and batch production MUST remain disabled until the common contract, at least one independently researched industry package, its benchmark, end-to-end vertical slice, failure cleanup, and legacy reset have passed. Additional industries MUST be enabled individually after equivalent acceptance.

#### Scenario: Manufacturing passes but finance is unresearched
- **WHEN** the manufacturing/materials package is approved for bounded production but finance is not
- **THEN** only the approved manufacturing/materials scope may be enabled
- **AND** finance companies do not receive the manufacturing package or a generic full extraction
