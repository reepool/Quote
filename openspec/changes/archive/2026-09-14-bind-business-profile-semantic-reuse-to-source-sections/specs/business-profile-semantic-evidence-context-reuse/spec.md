## ADDED Requirements

### Requirement: Semantic reuse is bound to its source selected artifact
The system MUST reuse a completed business-profile semantic family only together with the exact selected-section artifact that produced its governed evidence.

#### Scenario: Compatible semantic family is reused
- **WHEN** a completed semantic family, its source selected artifact, and all governed evidence records remain available and mutually consistent
- **THEN** verification uses that source selected artifact and no new semantic extraction request is created

#### Scenario: Current selector produces a different artifact
- **WHEN** the current select stage produces a different artifact but the completed semantic family's source evidence context remains valid
- **THEN** the system keeps the current selection as diagnostic context and verifies the reused records against their source artifact

### Requirement: Reuse validates the complete evidence manifest
The system MUST prove the source document identity, selected artifact hash, and every persisted exact evidence span before accepting semantic reuse.

#### Scenario: Evidence span is resolvable
- **WHEN** an evidence span's section identifier, section hash, page, quote hash, and quote content match the source selected artifact
- **THEN** that span is eligible for reused verification

#### Scenario: Evidence context is unavailable or inconsistent
- **WHEN** the source artifact is missing, its document or artifact hash differs, or any persisted evidence span cannot be resolved exactly
- **THEN** the system rejects that reuse candidate and performs semantic extraction against the current selected artifact

#### Scenario: Deterministic table evidence has no span manifest
- **WHEN** a reusable table-derived record has no semantic span list but its section path, section hash, page, document, and selected-artifact identities match
- **THEN** the record remains eligible for reuse without inventing a quote span

#### Scenario: Verification selected artifact cannot be loaded
- **WHEN** a verify input references a missing or malformed selected artifact
- **THEN** each affected target is persisted as `evidence_provenance_failed` machine rework and the verify stage remains recoverable instead of terminating the task

### Requirement: Verification resume is scoped to evidence context
The system MUST resume verification results and machine-rework state only when they belong to the same source document and selected-artifact evidence context as the current verify input.

#### Scenario: Verification resumes within the same context
- **WHEN** a prior verification artifact has the same evidence-context hash
- **THEN** completed target decisions are reused and pending targets continue without repeating completed verification work

#### Scenario: Verification context changes
- **WHEN** semantic reuse falls back to a new selected artifact or the evidence-context hash otherwise changes
- **THEN** stale verification decisions and evidence-provenance machine rework are not inherited into the new context

### Requirement: Recovery uses the normal production path
The system MUST recover evidence-context reuse failures through the existing business-profile backfill flow without deleting approved data or requiring a separate repair command.

#### Scenario: Existing provenance failure is rerun
- **WHEN** a normal backfill run encounters an open evidence-provenance failure caused by stale selected-section identity
- **THEN** it either reuses the proven source context or re-extracts against the current context and resolves the stale exception after successful verification
