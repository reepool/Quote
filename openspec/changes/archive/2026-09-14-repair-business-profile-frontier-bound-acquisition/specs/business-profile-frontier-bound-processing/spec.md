## ADDED Requirements

### Requirement: Work acquisition is bound to the selected frontier document
The system SHALL acquire a latest-annual work item from the active frontier row identified by that work item's `frontier_id`, and SHALL NOT perform another issuer-level announcement discovery during acquisition.

#### Scenario: Bound frontier PDF is archived
- **WHEN** a claimed acquire-stage work item references an active frontier row with an official full-report PDF URL
- **THEN** the system archives that exact frontier document through the shared annual-report archive service
- **AND** persists a source manifest whose source and announcement identity match the work item and frontier row

#### Scenario: Bound frontier is unavailable or inactive
- **WHEN** a claimed work item has no corresponding active frontier row or the row identity does not match the work item
- **THEN** acquisition fails with an explicit retryable stage error
- **AND** the work item does not advance to parse, semantic, or publish

### Requirement: A usable bound manifest gates stage advancement
The system MUST NOT mark acquire complete unless a locally usable PDF manifest matches the claimed work item's frontier document identity.

#### Scenario: Download or PDF validation fails
- **WHEN** the official frontier asset cannot be downloaded or does not pass PDF validation
- **THEN** the work item remains retryable under queue backoff policy
- **AND** no downstream stage completion is recorded

#### Scenario: Existing reusable asset matches the frontier
- **WHEN** the archive already contains a valid asset and manifest matching the bound frontier identity
- **THEN** the system reuses that asset idempotently
- **AND** allows the work item to advance to parse

#### Scenario: Manifest identity is ambiguous
- **WHEN** a local manifest exists but cannot be proven to match the bound frontier source and announcement identity
- **THEN** the system treats the bound manifest as unusable
- **AND** attempts acquisition or leaves the work retryable rather than advancing

### Requirement: Defective completed work can be recovered idempotently
The system SHALL provide an idempotent recovery operation that requeues only completed latest-annual work items lacking a usable manifest for their bound frontier identity.

#### Scenario: Recover known empty completion
- **WHEN** a latest-annual work item is completed but its bound frontier has no usable identity-matching manifest
- **THEN** recovery resets that item to acquire and pending, clears lease state, and preserves durable attempt history

#### Scenario: Preserve valid completed work
- **WHEN** a completed work item has a usable identity-matching manifest
- **THEN** recovery leaves the work item and its stage state unchanged

#### Scenario: Repeat recovery
- **WHEN** recovery is run again after a defective item has already been requeued
- **THEN** that item is not reset or counted again

### Requirement: Missing-document exceptions are reconciled after recovery
The system SHALL resolve open machine-rework exceptions for the known missing-bound-document condition after the corresponding bound manifest becomes usable, while preserving unrelated exceptions.

#### Scenario: Recovered document becomes usable
- **WHEN** a recovered work item successfully acquires a usable bound manifest
- **THEN** its open missing-document machine-rework exception is marked resolved

#### Scenario: Unrelated exception exists
- **WHEN** an instrument has an open exception for another reason or queue mode
- **THEN** successful bound acquisition does not resolve that unrelated exception

### Requirement: Backfill reporting reflects authoritative queue state
The system SHALL distinguish announcement discovery and work enqueue counts from worker completion counts, and SHALL report single-batch queue health and rollout readiness from the authoritative batch result.

#### Scenario: Discovery enqueues work without processing it
- **WHEN** a single batch inserts latest-annual work items but no worker completes them
- **THEN** the report shows the inserted count as enqueued rather than completed
- **AND** shows the work as pending or claimable in queue health

#### Scenario: Single-batch result is nested
- **WHEN** a completed command snapshot stores queue health or rollout readiness under `latest_result`
- **THEN** the operator report reads those nested values and does not substitute zero-valued top-level placeholders

#### Scenario: Worker completes source-backed work
- **WHEN** workers publish items backed by usable frontier manifests
- **THEN** the report exposes their count separately as worker-completed work
