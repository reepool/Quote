## Context

The latest-annual discovery flow already resolves one active official announcement per instrument and stores that identity in both the frontier and durable work item. The acquire stage currently discards that decision and performs a second issuer search. That search is both redundant and invalid because it supplies only an end date to a date-range API that requires both bounds. More importantly, acquisition failures are recorded as machine rework but still return a successful stage result, allowing parse, semantic, and publish to complete without a source document.

The first production batch exposed the defect: 3,188 work items were enqueued, 20 were claimed and marked completed, but none of those 20 created a PDF, manifest, parsed pages, semantic evidence, or LLM work. The operator report then obscured the failure by presenting enqueue insertions as completed tasks and by reading empty top-level snapshot fields instead of the nested batch result.

The repair must preserve asynchronous discovery, acquisition, parse, semantic, and serialized persistence. It must reuse the existing immutable annual-report archive and manifest tables, avoid destructive migration, and leave valid assets reusable by other modules.

## Goals / Non-Goals

**Goals:**

- Acquire the exact active frontier announcement selected for each durable work item without another issuer search.
- Prevent stage advancement unless a usable manifest matches the work item's frontier document identity.
- Keep transient download, validation, or archive failures retryable with explicit error state.
- Recover only completed latest-annual work that can be proven to lack a usable bound manifest.
- Resolve stale missing-document machine-rework exceptions after the bound asset is acquired.
- Report enqueue, worker completion, pending, running, terminal, coverage, and recovery counts from authoritative state.

**Non-Goals:**

- Re-designing announcement discovery or the correction-precedence rules.
- Reprocessing work with an already usable identity-matching manifest.
- Resetting all completed business-profile work or deleting existing PDFs, manifests, facts, or exceptions.
- Adding a database migration when existing frontier, work, source-file, and exception columns can express the repair.

## Decisions

### Bind acquisition to the durable frontier identity

The work repository will load the active frontier row named by the claimed work item's `frontier_id`. The acquire stage will convert that row into the existing `BusinessProfileDocumentCandidate` contract and pass it to `BusinessProfileDocumentArchiveService.archive_candidates`.

This is preferred to adding a missing discovery `start_date`: a bounded second search would run, but could select a different correction, waste one request per company, and weaken reproducibility when exchange results change between discovery and processing.

### Make manifest usability a stage completion gate

Acquisition is successful only when the repository can load a source manifest that matches the bound frontier source and announcement/document identity, references a locally usable PDF, and has passed the archive service's validation. The semantic production service must raise a retryable stage failure when the frontier is absent, inactive, mismatched, or cannot produce such a manifest.

The async worker already translates stage exceptions into retry/backoff or terminal outcomes. Reusing that contract keeps the queue durable and prevents empty parse/semantic/publish work.

### Keep archive acquisition idempotent

The existing archive service remains the sole owner of PDF download, validation, immutable storage, annual-report asset reuse, manifest persistence, and correction lineage. Repeated acquire attempts pass the same bound candidate and therefore reuse a valid asset or safely retry the same official document.

### Recover by positive defect evidence

Recovery will select only latest-annual work in a completed state whose bound frontier has no usable identity-matching manifest. It will reset those work items to acquire/pending, clear lease and stage completion state, and preserve attempt history. Running work and work with a valid manifest are excluded. The operation is idempotent: once an item is pending or has a usable manifest, subsequent runs do not reset it again.

Stale open machine-rework exceptions for the known missing-document reason will be resolved only after the relevant manifest becomes usable. Other exception categories remain untouched.

### Separate enqueue throughput from processing throughput

Scheduler results will expose distinct counters for discovered/selected/enqueued work and worker-completed work. Single-batch reporting will normalize the nested `latest_result` into the same shape used by continuous control snapshots, so queue health and rollout readiness are never inferred from absent top-level placeholders.

## Risks / Trade-offs

- [A frontier row is corrected after enqueue] -> Existing supersession logic creates or updates the active frontier and work identity; a worker always verifies that its bound row is still the expected active row before acquisition.
- [Official PDF is temporarily unavailable] -> The acquire stage remains retryable with backoff and never advances to parse.
- [Old manifests use incomplete identity fields] -> Usability checks accept only evidence sufficient to bind the asset to the work item; ambiguous legacy rows are reacquired rather than silently reused.
- [Recovery accidentally expands beyond the known defect] -> Selection requires completed status, latest-annual mode, and absence of a usable bound manifest; tests cover valid-manifest and unrelated-work exclusions.
- [Reporting changes affect message consumers] -> Existing fields remain where practical, while corrected counters are added and human-readable output uses the authoritative values.

## Migration Plan

1. Deploy the frontier accessor, bound acquisition, and manifest completion gate.
2. Run focused unit and repository integration tests against temporary databases.
3. Execute the idempotent recovery once against the production research database and verify that only the known defective completed items are requeued.
4. Run one bounded backfill batch and verify PDF, manifest, parse, semantic, publish, and queue counters before enabling continuous mode.
5. Rollback requires reverting code only. Requeued items remain valid pending work and no archived assets or facts are deleted.

## Open Questions

None. The existing schema and archive service provide all required identities and persistence contracts.
