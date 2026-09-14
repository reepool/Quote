## Why

The targeted business-profile backfill can finish semantic verification while reporting success even though value-chain roles and commodity exposures remain unpublished. Its progress message also labels successful detail output as a warning, reports missing readiness data as zero coverage, and attributes serial verification time to the publish stage, making a long-running production workflow difficult to trust or diagnose.

## What Changes

- Complete the existing verified semantic path through program-owned value-chain and commodity-exposure derivation and publication when the invocation selects the complete semantic publication workflow.
- Keep shadow/candidate execution explicit: candidate-only completion must not be reported as end-to-end publication success.
- Execute independent per-record semantic verification concurrently within the existing bounded LLM capacity, persist resumable partial results, and account its time to semantic verification rather than database publication.
- Raise the business-profile resumable per-field-family semantic token guard from 20,000 to 50,000 tokens.
- Report the actual rollout phase, annual-report coverage, candidate/verified/published counts, remaining exceptions, and final outcome; successful detail notifications must use a success or informational marker rather than a warning marker.
- Keep queue and progress counts explicitly scoped to the current processing identity and label that scope, rather than treating valid work for another policy or field family as stale.

## Capabilities

### New Capabilities

- `business-profile-complete-publication`: End-to-end publication semantics, bounded concurrent verification, durable resume, accurate progress reporting, and stale-work retirement for business-profile backfill.

### Modified Capabilities

- `scheduler`: Business-profile task reports distinguish candidate completion, full publication success, degradation, and failure using actual reconciliation and publication results.

## Impact

- Affected modules: business-profile semantic runtime/pipeline, asynchronous production service and repository, rollout configuration, scheduler report formatting, and focused tests.
- Existing Telegram command parameters, shared annual-report asset ownership, database schemas, single-writer persistence, LLM gateway interface, and financial availability-date semantics remain unchanged.
- The change reuses the current derivation and storage owners; it does not add another downloader, queue, publication service, or manual review gate.
