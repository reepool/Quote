## Why

Business-profile semantic runs can be reused after PDF parser or section-selection identities change, but the verify stage currently combines the reused governed records with the newly selected section artifact. The records still reference their original section identifiers, so valid evidence fails before LLM verification with `semantic verification evidence section is unavailable` and repeatedly enters machine rework.

## What Changes

- Bind every reused semantic family to the exact selected-section artifact that produced its governed evidence.
- Validate the persisted evidence manifest against that source artifact before accepting reuse.
- Reject stale or incomplete reuse and automatically perform semantic extraction against the current selected artifact.
- Scope verification resume and inherited machine rework to the same evidence context so stale failures do not survive a successful re-extraction.
- Add diagnostics and focused regression coverage for compatible reuse, stale-section fallback, and true evidence mismatch.

## Capabilities

### New Capabilities

- `business-profile-semantic-evidence-context-reuse`: Defines evidence-context-safe semantic reuse and fallback behavior across parser and section-selection changes.

### Modified Capabilities

None.

## Impact

- Affects the existing business-profile semantic runtime reuse and verify stages plus their focused unit tests.
- Keeps `/run business_profile_backfill`, `result_policy=reuse`, shared annual-report assets, governed record schemas, API output, and single-writer publication behavior unchanged.
- Adds no dependency, database migration, parallel execution path, or manual repair command.
