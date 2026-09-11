## Why

The frozen twenty-report shadow replay is fully source-reviewed, but the later invalid-item isolation fix has only fixture evidence. We need a provider-free, hash-bound audit that separates what the fix can plausibly improve from the report holds, precision gaps, and review workload that demonstrably remain before another empirical replay is justified.

## What Changes

- Bind the authoritative external replay, its complete source review/readiness result, and the archived invalid-item isolation audit by content hash.
- Quantify affected reports/scopes and an explicitly optimistic upper bound that assumes every failure-affected report becomes usable, without reconstructing unavailable provider payloads.
- Identify hold reports and source-review errors that are independent of the 29 historical failed extract calls.
- Quantify the maximum removable unresolved-review rows associated with failed scopes and retain the actual readiness metrics as authoritative.
- Produce one deterministic provider-free audit and closeout summary; do not rerun an LLM, change token budgets, mutate the bundle, or authorize production.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: Require post-fix scale-readiness claims to distinguish observed metrics, optimistic provider-free bounds, and unknowable historical salvage.

## Impact

- Adds only OpenSpec audit artifacts and their deterministic generator under this change.
- Reads the frozen external replay and archived review/isolation artifacts without modifying them.
- Does not affect runtime APIs, provider requests, models, prompts, Evidence, token limits, production storage, Stage 6, or downstream research consumers.
