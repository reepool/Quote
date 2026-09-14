## Why

The business-profile backfill now extracts useful structured facts, but newly approved unit rules can still leave the same semantic artifact in machine rework, cross-dimensional source units can block unrelated rows, and an unproved base token can be auto-approved with an unsafe conversion. PDF parsing is also the dominant runtime cost, yet concurrency changes have been made without a reproducible same-document benchmark.

## What Changes

- Make unit-rule approval immediately visible to the conversion that discovered it and replay the persisted semantic artifact without another extraction LLM call or retry-attempt charge.
- Recover conversion-pending artifacts automatically when an effective rule now resolves their source unit.
- Treat cross-dimensional or alternative source units as raw, conversion-pending facts without rejecting unrelated rows from the same semantic response.
- Add deterministic count classifiers for confirmed Chinese source units such as `项` and `艘`.
- Prevent an unknown base token from being auto-approved merely because an LLM labels it as a count alias; mechanically unproved proposals remain quarantined.
- Supersede the unsafe `万重箱 -> 10000 unit` runtime rule and govern the source term as a glass-industry mass unit only after deterministic program evidence defines its canonical conversion.
- Add a reproducible, read-only benchmark over the same cached PDFs for parser concurrency 4/6/8 and optional parser engines or process execution, reporting fidelity and resource metrics before any production default changes.
- Explicitly exclude annual-report discovery, downloading, correction selection, archive ownership, shared-asset APIs, and consumer cutover. Those remain owned by `establish-shared-announcement-asset-management`.

## Capabilities

### New Capabilities

- `business-profile-unit-normalization`: Deterministic unit proof, safe unknown-unit lifecycle, partial row acceptance, immediate artifact replay, and recovery semantics for company-profile facts.
- `business-profile-pdf-parsing-performance`: Reproducible same-document PDF parsing benchmarks and evidence gates for parser or concurrency changes.

### Modified Capabilities

None.

## Impact

- Affected code is limited to business-profile unit conversion/registry/semantic-runtime modules, their focused tests, and a read-only developer benchmark.
- The change may add a narrowly scoped recovery helper only when it can avoid the shared-asset consumer migration files; shared announcement-asset and DataManager files already modified by another change are not touched.
- Existing raw semantic JSON and annual-report PDFs are reused. No production PDF, database row, queue item, archive path, or announcement-asset state is mutated by benchmark execution.
- Public APIs and annual-report asset ownership remain unchanged.
