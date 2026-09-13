## Why

The completed owner-closure replay executed all twenty reports and improved sampled precision to 97.5%, but every report remained `hold`. Source review found one recurring BusinessOverview substance-routing defect and two new noncritical owner/continuation defects, so the next production-readiness step is to correct those proven Evidence-planning failures before spending another provider call.

## What Changes

- Require BusinessOverview scopes to contain issuer-level substantive business or operating-model text, excluding subsidiary financial tables and other pages whose business terms are only row labels.
- Require `segment_dimension` legal-empty or positive coverage to remain bound to a segment/revenue-cost owner rather than management-plan, production, or generic commentary pages.
- Bind material-input continuation disclosures to their owning heading/page so an adjacent continuation with disclosed consumed materials cannot be reported as `not_disclosed`.
- Add provider-free regression cases for `000055.SZ`, `920016.BJ`, and `920076.BJ`, rebuild the same twenty-report plan under a new version, and publish an immutable correction/preparation audit.
- Do not change the model, prompt, semantic schema, token/deadline settings, readiness thresholds, historical batches, or production authorization; do not run an LLM in this change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: tighten source-owner and continuation requirements for overview, segment, and material-input Evidence before another empirical replay may be proposed.

## Impact

- Affected implementation: `ShadowEvidencePlanner` routing/range selection, its provider-free audit path, and focused tests.
- Affected artifacts: one new Evidence-plan version and hash-bound provider-free correction/preparation audit for the existing twenty-report cohort.
- No public API, database, approved writer, scheduler/backfill, Stage 6, commodity exposure, value-chain publication, DCF, model, prompt, Gold, or runtime semantic contract change.
