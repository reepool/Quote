## Why

The completed twenty-report post-stability replay removed execution failures, but source-bound review still found two critical semantic errors and seven noncritical Evidence/row-identity errors. Those observed defects must be closed before another scale-validation run can provide meaningful evidence toward production readiness.

## What Changes

- Strengthen the existing business-regime guard so explicit consolidation-scope changes cannot be closed as `not_applicable` when the response omits the source-supported event.
- Block top-five customer/supplier aggregate labels from becoming counterparty Relationships while preserving named or anonymous ranked counterparties and independently disclosed aggregate related-party identities.
- Prevent cost-component rows from being accepted as business Segments and tighten chapter ownership for the six reviewed material-input, operating-quantity, and business-regime Evidence misroutes.
- Add provider-free regression coverage for all nine reviewed errors and record an immutable closure audit without modifying or replaying the authoritative twenty-report bundle.
- Keep every output research-only with `production_authorization=not_authorized`; do not change model, token, timeout, Gold, Stage 6, approved writers, scheduler/backfill, commodity exposure, value chain, or DCF paths.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: require provider-free closure of all nine post-stability reviewed precision errors before another scale-validation proposal may be considered.

## Impact

- Affected implementation: `research/company_profile/stage5_provider.py`, `research/company_profile/stage5_service.py`, and `research/company_profile/shadow_evidence.py`.
- Affected verification: focused Stage 5 provider/service and shadow Evidence tests plus an immutable provider-free closure audit.
- No public API, database schema, production scheduler, semantic field vocabulary, provider profile, or historical batch content changes.
