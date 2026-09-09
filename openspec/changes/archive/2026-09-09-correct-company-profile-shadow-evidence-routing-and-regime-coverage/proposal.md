## Why

The reviewed twenty-report replay found 23 noncritical and two critical semantic errors even though Evidence traceability was complete. Most errors came from selecting the wrong disclosure section, while the two critical errors accepted `business_regime=not_applicable` despite source evidence of consolidation-scope changes; these current-result defects must be corrected before any further scale replay or production-promotion design.

## What Changes

- Make automatic Evidence planning prefer closed chapter owners: principal-business passages for overview, segment/revenue-cost tables for segments, procurement or explicitly named input passages for material inputs, customer/supplier sections for counterparties, and business/control/consolidation-change sections for regime.
- Reject chapter scopes whose selected source is only a financial-statement note, balance-sheet variance, generic business commentary, or another non-owning section when it cannot establish chapter-level legal-empty coverage.
- Prevent a narrow statement that principal-business statistical calibre was unchanged from closing the broader `business_regime` requirement.
- Reject or route to review any `business_regime=not_applicable` result contradicted by the same Evidence through an applicable consolidation-scope or control-change disclosure.
- Prevent generic cost categories such as `材料` or `原材料` from becoming a named `material_input` Relationship without source wording identifying a specific procured or consumed input.
- Rebuild and validate the same frozen twenty-report Evidence plan provider-free against all 25 reviewed error findings. Do not invoke an LLM or run another full batch in this change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: Require chapter-owner-aware Evidence selection and a provider-free regression audit bound to the reviewed twenty-report findings.
- `company-profile-bounded-semantic-workflow`: Reject contradicted regime legal-empty results and generic unnamed material-input relationships without weakening existing source-native or research-only rules.

## Impact

- Affects the existing `research/company_profile/shadow_evidence.py` planner, the bounded Stage 5 extract/verify contract only where needed for regime contradiction and generic input rejection, focused tests, and change-local provider-free audit artifacts.
- Reuses the current PDF artifacts, selector, Stage 5 semantic owner, Evidence models, verifier, projection, and shadow manifest. It does not create a new planner service, semantic field, confidence system, or execution loop.
- Does not change model selection, timeout, output-token budgets, repair behavior, Gold, historical bundles, approved tables, scheduler/backfill, commodity exposure, value-chain output, DCF, Stage 6, or `production_authorization=not_authorized`.
