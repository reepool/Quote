## Why

The authorized external validation reached Scorpio and resolved the annual-period conflict, but the complete report-segment-heading case still failed. The frozen scope included an unrelated adjacent page, and Gemini returned a very large first partition plus a short heading that the local contract correctly rejected; this prevents reliable scale without weakening source validation.

## What Changes

- Stop adding arbitrary adjacent pages to segment-financial Evidence; retain adjacent pages only when table-continuation evidence requires them.
- Make segment extraction requests explicitly require the exact table-owning heading and a compact row-only JSON response.
- Preserve the existing fail-closed full-heading validation and output-budget diagnostics; do not fuzzy-map ambiguous headings or silently truncate responses.
- Add provider-free regression coverage and one new-identity external validation of the previously failing scope.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: segment Evidence context and bounded partition extraction must avoid unrelated page context while preserving explicit continuation pages and exact source headings.

## Impact

- Affects `research/company_profile/shadow_evidence.py`, `research/company_profile/stage5_provider.py`, and focused tests.
- No public API, historical batch, Gold data, production table, or Stage 6 path changes.
