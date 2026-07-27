## Why

The remaining CNInfo corporate-action backlog is dominated by asymmetric share
reform, restructuring capitalization, and performance-compensation events.
The operator has authorized automatic asymmetric approval when the persisted
CNInfo XDXR event is independently consistent with the persisted TDX XDXR
event, while requiring every non-match to remain visible for review.

## What Changes

- Add a deterministic, persisted-data-only reconciliation for unresolved CNInfo
  special events against raw TDX XDXR rows.
- Require a unique, date-compatible, field-compatible TDX event before writing
  an asymmetric approval.
- Persist the approval as a normal resolved review while recording
  `approved_asymmetric` and complete CNInfo/TDX comparison lineage in review
  payloads.
- Report unmatched, ambiguous, date-conflicting, and economically conflicting
  events without changing their current governance state.
- Do not download announcements, run OCR, invoke an LLM, or modify either raw
  CNInfo or raw TDX records.

## Capabilities

### New Capabilities

- `cninfo-tdx-asymmetric-approval`: Defines conservative TDX-backed approval
  and auditable mismatch reporting for unresolved CNInfo special actions.

### Modified Capabilities

None.

## Impact

Affected code includes CNInfo resolution governance, the governance orchestration
report, focused unit tests, and a persisted-data validation script. The review
decision remains `resolved` for compatibility; `approved_asymmetric` is an
auditable approval classification in review and resolved-term lineage. No
database schema migration or production source-selection change is required.
