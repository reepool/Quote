## Why

The September 11, 2026 Gemini shadow batch completed all twenty reports, but 15 segment scopes produced no accepted rows after partition reconciliation. Eleven reported a `reported_period` identity conflict without retaining the raw conflicting values, and three showed that the model selected an ambiguous `报告分部` prefix even though the complete table heading existed in Evidence. The closed annual aliases and complete-heading schema can be corrected provider-free while preserving diagnostics for any remaining substantive conflict.

## What Changes

- Normalize only a closed set of equivalent annual duration labels to the prepared report year before segment-partition identity reconciliation.
- Expose the complete source-native `报告分部的财务信息` table heading as an allowed dimension choice when the Evidence contains it, so the model cannot select the ambiguous shorthand or the adjacent accounting-policy heading.
- Add provider-free regression fixtures for annual aliases, half-year/instant preservation, complete heading choices, and substantive identity conflicts.
- Leave the immutable September 11 shadow batch unchanged; validate the fix offline before any new provider-bearing replay.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: segment partition metadata reconciliation must accept only the closed annual-duration equivalences and must preserve complete source headings; substantive period/type, row, subject, duplicate-cell, and coverage conflicts remain blocking.

## Impact

- `research/company_profile/stage5_provider.py` and its focused unit tests.
- One delta specification and change-local offline audit; no PDF regeneration, LLM call, historical-batch mutation, Gold change, production writer, or Stage 6 path.
