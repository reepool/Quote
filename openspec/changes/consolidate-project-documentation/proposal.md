## Why

Quote documentation mixes current architecture, operator runbooks, completed requirements, investigation notes, and migration receipts in one development directory. Outdated documents now describe non-existent modules and obsolete task counts, so future implementation can follow the wrong path even when the code is correct.

## What Changes

- Establish `docs/README.md` as the only current documentation index.
- Classify documents as current, runbook, active requirements, or historical.
- Rewrite current architecture and development entry documents from code and configuration evidence.
- Merge documents that describe the same current capability and delete superseded requirements, receipts, investigations, and migration notes after preserving valid rules.
- Add a documented replacement and reference check for every deleted document.
- Reconcile completed OpenSpec changes with the current spec/archive lifecycle.
- Include the root-level `implementation_plan.md` in the inventory and disposition matrix.

## Capabilities

### New Capabilities

- `project-documentation-governance`: Defines authoritative document types, consolidation and deletion rules, current-index requirements, and OpenSpec documentation lifecycle.

### Modified Capabilities

None.

## Impact

- Affects `docs/`, `AGENTS.md`, OpenSpec change lifecycle, and links from current runbooks.
- Does not change production code, scheduler configuration, APIs, databases, or data collection.
- Implements workstream W1 and requirements FR-05, FR-13, FR-14, and FR-15 from `framework_refactoring_program.md`.
