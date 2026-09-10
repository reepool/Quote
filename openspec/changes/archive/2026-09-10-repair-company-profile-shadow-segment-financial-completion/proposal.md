## Why

The latest immutable twenty-report shadow replay completed every report with 100%
Evidence traceability and zero critical semantic errors, but all reports remained
`hold`. Segment financials are the largest source-bound blocker: 30 of 72 report-local
blockers and 135 of 273 unresolved review rows, including 25 high-cardinality scopes
whose provider partitions succeeded before local merge/schema validation rejected the
result.

## What Changes

- Repair the existing Stage 5 segment-partition merge so revenue, cost, and margin cells
  for the same physical segment row may cite different approved Evidence without being
  treated as different row identities.
- Keep program-owned non-adjustment segment metadata stable across partitions while
  preserving strict rejection of real subject, period, row-class, or unsupported
  consolidated-scope conflicts.
- Allow a governed table dimension heading to be proven by the complete controlled
  table scope while each row label and numeric cell remains bound to its physical row
  Evidence.
- Reject cost components, industry-policy discussion, audit references, and ordinary
  company-level income statements as segment owners; retain explicit one-segment and
  source-supported legal-empty disclosures.
- Preserve the exact post-merge failure reason in provider traces instead of replacing
  every local merge failure with a generic schema message.
- Prove the repair with provider-free fixtures and focused tests before any later replay
  is considered. This change does not invoke an LLM or authorize another cohort run.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `company-profile-manufacturing-materials-shadow-batch`: require source-bound segment
  owner selection, deterministic partition reconciliation, cross-page table-dimension
  validation, and precise local failure retention before another shadow replay may be
  proposed.

## Impact

- Affected implementation: the existing Stage 5 semantic provider's segment partition
  merge/expansion path, existing shadow Evidence owner routing where necessary, and
  directly related provider-free tests.
- No new service, runner, schema field, model profile, prompt, token/deadline setting,
  Gold expectation, public API, database, approved writer, scheduler/backfill,
  commodity exposure, value-chain publication, DCF, or Stage 6 change.
- The authoritative replay
  `manufacturing-materials-shadow-routing-continuation-gemini-20260910-a` remains
  immutable and `production_authorization=not_authorized` remains unchanged.
