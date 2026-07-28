## Why

CNInfo governance already marks explicit non-effective, scope-mismatched, and
pre-2002 archive-unavailable events as terminal, but the factor builder ignores
those dispositions. A small number of root events therefore produces thousands
of misleading downstream pending rows, while corporate actions implemented
before listing are incorrectly sent to quote lookup and fail because no listed
market quote can exist.

## What Changes

- Make CNInfo factor derivation consume the persisted resolution-state
  disposition for each source event.
- Exclude explicit `non_effective` and `scope_mismatch` events from factor
  calculations while preserving their source observations and audit lineage.
- Exclude events whose governed or source effective date is strictly before the
  instrument listing date, without requesting quote evidence.
- Keep `official_archive_unavailable` events as explicit historical gaps rather
  than silently assigning factor 1 or treating them as economically ineffective.
- Prevent one unresolved historical root from expanding into a pending row for
  every later event; report the root gap and affected path coverage separately.
- Permit a unique TDX event match to supply only the effective date for an
  archive-unavailable CNInfo event; all economic terms and factor calculations
  remain CNInfo-derived.

## Capabilities

### New Capabilities

- `cninfo-factor-path-governance`: Governs exclusion, historical-gap reporting,
  pre-listing treatment, and date-only reference evidence in the CNInfo factor
  path.

### Modified Capabilities

None.

## Impact

- Affects CNInfo primary factor input assembly in `data_manager.py`, factor-path
  derivation and reconciliation in `data_sources/cninfo_factor_governance.py`,
  database read helpers, and focused unit tests.
- Does not change CNInfo source observations, overwrite CNInfo economic terms,
  promote a production factor series, or require new document downloads or LLM
  analysis.
- Existing TDX factor values remain audit/reference data and are not copied into
  the CNInfo factor path.
