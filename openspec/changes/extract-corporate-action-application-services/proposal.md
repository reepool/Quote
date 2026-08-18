## Why

Corporate-action observation, semantic resolution, manual review, canonical selection, promotion, and factor rebuild are valid lifecycle stages, but their state flow is spread across DataManager methods, provider modules, and scheduler job names. The largest methods are now too coupled to review or evolve safely.

## What Changes

- Document one authoritative corporate-action state flow from observation to canonical factor reads.
- Separate observation, resolution, operator review, and canonical-factor application services.
- Keep provider-specific acquisition and parsing in provider modules and move cross-stage orchestration out of DataManager.
- Preserve TDX, CNInfo, manual decision, canonical table, factor rebuild, and query semantics.
- Keep scheduler jobs as stage triggers that invoke the same application services.
- Convert DataManager methods to compatibility delegates and remove them after caller migration.

## Capabilities

### New Capabilities

- `corporate-action-application-service-boundaries`: Defines stage ownership, state transitions, canonical authority, provider boundaries, compatibility, and equivalence requirements.

### Modified Capabilities

None.

## Impact

- Affects large corporate-action regions in `data_manager.py`, `data_sources/cninfo_*`, factor governance modules, scheduler tasks, operator tools, and regression tests.
- Depends on W2 identity boundaries and precedes the corporate-action portion of W7 scheduler migration.
- Implements W6, FR-06, FR-10, and FR-11 without changing factor values or backtest adjustment semantics.
