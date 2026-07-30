## Why

The project now has complete independent CNInfo and TDX adjustment-factor paths, but the
canonical table still has no governed source-selection decision. A third independent
AkShare path is also too sparse and inconsistently sourced to support a defensible
three-source decision, so production promotion needs a normalized, auditable path selector
rather than a global source preference or event-by-event mixing.

## What Changes

- Add an A-share AkShare factor adapter that prefers Tencent adjusted and unadjusted daily
  prices, falls back to Eastmoney, and reuses the process-level `akshare_proxy_patch`
  bootstrap for supported upstream domains.
- Normalize CNInfo, TDX, and AkShare paths to the same event-ratio and latest-session unit
  anchor while preserving the upstream source profile and raw evidence.
- Compare both cumulative paths and individual factor jumps, with explicit tolerance and
  coverage checks that prevent rounded adjusted-price noise from becoming a vote.
- Select one complete source path per instrument and legal-entity continuity segment.
  CNInfo remains the default and wins when it agrees with either independent source.
  TDX/AkShare consensus may supersede CNInfo only for ordinary symmetric actions; governed
  special actions remain on the CNInfo policy path.
- Persist versioned canonical candidates and per-instrument selection audit data without
  modifying any source observation table or automatically changing production reads.
- Add dry-run-first manual task parameters and bounded reports for AkShare backfill,
  three-source scoring, low-confidence conflicts, and promotion eligibility.

## Capabilities

### New Capabilities

- `a-share-canonical-factor-selection`: Three-source normalization, continuous-path scoring,
  special-action policy, canonical candidate construction, and auditable source selection.

### Modified Capabilities

- `data-source-routing`: A-share AkShare factor acquisition gains governed Tencent-first and
  Eastmoney-fallback routing with explicit provider profiles.
- `scheduler`: Operators can run a resumable, dry-run-first three-source factor selection
  workflow without automatically promoting the production factor series.

## Impact

- Factor acquisition: `data_sources/akshare_source.py` and factor-governance helpers.
- Orchestration: `data_manager.py`, scheduler task parameters, reports, and configuration.
- Storage: additive selection metadata in existing canonical/status governance surfaces;
  source observations remain immutable and isolated.
- Tests and documentation: provider routing, price-ratio stabilization, path consensus,
  special-action safeguards, write isolation, and task delegation.
- External dependencies: no new package; existing AkShare and `akshare_proxy_patch`
  bootstrap are reused.
