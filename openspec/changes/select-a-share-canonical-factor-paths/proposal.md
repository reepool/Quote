## Why

The project has independently persisted CNInfo and TDX adjustment-factor paths. Its current
production fallback is the established `adjustment_factors` legacy path: BaoStock supplies
the historical base and direct Sina factors obtained through AkShare are rebased onto its
tail during normal maintenance. The canonical table still needs a governed source-selection
decision without introducing another full-market source download.

## What Changes

- Keep the direct Sina `stock_zh_a_daily(adjust="hfq-factor")` A-share factor route for
  ordinary incremental maintenance of the existing legacy path.
- Remove Tencent/Eastmoney A-share price-ratio acquisition, provider snapshot state,
  extraction configuration, tests, documentation, and stale runtime artifacts.
- Use the existing BaoStock-plus-Sina legacy path as one composite voting source; do not
  split its rows into separate BaoStock and Sina votes and do not require a full-market
  Sina backfill.
- Normalize CNInfo, TDX, and the legacy composite path to the same event-ratio and
  latest-session unit anchor while preserving upstream source profiles and evidence.
- Compare cumulative paths and individual factor jumps, then select one complete source
  path per instrument and legal-entity continuity segment.
- Keep CNInfo as the default. CNInfo wins when it agrees with TDX or legacy. TDX/legacy
  consensus may supersede CNInfo only for ordinary symmetric actions; governed special
  actions remain on the CNInfo policy path.
- Treat CNInfo and TDX endpoint-request intervals as audit evidence. A completed factor
  path with no pending events or historical gaps remains eligible even when a recent
  instrument-specific endpoint interval is absent.
- Report exact and trading-session-shifted event matches plus bounded factor-difference
  buckets before cumulative-path differences are interpreted.
- Persist versioned canonical candidates and selection audit evidence without modifying
  source observations or automatically changing production reads.
- Keep a dry-run-first, local-only manual task for three-source scoring, low-confidence
  conflicts, and promotion eligibility.

## Capabilities

### New Capabilities

- `a-share-canonical-factor-selection`: Three-source normalization, continuity-segment
  scoring, special-action policy, canonical candidate construction, and auditable source
  selection using CNInfo, TDX, and the existing legacy composite path.

### Modified Capabilities

- `data-source-routing`: A-share AkShare factor acquisition continues using the direct Sina
  `hfq-factor` endpoint to append sparse events to the rebased legacy path.
- `scheduler`: Operators can run local-only, dry-run-first three-source factor selection
  without automatically promoting the production series.

## Impact

- Factor acquisition: `data_sources/akshare_source.py`; the temporary A-share price-ratio
  adapter is removed.
- Orchestration: `data_manager.py`, scheduler task parameters, reports, and configuration.
- Storage: existing CNInfo and TDX observations, the existing `adjustment_factors` legacy
  table, and canonical staging tables; no new full-market provider snapshot is required.
- Tests and documentation: direct factor parsing, snapshot coverage, path consensus,
  special-action safeguards, write isolation, and task delegation.
- External dependencies: no new package; existing AkShare is reused.
