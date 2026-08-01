## Why

The project has independently persisted CNInfo and TDX adjustment-factor paths. Its current
production fallback is the established `adjustment_factors` BaoStock-Sina composite path: BaoStock supplies
the historical base and direct Sina factors obtained through AkShare are rebased onto its
tail during normal maintenance. The canonical table still needs a governed source-selection
decision without introducing another full-market source download.

## What Changes

- Keep the direct Sina `stock_zh_a_daily(adjust="hfq-factor")` A-share factor route for
  ordinary incremental maintenance of the existing BaoStock-Sina composite path.
- Remove Tencent/Eastmoney A-share price-ratio acquisition, provider snapshot state,
  extraction configuration, tests, documentation, and stale runtime artifacts.
- Use the existing BaoStock-plus-Sina composite path as one voting source; do not
  split its rows into separate BaoStock and Sina votes and do not require a full-market
  Sina backfill.
- Normalize CNInfo, TDX, and the BaoStock-Sina composite path to the same event-ratio and
  latest-session unit anchor while preserving upstream source profiles and evidence.
- Compare cumulative paths and individual factor jumps, then select one complete source
  path per instrument and legal-entity continuity segment.
- Keep CNInfo as the default. CNInfo wins when it agrees with TDX or the composite.
  TDX/composite
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
- Add a separate dry-run-first manual promotion task that validates a full-market staging
  candidate, atomically copies it to a stable canonical version, and activates production
  reads only after explicit operator confirmation.
- Persist canonical activation in a project-runtime manifest so activation survives a
  restart without rewriting source-controlled configuration; support an explicit rollback
  to the BaoStock-Sina composite read path.
- Once canonical reads are active, extend daily corporate-action maintenance to rebuild a
  three-source staging path only for affected or newly uncovered instruments and atomically
  merge validated rows into the stable canonical version.
- Bound zero-event coverage and continuity segments to each instrument's listed/delisted
  lifecycle instead of requiring impossible pre-listing or post-delisting evidence.
- Allow an explicitly labelled low-confidence TDX historical fallback only for a completed
  delisted lifecycle whose CNInfo archive is unavailable, including when the
  BaoStock-Sina composite path conflicts.
- Apply strictly validated, reviewed whole-lifecycle source overrides before automatic
  consensus, initially fixing `000004.SZ` and `600455.SH` to the complete TDX path.
- Use `BaoStock_Sina composite` in operator-facing source names and persisted selection
  provenance while retaining the physical `adjustment_factors` table for compatibility.
- Report blocked decisions independently and before bounded low-confidence samples so a
  sample limit cannot hide promotion blockers.
- Use a 0.1% default relative factor tolerance for cross-provider consensus while retaining
  stricter configurable overrides and bounded difference buckets.
- Keep BaoStock quota and session-lock state under the project runtime directory so normal
  source initialization does not depend on a writable home cache.

## Capabilities

### New Capabilities

- `a-share-canonical-factor-selection`: Three-source normalization, continuity-segment
  scoring, special-action policy, canonical candidate construction, and auditable source
  selection using CNInfo, TDX, and the existing BaoStock-Sina composite path.

### Modified Capabilities

- `data-source-routing`: A-share AkShare factor acquisition continues using the direct Sina
  `hfq-factor` endpoint to append sparse events to the rebased composite path.
- `scheduler`: Operators can run local-only, dry-run-first three-source factor selection
  without automatically promoting the production series, then use a separate confirmed
  task for promotion or rollback.

## Impact

- Factor acquisition: `data_sources/akshare_source.py`; the temporary A-share price-ratio
  adapter is removed.
- Orchestration: `data_manager.py`, scheduler task parameters, reports, and configuration.
- Runtime activation: a strictly validated manifest below `data/runtime/` records the
  active canonical version or the BaoStock-Sina composite rollback state.
- Storage: existing CNInfo and TDX observations, the existing `adjustment_factors`
  BaoStock-Sina composite
  table, and canonical staging tables; no new full-market provider snapshot is required.
- Tests and documentation: direct factor parsing, snapshot coverage, path consensus,
  special-action safeguards, write isolation, and task delegation.
- External dependencies: no new package; existing AkShare is reused.
