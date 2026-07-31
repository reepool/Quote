## Why

The project has independently persisted CNInfo and TDX adjustment-factor paths and an
existing Sina `hfq-factor` path maintained through AkShare. The canonical table still needs
a governed source-selection decision. A previous implementation replaced the direct Sina
factor endpoint with Tencent/Eastmoney adjusted-to-raw price ratios. That replacement
downloads two complete price histories per instrument, duplicates existing market-data
work, and makes rounded prices an unnecessary prerequisite for factor acquisition.

## What Changes

- Restore the direct Sina `stock_zh_a_daily(adjust="hfq-factor")` A-share factor path and
  continue its existing daily and maintenance updates through the AkShare factor route.
- Remove Tencent/Eastmoney A-share price-ratio acquisition, provider snapshot state,
  extraction configuration, tests, documentation, and stale runtime artifacts.
- Persist complete Sina factor snapshots in the governed observation/status surfaces so
  selection never combines stale partial histories.
- Normalize CNInfo, TDX, and Sina paths to the same event-ratio and latest-session unit
  anchor while preserving upstream source profiles and evidence.
- Compare cumulative paths and individual factor jumps, then select one complete source
  path per instrument and legal-entity continuity segment.
- Keep CNInfo as the default. CNInfo wins when it agrees with TDX or Sina. TDX/Sina
  consensus may supersede CNInfo only for ordinary symmetric actions; governed special
  actions remain on the CNInfo policy path.
- Persist versioned canonical candidates and selection audit evidence without modifying
  source observations or automatically changing production reads.
- Keep a dry-run-first manual task for optional resumable Sina backfill, three-source
  scoring, low-confidence conflicts, and promotion eligibility.

## Capabilities

### New Capabilities

- `a-share-canonical-factor-selection`: Three-source normalization, continuity-segment
  scoring, special-action policy, canonical candidate construction, and auditable source
  selection using CNInfo, TDX, and Sina.

### Modified Capabilities

- `data-source-routing`: A-share AkShare factor acquisition uses the direct Sina
  `hfq-factor` endpoint and stores sparse governed factor observations.
- `scheduler`: Operators can run a resumable, dry-run-first Sina backfill and three-source
  factor selection workflow without automatically promoting the production series.

## Impact

- Factor acquisition: `data_sources/akshare_source.py`; the temporary A-share price-ratio
  adapter is removed.
- Orchestration: `data_manager.py`, scheduler task parameters, reports, and configuration.
- Storage: existing observation/status and canonical staging tables; obsolete
  Tencent/Eastmoney snapshot rows are removed while source CNInfo/TDX/Sina data remains.
- Tests and documentation: direct factor parsing, snapshot coverage, path consensus,
  special-action safeguards, write isolation, and task delegation.
- External dependencies: no new package; existing AkShare is reused.
