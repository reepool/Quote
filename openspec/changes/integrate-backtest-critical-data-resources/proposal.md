## Why

The external backtest platform still lacks point-in-time index composition, historical security states, filing vintages, and a production-safe corporate-action read contract, but this repository already downloads adjacent evidence through index governance, stock-master refresh, daily quotes, unified official announcements, financial disclosure maintenance, and corporate-action maintenance. The next implementation should therefore prove and reuse those resources first, adding a provider or scheduled download only when a bounded capability assessment shows that the existing route cannot supply the required historical depth or field semantics.

## What Changes

- Add a resource-first governance contract and machine-readable capability inventory for every backtest-critical dataset, including existing provider routes, parent jobs, stores, APIs, freshness, point-in-time semantics, coverage, and source gaps. A new download route or standalone scheduler job is blocked until reuse evidence is recorded.
- Add historical index composition and weight snapshots with effective, publication, local-availability, and time-versioned validity evidence. Extend the existing official index/AkShare adapters and index-master governance path for forward maintenance, and extend the governed A-share historical backfill workflow for history rather than creating an unrelated crawler.
- Add historical security-state facts for ST/*ST intervals, daily price-limit revisions, suspension and delisting lifecycle events. Reuse the stock-master refresh, unified announcement acquisition, current quote universe, official delisting sources, and daily quote maintenance; interval interpretations remain knowledge-time qualified and derived limit prices remain explicitly distinguishable from source-reported values.
- Add immutable financial filing vintages and point-in-time fact selection by completing `financial_source_files` publication lineage and preserving append-only supersession decisions and parse revisions of long-form facts. Extend `financial_disclosure_incremental_sync` and reconciliation rather than adding another financial downloader, and make single-quarter, YTD, annual, instant, derived, and unknown period semantics explicit.
- Add an immutable, versioned canonical corporate-action event read model and API over the existing CNInfo/TDX observations, resolved terms, effective-date evidence, factor decisions, coverage states, and change watermarks. Existing daily and weekly corporate-action tasks remain the only production acquisition paths.
- Publish the already available Shenwan industry-return and industry-membership-as-of contracts in the capability inventory and readiness output; no duplicate industry acquisition is introduced.
- Extend scheduler orchestration, instrument-master governance, and change-watermark requirements so new maintenance stages reuse existing parent-job universes, checkpoints, transports, rate limits, and reports, while historical backfills remain explicit, bounded, resumable, and dry-run-first.
- Keep existing public endpoints backward compatible. New read endpoints and fields expose source, source profile, effective date, publication/availability date, revision identity, quality state, and coverage diagnostics so consumers can reject non-point-in-time or incomplete rows.

## Capabilities

### New Capabilities

- `backtest-data-resource-governance`: Resource inventory, reuse-decision gates, shared coverage/readiness reporting, and admission rules for new sources or jobs.
- `historical-index-composition`: Point-in-time index constituent and weight snapshots, forward maintenance, governed historical backfill, and as-of reads.
- `historical-security-state`: ST/*ST intervals, daily price-limit references, suspension and delisting events, provenance, and as-of reads.
- `financial-filing-vintages`: Immutable filing revisions, publication/availability lineage, explicit period semantics, and point-in-time financial fact selection.
- `canonical-corporate-action-events`: Consumer-safe canonical corporate-action projection, completeness gates, lineage, pagination, and change tracking over existing evidence.

### Modified Capabilities

- `scheduler`: Backtest-critical maintenance must attach to existing parent jobs or the dependency DAG when resources and cadence overlap, and must not introduce redundant full-market scheduled downloads.
- `instrument-master-governance`: Current stock-master refreshes must emit governed security-state transitions and reuse official lifecycle evidence without treating current snapshots as historical truth.
- `daily-sync-change-watermarks`: New index-composition, security-state, filing-vintage, and canonical corporate-action datasets must emit database-scoped changes and resumable consumer watermarks.

## Impact

- Affected orchestration: `daily_data_update`, shared instrument-master governance, `index_master_governance_sync`, `a_share_daily_data_historical_backfill`, `financial_disclosure_incremental_sync`, `financial_disclosure_reconciliation_sync`, `a_share_cninfo_corporate_action_daily_sync`, the TDX weekly refresh, and the scheduler dependency DAG.
- Affected source layers: existing official index sources, AkShare index/ST/limit/delisting adapters, source-neutral official announcement acquisition, official financial filing profiles, CNInfo corporate actions, TDX XDXR, and existing HTTP/adaptive-backoff utilities. Any new optional source remains configuration-gated and requires capability evidence; no paid dependency or credential is assumed.
- Affected storage: additive, versioned tables or projections in `quotes.db` for index composition and security state; additive lineage/semantic fields and immutable version use in `financials.db`; existing corporate-action evidence remains authoritative in `quotes.db`; existing Shenwan data remains in `research.db`.
- Affected APIs: additive capability/readiness, index-composition as-of, security-state/price-limit as-of, financial-facts as-of/vintage, and canonical corporate-action endpoints. Existing quote, instrument, financial statement, industry, and corporate-action evidence APIs retain their defaults.
- Historical backfill can be large and source-limited, so deployment and migrations perform no implicit full-market network crawl. Production writes require bounded dry-run evidence, coverage gates, checkpoints, and operator-selected scopes.
