## Why

The financial disclosure incremental sync currently routes fallback at the whole-target level: because CNInfo data20 condensed statements systematically lack `equity_parent` (831/831 parsed instruments have 4 of 5 core facts; the balance sheet endpoint only returns total owners' equity), every target is classified not-ready and the Sina/THS fallback bundle overwrites all core fields. The authoritative core layer is therefore stamped third-party for all five fields even though four of them have identical official values.

The module config already declares the intended policy (`official_first: true`, `fallback_only_missing_or_ambiguous_facts: true`, with a note stating THS/Sina are used "only for missing, failed, or semantically ambiguous canonical facts"), but no code consumes those flags.

Additionally, the fallback fetch returns complete bundles that overlap four fields with official data, yet the two sides are never compared. A same-fetch comparison would surface source discrepancies on either side for free.

## What Changes

- Fill only missing fields: when the official pass leaves individual canonical facts missing or invalid, the fallback bundle fills exactly those fields; official-sourced fields keep their official values and are not overwritten by fallback values.
- Per-field source provenance: core fact rows record which source supplied each canonical field (via the existing `lineage_json` column, no schema migration of the core tables).
- Same-fetch source reconciliation: canonical facts within the configured scope (initially the required core facts) that are available from both official and fallback are compared using a configurable tolerance; discrepancies are marked with both values and surfaced in the operator report. The comparison reuses the fallback fetch already performed for filling — no additional network requests.
- Single-sourced fill fields (no second source to compare, currently `equity_parent`) receive a definitional plausibility check against same-row official facts (implied minority interest share); implausible results are marked as review-only anomalies without blocking the write.
- Transient process data: fallback numeric facts fetched only for comparison are removed at the end of the run, and comparison result rows keep only the latest run (cleared at the start of the next run).
- Implement the declared `fallback_only_missing_or_ambiguous_facts` flag: `true` (config default, new behavior) enables field-level fill; `false` preserves the legacy whole-bundle behavior as a rollback switch.
- Report clarity: the `CNInfo待补字段` line distinguishes systematically missing fields (missing across most officially parsed targets, currently `equity_parent`) from per-instrument missing fields, and the report gains a source-comparison summary (compared fields, consistent count, discrepancy count, top samples).

## Capabilities

### New Capabilities

- `financial-fallback-field-merge`: field-level fallback fill with per-field source provenance.
- `financial-source-fact-reconciliation`: same-fetch official/fallback fact comparison, discrepancy marking, transient result retention.

### Modified Capabilities

- `financial-operations-scheduler`: incremental/reconciliation task reports distinguish systematic versus per-instrument missing official fields and include the source-comparison summary.

## Impact

- `research/storage.py`: official-first per-field source preference in `derive_financial_core_facts_from_numeric_facts` (the numeric-facts pool is where official and fallback facts already coexist at write time); `core_fact_alias_matches` lineage gains source/role labels; run-scoped comparison result table with single-run retention; per-source-file cleanup of comparison-only fallback numeric facts.
- `research/financial_source_fact_reconciliation.py` (new): pure comparison service (tolerance, discrepancy and sanity records, counts), no network access.
- `research/financial_statement_maintenance_repair.py`: invokes the comparison and end-of-run cleanup; readiness and fallback-target selection unchanged; `run_local_core_dryrun` stays a parameter pass-through.
- `scheduler/tasks.py`: report rendering for systematic/per-instrument missing fields, comparison summary, and review-only sanity anomalies.
- `config/10_research.json`: consumes existing flags; adds `source_fact_reconciliation` settings (enabled, relative tolerance, absolute floor, sanity thresholds, report sample cap).
- Unit tests for derivation preference, reconciliation, cleanup, retention, and report rendering.
- No destructive migration: new table is additive; `lineage_json` extension is backward compatible; historical rows are not rewritten.
- Deferred (explicitly out of scope): CNInfo `getMainIndicators` per-share derivation of `equity_parent`, SSE `sse_commonquery` restoration, historical re-stamping of existing core rows.
