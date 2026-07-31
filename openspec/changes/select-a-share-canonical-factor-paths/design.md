## Context

CNInfo and TDX event-derived paths are complete and independently persisted. The project
also has an established AkShare A-share path backed by Sina
`stock_zh_a_daily(adjust="hfq-factor")`. That endpoint returns a daily cumulative factor
series which can be compressed into sparse factor events without downloading raw and
adjusted price histories.

A temporary implementation replaced Sina with Tencent-first/Eastmoney-fallback raw and
hfq daily prices. It required two historical price downloads per instrument, introduced
rounded-price extraction failures, and left the direct Sina path only partially represented
in the governed observation table. The canonical selector should instead use the three
independent factor datasets the project already intends to maintain: CNInfo, TDX, and Sina.

Known absorption mergers and other legal-subject discontinuities remain available through
instrument lineage metadata. Cumulative factors must reset at transitions marked
`price_continuity=non_continuous` without manufacturing a factor at the boundary.

## Goals / Non-Goals

**Goals:**

- Restore and continuously maintain direct Sina A-share `hfq-factor` snapshots.
- Normalize CNInfo, TDX, and complete Sina paths to comparable event ratios and
  latest-session unit anchors.
- Select one internally consistent source path per continuity segment using deterministic
  consensus and special-action rules.
- Produce versioned canonical staging rows, per-instrument selection evidence, bounded
  conflict samples, and promotion gates without changing production reads.
- Support dry-run, targeted pilot, checkpointed full-market Sina backfill, and repeatable
  operator execution.
- Remove the unused Tencent/Eastmoney price-ratio implementation and its persisted state.

**Non-Goals:**

- Treating any provider as absolute truth or overwriting source observations.
- Event-by-event source splicing inside one continuity segment.
- Automatically promoting or changing `read_dataset`.
- Recomputing special restructuring economics from market prices.
- Downloading raw or adjusted price histories to derive the Sina factor path.
- Using BaoStock as a voting source.

## Decisions

### Use the direct Sina cumulative factor endpoint

The A-share AkShare factor route calls the same direct Sina `hfq-factor` endpoint used by
`stock_zh_a_daily(symbol=<market-prefixed symbol>, adjust="hfq-factor")`. Because the
upstream wrapper does not expose a request timeout, the adapter owns the small factor-only
transport boundary and applies bounded connect/read timeouts. It validates the declared
row count and requires an anchor at or before the requested lifecycle start before a
snapshot can be certified complete. It parses positive dated cumulative factors, includes
a pre-range anchor for incremental requests, and emits only material factor-ratio changes
inside the requested range. Observations use the stable source profile
`sina_hfq_factor`.

Each successful instrument fetch atomically replaces one Sina snapshot and its coverage
status. The status records the requested range, ingestion id, and whether the complete
snapshot contains events. Selection loads only rows matching the current complete snapshot
id. A valid zero-event response remains distinguishable from an unavailable response.
Each request has configurable socket-level connect/read timeouts; a timeout releases the
worker, preserves the prior snapshot, records the instrument failure, and lets the
checkpointed batch continue.

The endpoint may return full history even for a bounded request. The parser filters the
result before persistence; it does not store daily factor plateaus or price histories.
Adjacent ratios within the configured material-change threshold are treated as provider
precision drift and do not create factor events.

Alternative rejected: Tencent/Eastmoney adjusted-to-raw price ratios. They duplicate price
downloads, are sensitive to rounded close prices, and add no benefit while a direct factor
endpoint is available.

### Normalize comparisons at continuity-segment level

Every source path is rebuilt from adjacent positive event ratios. Cumulative comparisons
are rebased to one at the latest common event date in each segment, while event-jump
comparisons retain exact and bounded trading-session shifts. Non-continuous lineage
transitions start a new segment and reset the cumulative product.

### Select one complete path per segment

The selector applies deterministic precedence:

1. CNInfo, TDX, and Sina agree: select CNInfo with high confidence.
2. CNInfo agrees with TDX or Sina: select CNInfo with high confidence.
3. TDX and Sina agree while CNInfo differs: select the consensus path only for ordinary
   symmetric actions.
4. A governed special-action segment retains CNInfo policy and records other sources as
   differing market-account evidence.
5. No eligible consensus: select complete CNInfo with low confidence and emit an audit
   conflict. If CNInfo is incomplete, the segment remains blocked.

Agreement requires both event-jump and normalized cumulative-path tolerances. Selected rows
come from one source path within a segment; dates and factors are never mixed.

### Preserve audit and isolate promotion

Canonical rows use `adjustment_factors_canonical`. Compact metadata remains in existing row
fields; full decisions, scores, agreements, lineage boundaries, and reasons remain in
`adjustment_factor_series_status.report_json`. Source observations remain isolated.

The manual workflow can backfill Sina observations and build a staging candidate.
`build_canonical=true` never switches production reads. Explicit promotion remains a later
operator decision.

## Risks / Trade-offs

- [Sina endpoint is temporarily unavailable] -> Preserve the prior complete snapshot,
  report the instrument as pending, and continue the batch.
- [Incremental response lacks a pre-range anchor] -> Request bounded lookback context and
  do not fabricate a first event without an anchor.
- [A whole-segment choice retains a local source error] -> Preserve event-level conflicts
  and confidence; do not create an apparently precise mixed path.
- [Legacy Sina production rows lack governed snapshot status] -> Run one checkpointed
  full-market Sina backfill before relying on three-source completeness.
- [Obsolete price-ratio rows exist] -> Delete only profiles and statuses owned by the
  removed Tencent/Eastmoney implementation; do not touch Sina, CNInfo, or TDX rows.

## Migration Plan

1. Restore and test direct Sina `hfq-factor` acquisition and governed snapshot persistence.
2. Remove Tencent/Eastmoney price-ratio code, configuration, tests, documentation, and
   exact owned runtime artifacts. Database initialization performs an idempotent cleanup
   of only the retired provider profiles and snapshot series.
3. Rewire selection coverage and reporting to `sina_hfq_factor`.
4. Run a targeted Sina backfill and three-source dry-run.
5. Run a checkpointed full-market Sina backfill.
6. Build and inspect a full-market staging candidate before explicit promotion.

Rollback disables optional Sina backfill and candidate construction. Existing CNInfo and
TDX paths and legacy production reads remain available.
