## Context

CNInfo and TDX event-derived paths are complete and independently persisted. The current
production fallback is `adjustment_factors`, a composite legacy path. BaoStock supplies its
historical base and the AkShare route obtains direct Sina `hfq-factor` changes during daily
and maintenance updates. New tail ratios are rebased onto the existing cumulative tail, so
the table is one operational path rather than two independent votes.

A temporary implementation replaced Sina with Tencent-first/Eastmoney-fallback raw and
hfq daily prices. It required two historical price downloads per instrument, introduced
rounded-price extraction failures, and left the direct Sina path only partially represented
in the governed observation table. A separate complete Sina source would require a new
full-market backfill that is not needed for the current objective. The canonical selector
should use the three paths already maintained locally: CNInfo, TDX, and legacy composite.

Known absorption mergers and other legal-subject discontinuities remain available through
instrument lineage metadata. Cumulative factors must reset at transitions marked
`price_continuity=non_continuous` without manufacturing a factor at the boundary.

## Goals / Non-Goals

**Goals:**

- Keep direct Sina A-share `hfq-factor` acquisition for normal legacy-tail maintenance.
- Normalize CNInfo, TDX, and the existing legacy composite path to comparable event ratios and
  latest-session unit anchors.
- Select one internally consistent source path per continuity segment using deterministic
  consensus and special-action rules.
- Produce versioned canonical staging rows, per-instrument selection evidence, bounded
  conflict samples, and promotion gates without changing production reads.
- Support local-only dry-run, targeted pilot, and repeatable operator execution.
- Remove the unused Tencent/Eastmoney price-ratio implementation and its persisted state.
- Keep endpoint-request coverage and cross-source differences visible as audit evidence
  without using those audit intervals to invalidate complete CNInfo or TDX factor paths.

**Non-Goals:**

- Treating any provider as absolute truth or overwriting source observations.
- Event-by-event source splicing inside one continuity segment.
- Automatically promoting or changing `read_dataset`.
- Recomputing special restructuring economics from market prices.
- Downloading raw or adjusted price histories to derive the Sina factor path.
- Downloading a separate full-market Sina history solely for canonical selection.
- Treating BaoStock and Sina rows inside the legacy table as separate voting sources.

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

The endpoint may return full history even for a bounded request. The parser filters the
result before persistence; it does not store daily factor plateaus or price histories.
Adjacent ratios within the configured material-change threshold are treated as provider
precision drift and do not create factor events.

Alternative rejected: Tencent/Eastmoney adjusted-to-raw price ratios. They duplicate price
downloads, are sensitive to rounded close prices, and add no benefit while a direct factor
endpoint is available.

### Use the existing legacy composite as the third path

The selector reads `adjustment_factors` as one path. Its row-level `source` remains
available for lineage and diagnostics, but rows are never split into BaoStock and Sina
votes. A legacy tail append is eligible only after it has been rebased onto the existing
cumulative tail by the established persistence path. An instrument with no legacy rows is
reported as unavailable rather than assumed to be a complete zero-event vote.

BaoStock history may store cumulative levels in both `factor` and `cumulative_factor`.
Selection reads the complete local prefix through the requested end date, derives adjacent
ratios from consecutive cumulative levels, and then restricts scoring to the requested
range. At a BaoStock-to-Sina source switch, existing direct writers may have retained the
Sina absolute cumulative basis instead of rebasing it. When the cross-source cumulative
ratio materially conflicts with the stored positive adjacent event factor, selection uses
the stored event factor and rebuilds an internal continuous cumulative chain. It retains
the provider cumulative level, chosen normalization method, and conflict flag for audit.
If a provider switch has no positive stored event factor, or any prefix row cannot be
normalized, the legacy path is ineligible even when the failure falls before a bounded
request's start date.
Exact no-change ratios are omitted. This conversion is read-only and retains the underlying
row source for diagnostics.

The canonical workflow does not call an external provider. Source acquisition remains the
responsibility of existing daily and maintenance jobs.

### Normalize comparisons at continuity-segment level

Every source path is rebuilt from adjacent positive event ratios. Event correspondence is
established first with ordered one-to-one matching, exact dates, and bounded
trading-session shifts. Reports separate exact, shifted, unmatched, and conflicting events
and bucket relative factor differences. Cumulative comparisons are then rebased to one at
the latest common event date in each segment. Non-continuous lineage transitions start a
new segment and reset the cumulative product.

### Select one complete path per segment

The selector applies deterministic precedence:

1. CNInfo, TDX, and legacy agree: select CNInfo with high confidence.
2. CNInfo agrees with TDX or legacy: select CNInfo with high confidence.
3. TDX and legacy agree while CNInfo differs: select the consensus path only for ordinary
   symmetric actions.
4. A governed special-action segment retains CNInfo policy and records other sources as
   differing market-account evidence.
5. No eligible consensus: select complete CNInfo with low confidence and emit an audit
   conflict.
6. If the lifecycle has ended, CNInfo supplies no event rows, TDX supplies a complete
   non-empty path, and legacy supplies no conflicting path, select TDX as an explicitly
   labelled low-confidence historical single-source fallback. Complete CNInfo zero-event
   endpoint evidence does not win this branch because the non-empty TDX history directly
   contradicts that archive assumption.
7. Otherwise an incomplete CNInfo segment remains blocked.

Agreement requires both event-jump and normalized cumulative-path tolerances. Selected rows
come from one source path within a segment; raw source dates remain auditable. Reviewed
effective-date overrides and continuity boundaries are applied before comparison.

CNInfo is source-complete when its derived path has no pending factor events and no
historical factor gaps. TDX is source-complete when its derived path has no pending factor
events. Recent endpoint-request interval rows are audit checks only. Legacy is available
when the existing composite table contains a valid path for the instrument.

An empty CNInfo path is eligible when accepted endpoint evidence covers the lifecycle start
and the derived path has no pending event or historical gap. Later missing per-instrument
endpoint intervals remain an audit warning because normal daily maintenance discovers
announcements market-wide and refreshes only affected instruments. Full lifecycle endpoint
coverage remains stronger evidence when available. Coverage starts at the later of the
requested start and listing date, and ends at the earlier of the requested end and
delisting date. A non-empty derived CNInfo or TDX path remains eligible without recent
endpoint evidence. Legacy has no independent zero-event coverage contract, so it is
eligible only in continuity segments containing valid normalized legacy events.

An empty CNInfo path contradicted by a complete non-empty TDX path is not treated as
ordinary eligible CNInfo evidence. An active lifecycle remains blocked unless TDX and
legacy form an independent consensus. A completed lifecycle may use the narrower historical
TDX fallback only when legacy supplies no eligible conflicting path.

The historical TDX fallback is deliberately narrower than normal consensus. It cannot be
used for an active lifecycle, including an earlier continuity segment of an otherwise
active instrument, a special action whose CNInfo policy is known, an incomplete TDX path,
or a segment where an eligible legacy path disagrees. Its selected rows retain the TDX
source and use `historical_single_source` confidence so they remain distinguishable from
independent consensus. The branch is evaluated before the ordinary CNInfo zero-event
fallback so contradicted empty CNInfo archive coverage cannot hide known TDX events.

When an instrument is explicitly marked `status=delisted` but lacks a delisting date, its
last local quote date is used as an auditable inferred lifecycle end. Generic inactive or
automatic deactivation states are insufficient because they may describe a still-listed
instrument with stale local data.

A CNInfo-supported instrument whose lifecycle starts on the requested end date has no
post-listing interval in which an adjustment event could affect the candidate. When
CNInfo, TDX, and legacy all contain no event on that boundary, the empty CNInfo path is
accepted as a low-confidence listing-boundary zero-event path instead of blocking the whole
full-market candidate.

Cross-provider factor ratios use a default relative tolerance of 0.1%. This accommodates
normal published precision differences without hiding larger differences; operators may
still provide a stricter value and every run retains the existing factor-difference
buckets.

### Keep BaoStock runtime state writable

BaoStock's persistent daily quota and cross-process session lock remain project-local
runtime state. Defaults resolve below `data/runtime/baostock/`, which is writable in the
deployed service and remains outside source-controlled datasets. Explicit absolute paths
remain supported for tests and custom deployments. During migration from the previous
user-cache defaults, the governor reads the larger current-day counter, coordinates both
session locks, and mirrors state to the legacy path when writable. An unavailable
read-only legacy path does not disable the project-local governor.

### Preserve audit and isolate promotion

Canonical rows use `adjustment_factors_canonical`. Compact metadata remains in existing row
fields; full decisions, scores, agreements, lineage boundaries, and reasons remain in
`adjustment_factor_series_status.report_json`. Source observations remain isolated.

The manual workflow reads all three local paths and builds a staging candidate.
`build_canonical=true` never switches production reads. Explicit promotion remains a later
operator decision.

## Risks / Trade-offs

- [Legacy path has no rows] -> Mark only that reference source unavailable; do not treat an
  unknown path as a zero-event consensus vote.
- [A whole-segment choice retains a local source error] -> Preserve event-level conflicts
  and confidence; do not create an apparently precise mixed path.
- [Legacy rows change provider at the tail] -> Rely on the existing rebase-on-append
  contract and preserve row source lineage for audit.
- [Recent endpoint status does not cover a non-targeted instrument] -> Report an audit
  coverage warning without invalidating an otherwise complete factor path.
- [TDX is the only surviving source for a delisted instrument] -> Permit only a labelled
  historical single-source fallback after lifecycle and completeness checks; never apply
  it to active instruments.
- [Home filesystem is read-only] -> Store BaoStock lock/quota state in project runtime
  storage rather than disabling the configured fallback source.
- [Obsolete price-ratio rows exist] -> Delete only profiles and statuses owned by the
  removed Tencent/Eastmoney implementation; do not touch Sina, CNInfo, or TDX rows.

## Migration Plan

1. Keep and test direct Sina `hfq-factor` acquisition for legacy incremental maintenance.
2. Remove Tencent/Eastmoney price-ratio code, configuration, tests, documentation, and
   exact owned runtime artifacts. Database initialization performs an idempotent cleanup
   of only the retired provider profiles and snapshot series.
3. Rewire selection coverage and reporting to the existing legacy composite path.
4. Run a targeted local-only three-source dry-run.
5. Build and inspect a full-market staging candidate before explicit promotion.
6. Re-run the full-market preview with lifecycle bounds, historical fallback, and the
   production default tolerance before writing staging.

Rollback disables candidate construction. Existing CNInfo, TDX, and legacy production
reads remain available.
