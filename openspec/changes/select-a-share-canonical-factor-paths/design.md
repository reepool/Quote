## Context

CNInfo and TDX event-derived paths are complete and independently persisted. The current
production fallback is `adjustment_factors`, the BaoStock-Sina composite path. BaoStock supplies its
historical base and the AkShare route obtains direct Sina `hfq-factor` changes during daily
and maintenance updates. New tail ratios are rebased onto the existing cumulative tail, so
the table is one operational path rather than two independent votes.

A temporary implementation replaced Sina with Tencent-first/Eastmoney-fallback raw and
hfq daily prices. It required two historical price downloads per instrument, introduced
rounded-price extraction failures, and left the direct Sina path only partially represented
in the governed observation table. A separate complete Sina source would require a new
full-market backfill that is not needed for the current objective. The canonical selector
should use the three paths already maintained locally: CNInfo, TDX, and BaoStock-Sina
composite.

Known absorption mergers and other legal-subject discontinuities remain available through
instrument lineage metadata. Cumulative factors must reset at transitions marked
`price_continuity=non_continuous` without manufacturing a factor at the boundary.

## Goals / Non-Goals

**Goals:**

- Keep direct Sina A-share `hfq-factor` acquisition for normal composite-tail maintenance.
- Normalize CNInfo, TDX, and the existing BaoStock-Sina composite path to comparable event ratios and
  latest-session unit anchors.
- Select one internally consistent source path per continuity segment using deterministic
  consensus and special-action rules.
- Produce versioned canonical staging rows, per-instrument selection evidence, bounded
  conflict samples, and promotion gates without changing production reads.
- Promote an explicitly confirmed, current, full-market staging version atomically and
  keep the stable canonical version current through affected-instrument daily merges.
- Support local-only dry-run, targeted pilot, and repeatable operator execution.
- Remove the unused Tencent/Eastmoney price-ratio implementation and its persisted state.
- Keep endpoint-request coverage and cross-source differences visible as audit evidence
  without using those audit intervals to invalidate complete CNInfo or TDX factor paths.

**Non-Goals:**

- Treating any provider as absolute truth or overwriting source observations.
- Event-by-event source splicing inside one continuity segment.
- Automatically promoting or changing production reads from the selection task.
- Recomputing special restructuring economics from market prices.
- Downloading raw or adjusted price histories to derive the Sina factor path.
- Downloading a separate full-market Sina history solely for canonical selection.
- Treating BaoStock and Sina rows inside the composite table as separate voting sources.

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

### Use the existing BaoStock-Sina composite as the third path

The selector reads `adjustment_factors` as one path. Its row-level `source` remains
available for lineage and diagnostics, but rows are never split into BaoStock and Sina
votes. A composite tail append is eligible only after it has been rebased onto the existing
cumulative tail by the established persistence path. An instrument with no composite rows is
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
normalized, the composite path is ineligible even when the failure falls before a bounded
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

1. A strictly validated reviewed whole-lifecycle source override selects its configured
   complete path and records the catalog version and reason.
2. CNInfo, TDX, and the BaoStock-Sina composite agree: select CNInfo with high confidence.
3. CNInfo agrees with TDX or the composite: select CNInfo with high confidence.
4. TDX and the composite agree while CNInfo differs: select the consensus path only for ordinary
   symmetric actions.
5. A governed special-action segment retains CNInfo policy and records other sources as
   differing market-account evidence.
6. No eligible consensus: select complete CNInfo with low confidence and emit an audit
   conflict.
7. If the lifecycle has ended, CNInfo supplies no event rows, and TDX supplies a complete
   non-empty path, select TDX as an explicitly labelled low-confidence historical path.
   A conflicting BaoStock-Sina composite path remains audit evidence but does not block
   this reviewed lifecycle policy. Complete CNInfo zero-event endpoint evidence does not
   win this branch because the non-empty TDX history directly contradicts that archive
   assumption.
8. Otherwise an incomplete CNInfo segment remains blocked.

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
eligible only in continuity segments containing valid normalized composite events.

An empty CNInfo path contradicted by a complete non-empty TDX path is not treated as
ordinary eligible CNInfo evidence. An active lifecycle remains blocked unless TDX and the
composite form an independent consensus. A completed lifecycle may use the narrower historical
TDX fallback even when the BaoStock-Sina composite path conflicts.

The historical TDX fallback is deliberately narrower than normal consensus. It cannot be
used for an active lifecycle, including an earlier continuity segment of an otherwise
active instrument, a special action whose CNInfo policy is known, an incomplete TDX path,
or a special action whose CNInfo policy is known. Its selected rows retain the TDX source
and use `historical_single_source` confidence so they remain distinguishable from
independent consensus. A composite conflict is retained in pairwise evidence and the
decision reason. The branch is evaluated before the ordinary CNInfo zero-event fallback
so contradicted empty CNInfo archive coverage cannot hide known TDX events.

Reviewed source overrides are stored in a small source-controlled catalog, validated at
load time, and applied only when the selected source path is complete and non-empty. The
initial reviewed decisions select TDX for the full lifecycles of `000004.SZ` and
`600455.SH`. Invalid, unknown-source, or ineligible overrides fail closed rather than
silently selecting a path. Source observations and the physical `adjustment_factors`
table remain unchanged.

When an instrument is explicitly marked `status=delisted` but lacks a delisting date, its
last local quote date is used as an auditable inferred lifecycle end. Generic inactive or
automatic deactivation states are insufficient because they may describe a still-listed
instrument with stale local data.

A CNInfo-supported instrument whose lifecycle starts on the requested end date has no
post-listing interval in which an adjustment event could affect the candidate. When
CNInfo, TDX, and BaoStock-Sina composite all contain no event on that boundary, the empty CNInfo path is
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

Operator-facing reports and newly persisted selection provenance use the unambiguous
`baostock_sina_composite` source identifier and `BaoStock_Sina composite` display name.
The existing physical `adjustment_factors` table is not renamed because that would require
an unrelated storage migration.

Blocked decisions are collected independently from low-confidence and historical
single-source samples. Reports show the blocked list first, bounded by the configured
sample limit, and retain the other decision classes under their existing confidence and
reason labels.

The manual workflow reads all three local paths and builds a staging candidate.
`build_canonical=true` never switches production reads.

### Promote and activate only through a separate confirmed task

The promotion task defaults to dry-run and accepts one exact staging version plus one
stable target version. It validates the persisted staging state rather than trusting
runtime arguments. A promotable full-market candidate must be `validated_staging`, carry
`candidate_promotion_eligible=true`, have all blocking gates true, have no incomplete
instrument status, and have row, event-count, and instrument-count totals that match the
persisted report. The staging prefix must match the target stable version, and its end date
must cover the latest completed SSE/SZSE trading session.

A write requires `confirm=true`. The database operation rechecks the persisted staging
state inside the same transaction that replaces the target rows and coverage statuses.
After the target version is committed, the task optionally activates canonical reads by
atomically replacing a strictly validated project-runtime manifest. Activation failure
does not corrupt the promoted database version: the prior read path remains active and the
task reports a partial result that can be safely retried.

The runtime manifest records only `canonical` plus the stable series version, or
`baostock_sina_composite` for rollback. It overrides the compatibility defaults in
`config/03_data.json`, is read dynamically with bounded caching, and therefore survives a
restart without modifying source-controlled configuration. Invalid manifests fail closed
to the configured BaoStock-Sina compatibility path.

### Continue the promoted canonical version during daily maintenance

Daily source acquisition and independent CNInfo/TDX path rebuilding remain unchanged.
When the runtime manifest activates a promoted canonical version, the daily task adds a
final local-only three-source stage for affected instruments and active SSE/SZSE
instruments missing canonical coverage. The targeted candidate is written to its own
staging version. It may be merged only when every non-full-market blocking gate succeeds,
no segment or instrument is incomplete, and the stable target is still promoted and
active.

The database merges only the targeted instruments in one transaction, preserving all
other canonical rows. It replaces their coverage states, updates full decision provenance,
recomputes stable row and instrument totals, and records bounded incremental history.
Failure leaves the prior stable rows unchanged and carries the affected instruments into
the existing retry queue. An inactive canonical manifest never causes the daily task to
write the stable canonical version.

## Risks / Trade-offs

- [Legacy path has no rows] -> Mark only that reference source unavailable; do not treat an
  unknown path as a zero-event consensus vote.
- [A whole-segment choice retains a local source error] -> Preserve event-level conflicts
  and confidence; do not create an apparently precise mixed path.
- [Legacy rows change provider at the tail] -> Rely on the existing rebase-on-append
  contract and preserve row source lineage for audit.
- [Recent endpoint status does not cover a non-targeted instrument] -> Report an audit
  coverage warning without invalidating an otherwise complete factor path.
- [TDX is selected over conflicting historical composite evidence] -> Permit only a
  labelled completed-lifecycle fallback after lifecycle and TDX completeness checks;
  preserve the conflict evidence and never apply the rule to active instruments.
- [Reviewed override becomes stale] -> Require a complete non-empty selected path and
  preserve catalog version and reason; otherwise block instead of falling back silently.
- [Staging report and rows diverge] -> Recount rows, statuses, and event totals before and
  inside promotion; fail closed without touching the stable version.
- [Daily targeted candidate fails] -> Preserve the previous stable rows and keep affected
  instruments in the factor retry queue.
- [Activation file is invalid or unwritable] -> Keep the prior read path active and report
  the promoted-but-not-activated state explicitly.
- [Home filesystem is read-only] -> Store BaoStock lock/quota state in project runtime
  storage rather than disabling the configured fallback source.
- [Obsolete price-ratio rows exist] -> Delete only profiles and statuses owned by the
  removed Tencent/Eastmoney implementation; do not touch Sina, CNInfo, or TDX rows.

## Migration Plan

1. Keep and test direct Sina `hfq-factor` acquisition for composite incremental maintenance.
2. Remove Tencent/Eastmoney price-ratio code, configuration, tests, documentation, and
   exact owned runtime artifacts. Database initialization performs an idempotent cleanup
   of only the retired provider profiles and snapshot series.
3. Rewire selection coverage and reporting to the existing BaoStock-Sina composite path.
4. Run a targeted local-only three-source dry-run.
5. Build and inspect a full-market staging candidate before explicit promotion.
6. Apply the reviewed source-override catalog and emit blocked-first reports.
7. Re-run the full-market preview with lifecycle bounds, historical fallback, and the
   production default tolerance before writing staging.
8. Dry-run the explicit promotion task, then confirm atomic promotion and canonical
   activation.
9. Verify one daily affected-instrument merge and the explicit composite rollback path.

Rollback writes the runtime activation manifest back to `baostock_sina_composite`.
Existing CNInfo, TDX, composite, staging, and promoted canonical rows remain available.
