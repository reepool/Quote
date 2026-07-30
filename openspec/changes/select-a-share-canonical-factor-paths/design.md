## Context

CNInfo and TDX event-derived paths are complete and independently persisted. The current
CNInfo rebuild also compares legacy BaoStock and sparse Sina/AkShare observations, but
`source_selection_status` remains `deferred` and the canonical candidate is still a
CNInfo-only construction. The production canonical table therefore cannot yet explain why
one path was selected when sources disagree.

AkShare exposes Tencent and Eastmoney raw/qfq/hfq daily prices, not a direct factor table.
Their factor evidence must be derived from same-day adjusted/raw price ratios. Those prices
are rounded, so the adapter must identify stable piecewise levels instead of treating every
daily ratio change as a corporate action. The process already installs
`akshare_proxy_patch` before importing AkShare; Eastmoney fallback must reuse that bootstrap
and must not inject credentials or proxy behavior locally.

Known absorption mergers and other legal-subject discontinuities are available through the
instrument lineage metadata used by the CNInfo factor path. A canonical selector must not
carry cumulative factors across transitions marked `price_continuity=non_continuous` or
manufacture a factor at those boundaries.

## Goals / Non-Goals

**Goals:**

- Build a Tencent-first, Eastmoney-fallback AkShare factor observation path with explicit
  provider profiles and quality diagnostics.
- Normalize CNInfo, TDX, and eligible AkShare paths to comparable event ratios and
  latest-session unit anchors.
- Select one internally consistent source path per instrument continuity segment using
  deterministic consensus and special-action rules.
- Produce complete versioned canonical staging rows, per-instrument selection evidence,
  bounded conflict samples, and promotion gates without changing production reads.
- Support dry-run, targeted pilot, checkpointed full-market backfill, and repeatable
  operator execution.

**Non-Goals:**

- Treating any provider as an absolute truth or overwriting source observations.
- Event-by-event source splicing inside one continuity segment.
- Automatically promoting or changing `read_dataset`.
- Recomputing special restructuring economics from rounded market prices.
- Using BaoStock or the old sparse Sina path as a voting source.

## Decisions

### Use a dedicated AkShare market-factor adapter

The adapter calls Tencent raw and hfq daily history first. If Tencent is unavailable or
fails structural validation, it retries the same contract through Eastmoney. Each result
is tagged `akshare_tencent_price_ratio_v1` or
`akshare_eastmoney_price_ratio_v1`; a fallback response is never labelled Tencent.

Each successful instrument fetch also replaces one provider-snapshot coverage status. The
status identifies the chosen provider profile, requested range, snapshot ingestion id, and
whether the complete path contains events. Selection loads only observation rows matching
that snapshot id and profile. This preserves successful zero-event coverage and prevents
append-only observations from combining stale Tencent and Eastmoney paths.

The adapter aligns raw and hfq closes by trading date, computes positive ratios, and
compresses them into stable piecewise levels. A level transition is eligible only when the
relative change exceeds a configurable threshold and both sides have enough stable
observations. Acquisition includes bounded pre-range and post-range context while emitted
events remain restricted to the requested range. A provider snapshot is complete only
when its aligned overlap covers the requested security lifecycle within the configured
calendar-day tolerance. No-overlap, truncated history, non-positive prices, excessive
within-level dispersion, or insufficient persistence produces an indeterminate result
rather than fabricated events.

Alternative considered: continue using Sina `hfq-factor`. It provides direct factors but
has sparse local coverage and poor current benchmark normalization, so it remains audit
evidence only.

### Normalize comparisons at continuity-segment level

Every source path is rebuilt from adjacent positive event ratios. Cumulative comparisons
are rebased to one at the latest common event date in each segment, while event-jump
comparisons retain exact and bounded trading-session shifts. Non-continuous lineage
transitions start a new segment and reset the cumulative product.
Segment bounds are persisted in the version report and attached to canonical factors by
the governed read path. The adjustment engine anchors each reported segment independently,
including event-free segments, without inserting a synthetic factor at the transition.

Alternative considered: compare only final cumulative factors. This can hide offsetting
errors and lets one early mistake dominate the entire history.

### Select one complete path per segment

The selector scores complete source paths but applies deterministic precedence:

1. Three eligible sources agree: select CNInfo with high confidence.
2. CNInfo agrees with TDX or AkShare: select CNInfo with high confidence.
3. TDX and AkShare agree while CNInfo differs: select the consensus source only for a
   segment containing ordinary symmetric actions.
4. A governed special-action segment always retains CNInfo policy and records other
   sources as differing market-account evidence.
5. No eligible consensus: select CNInfo as a low-confidence complete fallback and emit an
   audit conflict. If CNInfo is incomplete, the segment remains blocked rather than being
   silently filled.

Agreement requires both event-jump and normalized cumulative-path tolerances. The selected
rows all come from one source path in the segment; dates or factors are not mixed.

### Preserve selection audit in existing versioned surfaces

Canonical rows continue to use `adjustment_factors_canonical`. Compact selection metadata
is stored in the existing `source_profile`, `quality_status`, and `evidence_count` fields.
The full per-instrument/segment decision, scores, agreements, lineage boundaries, and
reasons are stored in `adjustment_factor_series_status.report_json`. This avoids a schema
migration while keeping the result reproducible. Source observations remain unchanged.

### Separate candidate construction from promotion

The manual workflow can backfill AkShare observations and build a staging candidate.
`build_canonical=true` never changes production reads. Eligibility requires full-market
scope, complete CNInfo factor paths, successful candidate writes, and no blocked segments.
Low-confidence CNInfo fallbacks are reported and configurable as an audit warning, not a
silent success. A later explicit promotion action remains required.

## Risks / Trade-offs

- [Rounded adjusted prices can create false ratio changes] -> Require persistent levels,
  minimum relative jumps, bounded dispersion, and unit tests with noisy fixtures.
- [Two calls per instrument increase upstream load] -> Keep the workflow manual,
  checkpointed, rate-limited, and resumable; do not add it to the daily CNInfo task.
- [Tencent or Eastmoney format changes] -> Validate required columns and return
  indeterminate provider diagnostics without deleting prior observations.
- [Special actions use different shareholder perspectives] -> Keep governed CNInfo policy
  and prohibit automatic two-source override for special segments.
- [A whole-segment source choice may retain a local provider error] -> Preserve event-level
  conflicts and confidence; do not create an apparently precise mixed path.
- [Existing canonical schema has compact row metadata] -> Store full selection evidence in
  the version report and expose bounded row metadata for query compatibility.

## Migration Plan

1. Add and test the Tencent/Eastmoney factor adapter and stable ratio segmentation.
2. Backfill a targeted sample in dry-run and write modes; inspect provider profiles and
   source coverage without building canonical rows.
3. Add and test the three-source segment selector against symmetric, special, conflict,
   empty-source, and non-continuous-lineage fixtures.
4. Run a full-market dry-run selection report.
5. Persist a full-market staging candidate with production reads unchanged.
6. Review low-confidence and blocked segments before any explicit production promotion.

Rollback disables AkShare backfill and canonical candidate construction. All additions are
isolated observations or versioned staging/status records, so existing CNInfo/TDX paths and
legacy production reads remain available.

## Open Questions

- The final numeric tolerances should be calibrated from the first full-market Tencent and
  Eastmoney distribution; defaults are conservative and configuration-controlled.
- Production promotion remains an explicit operator decision after the full-market preview.
