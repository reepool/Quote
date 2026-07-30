## Why

The daily CNInfo corporate-action sync is functionally complete but spends about
92 minutes per run because it queries both structured endpoints for every
candidate and refreshes the full TDX market every day. Sustained CNInfo 403
responses also show that per-request retry and short cooldowns are insufficient
for a reliable unattended daily service.

## What Changes

- Route CNInfo refresh work as endpoint-aware `(instrument_id, source_profile)`
  targets so dividend and allotment endpoints are queried only when relevant.
- Retry only failed endpoint targets and add one shared-throttle, low-speed final
  recovery pass.
- Split TDX reference refresh into targeted daily work and a configurable
  periodic/manual full-market sweep.
- Extend shared adaptive source throttling with density-based global circuit
  breaking, slow recovery, and auditable wait/rate-limit metrics.
- Apply issuer-lineage boundaries during CNInfo/TDX reconciliation so raw
  predecessor records remain stored but do not create false current-issuer
  differences or synthetic factors.
- Align reviewed TDX reference events that occur during long suspensions to
  the next observed trading session within explicit lifecycle bounds, and
  suppress terminal events that have no later traded session without changing
  raw TDX storage.
- Report endpoint request counts, limiter behavior, TDX mode, LLM duration, and
  per-stage durations while keeping CNInfo primary readiness independent from
  reference-path completeness.
- Investigate, without assuming, whether a verified bounded and paginated
  market-wide CNInfo structured endpoint can safely replace per-instrument
  requests.

## Capabilities

### New Capabilities

- `cninfo-daily-endpoint-targeting`: Profile-aware CNInfo candidate routing,
  endpoint-level retry, targeted/full TDX refresh selection, and daily-sync
  observability.
- `adaptive-source-circuit-breaking`: Sustained rate-limit density detection,
  shared long cooldowns, gradual recovery, and limiter metrics.
- `corporate-action-lineage-reconciliation`: Auditable suppression of
  predecessor and non-continuous transition reference events during
  current-issuer reconciliation.

### Modified Capabilities

- `scheduler`: The daily corporate-action task defaults to targeted TDX
  reference refresh, supports a configurable periodic full sweep, and reports
  primary and reference outcomes independently.

## Impact

- Affected runtime paths include `DataManager` CNInfo candidate discovery and
  backfill, TDX XDXR refresh selection, shared adaptive throttling, corporate
  action reconciliation/factor governance, bounded quote-evidence alignment,
  scheduler task parameters, and notification/report formatting.
- Existing explicit historical backfills remain compatible and can continue to
  request both CNInfo profiles or a full TDX sweep.
- CNInfo remains the primary source. TDX data is neither copied into CNInfo
  events nor allowed to block CNInfo readiness.
- No new external dependency or unverified production endpoint is introduced.
