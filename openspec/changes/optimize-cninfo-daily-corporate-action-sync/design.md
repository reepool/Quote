## Context

The daily corporate-action workflow currently discovers a bounded set of
instruments, but loses source-profile information before refresh. The backfill
then calls both CNInfo structured endpoints for every instrument. The same run
also executes a full-market TDX XDXR refresh even though TDX is a reference
source. Recent production timing attributes roughly 72 of 92 minutes to CNInfo
refresh and another 18 minutes to TDX refresh.

The existing shared adaptive throttle slows requests after 403/429 responses,
but interspersed successes can reset cooldown escalation while rolling
rate-limit density remains high. The repository also already persists issuer
lineage and supports suppressed reference events, but current reconciliation
does not fully apply non-continuous issuer boundaries to TDX-only events.

CNInfo remains the primary source. Existing cross-day announcement discovery,
structured-event validation, anomaly-triggered LLM governance, and factor
promotion behavior must remain intact.

## Goals / Non-Goals

**Goals:**

- Preserve endpoint intent from candidate discovery through refresh and retry.
- Remove unconditional daily full-market TDX work.
- Make sustained CNInfo blocking produce a source-wide pause and slow recovery.
- Reconcile reference events only within the active issuer segment.
- Make runtime and limiter behavior measurable from one structured result.
- Retain existing public call behavior for explicit historical refreshes.

**Non-Goals:**

- Increasing CNInfo concurrency or bypassing source controls.
- Replacing CNInfo values with TDX values.
- Rebuilding document/OCR/LLM orchestration.
- Adding an unverified bulk CNInfo endpoint.
- Synthesizing factors across a non-continuous absorption-merger boundary.

## Decisions

### 1. Represent daily CNInfo work as normalized endpoint targets

Candidate discovery will maintain a mapping from instrument to a set of
`cninfo_dividend` and `cninfo_allotment` profiles. A small normalization helper
will merge repeated evidence, expand `both`, deduplicate targets, and expose
both unique-instrument and endpoint-target counts.

The daily orchestrator will pass these targets into the existing backfill
surface. The historical/default path will remain profile-complete when no
target mapping is supplied, preserving compatibility. Retry candidates will
derive their profile from the failed source record instead of retrying the
other endpoint.

Alternative considered: split the task into two independent endpoint jobs.
That would duplicate candidate discovery, anomaly governance, and factor
rebuild coordination, so a shared target plan is preferred.

### 2. Add one final recovery pass, not another retry subsystem

Endpoint failures classified as transient will be collected by target. After
the normal pass, the orchestrator will perform at most one final pass through
the same client and shared throttle. The limiter will impose current
source-wide waits; the retry loop will not sleep independently or recurse.

Alternative considered: increase per-request retries. That holds worker
execution during high-density blocking and obscures how many targets were
recovered. A bounded final pass is easier to audit and stops retry amplification.

### 3. Make TDX refresh mode explicit

The daily task will accept `tdx_refresh_mode` with `targeted`, `full`, and
`auto`. `targeted` will combine CNInfo endpoint-plan instruments, announcement
instruments, reference retry/carryover instruments, and a bounded deterministic
rotation. `full` preserves the existing all-market path. `auto` resolves from
configuration/schedule policy while reporting the effective mode.

The normal daily scheduler entry will use targeted mode. A separate configured
weekly invocation will use full mode, avoiding hidden full-market work inside
otherwise incremental runs. The existing manual command can force either mode.

Alternative considered: remove TDX refresh from the daily workflow entirely.
That would delay reference confirmation for newly announced events; targeted
refresh retains useful daily evidence at bounded cost.

### 4. Extend the existing throttle state with a circuit, not a new limiter

The source-scoped state will track rolling rate-limit density, consecutive
stable successes, total adaptive wait, short cooldowns, circuit trips, and
circuit wait. When density remains above the configured threshold at or near
maximum interval, the state sets a shared 60-120 second `blocked_until`.

One success will update the rolling window but will not clear a circuit or
restore the minimum interval. Recovery requires a configured stable-success
streak and then decreases intervals one step at a time. Time and random jitter
will remain injectable so tests are deterministic.

Alternative considered: raise the maximum ordinary request interval beyond
eight seconds. This continuously penalizes every request and still lets
multiple callers interleave. A shared source-wide circuit better communicates
that the provider is actively refusing traffic.

### 5. Filter reconciliation by issuer segment while preserving raw data

The factor-governance reconciliation layer will resolve lineage metadata for
the requested instrument and classify TDX reference events outside the active
issuer segment or on a non-continuous no-synthetic-factor transition. Such
events remain in raw TDX storage but move into
`suppressed_reference_events` with a stable reason and relevant lineage
metadata.

This is generic metadata-driven behavior, not a `600018.SH` exception. For the
known absorption merger it suppresses six predecessor events and the
2006-10-26 transition event, while retaining 22 comparable current-issuer
events.

Alternative considered: mark the seven events as accepted TDX-only
differences. That leaves false incompleteness in every reconciliation and does
not encode the non-continuous price regime.

### 6. Separate primary, reference, and reconciliation status

The daily structured result will preserve distinct CNInfo readiness, TDX
reference readiness, and cross-source reconciliation states. Overall wording
will identify whether production-primary work succeeded even when optional
reference checks are partial. Existing `build_canonical=false` behavior remains
unchanged.

Stage timing will use monotonic elapsed time around discovery, CNInfo refresh,
TDX refresh, factor rebuild, and anomaly LLM phases. Endpoint and limiter
metrics will be returned as structured dictionaries and rendered in the
operator report.

### 7. Treat bulk retrieval as a contract investigation

The implementation will inspect existing documented/requested CNInfo
structured endpoints for bounded date-window and pagination support. A finding
will record evidence and constraints. Production code will only change if a
complete, repeatable contract is verified; otherwise per-target endpoint
routing remains the supported implementation.

### 8. Align long-suspension reference events only within lifecycle bounds

The factor rebuild will explicitly request `next_observed_trade` alignment for
corporate-action evidence. Under that policy, an event date without a traded
quote may use the first later row with `tradestatus=1` and a positive close,
even when the gap exceeds fourteen days. The search remains bounded by the
rebuild end date and, when present, the instrument's delisting date. Explicit
non-continuous issuer transitions continue to be removed before quote
alignment by the lineage partition.

The database evidence API will retain its conservative fourteen-day/default
behavior unless the caller explicitly enables this policy with a finite end
date. This prevents unrelated callers from interpreting an arbitrary long
local quote gap as a suspension.

When a TDX event belongs to an instrument whose delisting date is within the
rebuild window and no traded quote exists from the event through that terminal
date, the derived TDX path will classify it as
`terminal_no_post_event_trade`. The raw TDX row remains unchanged, the
suppression is included in reconciliation audit output, and the event does not
block later factor-path processing.

## Risks / Trade-offs

- **[Misclassified announcement skips an endpoint]** → Uncertain and special
  titles route to both endpoints; recent structured events and safety rotations
  provide later correction.
- **[Targeted TDX misses an unrelated provider correction]** → A weekly full
  sweep remains configured and manual full mode is preserved.
- **[Long circuit cooldown increases one run's latency]** → It reduces repeated
  blocking and reports wait time explicitly; the workflow remains bounded.
- **[Lineage metadata is incomplete]** → Suppression only occurs for explicit,
  validated segment/continuity policies; otherwise reconciliation behavior is
  unchanged.
- **[A long local quote gap is not a suspension]** → Long-gap alignment is
  opt-in, bounded by rebuild/lifecycle dates, and never crosses an explicit
  non-continuous lineage boundary.
- **[Status compatibility for downstream reports]** → Existing keys remain
  available while new nested metrics and effective-mode fields are additive.

## Migration Plan

1. Add normalized endpoint-target and TDX-mode helpers with unit tests.
2. Route the daily orchestrator through the new helpers while keeping old
   backfill defaults.
3. Add circuit-breaker state and metrics behind conservative defaults.
4. Add lineage suppression to reconciliation and run the `600018.SH` targeted
   regression.
5. Update scheduler defaults/reporting and add the weekly full sweep.
6. Deploy without data migration; if operational issues occur, force
   profile-complete CNInfo targets and `tdx_refresh_mode=full` through existing
   manual parameters.

## Open Questions

- Whether CNInfo exposes a stable market-wide structured endpoint with bounded
  date pagination remains an investigation item. No production behavior
  depends on a positive answer.
