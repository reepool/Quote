## Context

CNInfo has two project-owned synchronous request paths relevant to corporate actions:

- AkShare structured dividend/allotment loaders, wrapped by a local `requests.post` timeout proxy.
- Direct CNInfo announcement metadata and stock-identity requests.

Each path currently owns fixed retry sleeps and request intervals. When CNInfo begins returning HTTP 403 responses, one request slows only its own retry loop; other requests and the other CNInfo path do not see that signal. The result is repeated 403 bursts, unnecessary upstream pressure, and inefficient daily runs.

The capability must be reusable by other synchronous providers, thread-safe for concurrent callers, deterministic in unit tests, and additive to existing provider semantics. It must not change the already running process until that process reloads the new code.

## Goals / Non-Goals

**Goals:**

- Coordinate request admission by logical source key within one Python process.
- React quickly to consecutive or dense 403/429 responses.
- Apply bounded cooldowns with jitter and valid `Retry-After` guidance.
- Recover throughput gradually after sustained successful responses.
- Preserve provider retry counts, timeout handling, parsing, and persistence semantics.
- Make current state and transition counts observable without logging every request.
- Integrate both CNInfo request paths with the same `cninfo` state.

**Non-Goals:**

- Do not implement distributed coordination across processes or hosts.
- Do not migrate every data source in this change.
- Do not replace existing endpoint retries, pagination, or payload validation.
- Do not infer anti-crawl state from business-level empty results.
- Do not change scheduler frequency, storage schemas, or API contracts.

## Decisions

### Use a source-keyed process registry with explicit opt-in

A thread-safe registry will return one `AdaptiveSourceThrottle` per logical source key. Both CNInfo paths use `cninfo`, so either path's 403/429 signal slows both paths. Other providers retain existing behavior until they explicitly request a throttle.

Alternative considered: one global throttle for all HTTP traffic. Rejected because pressure and limits are source-specific, and a CNInfo incident must not slow unrelated exchanges or providers.

### Separate immutable policy from mutable state

`AdaptiveThrottlePolicy` will validate bounds and hold minimum/maximum interval, rolling window size, throttle-density threshold, slowdown/recovery factors, success threshold, cooldown stages, jitter, and maximum cooldown. `AdaptiveSourceThrottle` will hold mutable counters, rolling outcomes, the next admission time, and cooldown deadline.

Clock, sleep, and random functions are injectable so state transitions and waits can be tested without wall-clock delays.

Alternative considered: hard-code CNInfo sleep stages inside each provider. Rejected because it duplicates behavior and cannot be reused safely.

### Reserve admissions under a lock and sleep outside it

Each caller reserves its next request start under a lock using the later of the current time, the next paced start, and the cooldown deadline. It then sleeps outside the lock. Before returning, it rechecks state so a cooldown raised while it was waiting is respected. This keeps state updates responsive and prevents concurrent callers from all selecting the same start time.

Alternative considered: hold the lock during sleep. Rejected because response feedback could not extend or recover the shared state while another request was waiting.

### Treat HTTP 403 and 429 as hard throttle signals

Every 403/429 response increments the rolling throttle outcome and consecutive-throttle count. The current interval increases within configured bounds. Consecutive responses select progressively stronger cooldown stages; rolling density keeps pacing elevated even when successes are interspersed. A valid `Retry-After` value raises the cooldown floor, capped by policy. Jitter is non-negative so it never shortens upstream guidance.

Network errors and non-throttle HTTP failures reset the stable-success streak but do not masquerade as anti-crawl evidence.

Alternative considered: adapt on every exception. Rejected because TLS errors, payload errors, and upstream 5xx failures do not necessarily mean the client is sending too quickly.

### Recover only after sustained stable success

Successful HTTP responses clear the consecutive-throttle count and build a success streak. Once the configured streak is reached and recent throttle density is below the recovery threshold, the current interval decreases by one bounded step. Multiple stable periods are required to return to the minimum interval.

Alternative considered: reset to the minimum interval after the first success. Rejected because intermittent success during a throttling episode would immediately recreate request bursts.

### Keep endpoint retries as a second layer

The shared throttle controls when a request may start. Existing retry loops continue to control whether an endpoint attempt is repeated and how endpoint coverage is reported after exhaustion. This preserves `indeterminate` behavior and avoids turning the throttle into a business retry engine.

The CNInfo corporate-action proxy reports the actual response status before AkShare parses it. The announcement transport wraps its direct POST calls with the same admission and response reporting.

## Risks / Trade-offs

- [Process-local state does not coordinate multiple workers] -> Keep the interface independent of storage so a distributed coordinator can be added later if production concurrency requires it; document current scope.
- [Conservative cooldowns increase run time] -> Bound all intervals/cooldowns and recover in measured steps after sustained success.
- [Multiple pacing layers can add delay] -> Retain existing provider intervals for compatibility, while the adaptive layer enforces request-start spacing only when its reservation is later.
- [Reserved concurrent admissions may be superseded by a new cooldown] -> Recheck shared state after sleeping and reserve again when the cooldown changed.
- [A malformed successful response could count as transport stability] -> Corporate-action proxy records malformed JSON as a non-success; business payload validation remains provider-owned.
- [Excessive logs can obscure task progress] -> Log only slowdown, cooldown, and recovery transitions; expose detailed counters through snapshots for tests and diagnostics.

## Migration Plan

1. Add the generic adaptive throttle module and deterministic unit tests.
2. Pass the shared `cninfo` throttle into the structured corporate-action request proxy.
3. Wrap announcement and stock-identity POST requests with the same shared throttle.
4. Run focused CNInfo and utility tests, then the relevant OpenSpec validation.
5. Reload the service/worker after deployment; do not interrupt an in-flight daily task.

Rollback is code-only: remove the CNInfo opt-in calls while leaving the additive utility unused. No data migration or persisted-state rollback is required.

## Open Questions

- Whether future multi-process download workers require Redis or database-backed throttle coordination should be decided from observed deployment topology and 403 telemetry, not added preemptively.
- Provider-specific policy configuration can move into source config when a second source adopts the capability; the first integration keeps a validated default policy to avoid premature configuration surface.
