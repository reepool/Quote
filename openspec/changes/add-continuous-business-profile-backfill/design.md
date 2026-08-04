## Context

`business_profile_backfill` currently performs one bounded discovery/enqueue/drain pass. Its durable SQLite queue, immutable annual-report assets, stage checkpoints, retry state, and leases already make a later invocation resumable, but an operator must start every pass and can only inspect the final report. The generic scheduler exposes a 90-minute timeout and tracks that a job is running, while its UI cancellation callback does not cancel active work.

The continuous controller must preserve the existing `discover -> enqueue -> acquire/parse/semantic/publish` boundaries, concurrent compute, and cooperative single SQLite writer. It must also remain safe when the process is cancelled or killed at any await point: claimed work is recovered by leases and all durable artifacts remain authoritative.

## Goals / Non-Goals

**Goals:**

- Let one opt-in command drain repeated bounded passes without operator resubmission.
- Stop automatically when the active rollout phase is complete or cannot make automatic progress.
- Accept a cooperative stop request without interrupting an in-flight SQLite transaction.
- Persist enough progress to inspect a live, stopped, interrupted, or restarted run.
- Keep a single active backfill and make restart reuse the existing durable queue and assets.

**Non-Goals:**

- Enabling daily cron or automatically advancing rollout phases.
- Bypassing promotion manifests, exception gates, retries, or terminal-failure handling.
- Killing a network call, PDF parser, LLM request, or database transaction in the middle of an item.
- Estimating a misleading exact completion time while the discovery denominator is incomplete.
- Generalizing cancellation for every scheduler task.

## Decisions

### Keep single-batch behavior as the default

Add `continuous=false` to the existing task. The first validation run and specialist `expanded` runs retain current behavior. With `continuous=true`, the scheduler task invokes the existing DataManager backfill repeatedly; it does not duplicate queue or stage logic.

Changing the existing default to continuous was rejected because an initial production probe must remain bounded and easily reviewed.

### Use a separate control task

Add manual-only `business_profile_backfill_control` with `action=status|stop`. A separate job can run while the backfill owns its `max_instances=1` slot. `status` is read-only. `stop` atomically records a request targeted to the current run identity.

Reusing `/status` alone was rejected because it only reports generic scheduler state and has no business-profile queue details. Reusing the same backfill job for stop was rejected because max-instance protection would correctly reject the competing invocation.

### Persist progress and control beside existing checkpoints

Store atomic JSON files under `data/checkpoints/business_profile_async/control/`. The progress snapshot contains schema version, run id, mode, rollout phase, state, start/heartbeat/finish timestamps, cycle number, cumulative worker counters, latest discovery/enqueue/queue/readiness reports, stop state, and reason codes. A stop request contains its target run id, request timestamp, and reason.

Atomic replace avoids partial reads without adding a second SQLite writer. A newly started run receives a new identity and ignores stop requests targeting an older run. Existing work truth remains in SQLite; the JSON snapshot is operational telemetry, not a processing checkpoint.

### Stop cooperatively at item-batch boundaries

Thread a synchronous `should_stop` callback through the async production service. Each stage checks before claiming another batch and after the current concurrent batch settles. This bounds stop latency to the longest currently running item while ensuring acknowledgements/failures and SQLite writes complete normally.

Cancelling an in-flight network or LLM call was rejected because it can leave ambiguous external work and expired leases; the durable queue already provides safe recovery if the whole process is forcibly stopped.

### Define completion separately from daily readiness

Expose `phase_ready` from rollout readiness: all current-phase discovery, queue, coverage, failure, exception, and field-family gates pass, excluding the requirement that `daily_incremental` be active. `daily_ready` remains `phase_ready` plus the daily-phase requirement.

Continuous mode stops as `completed` on `phase_ready`. It stops as `blocked` on terminal failures or after a configurable number of no-progress cycles with no claimable work. It otherwise sleeps for a bounded interval using an interruptible wait and starts the next pass.

### Report progress without notification noise

Every cycle atomically updates the snapshot and emits a structured log. The control task returns a compact report on demand. Optional periodic task reports are rate-limited by `progress_report_interval_seconds`; zero disables periodic messages. Percentages are limited to known coverage and field-family ratios, while discovery backlog and queue counts remain explicit.

## Risks / Trade-offs

- [A stop request arrives during a slow LLM call] -> Mark `stop_requested` immediately and stop before the next claim; expose heartbeat and stop-request time so latency is visible.
- [The process is killed before final progress update] -> On restart, supersede a stale running snapshot and continue from SQLite leases/checkpoints.
- [No-progress detection stops during source throttling] -> Count a cycle idle only when discovery and worker counters do not advance; configure several idle cycles and persist the blockers for diagnosis.
- [An unlimited controller hides a wedged worker] -> Per-stage item/time budgets, request deadlines, heartbeat age, backpressure, idle-cycle stop, and terminal-failure stop remain active.
- [Progress JSON diverges from SQLite] -> Treat queue health and reconciliation recomputed by each pass as authoritative; never use progress JSON to decide item completion.

## Migration Plan

1. Add control/progress storage and unit tests.
2. Add cooperative stop checks to queue draining without changing default behavior.
3. Add continuous scheduler loop, control task, parameters, and runbook commands.
4. Run one ordinary batch as the operator's production probe.
5. If the probe is healthy, start `continuous=true`; observe with the control status action.
6. Roll back by stopping the controller and reverting to `continuous=false`; queue, assets, checkpoints, candidates, and facts remain intact.

## Open Questions

None. Initial defaults favor low operator noise and fail-closed termination; they remain runtime-overridable.
