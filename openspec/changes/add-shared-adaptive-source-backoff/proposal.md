## Why

CNInfo downloads currently use fixed per-request delays and isolated retries, so dense HTTP 403 responses can cause multiple business paths to keep applying pressure to the same upstream source. A shared adaptive mechanism is needed to reduce anti-crawl risk during degraded periods while gradually restoring throughput after the source becomes stable.

## What Changes

- Add a reusable, source-keyed adaptive request throttle for synchronous upstream downloads.
- Increase pacing and apply bounded, jittered cooldowns when HTTP 403/429 responses become consecutive or dense within a rolling outcome window.
- Honor valid `Retry-After` guidance within configured safety bounds.
- Gradually reduce pacing after a sustained sequence of successful responses instead of immediately returning to full speed.
- Expose thread-safe state snapshots and transition-level diagnostics for operations and tests.
- Integrate both CNInfo structured corporate-action requests and CNInfo announcement metadata requests with the shared `cninfo` throttle.
- Preserve existing endpoint retry limits, parsing, coverage semantics, and storage formats.
- Keep other providers opt-in; this change does not migrate every project HTTP path.

## Capabilities

### New Capabilities

- `adaptive-source-throttling`: Defines shared source-scoped admission, adaptive slowdown, bounded cooldown, gradual recovery, isolation, and observability behavior for upstream requests.

### Modified Capabilities

- None.

## Impact

- Affected shared code:
  - `utils/adaptive_throttle.py`
- Affected CNInfo adapters:
  - `data_sources/cninfo_corporate_actions.py`
  - `research/providers/cninfo_announcements.py`
- Affected validation:
  - deterministic unit tests for adaptive state transitions and source isolation
  - focused CNInfo transport tests for 403/429 reporting and successful recovery
- No API, scheduler contract, database schema, persisted data format, or dependency changes are intended.
- The currently running process retains its loaded code; the new behavior takes effect after the service or worker is reloaded.
