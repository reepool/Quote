## Why

The CNInfo daily corporate-action job conservatively retains exceptional announcements that have no matching structured event, but it has no durable announcement-level disposition for an operator-confirmed non-XDXR notice. As a result, reviewed notices remain in the pending queue indefinitely and make semantic readiness partial on every subsequent run.

## What Changes

- Add an auditable, announcement-keyed operator decision catalog for confirmed non-XDXR CNInfo notices.
- Revalidate new and carried exceptional announcements against exact operator decisions before routing them to structured refresh or semantic review.
- Clear matching pending announcements and candidate instruments idempotently without changing corporate-action observations or adjustment factors.
- Provide a bounded validation/apply script for the two approved announcements and report the resulting queue state.
- Keep unmatched exceptional announcements deferred by default when no exact operator decision exists.

## Capabilities

### New Capabilities

- `cninfo-non-xdxr-announcement-decisions`: Exact, auditable operator dispositions for exceptional CNInfo announcements that do not affect listed-company share capital or XDXR factors.

### Modified Capabilities

None.

## Impact

- Affects CNInfo daily announcement classification and carryover revalidation in `data_sources/cninfo_corporate_action_incremental.py` and `data_manager.py`.
- Adds a small versioned decision catalog and an idempotent operator application/validation script.
- Adds focused unit tests and OpenSpec validation; no public API, event schema, factor schema, or dependency changes.
