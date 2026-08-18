## Context

The project uses database ids such as `.SH`, `.SZ`, `.BJ`, `.HK`, and `.US`, while public/provider inputs can contain `.SSE`, `.SZSE`, `.BSE`, `.HKEX`, vendor aliases, or bare symbols. `utils/code_utils.py`, `BaseDataSource`, and research modules currently normalize these inputs independently. Equity maintenance also has a heuristic holiday fallback even though `quotes.db` owns an exchange trading calendar.

## Goals / Non-Goals

**Goals:**

- Add one structured identity boundary without migrating database keys.
- Centralize alias parsing and explicit rendering for API, storage, exchange, and vendor boundaries.
- Make equity write decisions calendar-backed and explicit.
- Preserve master-governance behavior and current universe selection.

**Non-Goals:**

- Inferring an exchange from every ambiguous bare symbol.
- Changing instrument master source priorities or lifecycle policy.
- Rewriting futures, FX, commodity, or macro calendars.
- Introducing a global security master database.

## Decisions

1. **Extend `utils/code_utils.py` with a structured identity.** Keep existing conversion functions as compatibility adapters, but make them delegate to `InstrumentKey` parsing/rendering.
2. **Use explicit exchange aliases.** A bare symbol is accepted only where the existing caller already supplies a deterministic exchange or master lookup; otherwise return a validation error instead of guessing.
3. **Keep storage ids stable.** Render canonical database ids using the existing `.SH/.SZ/.BJ/.HK/.US` convention and preserve accepted external aliases at the boundary.
4. **Introduce an equity calendar port backed by `DatabaseOperations`/`SourceFactory`.** `DateUtils` may remain useful for display or explicit degraded diagnostics, but cannot decide equity writes or gaps.
5. **Make master refresh a command result.** Daily and repair commands consume the same governed universe and include refresh status in their structured result.

Alternatives rejected: a string-replacement-only helper cannot preserve symbol/exchange semantics; a universal calendar would erase domain-specific calendars; a database-wide key migration is unnecessary and unsafe.

## Risks / Trade-offs

- **[Some callers rely on permissive aliases] ->** Keep compatibility adapters and table-driven tests before tightening invalid inputs.
- **[Calendar is unavailable during startup] ->** Fail closed for write decisions and report a blocked/degraded result; do not silently use holidays.
- **[Index and non-equity ids do not follow stock suffix rules] ->** Keep type-aware renderers and metadata-only fixtures.
- **[Master refresh changes a universe unexpectedly] ->** Persist/report the governed snapshot and compare representative counts before enabling the new path.

## Migration Plan

1. Inventory normalization call sites and expected formats.
2. Add identity/calendar contract tests with existing aliases and fixtures.
3. Implement the structured parser and compatibility adapters without changing callers.
4. Migrate API, master, quote, research, and backtest boundaries in vertical slices.
5. Route equity write/gap decisions through the authoritative calendar.
6. Remove duplicate suffix maps only after call-site coverage passes.
7. Rollback by retaining compatibility adapters and restoring the previous caller binding; no database migration is required.

## Open Questions

- Which existing callers intentionally accept ambiguous bare symbols, and can their API contracts be tightened?
- Should the identity type live only in `utils/code_utils.py` initially or move to a quote-domain package after adoption?
