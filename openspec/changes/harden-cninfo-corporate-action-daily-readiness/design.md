## Context

The scheduled task runs at 03:30 Asia/Shanghai on Tuesday through Saturday. It
currently uses one date range for announcement discovery, structured endpoint
refresh, and factor evidence. A seven-day rolling range plus three-day overlap
causes both market announcement scans to exhaust `60 * 30` rows. Because an
incomplete scan cannot commit its cursor and every matched stock announcement is
treated as a refresh trigger, each run repeats the same irrelevant disclosures and
keeps a large deferred queue.

The factor stage also uses the run's calendar date even before that trading session
has opened. Its readiness summary unions CNInfo pending events, TDX reference-path
pending events, endpoint coverage, conflicts, and source-only events. These are
different operational meanings and must remain separate.

## Goals / Non-Goals

**Goals:**

- Cover all announcements since the required prior-day boundary, including
  overnight announcements and long non-trading intervals.
- Advance a conservative shared announcement cursor and retain only relevant
  deferred work.
- Avoid structured CNInfo refreshes caused by unrelated disclosure activity.
- Restrict factor evidence to completed, locally available market sessions.
- Report CNInfo readiness independently from TDX and reconciliation diagnostics.
- Preserve strict full-market canonical promotion quality gates.

**Non-Goals:**

- Use LLM classification in the scheduled daily task.
- Download or parse announcement documents.
- Change CNInfo or TDX economic values.
- Promote or overwrite production adjustment factors.
- Resolve independent TDX historical defects as part of CNInfo daily freshness.

## Decisions

### 1. Resolve announcement and factor windows independently

The task records an Asia/Shanghai `run_at` timestamp. Announcement discovery uses
date-bounded provider queries plus an in-memory timestamp upper bound:

- `calendar_daily`: previous natural day 00:00 through `run_at`;
- `trading_day`: previous completed SSE/SZSE trading day 00:00 through `run_at`.

The trading-day boundary is strictly earlier than the run date. It therefore spans
weekends and exchange holidays without relying on a fixed number of calendar days.
The provider query ends on the run date so announcements published after midnight
are included.

The configured Tuesday-to-Saturday 03:30 schedule also uses a two-calendar-day
query overlap. This covers announcements published after Saturday's run or on
Sunday before the next Tuesday run. The committed provider cursor prevents this
overlap from repeatedly scanning completed history.

The structured CNInfo endpoint keeps its existing rolling date range. Factor
rebuilding instead caps its end date at the minimum of every currently tradable
stock's latest complete local quote date across the requested A-share exchanges,
never later than the requested end date. Suspended stocks do not hold back the
market-completion cutoff; their unresolved effective-session evidence remains in
the factor retry queue. Stocks first listed after the candidate completed session
are excluded from that earlier-session coverage denominator because they cannot
have a prior quote. Quote rows are bounded at the prior completed session, and the
denominator uses the latest bounded quote status rather than the A-share master
`trading_status`, which upstream master loaders do not maintain intraday. Stocks
whose exchange names have entered the `退` exit stage are also excluded. If any
requested exchange has no complete local quote coverage, factor rebuilding is
deferred rather than falling back to the unopened requested date.

Alternative considered: use yesterday for every schedule. This would miss a long
holiday interval when the job only runs after trading sessions.

### 2. Apply a deterministic daily title trigger

The existing non-implementation title prefilter is reused first. A surviving title
must also contain a corporate-action subject marker and an implementation-grade
marker. Ordinary annual/interim reports, board materials, legal opinions, pledges,
guarantees, repurchases, and unrelated share-listing notices do not select an
instrument.

Positive subjects cover dividends, distributions, bonus/capitalization shares,
rights issues, share reform, restructuring capitalization, compensation shares,
debt settlement conversions, repurchase/treasury-share cancellation, and capital
reduction. Implementation
markers cover implementation, completion, payment, registration, arrival,
ex-right/ex-dividend, resumption, issuance, listing, execution, and completion.

Retry-indeterminate, recent-event, prior deferred, explicit, and rotating safety
candidates remain independent of the title gate. This preserves recovery from
silent structured-source corrections and unfamiliar announcement wording.

Alternative considered: LLM title classification. It adds availability, cost, and
latency coupling to a scheduled source refresh and is unnecessary for a high-recall
deterministic trigger.

### 3. Complete the cursor catch-up instead of accepting a permanent degraded loop

The scan scope remains stable by market. Its provider page bound is raised to a
bounded catch-up value suitable for the prior-session window. A cursor is committed
only when the provider declares the scan complete. Once established, subsequent
runs stop at the prior cursor with overlap.

The in-memory `run_at` upper bound also applies to cursor progress. If a provider
returns a later-dated announcement during a complete scan, the record is deferred
and the temporal cursor is capped at `run_at` without moving backward behind an
existing committed cursor. A subsequent run can therefore reconsider the record
instead of losing it behind a future provider watermark.

Only title-relevant instruments enter `pending_candidate_ids`. If a complete scan
still produces more relevant instruments than the configured candidate limit, the
task remains operationally `partial` and drains that real queue by priority. An
incomplete provider scan also remains `partial`.

Existing scan state predates the title-policy version and may already contain
deferred instruments behind its committed cursor. Those candidates are drained
once during migration instead of being discarded. Newly deferred work is then
stored under the current policy.

An affected event newer than the common quote cutoff is stored in
`pending_factor_instrument_ids`. The queue is included in the next targeted rebuild
and is cleared only after the cutoff catches up and the instrument no longer has a
pending factor path. Queue persistence uses the rebuild's complete pending
instrument sets, not bounded diagnostic samples, and includes newly pending as
well as previously carried instruments.

The retry set is also persisted in corporate-action instrument status independently
of announcement scan state. This preserves retries for BSE-only and explicit
instrument runs, where no market announcement scan context exists, while scoped
updates leave retries from other exchanges or instruments untouched. A retry-queue
read failure is distinct from an empty queue: the run remains partial and skips the
replacement write so transient database errors cannot clear persisted retries.

### 4. Use source-separated readiness without weakening canonical gates

The factor rebuild exposes:

- `cninfo`: CNInfo pending events, historical gaps, endpoint gaps, and affected
  instruments;
- `tdx_reference`: independent TDX pending events and instruments;
- `reconciliation`: conflicts and source-only audit counts.

The legacy combined completeness and strict quality gates remain available for
manual full-market canonical evaluation. The daily report labels each source
separately and does not call TDX or reconciliation differences CNInfo historical
unreadiness.

### 5. Enforce the CNInfo-supported-exchange boundary in the factor path

CNInfo observations and endpoint requirements are evaluated only for SSE and SZSE.
BSE remains in the TDX refresh and reference audit but cannot create a CNInfo
pending event or CNInfo readiness failure.

## Risks / Trade-offs

- [A novel relevant title misses deterministic markers] -> Retain recent-event and
  rotating safety paths, log filtered/title-trigger counts, and keep manual
  discovery available.
- [A disclosure burst still exceeds the provider bound] -> Preserve the prior
  cursor, return operational `partial`, and retry without falsely claiming success.
- [Provider timestamps lead the task clock] -> Defer later-dated records and cap
  temporal cursor progress at the run timestamp.
- [One requested exchange quote feed is stale] -> Use a conservative common factor
  cutoff, persist cutoff-deferred rebuild candidates, and report per-exchange
  latest quote dates.
- [Historical TDX defects remain visible] -> Report them under `tdx_reference`
  without blocking CNInfo freshness or modifying TDX data.
- [Existing benchmark consumers depend on combined completeness] -> Preserve the
  current combined fields and add source-separated fields rather than removing
  them.

## Migration Plan

1. Add pure window and title-trigger helpers with unit tests.
2. Wire explicit schedule mode and completed-quote cutoff into daily maintenance.
3. Add source-separated factor completeness and report fields.
4. Raise the bounded announcement catch-up page limit and deploy with
   `build_canonical=false`.
5. Observe one scheduled run. The initial run may perform a larger complete cursor
   catch-up; later runs should scan only new/overlap announcements.
6. Roll back the orchestration/config commit if source volume is unexpectedly high;
   no production factor table or source values are migrated.

## Open Questions

None. The configured task uses `trading_day`; `calendar_daily` remains available for
a future 365-day schedule.
