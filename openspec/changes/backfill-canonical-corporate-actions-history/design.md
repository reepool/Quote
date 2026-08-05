## Context

The quote database contains the governed corporate-action evidence and related
resolution tables, but `canonical_corporate_action_revisions` and
`canonical_corporate_action_current` have no historical rows. The existing
`CanonicalCorporateActionProjector` already performs deterministic projection
without acquisition, but it is currently invoked only as a changed-event stage
of existing CNInfo/TDX workflows and has no standalone historical operator
workflow.

The first backfill must therefore reuse the existing projector and evidence
tables. It must not introduce another CNInfo or TDX downloader, and it must not
alter raw observations while populating the consumer projection.

## Goals / Non-Goals

**Goals:**

- Provide a bounded, manual-only historical projection entry point over the
  existing observation universe.
- Preserve deterministic event/revision identities, lineage, decision
  availability, readiness and blocker reasons.
- Support dry-run, explicit scopes, resumable batches and idempotent reruns.
- Validate the entire projection in a temporary database before production
  execution and report database-scoped watermark changes.
- Keep the existing canonical read API and current projection compatibility
  behavior.

**Non-Goals:**

- Downloading or reparsing CNInfo, TDX or any other provider data.
- Resolving blocked events by guessing an ex-date, economic term or coverage
  state.
- Enabling a new automatic full-market cron.
- Rewriting or deleting raw evidence, resolution decisions or factor tables.
- Adding BaoStock requests or changing BaoStock quota configuration.

## Decisions

### 1. Reuse the existing projector and store

The backfill task SHALL call `CanonicalCorporateActionProjector.project` and
`BacktestQuoteStore.append_canonical_action`. This keeps the existing semantic
hash, lineage and PIT selection rules in one implementation. A second bulk
projection implementation would risk divergent readiness decisions.

### 2. Use deterministic source-event batches

The operator task SHALL select current observations ordered by
`instrument_id, source_event_key, id`, apply optional instrument/event filters,
and split the selected identities into stable batches. The checkpoint stores
the request parameters, universe hash, completed batch identities and last
report. A changed source universe produces a new checkpoint identity instead of
silently reusing old progress.

### 3. Make dry-run and temporary-database validation mandatory

Dry-run SHALL execute projection and readiness calculation without calling any
write method. Before production execution, the operator runbook SHALL support a
temporary copy of `quotes.db`; validation compares considered, ready, blocked,
inserted and unchanged counts, raw-evidence counts, lineage presence, and
second-run idempotency.

### 4. Preserve blocked events as first-class output

The projector SHALL append blocked revisions with explicit blocker reasons when
the event is not ready, while `backtest_ready=0` prevents strict consumers from
using them. Missing effective dates, incomplete terms, conflicts and incomplete
coverage remain blocked. No blocked event is dropped merely to make the batch
appear successful.

### 5. Keep canonical changes database-scoped

Only material canonical revision changes SHALL append quote-domain change records
and advance the existing canonical watermark. Dry-runs and unchanged semantic
hashes SHALL not create changes. Reports SHALL include the database id,
checkpoint id, batch counters and latest watermark.

### 6. Expose a manual-only scheduler surface

The task SHALL be registered as a manual-only job so `/run` can execute the same
bounded workflow used by an SSH operator or test harness. It SHALL not receive a
cron trigger and SHALL inherit no unbounded current universe or provider scope.

## Risks / Trade-offs

- [Large local write] → Use stable batches, checkpoint after each successful
  batch, and require a temporary database run before production.
- [Evidence changes during a long run] → Freeze the selected current-observation
  universe in the checkpoint and start a new run when the universe hash changes.
- [Blocked rows mistaken for usable data] → Keep `backtest_ready=0`, expose
  blocker counts, and make strict API filters fail closed.
- [Partial production interruption] → Append-only revisions and idempotent
  semantic hashes make resume safe; do not roll back raw evidence.
- [Telegram/reporting delay] → The scheduler task result is authoritative and
  report delivery remains bounded by the existing task-report contract.

## Migration Plan

1. Add the manual-only task and its checkpoint/report contract with rollout
   disabled for automatic workflows.
2. Run unit tests and a dry-run against the current local database.
3. Copy `data/quotes.db` to a temporary path and execute the full projection;
   verify counts and idempotency.
4. Back up the production database, run the same scope in bounded write mode,
   and record the checkpoint and watermark.
5. Validate canonical API pages and representative historical events.
6. Leave the manual-only task available for repair and keep existing daily/weekly
   changed-only hooks unchanged.

## Open Questions

- Whether the production operator wants one full-market run or separate exchange
  batches for the initial write.
- The exact production backup destination and retention policy before the first
  canonical write.
