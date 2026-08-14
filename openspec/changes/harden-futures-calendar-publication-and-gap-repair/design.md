## Context

The futures daily sync currently combines official calendar probing, governed target-date expansion, provider fetching, persistence, and scheduler reporting. A generic empty official daily payload can be stored as a closed-day decision even when the exchange has not published that day's settlement data or an anti-bot response has hidden the payload. The resulting calendar row can suppress later downloads, while the run still reports success because it only evaluates the older dates selected by that calendar.

The DCE gaps on 2026-08-12 and 2026-08-13 demonstrate that these stages need a shared publication-time and completion contract. The change must remain exchange-scoped, preserve explicit historical backfill ranges, and reuse the existing calendar, ingestion-run, task, and report models.

## Goals / Non-Goals

**Goals:**

- Distinguish positive trading evidence, explicit closure evidence, and unresolved empty responses.
- Resolve the latest expected trading date using each exchange's publication cutoff in its configured timezone.
- Re-probe a bounded recent window so later positive evidence repairs weak or incorrect calendar rows.
- Detect exchange-level recent price gaps after fetching and prevent stale runs from reporting success.
- Expose enough per-exchange dates and blockers for an operator to understand what was targeted and what remains missing.

**Non-Goals:**

- Building a general-purpose calendar or data-quality platform.
- Inferring holidays from empty market-data payloads or weekday-only guesses.
- Changing explicit `start/end` historical backfill boundaries.
- Requiring full contract-by-contract completeness auditing beyond the task's existing resolved scope and lifecycle rules.
- Replacing the existing DCE proxy and anti-bot recovery path.

## Decisions

### 1. Use an evidence hierarchy instead of treating all no-data responses alike

Parseable official contract rows are positive proof that a date traded and SHALL override an earlier weak closed-day classification. A date can be classified as closed only from deterministic weekend policy or date-specific official closure evidence, such as an official holiday or temporary-closure notice. A successful HTTP response with no contract rows, a generic no-data message, an anti-bot response, a parse failure, or a pre-publication response remains unresolved.

Existing rows whose reason is derived only from an empty payload, including `official_empty_payload`, are weak evidence. When encountered in the rolling repair window they are re-probed and can be replaced by positive official trading evidence without manual database edits. Explicit, date-specific closure evidence is not weakened by this migration rule.

Alternative considered: keep empty responses as closed days and distinguish only by request time. This still misclassifies upstream filtering and anti-bot responses, so publication timing alone is insufficient.

### 2. Resolve a publication-aware as-of date per exchange

Each exchange configuration carries a local timezone and daily publication cutoff. At run time the service computes a publication-eligible as-of date: before the cutoff, the current local date is not overdue; at or after the cutoff, it is eligible for completeness checking. Governance then selects the latest verified trading date on or before that as-of date. On weekends and verified holidays this naturally resolves to the preceding trading date.

The configuration has an explicit value for SHFE, INE, DCE, CZCE, and GFEX rather than one implicit global time. Tests use an injected clock. Operators can adjust a cutoff when an exchange changes its publication schedule without changing code.

Alternative considered: always target the local calendar date. That makes daytime manual runs falsely fail before settlement data is expected and was the source of ambiguous DCE results.

### 3. Daily maintenance repairs a bounded recent window

The default daily path re-probes the most recent five natural dates for each selected exchange. The lookback is configurable but constrained to three through five natural dates. The repair set includes missing calendar rows, weak empty-payload rows, unresolved dates, and governed trading dates without persisted price coverage. Explicit operator `start/end` requests retain their requested inclusive range and are not silently widened.

The window is deliberately small: it covers delayed publication and short anti-bot incidents without turning every daily run into a historical backfill. Later positive evidence is authoritative for trading status and causes the repaired date to re-enter target-date expansion.

Alternative considered: probe only the current target date. A bad calendar decision then permanently hides that date from later syncs.

### 4. Gate success on governed target coverage, not provider call completion

After provider processing, the sync builds an exchange-level completeness result from the resolved task scope and lifecycle rules. It records the requested range, governed target dates, expected latest trading date, actual latest persisted price date, repaired dates, missing target dates, and governance/provider blockers. A target date is covered when persisted bars exist for the task's resolved exchange/date scope; dates excluded by existing listing, delisting, or lifecycle policy remain explicit skips rather than gaps.

The overall run is `success` only when every selected exchange has no governance blocker, no required missing target date, and actual coverage through its expected latest date. A run that completes useful work but leaves one or more exchanges stale or gapped is `partial`. A run stopped before provider work by calendar governance remains `blocked`, while provider exceptions retain the existing failure semantics. Dry-runs that encounter governance or completeness blockers retain a non-success status instead of converting the warning into success.

Alternative considered: compare only the maximum stored date. The maximum catches the DCE incident but can miss an internal recent-date gap, so the gate checks the bounded target-date set as well.

### 5. Carry one diagnostic contract through service and scheduler reports

The calendar repair, target-date resolver, sync result, persisted run metadata, and Telegram formatter share the same exchange-level fields rather than recomputing status in the presentation layer. The scheduler derives task status from the data-task result and reports each exchange's target and freshness fields. This keeps manual `/run` and scheduled daily behavior consistent.

## Risks / Trade-offs

- [Exchange publication time changes] -> Keep cutoffs configurable per exchange and log the cutoff used in each run.
- [Official closure response is structurally ambiguous] -> Leave the date unresolved and block or partially complete the run instead of guessing.
- [A five-day outage exceeds the rolling window] -> Preserve explicit `start/end` backfill for operator recovery and report the oldest remaining gap.
- [Coverage defined at exchange/date level does not prove every contract row is present] -> Keep this change focused on the observed missing-date failure; contract-level completeness can be specified separately if a real gap requires it.
- [Repair changes a previously persisted weak closed row] -> Preserve source outcome and ingestion audit metadata so the correction remains explainable.

## Migration Plan

1. Add exchange publication-cutoff and repair-lookback configuration with backward-compatible defaults.
2. Deploy the evidence classifier and positive-evidence overwrite rule before enabling the freshness gate.
3. Run the bounded repair path for recent dates, which corrects weak empty-payload rows opportunistically.
4. Enable exchange-level completeness status and updated scheduler reporting.
5. Validate with a DCE case where an empty response is followed by official rows, plus pre-cutoff and post-cutoff runs.

Rollback disables the new repair/freshness configuration and restores the previous task path; corrected positive trading rows remain valid and do not require reversal.

## Open Questions

None. Exact initial cutoff clock values will be taken from the current exchange publication behavior and captured in configuration and tests during implementation.
