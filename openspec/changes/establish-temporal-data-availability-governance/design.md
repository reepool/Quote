## Context

Special-commodity governance currently treats an observation as eligible after a fixed number of calendar days. That approximation does not represent sources such as the National Bureau of Statistics (NBS), whose ten-day commodity observations are normally published on specific monthly release days and can be cancelled or rescheduled. It can therefore classify a legally not-yet-due observation as missing. Commodity consumers also select rows by `observation_date` alone, so a historical valuation can see a value that was published after the valuation cutoff.

The repository already has source-specific calendar governance for FX publication days and futures trading days. Those services remain authoritative for their domains, but they do not expose one reusable contract for observation time, planned release time, actual publication time, local first-seen time, evidence quality, and point-in-time eligibility. This change introduces that contract and applies it first to special commodities without changing existing public APIs or replacing FX/futures storage.

## Goals / Non-Goals

**Goals:**

- Define a reusable, timezone-aware temporal availability model and deterministic lifecycle evaluation.
- Separate release planning from source fetching so adapters receive periods that are actually due.
- Model NBS ten-day observations using the 4th, 14th, and 24th release convention, configurable release time, grace period, and evidenced exceptions.
- Extend `commodity_publication_calendar` additively and preserve existing databases and callers.
- Make special-commodity point-in-time reads and DCF context use governed `available_at` cutoffs.
- Preserve enough evidence and status detail to distinguish not due, grace, delay, cancellation, unresolved gap, and source failure.

**Non-Goals:**

- Replacing the existing FX publication-calendar or futures trading-calendar services.
- Migrating every research dataset to the shared contract in this change.
- Inventing release timestamps when neither source evidence nor a configured policy exists.
- Performing remote source requests from DCF or other valuation-time reads.
- Adding a new third-party calendar or scheduling dependency.

## Decisions

### Use a small shared domain module

Add a research-layer module containing typed value objects and pure functions for release plans, availability evidence, lifecycle evaluation, and point-in-time eligibility. Source adapters provide domain evidence; the shared module does not know NBS URLs, SQLite, schedulers, or DCF.

This keeps status rules consistent and unit-testable while avoiding a central service that owns unrelated source behavior. Keeping the logic embedded in the commodity adapter was rejected because future datasets would repeat the same cutoff and status semantics.

### Make timestamps timezone-aware and persist ISO-8601 values

Expected release, grace deadline, actual publication, first-seen, and effective availability are timezone-aware datetimes. Persistence uses ISO-8601 strings with offsets. Date-only source evidence is interpreted using a configured source timezone and conservative release time, and its evidence quality remains explicit.

Naive datetimes are rejected instead of being silently interpreted in the host timezone. UTC-only persistence was considered, but retaining the offset improves operational diagnosis while comparisons still normalize through aware datetime semantics.

### Derive lifecycle status from plan, evidence, and evaluation time

The shared evaluator produces stable statuses: `not_due`, `due_in_grace`, `available`, `delayed_available`, `cancelled`, `rescheduled`, `unresolved_gap`, and `source_failure`. A planned release is not a claim that data exists. `available_at` is set only from actual publication evidence or local first-seen evidence according to a declared quality policy.

One generic success/failure flag was rejected because it cannot distinguish an expected wait from an actionable data-quality incident.

### Implement NBS release planning as source policy

NBS ten-day periods retain their source observation ends (10th, 20th, and 30th, with February ending on its actual final day). Their normal planned release dates are the next 14th, 24th, and following-month 4th respectively, at a configurable Asia/Shanghai time. Configured exceptions can cancel or reschedule an individual observation period only when reason and evidence URL are present.

The existing fixed `publication_lag_days` rule is retained only as a compatibility fallback for sources without a release policy. It is no longer used for the NBS ten-day series.

### Extend commodity calendar storage additively

Add nullable columns for observation-period bounds, expected release, grace deadline, actual publication, first seen, available at, availability quality, release status, and evidence URL. Initialization inspects the existing table and adds missing columns. Existing rows remain readable and retain their legacy `status`, `quality_flag`, and metadata.

Rebuilding the table was rejected because it adds migration and rollback risk without benefit. Existing rows without availability evidence are not backfilled from observation dates because doing so would create false point-in-time precision.

### Reconcile fetched observations with release plans

Calendar governance creates or updates planned rows before fetching. Fetched source evidence then reconciles those rows and computes `available_at`. A due period with no official article after grace becomes `unresolved_gap`; a provider transport/business rejection becomes `source_failure`; an evidenced cancellation is non-actionable.

This separation prevents providers from deciding whether a missing response is normal and lets reports aggregate temporal states consistently.

### Fail closed for point-in-time use

Commodity storage reads accept an optional `available_at_lte` cutoff and join the publication calendar on series and observation date. With a cutoff, rows without governed `available_at` are excluded. DCF commodity context uses the valuation timestamp cutoff and reports an availability gap when no eligible observation remains.

Falling back to `observation_date` for historical valuation was rejected because it preserves the data-leakage path. Current, non-PIT operational reads remain backward compatible when no cutoff is supplied.

## Risks / Trade-offs

- [Historical calendar rows lack actual publication evidence] -> Leave `available_at` null, expose the quality gap, and require an explicit evidence backfill before PIT use.
- [A configured release time differs from the source's actual posting time] -> Use the plan only for due-state evaluation; use actual publication or first-seen evidence for `available_at`.
- [NBS changes or suspends its convention] -> Keep release rules and exceptions configuration-driven and preserve evidence URLs.
- [Joining calendar rows changes query cost] -> Apply the join only for PIT reads and index `available_at` with the calendar scope.
- [Status naming diverges from FX/futures services] -> Keep compatibility adapters outside this change and document mapping boundaries before later migration.

## Migration Plan

1. Deploy the shared temporal module and unit tests.
2. Initialize commodity storage so missing calendar columns and indexes are added transactionally.
3. Enable the NBS release policy and reconcile new runs into the extended fields while retaining legacy fields.
4. Enable PIT filtering for commodity DCF reads; rows without governed availability fail closed.
5. Backfill historical availability only from source documents or recorded first-seen evidence in a separate operational run.

Rollback disables the NBS policy and PIT caller changes. Additive columns can remain unused; no destructive schema rollback is required.

## Open Questions

- Whether additional commodity sources provide trustworthy intraday publication timestamps or only publication dates; each adapter must declare its evidence quality.
- Whether a later change should map FX and futures calendar statuses into the shared vocabulary without changing their domain-specific tables.
