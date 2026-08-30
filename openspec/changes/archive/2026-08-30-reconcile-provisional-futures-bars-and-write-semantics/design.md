## Context

The futures daily sync can be invoked before an exchange has published final settlement data. Run 289 stored 2026-08-14 INE and SHFE rows at approximately 10:08 with `quality_flag=partial`, null close/settlement values, and official lineage. The current 21:30 scheduled job re-fetches the publication-eligible date and the upsert replaces changed payloads, but completeness currently checks only whether any persisted row exists for the exchange/date. A failed post-cutoff fetch can therefore leave an earlier provisional row present without explicit persisted proof that the final publication was verified.

The write summary also reflects physical keys. When run 292 replaced 2026-08-10 DCE AkShare fallback rows with official rows, the storage path emitted a fallback delete marker plus an official insert, and the operator report showed 23 generic writes. That is correct storage accounting but ambiguous business reporting.

## Goals / Non-Goals

**Goals:**

- Preserve useful pre-cutoff rows while making their provisional status explicit.
- Require post-cutoff verification or finalization before publication-eligible coverage can satisfy success.
- Ensure the 21:30 daily run reconciles same-day provisional rows without manual deletion.
- Distinguish genuinely new date coverage from source upgrades, same-source corrections, and verified unchanged rows.
- Keep existing aggregate counters and storage keys compatible.

**Non-Goals:**

- Deleting all intraday futures observations or forbidding manual pre-cutoff diagnostics.
- Building a general temporal-quality framework for every market-data domain.
- Changing exchange publication cutoffs or the five-natural-day repair window.
- Performing contract-by-contract completeness governance beyond the current resolved scope.

## Decisions

### 1. Persist publication finality on the existing bar metadata

Each normalized series bar and contract bar carries acquisition time, exchange publication cutoff context, and a publication state of `provisional` or `final`. A pre-cutoff same-day row is provisional even when it came from an official endpoint. A post-cutoff successful fetch records final verification time and ingestion run id.

If a post-cutoff payload is semantically identical to the provisional row, the storage path may update only finalization metadata while counting the price values as unchanged. This is represented as `post_cutoff_verified_unchanged`, not as a price correction or generic insert.

Alternative considered: infer finality forever from `created_at` and the current clock. That cannot prove a later official request succeeded and would let stale rows pass after a failed nightly request.

### 2. Reconcile rather than delete provisional rows

The daily rolling window includes publication-eligible dates represented only by provisional rows. A successful final fetch updates or replaces those rows through the existing source-priority path. Until that succeeds, the provisional rows remain readable and auditable but do not satisfy final completeness after cutoff.

Alternative considered: delete all 2026-08-14 INE/SHFE rows before the nightly run. Deletion discards useful evidence and creates an avoidable availability gap; it also does not solve future pre-cutoff runs.

### 3. Gate success on final coverage for publication-eligible dates

Exchange completeness keeps date-presence coverage but adds provisional and final-verification diagnostics. Before cutoff, the current local date is not required and provisional rows do not block the run. At or after cutoff, the expected latest trading date must have acceptable finalized coverage or a successful current post-cutoff verification. If the provider fails and only an old provisional row remains, that exchange is partial or blocked even though `actual_latest_price_date` reaches the date.

Alternative considered: rely on maximum persisted date. This reproduces the exact false-success risk raised by the pre-cutoff INE/SHFE rows.

### 4. Compute business write semantics before source supersession

Before deleting a superseded fallback row or upserting the preferred row, storage checks whether the series/date/mode already has any business observation and records the transition:

- `new_business_date`: no prior observation exists for that series/date/mode;
- `source_upgrade`: a lower-priority source is replaced by a preferred source;
- `same_source_correction`: the same storage key receives changed semantic values;
- `post_cutoff_verified_unchanged`: values match but final verification is newly established;
- `unchanged`: values and finality evidence require no write.

Unique affected trade dates are reported separately from row counts. Existing `inserted`, `changed`, `unchanged`, and changelog counters remain unchanged for compatibility.

### 5. Carry the same reconciliation contract into operator reports

The sync result, ingestion metadata, scheduler report, and manual `/run` report use the same per-exchange counters and provisional-date fields. Presentation code does not infer source upgrades from physical insert/delete counts.

## Risks / Trade-offs

- [A provider publishes final data later than the configured cutoff] -> Keep the cutoff configurable and retain a truthful partial result until a later rolling-window run finalizes the row.
- [Legacy rows lack publication metadata] -> Classify only recent same-day rows conservatively from quality, acquisition time, and cutoff; do not bulk rewrite historical data.
- [Metadata-only final verification adds a write for identical prices] -> Count it separately, avoid a semantic price-change record, and perform it only when finality actually advances.
- [Fallback data is available after official failure] -> Accept it as final only when existing source-quality policy permits it and required final fields are complete; otherwise retain it as provisional/degraded evidence.

## Migration Plan

1. Add backward-compatible publication-state metadata and semantic counters.
2. Treat recent `partial` same-day rows acquired before cutoff, including the current INE/SHFE 2026-08-14 rows, as provisional during completeness evaluation.
3. Enable post-cutoff reconciliation in the existing rolling daily target path and require finalized coverage for success.
4. Update scheduled/manual reports while retaining legacy aggregate counters.
5. Validate a pre-cutoff partial row followed by changed final data, identical final data, provider failure, and fallback-to-official source upgrade.

Rollback can disable the finality gate and semantic report fields without deleting stored bars. Rows already finalized remain valid.

## Open Questions

None.
