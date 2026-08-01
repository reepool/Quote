## Context

The promoted A-share canonical series is complete at the factor-row and instrument-status layers, but its operational metadata is stored as a full-market JSON document containing every source-selection decision. Quote reads parse that document per instrument and retain it in the per-instrument cache. Promotion is recorded in an ignored runtime manifest while tracked defaults still select the obsolete composite path. Full rebuilds and daily targeted merges also use different summary calculations, and no lifecycle exists for obsolete staging and endpoint-status snapshots.

BaoStock does not expose a corporate-action event ledger equivalent to CNInfo or TDX. Its `query_adjust_factor` output and the Sina tail are sparse cumulative-factor paths. The composite can therefore prove path validity and independent agreement, but cannot prove that every legal XDXR event was observed.

## Goals / Non-Goals

**Goals:**

- Keep adjusted quote reads bounded by one series row, one instrument status, the instrument's decisions, and its factor rows.
- Make the tracked production default agree with the promoted stable canonical version and surface invalid activation state.
- Persist compact version summaries and separately queryable selection decisions.
- Use one deterministic summary builder for full build, promotion, and targeted merge.
- Define BaoStock/Sina eligibility as factor-path integrity rather than event completeness.
- Add safe preview/confirmation semantics for retention and deprecation.
- Make tests and documentation independent of mutable production runtime files.

**Non-Goals:**

- Reconstruct BaoStock XDXR events that its API does not provide.
- Rewrite CNInfo, TDX, BaoStock, Sina, LLM, review, or promoted factor economics.
- Automatically delete current production evidence during rollout.
- Replace the existing SQLite storage engine or introduce a new dependency.

## Decisions

### Store decisions separately and keep status summaries compact

Add a canonical decision table keyed by series version, instrument, and segment. Each row stores indexed routing fields and one bounded decision payload. `adjustment_factor_series_status.report_json` retains aggregate counts, bounded samples, promotion metadata, and incremental history, but not the unbounded `decisions` collection.

Alternative considered: cache the existing report once globally. This reduces repeated parsing but leaves 49 MB API responses, duplicated reports, stale merges, and unbounded growth unresolved.

### Introduce lightweight and detailed status reads

Quote reads use scalar columns from `adjustment_factor_series_status` and query only decisions for the requested instrument. Quality APIs return compact status by default and expose paged decision details separately. Full report loading remains an explicitly named compatibility/audit operation during migration only.

### Use tracked canonical defaults plus a validated runtime override

The tracked configuration and template select `canonical / a_share_cninfo_primary_v1`. A valid runtime manifest can select the same stable canonical version or an explicit BaoStock/Sina rollback. An invalid manifest is surfaced as an availability error and must not silently redirect adjusted quote requests. A missing manifest uses the tracked canonical default.

This avoids introducing asynchronous database access into the synchronous configuration resolver and remains deployable through the existing configuration system.

### Define composite qualification as path integrity

Rename `legacy_complete` concepts to `composite_path_eligible`. Eligibility requires positive finite factors, a valid normalized cumulative chain, and bridgeable provider transitions. Diagnostics disclose first/last factor dates, upstream sources, normalization methods, and any invalid rows. The status explicitly says `event_completeness=not_asserted`.

The composite may corroborate CNInfo or TDX only when path-eligible. It is not described as an official event source, and its agreement does not overwrite either source table. Requiring listing-date or recent-event coverage was rejected because sparse factor series legitimately contain no row when no factor changes.

### Recompute summaries from normalized decisions and persisted states

A shared summarizer derives selection, confidence, agreement, conflict, coverage, and completeness fields. Full build, promotion, and targeted merge all call it after determining the complete merged decision set and persisted instrument statuses. `conflict_count` represents blocked decisions only; low-confidence and historical-single-source counts remain separate.

### Protect retention with preview and explicit confirmation

Retention identifies obsolete benchmark and staging versions by age/count while protecting the active stable version, versions referenced by the activation manifest, and the most recent validated staging candidates. Endpoint status compaction retains the latest row per instrument/source/profile and recent history. Apply requires `dry_run=false` and `confirm=true`; this change ships the mechanism but does not automatically prune the current database.

Endpoint status rows also carry interval evidence. Retention therefore preserves every non-dominated historical interval in addition to the latest row, and deletes an old row only when another accepted complete row fully covers its requested date range. Partial or indeterminate attempts never dominate complete historical evidence.

### Reduce orchestration risk at operational boundaries

Extract canonical status/decision persistence, summary construction, source qualification, and retention planning into focused modules. The larger historical workflow remains behavior-compatible; this change does not attempt a wholesale rewrite of all CNInfo/LLM code.

## Risks / Trade-offs

- [Existing promoted series has decisions only in JSON] -> Provide an idempotent migration that copies decisions first, verifies counts, then compacts the report; keep the original report unchanged on any mismatch.
- [Reads begin before an existing report is migrated] -> Use the embedded report only as a temporary read-compatibility fallback when no normalized rows exist; normalized rows remain the ordinary bounded path.
- [Tracked canonical default makes missing data visible] -> Preserve explicit operator rollback to `baostock_sina_composite` and return actionable availability metadata.
- [Retention could remove audit evidence] -> Default to dry-run, protect active/recent versions, require confirmation, and report exact row/byte candidates before deletion.
- [Composite qualification cannot prove event completeness] -> Disclose that limitation and use the source only as an independent factor-path corroboration signal.
- [Daily predecessor state can be temporarily unavailable] -> Advance the predecessor only through the exchange's actual completed trading session and only when quote persistence and factor refresh both succeed; otherwise defer canonical merge without discarding downloaded CNInfo observations.
- [The active canonical series is A-share-only] -> Route non-A-share and B-share stock reads to their maintained BaoStock/Sina composite path instead of treating absent A-share coverage as an error.

## Migration Plan

1. Create the decision table and lightweight database accessors.
2. Migrate the active stable and retained staging decisions transactionally; verify decision and instrument counts.
3. Switch quote and quality reads to lightweight status and per-instrument decisions.
4. Compact newly written reports immediately; compact existing reports only after successful decision migration.
5. Change tracked defaults to the stable canonical version and restart the application.
6. Run focused API, daily maintenance, selection, promotion, and migration tests.
7. Preview retention separately; do not apply it until the operator reviews the candidate list.

Rollback keeps normalized decisions and source evidence, selects `baostock_sina_composite` through a valid activation manifest, and does not require deleting canonical rows.

## Open Questions

- The first production retention execution remains an operator decision after preview; no automatic schedule is enabled by this change.
