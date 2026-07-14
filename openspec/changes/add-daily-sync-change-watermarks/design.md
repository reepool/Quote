## Context

The platform currently protects daily sync correctness with overlap-window refetches, catch-up windows, weekly/monthly reconciliation, gap repair, and domain-specific governance. This is necessary because free/public upstream sources generally do not publish reliable CDC streams. However, local consumers still need an efficient way to know which locally stored business rows changed since their last sync.

This design implements the standalone requirements document `docs/development/incremental_sync_change_watermarks_requirements.md`. That document is the source of truth for domain scope, business impact, and review criteria; this design explains the engineering approach.

The highest-risk current gap is `daily_quotes`: the table has `updated_at`, but the write path performs conflict-update even when values are identical. Using `updated_at` directly as a watermark would therefore turn every overlap-window refetch into false CDC. Other domains already have better building blocks: futures, FX, special commodities, shareholders, financial facts, and industry syncs use `raw_payload_hash`, `row_hash`, `ingestion_run_id`, `data_as_of`, or inserted/changed/unchanged counters.

Target flow:

```text
provider fetch
    |
normalize business row
    |
compute semantic row hash
    |
compare with existing row hash
    |
insert/change only? ---- no ----> count unchanged, no changelog
    |
   yes
    |
upsert row + increment row_version
    |
append change_log sequence
    |
API/sync clients read changes after sequence
```

## Goals / Non-Goals

**Goals:**

- Provide a local, append-only watermark stream for all daily/scheduled sync domains.
- Avoid false positives from normal overlap-window refetches.
- Keep existing API defaults, scheduler timing, governance behavior, and research read paths stable.
- Reuse existing hashes, ingestion runs, and write-result counters wherever available.
- Make adjustment-factor changes visible as separate changes because they affect adjusted quote output.
- Let downstream clients re-fetch by business key after a watermark rather than re-downloading full windows.

**Non-Goals:**

- Do not claim complete upstream CDC for free sources that do not publish official revision feeds.
- Do not remove overlap-window daily sync, reconciliation, gap repair, trading-calendar governance, or master-governance tasks.
- Do not rewrite all historical rows during initial rollout except for optional, bounded hash backfill jobs.
- Do not change existing default read API outputs or make policy-discovery events affect market-data consumers by default.
- Do not automatically cascade recomputation of every derived dataset in this change; record invalidation signals first.

## Decisions

### Decision 1: Use semantic row hashes before watermarks

Each write path computes a canonical hash over business fields that define data meaning. Metadata such as `updated_at`, ingestion run id, retry count, and batch id MUST NOT be part of the hash.

Alternatives considered:

- `updated_at` watermark only: rejected because current overlap upserts would produce false positives.
- Full raw payload hash only: useful where available, but some providers return unstable raw payload ordering or extra fields; canonical normalized business hashes are safer for cross-provider semantics.

### Decision 2: Maintain append-only change records separate from source tables

Each participating database gets a changelog table, or a shared registry table where practical, with a monotonic integer `sequence_id`. A change record stores domain, dataset, business key, observation date or period, change type, old/new hashes, row version, source, batch/run id, and changed timestamp.

Minimum record envelope:

```text
sequence_id, domain, dataset, change_type,
business_key_json, instrument_id, series_id,
observation_date, period,
old_hash, new_hash, row_version,
source, source_mode, source_profile,
ingestion_run_id, batch_id, changed_at
```

Fields that do not apply to a domain stay nullable. `business_key_json` remains the canonical lossless key, while top-level `instrument_id`, `series_id`, and date columns exist for indexing and common filters.

Alternatives considered:

- Add version-only columns to every table and query each source table directly: lower storage cost but hard for API clients to consume across domains.
- Database triggers: attractive for uniformity, but SQLite trigger logic would be brittle for semantic hashing and multi-database research storage.

### Decision 3: Keep domain-specific business keys

The changelog uses a shared envelope but preserves domain keys:

- quotes: `instrument_id`, `trade_date`, optional `adjustment_scope`
- adjustment factors: `instrument_id`, `ex_date`
- futures: `series_id` or `contract_id`, `trade_date`, `source_mode`
- FX: `series_id`, `observation_date`, `revision_id`
- commodities: `series_id`, `observation_date` or `period`
- financials: `instrument_id`, `report_period`, `fact_name` or artifact id
- industry: `instrument_id`, taxonomy version, effective date range
- valuation/technical/risk: `instrument_id`, `as_of_date`, `calc_version`, `parameter_hash`
- policy discovery: adapter id, document/event id, publication/effective date

Alternatives considered:

- Force every domain into `(instrument_id, date)`: rejected because FX, futures contracts, monthly benchmarks, financial facts, industry membership, and policy events need richer keys.

### Decision 4: Expose read-only surfaces, not push-based sync

Initial API consumers query latest watermark and changes after a watermark. Existing data endpoints remain the source for full row retrieval.

Initial read contract:

```text
GET /api/v1/changes/latest?domain=quotes
GET /api/v1/changes?domain=quotes&since_sequence=12345&limit=1000
GET /api/v1/quotes/daily/changes?since_sequence=12345&exchange=SSE
```

The final route shape may be adjusted during implementation, but it must support domain/dataset filters, stable `sequence_id ASC` pagination, `latest_sequence`, and continuation metadata. Changelog endpoints return business keys and metadata, not full source rows.

Alternatives considered:

- Webhooks or streaming CDC: rejected for this local-first SQLite platform and current operational model.
- Returning full changed rows from the changelog endpoint: deferred because large rows and domain-specific schemas would complicate pagination and compatibility.

### Decision 5: Treat derived-data invalidation as explicit metadata

When a raw quote, factor, financial fact, industry membership, or valuation input changes, derived datasets may be stale. This change records source watermarks/input hashes on derived writes and can emit derived-row changes when recomputed, but it does not automatically recompute all downstream datasets.

Alternatives considered:

- Immediate cascade recompute from every raw change: rejected due to runtime, lock contention, and risk of changing scheduler behavior.

## Risks / Trade-offs

- **False negatives from unobserved upstream revisions** -> Keep overlap windows and reconciliation jobs; document that changelog is local-observed CDC.
- **False positives from unstable hashes** -> Define per-domain canonical hash fields and tests; exclude non-semantic metadata.
- **Large changelog growth** -> Add indexes, retention/export policy, and optional compaction summaries after stable rollout; never prune before consumers can persist watermarks.
- **SQLite write contention** -> Write changelog records in the same short transaction as source row changes where practical; keep batch sizes bounded.
- **Adjusted quote ambiguity** -> Emit separate factor changes and document that `adjust=none` raw quote changes differ from qfq/hfq invalidation.
- **Derived-data fan-out** -> Start with invalidation visibility and recompute reporting; avoid automatic cascade until source domains are stable.
- **Multi-database consistency** -> Use per-database sequence ids and include `database_id`/`domain`; do not promise global total ordering across all databases in phase one.
- **Policy discovery noise** -> Isolate policy/document domains so market-data clients only receive them when they explicitly filter for those domains.

## Migration Plan

1. Add storage DDL for changelog tables and missing hash/version columns without destructive rewrites.
2. Introduce a small shared change-recording helper that can be used by SQLite/SQLAlchemy and research storage paths.
3. Start with quote database daily quotes and adjustment factors because they currently have the highest false-positive risk and broadest API consumption.
4. Adapt existing hash-aware futures, FX, and special commodity write paths to emit changelog records.
5. Adapt research domains by reusing existing `ingestion_run_id`, payload hashes, row hashes, and derived input hashes.
6. Add read-only API surfaces and docs after at least quote/factor writes emit reliable records.
7. Update scheduler reports to include changelog counters and unchanged counts.
8. Provide optional bounded hash backfill for existing rows, marked as an operational task and not required for normal daily sync.

Phase gates:

- P0 is complete only when quote/factor repeat writes do not advance watermarks, material quote/factor changes do advance watermarks, and existing `/api/v1/quotes/daily` defaults are unchanged.
- P1 is complete only when futures, FX, and commodity dry runs remain non-persistent and changed/unchanged counters agree with changelog rows.
- P2 is complete only when research read APIs remain backward compatible and financial/industry/valuation lineage preserves point-in-time semantics.
- P3 is complete only when governance and policy domains are isolated from market-data consumers by default.

Rollback strategy:

- Keep changelog writes additive and non-blocking behind configuration during rollout.
- Existing source tables and APIs remain authoritative; disabling changelog emission restores prior behavior without data loss.
- If a domain emits noisy records, disable that domain's changelog registration while keeping other domains active.

## P1 Implementation Notes

Futures, FX, and special commodity storage use `research/change_watermarks.py` as a small shared SQLite adapter. It creates the same `data_change_log` envelope in each participating database and appends records through the caller's existing source-row transaction.

- `futures.db` owns independent sequence ordering for `futures_contract_price_bars`, `futures_price_bars`, commodity observations, and commodity policy/evidence datasets.
- `fx.db` owns independent sequence ordering for direct and derived `fx_observations`.
- Participating source tables receive additive `row_version` columns. Existing read payload decoders remove this internal field so default API/service response shapes remain unchanged.
- Existing stable `raw_payload_hash` values remain source lineage. FX compares a normalized semantic hash that also covers value, currencies, multiplier, publication time, quality flag, revision id, and governed input lineage; `ingestion_run_id` and other operational metadata remain excluded.
- Futures and commodity unchanged writes return counters without updating source rows or appending changelog records.
- Official futures bars that supersede fallback rows emit one `delete_marker` per removed fallback key in the same transaction as the official write.
- Commodity price records use the `commodity` domain. Policy events, source documents, candidates, and review actions use the `policy` domain so price consumers cannot receive policy evidence accidentally. Source-document semantic comparison includes title, document number, publication date, content hash, content type, and parser version while excluding retrieval timestamps.
- `config/11_futures.json` and `config/12_fx.json` provide domain/dataset rollout switches. Disabling emission does not disable source-table writes.
- P1 does not extend the quote-database `/api/v1/changes*` routes across databases. Cross-database aggregation remains deferred and must not imply a global transactional sequence.

## P2 Implementation Notes

Research storage initializes the shared changelog independently in research, financial, valuation, and interests databases. `config/10_research.json` controls P2 domain and dataset emission.

- Shareholder snapshots exclude operational `data_as_of`, manifest recheck state, and ingestion timestamps from semantic hashes.
- Financial core and numeric facts preserve report period, point-in-time availability, source file, canonical mapping, parser, and schema lineage. Parser repair performs preserve-and-diff replacement, so unchanged facts retain versions and truly removed facts emit `delete_marker`. Internal row metadata is removed from existing read payloads.
- Industry taxonomy and membership changes remain separate datasets. Strict rebuild preserves unchanged tracked rows, deactivates missing taxonomy nodes, and removes only stale memberships. Membership keys include taxonomy version and effective date; targeted stale membership removal emits `delete_marker`, and reinsertion continues the prior version sequence.
- Valuation inputs retain source availability semantics. Valuation history, technical, and risk outputs include calculation identity and stored input/details lineage in their semantic comparison.
- Risk-free-rate definitions and observations use the interests database; observation identity includes series, date, source profile, and revision.
- Read-only and diagnostics-only scheduler jobs declare `change_watermark.expected=false` in scheduler config.
- `scripts/backfill_change_watermark_hashes.py` provides bounded, dry-run-first hash backfill. It never emits changelog rows or increments row versions.
- P2 does not add cross-database research changelog routes. Existing `/api/v1/research/*` responses remain unchanged.

## Post-Implementation Review

The completion review added correctness guards beyond the original task checkboxes:

- Quote and adjustment-factor rollback paths clear persisted-write counters after transaction failure; duplicate quote keys are collapsed before the transaction and malformed rows are isolated.
- Risk-free-rate and other existing read surfaces continue to hide internal `row_hash`/`row_version` fields.
- Financial parser repair and industry strict rebuild use preserve-and-diff semantics instead of delete-and-reinsert behavior.
- Missing hash-backfill database paths fail without creating empty databases.
- FX revision metadata, futures source supersession, and policy-document metadata corrections now produce observable CDC signals without crossing domain boundaries.
- Focused futures CDC tests pass. The full futures test module still has seven failures in provider quality expectations and governed-calendar setup. The same seven failures were reproduced from clean baseline commit `fb58f4479d29b8a9f41c36ff89c1463821f86354`, confirming they are not introduced by this change. They are tracked as `FUT-QUALITY-001` and `FUT-CALENDAR-002` and must be resolved in separate OpenSpec changes rather than by weakening calendar gates here.

## Open Questions

- Should phase one expose one global API endpoint across databases, or separate endpoints per domain/database with a future aggregation layer?
- What retention policy is acceptable for detailed changelog rows once downstream consumers have checkpointed watermarks?
- Which derived outputs should be recomputed automatically in phase two versus only marked stale?
- Should adjustment-factor changes emit explicit affected raw trade-date ranges for qfq/hfq consumers, or should clients re-fetch the full instrument history when factor sequence changes?
- Should changelog write failure be non-blocking by default only during rollout, then promoted to fail-fast for domains that are proven stable?
