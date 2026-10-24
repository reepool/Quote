# Design: classify-suspended-stock-empty-quotes-legitimate

## Context

Incident timeline (2026-10-24 investigation):

1. `000016.SZ` has been suspended since 2026-09-04. Its suspended-day rows
   (`tradestatus=0`, `volume=0`) had been served exclusively by `pytdx` — the other
   configured sources do not return rows for non-trading days.
2. From the 2026-10-23 evening run onward, TDX hosts return no bars at all for the
   security (not even history). `data_sources/tdx_source.py` already anticipates this:
   "pytdx 对退市股票也可能合法返回空". This is permanent server-side retirement, not
   flakiness; verified again on 2026-10-24 with all four sources and an official retry.
3. The empty result was classified `empty_unresolved`, incremented
   `stock_failure_count` for SZSE, and `_record_a_share_quote_composite_watermark`
   wrote `partial` (failure reason `SZSE:quote_update_failed`), freezing
   `a_share_quote_baostock_sina:SZSE` at `2026-10-22`.
4. The 03:30 corporate-action daily job requires both SSE and SZSE watermarks at the
   factor cutoff, so the canonical adjustment-factor merge is blocked nightly with
   `predecessor_watermark_stale` until the stock either resumes trading (delisting
   review period) or is removed from the exchange list and deactivated — a window of
   potentially weeks.

Key existing code anchors:

- Empty-result classification: `data_manager.py` `update_daily_data`, branch
  `if not prepared_data:` — `legitimate_no_quote` only when master
  `trading_status not in (None, 1)`; otherwise `empty_unresolved` plus
  `_adjust_daily_instrument_failure_counts` and retry candidacy.
- Watermark block predicate: `data_manager.py
  _quote_exchange_blocks_stock_watermark` — blocks on run error, changelog write
  failures, or `stock_failure_count > 0`. Index misses deliberately do not block.
- Coverage semantics: `database/operations.py
  get_latest_stock_quote_dates_by_exchange` — instruments whose latest observed quote
  has `tradestatus=0` are excluded from the per-exchange coverage minimum, and
  eligibility already filters `trading_status == 1` plus `name` not ending in `退`.
- Master semantics: `data_sources/a_share_official_stock_master.py` hardcodes
  `status='active', is_active=True, trading_status=1` for every row of the official
  current-list; delisting is only reflected after list removal plus a `退`-marked name
  (`_deactivate_official_absent_delisting_prefixed_stocks`).

The design goal is to make failure counting agree with coverage semantics: a stock
whose last observed state is suspended legitimately has no quote for a new day, and
that fact must not block exchange readiness.

## Goals / Non-Goals

Goals:

- Stop a suspended A-share stock's empty daily result from blocking the per-exchange
  stock watermark and the downstream canonical merge.
- Base the exemption on observed quote evidence (`tradestatus` of the latest persisted
  row), which exists today, instead of instrument-master fields that the official-list
  source cannot provide.
- Keep the change inside the existing daily-update classification branch; no new
  tables, services, or owners.
- Make the exemption visible in the operator report.

Non-goals (each independently justifiable, none required to unblock the incident):

- No consumption of delisting announcements into master lifecycle state (East
  Money-style early marking). Even if added later, suspended days before any
  announcement still need this exemption.
- No synthesis/forward-fill of suspended-day rows. The watermark does not need them
  (coverage excludes suspended-latest instruments), and fabricated rows would pollute
  price history.
- No HKEX/US behavior change: their masters have different semantics and no stock
  readiness watermark depends on them today.
- No TDX host-pool changes.

## Decisions

### D1: Evidence = latest persisted quote row's `tradestatus == 0`

Alternatives considered:

- Master `trading_status`: rejected — the official-list source hardcodes 1; making the
  master suspension-aware is the separate announcement-consumption project.
- A live per-stock suspension lookup (exchange API): rejected — adds a network
  dependency and a new failure mode inside the hottest loop.
- Recent-N-days suspension persistence (e.g. "suspended for ≥3 consecutive days"):
  rejected — it re-introduces exactly this incident for the first days of any
  suspension and adds an arbitrary parameter. A single latest-row observation is the
  same evidence the coverage query already trusts.

Implementation: a small helper on `DataManager`,
`_is_latest_quote_suspended(instrument_id, latest_quote_date)`, that reuses the
existing `db_ops.get_daily_quote(instrument_id, latest_quote_date)` getter (same
pattern as the `existing_target` lookup in the same loop) and returns True only when
the row exists and its `tradestatus` is `0`.

Known limitation, accepted: the evidence is the instrument's globally latest row, not
the row at the fetch-window end. For historical-window backfills of a stock that was
suspended then but trades now, the latest row says trading and the exemption would not
apply. This is benign in practice: sources do serve historical suspended-day rows for
live securities (BaoStock/TDX history), so an empty result there implies the
retired-security case — and a retired security's latest row is its final suspended
row, which the rule correctly reads.

### D2: Fail-closed on missing evidence

If `latest_quote_date` is None (no history at all), the getter is unavailable, or the
lookup raises/returns None, the instrument keeps today's failure classification. A
newly listed stock with no data must remain visible as a failure; an evidence outage
must not silently exempt real failures.

### D3: Only "nothing usable" qualifies

The exemption requires `rejected_count == 0`. If sources returned rows and all were
quality-rejected, that is a data-quality problem on a live response, not a suspension,
and must stay a failure. Window/stale filtering already happens upstream in the source
factory, so an empty `data` with zero rejections means the sources genuinely had
nothing usable.

### D4: A-share stocks only

Gated by the existing `is_a_stock` flag (`exchange in SSE/SZSE/BSE`) and
`instrument type == stock`. The blocked watermark is an A-share contract; HKEX/US
classifications are unchanged.

### D5: No time bound on evidence staleness

The latest suspended row ages while TDX refuses to serve the security (000016's last
row is 2026-10-22 and will not advance). Any staleness bound would re-freeze the
watermark after N days — recreating the incident. The terminal state is owned by the
existing master deactivation path (list removal + `退` name), which takes the
instrument out of the fetch universe entirely.

### D6: Exempted results skip retry candidacy

The `empty_unresolved_retries` append and the official retry pass must not run for
exempted instruments: retries cannot succeed against a permanently retired security
and only waste source quota (observed: `attempted=1, recovered=0` on 2026-10-24).

### D7: Watermark consistency without fabricated rows

With the exemption, `stock_failure_count` is 0 on a night whose only empty results are
suspended stocks; `_quote_exchange_blocks_stock_watermark` then does not block, and
the persisted-coverage check already passes because the coverage query excludes
suspended-latest instruments. No `factor_status_missing` regression is expected: the
factor-sync collection point in `update_daily_data` runs for every iterated stock
regardless of `should_update`, so factor stats are present on backfill reruns too
(verified against the 2026-10-23/24 watermark metadata, where the only failure reason
was `quote_update_failed`).

### D8: Operator visibility

`utils/report/engine.py` integrity line gains a `停牌无行情` count fed by
`legitimate_no_quote`. Two details matter:

- The section's early-return condition (skip when no unresolved empties, no samples,
  no retries) must also consider `legitimate_no_quote > 0`, otherwise a clean night
  with only exemptions would render nothing and hide the exemption.
- Exempted instruments are NOT appended to the shared integrity `samples` list: the
  report renders that list as anomalies. Exemptions are visible as the aggregate
  count plus one info-level log line per exempted instrument.

Without this, an exempted instrument silently disappears from `未覆盖` and operators
cannot distinguish "fixed" from "gone".

### D9: Relationship to existing governance patterns (no parallel mechanism)

Two adjacent mechanisms were checked for overlap so this change does not become a
special case or a duplicate:

- Index lifecycle governance filters the index universe BEFORE source requests,
  because official publisher announcements give a pre-fetch authority signal. Stock
  suspension has no pre-fetch authority signal today (the official-list master
  hardcodes `trading_status=1`), so post-fetch classification is the evidence-backed
  equivalent, not a divergent pattern. If announcement-driven stock lifecycle ever
  lands (the deferred 方案 B), it can pre-filter exactly like indexes and this
  classification remains the fallback for unannounced suspensions.
- `security_state_current_observations` / `security_state_events` (backtest rollout,
  currently disabled) are a stored mirror of lifecycle state. This change deliberately
  does NOT create a second stored state: it reads the primary quote evidence inline
  and stores nothing new. If the security-state rollout is ever enabled, the
  classification rule can migrate onto it without contract change (the spec speaks of
  observed trading state, not of a specific lookup).

The exemption also reuses the existing `legitimate_no_quote` counter rather than
introducing a new aggregate, and the report renders a single count without splitting
by exemption source (master-status vs quote-evidence): A-share masters always carry
`trading_status=1`, so in practice only the evidence path fires; a split counter would
be speculative generality. Per-instrument detail stays in the info log.

Downstream consistency was verified rather than assumed: `stock_failure_count` has no
consumer outside the watermark writer; the task-level `is_successful` / `no_op`
derivation in the scheduler wrapper already handles the exempted rerun correctly
(failure_count 0 → `no_op`/`success`); and the gap-repair ecosystem already tolerates
permanently missing rows via `gap_skip_list`, so leaving suspended days absent adds no
new chronic-failure loop.

## Risks / Trade-offs

- A stock that resumed trading exactly on the fetch day while every source
  simultaneously returns nothing for it alone would be wrongly exempted. Assessment:
  the last persisted row is the best available evidence at fetch time; a resumed
  stock is normally served by all sources; a broad source outage fails many trading
  stocks too, so `stock_failure_count > 0` still blocks the watermark. The residual
  window is a single-instrument false negative, recoverable by gap repair.
- The helper adds one DB read per empty result. Empty results are rare (0–5 per
  night; the 2026-10-23 run had exactly one stock plus three index misses).
- Report-only change; no storage or API compatibility impact.

## Migration Plan

Single slice, deployable independently: classification + helper + report line +
tests. After deployment, the standard repair runbook for the live incident is:
`/run daily_data_update 2026-10-23` (SZSE watermark advances to 2026-10-23), then
`/run a_share_cninfo_corporate_action_daily_sync` (canonical merge proceeds). No data
backfill, no schema migration, no config flag.

## Open Questions

None. D1–D8 are decided; the announcement-consumption enhancement is deliberately
out of scope.
