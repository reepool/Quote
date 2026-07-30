## Context

The local `600018.SH` master row uses the SSE security-code first-listing date
`2000-07-19`, while the current issuer, Shanghai International Port (Group),
lists `2006-10-26` after an absorption merger. The local daily quote series
starts on the current-issuer date even though pytdx and Tencent expose the
predecessor security-code history from 2000. This is a valid code-history
segment, but it is not a continuous issuer price series.

Existing gap diagnostics are inconsistent: the data manager can compare
stored coverage with `listed_date`, while the standalone repair script starts
from the first stored quote and therefore cannot find a leading gap. The
repair also needs explicit source arbitration because provider histories have
one missing date and one conflicting close.

The change must work with the existing SQLite schema, data-source adapters,
and `instrument_master_metadata` persistence. It must not overwrite existing
quotes, synthesize merger adjustment factors, or broaden a reviewed one-symbol
repair into an unrestricted historical crawl.

## Goals / Non-Goals

**Goals:**

- Represent reviewed A-share security-code lineage separately from the current
  issuer's listing date.
- Detect leading quote gaps from the governed code-history start date.
- Provide a dry-run-first, allowlisted, missing-only repair workflow.
- Reconcile pytdx with an independent AkShare/Tencent source and apply explicit
  per-date review decisions.
- Persist auditable lineage evidence and flag non-continuous issuer transitions.
- Repair and verify the approved `600018.SH` history without changing existing
  post-2006 rows.

**Non-Goals:**

- Automatically infer code lineage for every A-share security.
- Treat security-code history as a continuous issuer or total-return series.
- Derive an absorption-merger conversion or adjustment factor.
- Replace the canonical `instrument_id`, `listed_date`, or current issuer name.
- Repair dates or instruments that are not in the reviewed catalog.

## Decisions

### Use a versioned reviewed catalog

A JSON catalog under `config/` will hold typed entries for security-code
history start, issuer regimes, transition boundaries, source evidence, and
reviewed row decisions. A loader will validate dates, regime ordering, allowed
continuity values, and reviewed values before any network or database write.

This is preferred to hard-coding `600018.SH` in a script because the evidence
and decisions remain reviewable and testable. A new database table is not
introduced because the reviewed set is initially small and the existing
metadata table can persist the applied evidence.

### Keep canonical master fields stable

The SSE `listed_date=2000-07-19` remains the canonical security-code listing
date. The current issuer's `2006-10-26` listing date is recorded as an issuer
regime attribute in lineage metadata rather than replacing the canonical
field. This preserves existing instrument identity and makes the semantic
difference explicit.

### Reconcile sources before constructing a repair plan

The repair workflow will normalize pytdx and AkShare/Tencent rows to the local
daily quote contract, compare common dates and OHLC values, and then apply only
catalogued review decisions. Unreviewed conflicts fail closed and appear in
the report. A date absent from the primary source may be filled by the
independent source only when the catalog explicitly approves it.

For `600018.SH`, the approved decisions are:

- use the AkShare/Tencent row on `2001-08-16`;
- use the AkShare/Tencent row on `2003-07-16`, including close `13.71`,
  independently confirmed by Sohu history;
- use the pytdx row on `2003-11-17`, including close `12.95`.

### Insert only missing rows

The database write set is the reviewed source result minus existing local
dates. Existing rows are never updated by this repair. The command defaults to
dry-run and requires an explicit apply flag for writes. This makes reruns
idempotent and protects local history from provider revisions.

### Persist metadata through the existing master metadata repository

After a successful apply, the workflow stores the reviewed catalog version,
issuer regimes, source evidence, applied decisions, inserted-date range, and
transition policy in `instrument_master_metadata.metadata_json`. Metadata
writes occur only after quote insertion succeeds.

### Flag the issuer transition as non-continuous

The `2006-10-26` transition is recorded with `price_continuity:
non_continuous`. Audit output must identify the last predecessor quote and the
first current-issuer quote. No synthetic factor is added to
`adjustment_factors_tdx`; downstream raw-return analysis must split or exclude
the boundary.

### Detect leading gaps from governed history start

The lineage auditor compares the catalogued security-code history start with
the earliest local quote. It reports the leading range separately from
interior missing traded dates, provider-only dates, suspension rows, source
conflicts, and transition boundaries. This avoids classifying every exchange
calendar day as a missing quote.

## Risks / Trade-offs

- [Provider history may later change] -> Persist source names, catalog version,
  comparison diagnostics, and reviewed decisions; never overwrite existing
  rows automatically.
- [Security-code history may be mistaken for issuer continuity] -> Store issuer
  regimes and a fail-visible non-continuous boundary; do not create a merger
  factor.
- [pytdx availability may depend on a live server] -> Keep the command
  dry-run-first, expose fetch failures, and allow normalized fixtures in tests.
- [Tencent/Eastmoney anti-bot behavior may interrupt AkShare] -> Run the
  existing `akshare_proxy_patch` context for the independent-source fetch.
- [Metadata and quotes could diverge on partial failure] -> Write metadata only
  after quote insertion and verify the persisted date coverage before reporting
  success.
- [A reviewed catalog can become stale] -> Validate its schema and require an
  explicit version and evidence timestamps.

## Migration Plan

1. Add the reviewed catalog, loader, auditor, and focused unit tests.
2. Add the dry-run-first repair command and run it for `600018.SH`.
3. Confirm the plan contains the predecessor history, the approved
   `2001-08-16` row, and close `12.95` on `2003-11-17`.
4. Apply the missing-only repair to the production quote database.
5. Verify row counts, earliest/latest dates, duplicate absence, source counts,
   existing post-2006 row stability, and persisted transition metadata.
6. Roll back, if required, only by deleting rows identified by the recorded
   repair source/run evidence and restoring the prior metadata payload from a
   database backup. No automatic destructive rollback command is provided.

## Open Questions

- An authoritative absorption-merger conversion ratio and economically valid
  continuous-price treatment remain unresolved. Until supported by official
  evidence, the boundary stays non-continuous.
- Other A-share symbols with code reuse or issuer transitions require separate
  evidence and review before entering the catalog.
