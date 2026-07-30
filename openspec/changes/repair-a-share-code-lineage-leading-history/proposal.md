## Why

`600018.SH` exposes a broader A-share data-governance defect: the official SSE
master reports the security-code listing date as 2000-07-19, while current-
issuer sources report 2006-10-26 and the local quote series starts only on the
latter date. Leading quote gaps, issuer transitions, and code-lineage history
must be governed explicitly so the platform neither drops valid history nor
creates a false continuous return across a legal-entity transition.

## What Changes

- Add governed A-share code-lineage metadata that distinguishes the security
  code's first listing date, issuer regimes, transition dates, and continuity
  policy.
- Add leading-history gap detection from the governed code-history start
  instead of only checking dates between the first and last stored quote.
- Add a dry-run-first, allowlisted repair path that compares pytdx with an
  independent AkShare/Tencent history source, inserts only reviewed missing
  rows, and reports source conflicts without overwriting existing quotes.
- Add a reviewed `600018.SH` lineage entry and repair decision:
  - accept the independently confirmed 2001-08-16 Tencent/Eastmoney row;
  - use the Tencent 2003-07-16 close of 13.71, independently confirmed by
    Sohu history, instead of the pytdx close of 13.70;
  - retain the pytdx 2003-11-17 close of 12.95, independently confirmed by
    Eastmoney;
  - mark the 2006 absorption-merger boundary as non-continuous for raw-return
    analysis.
- Persist lineage evidence with instrument-master metadata and provide an
  audit report that separates missing traded rows, suspension dates, source
  conflicts, and issuer-transition boundaries.
- Add focused tests for leading gaps, reviewed conflict resolution,
  idempotent missing-only writes, and transition-boundary reporting.

## Capabilities

### New Capabilities

- `a-share-code-lineage-history`: Governs A-share security-code history,
  issuer regimes, leading quote-gap detection, multi-source repair decisions,
  and non-continuous transition reporting.

### Modified Capabilities

- `instrument-master-governance`: Distinguishes a security code's official
  first-listing date from current-issuer listing dates and persists reviewed
  lineage evidence without replacing the canonical instrument ID.

## Impact

- Affected code:
  - A new code-lineage loader/auditor under `data_sources/` or `database/`.
  - A dry-run-first targeted repair command under `scripts/`.
  - Instrument-master metadata persistence and quote-gap diagnostics.
- Affected configuration:
  - A versioned reviewed lineage catalog under `config/`.
- Affected storage:
  - Missing raw rows in `daily_quotes` for explicitly approved instruments.
  - Reviewed lineage evidence in `instrument_master_metadata.metadata_json`.
- No public API or existing quote schema is removed or changed.
- No synthetic merger adjustment factor is introduced without authoritative
  conversion evidence.
