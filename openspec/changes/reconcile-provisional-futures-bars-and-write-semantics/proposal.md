## Why

Futures rows fetched before an exchange's publication cutoff can be valid intraday snapshots but are not final daily settlement data. Today those rows can remain in storage and satisfy a later date-presence completeness check, while source replacement is reported as a generic insert, making operators unable to distinguish a new trade date from a quality upgrade.

## What Changes

- Classify same-day rows obtained before the exchange publication cutoff as provisional and retain their acquisition timing and quality evidence.
- Require a post-cutoff daily run to re-fetch and finalize every publication-eligible target date that is represented only by provisional rows; stale provisional presence alone cannot satisfy success.
- Preserve provisional rows until a verified official replacement succeeds, rather than deleting useful data before the final publication is available.
- Split write outcomes into new business-date coverage, same-source corrections, source upgrades/replacements, and unchanged rows while retaining the existing aggregate counters for compatibility.
- Show provisional/finalized state, post-cutoff verification, source upgrades, and remaining stale provisional dates in scheduled and manual reports.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `futures-market-data`: Add provisional-to-final reconciliation, post-cutoff freshness requirements, and business-semantic write outcomes.
- `scheduler`: Require the nightly/manual report and final task status to preserve provisional reconciliation failures and expose source-upgrade versus new-date counts.

## Impact

- Affects futures bar metadata and upsert result accounting, exchange-level completeness checks, ingestion-run metadata, scheduler/manual orchestration, and Telegram formatting.
- Existing `inserted`, `changed`, and `unchanged` fields remain available; new semantic counters clarify their business meaning.
- Existing pre-cutoff INE/SHFE rows for 2026-08-14 remain in place and are expected to be overwritten or verified by the 21:30 post-cutoff run. No destructive cleanup is required before reconciliation.
