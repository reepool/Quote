## Why

The financial disclosure incremental sync currently scans unfiltered CNInfo market announcements until SSE and SZSE exhaust their 40-page bounds, then applies title filters locally. This wastes requests, prevents cursor advancement, and can still report success even though discovery is incomplete.

## What Changes

- Push a normalized four-period-report category into SSE and SZSE announcement discovery so CNInfo market scans and compatible exchange consumers query first-quarter, semiannual, third-quarter, and annual reports upstream.
- Keep local full-report title classification to reject abstracts, subsidiary notices, progress notices, briefings, and other non-primary records returned inside provider categories.
- Separate periodic-report disclosure anomalies from the main report stream and only retain delayed or risk notices explicitly tied to a report period; generic suspension and delisting-risk notices no longer drive financial repair discovery.
- Route BSE financial discovery through the official advanced-filter endpoint with verified periodic-report subtypes instead of scanning the full NEEQ announcement column.
- Treat page-bound or otherwise incomplete announcement scans as degraded instead of allowing an empty error list to produce success.
- Reconcile CNInfo reported page totals with record-count-derived page totals so the final partial page is not skipped.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `financial-operations-scheduler`: Financial disclosure discovery becomes provider-filtered, bounded, and completeness-aware while retaining narrowly scoped disclosure-anomaly evidence.

## Impact

- Affects normalized announcement category mappings, CNInfo pagination interpretation, financial disclosure incremental scanning, the financial service's BSE provider override, and focused unit/live validation.
- The task command, financial database schema, candidate identity, report-period inference, repair routing, and downstream Telegram result fields remain compatible.
- Changing the category changes the stable scan scope key, so the first filtered run starts a new cursor stream without rewriting legacy scan state.
