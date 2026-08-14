## Context

The incremental financial task uses the shared announcement acquisition service but creates category-free market scopes. CNInfo therefore returns every SSE and SZSE announcement, while local selectors discard most rows after download. Recent production runs exhausted 40 pages for both markets and persisted incomplete scans without adding a scan error, so the parent task could still return `success`.

The shared announcement layer already supports normalized categories and stable category-specific cursor scopes. Announcement assets and business-profile discovery use this path for annual reports. Live CNInfo evidence confirms that one semicolon-delimited category value covers first-quarter, semiannual, third-quarter, and annual reports for SSE and SZSE. BSE's CNInfo column returns the full NEEQ stream and does not honor that taxonomy, while BSE's official `companyAnnouncement.do` advanced-filter endpoint exposes verified periodic-report subtypes and supports an empty-company market query. The SSE exchange provider remains instrument-scoped, so the existing CNInfo-only market route is retained rather than introducing an ineligible fallback.

## Goals / Non-Goals

**Goals:**

- Make the normal financial discovery stream query only four periodic-report classes upstream for SSE and SZSE.
- Preserve local full-report classification and explicit periodic-report disclosure anomalies.
- Filter BSE through its verified official periodic-report taxonomy without changing other BSE announcement consumers.
- Ensure bounded or truncated discovery cannot be reported as complete.
- Read the actual final CNInfo page when provider page-count fields disagree.

**Non-Goals:**

- Migrating financial repair to a new storage model or announcement asset consumer.
- Changing report-period inference, financial fact mappings, repair-source priority, or Telegram commands.
- Building a generalized query-expression or multi-category framework beyond the existing category mapping.
- Treating generic suspension, price-triggered delisting, or market-cap delisting notices as financial report events.

## Decisions

### Add one normalized combined periodic-report category

Add `periodic_report` to the existing provider-owned category mapping. CNInfo maps it to the four existing category tokens joined with semicolons, SSE maps it to `DQBG/ALL`, SZSE maps it to the four periodic `bigCategoryId` values, and BSE maps it to the verified annual, semiannual, first-quarter, third-quarter, and correction subtype codes. Add `periodic_report_anomaly` for BSE's official expected-late-disclosure subtype. The financial task sets the combined category for every market; SSE/SZSE keep the CNInfo market route, while BSE uses a service-local provider override for `companyAnnouncement.do` and a service-local BSE route. Other BSE announcement consumers keep their configured recent-market endpoint.

Adding four separate scans was rejected because CNInfo accepts the combined value and one cursor stream is simpler and cheaper. Passing raw provider tokens from financial business logic was rejected because provider mappings already belong in `research.announcements.categories`.

### Keep local classification after upstream filtering

The provider category contains abstracts and other non-primary records. The existing financial selector remains on the main stream, including the explicit `子公司` and `进展` exclusions, so upstream classification narrows acquisition without becoming the final business classifier.

Removing local classification was rejected because provider categories do not express the project's full-report and correction semantics.

### Discover disclosure anomalies through narrow scopes

Normal SSE/SZSE runs add separate CNInfo market scopes for `披露` and `定期报告`; BSE uses its official expected-late-disclosure subtype. A dedicated anomaly selector requires an unambiguous periodic-report phrase plus a delay or trading-risk signal. Generic risk-only records are excluded. Each category or keyword is part of the stable scope key and therefore owns its own cursor.

Keeping the all-announcement stream for anomaly discovery was rejected because it recreates the current bottleneck. Dropping anomaly evidence entirely was rejected because delayed reports are an existing accepted-gap contract.

### Propagate scan completeness independently from provider errors

The financial scanner records an explicit error when a route result is absent, failed, or incomplete. `max_pages_exhausted`, `estimated_pages_exceed_bound`, and other incomplete stop reasons therefore produce a degraded/failed parent status even when the provider returned partial records and no transport error.

Treating partial rows as successful discovery was rejected because it advances operational state without proving the requested window was covered.

### Reconcile CNInfo page totals conservatively

CNInfo live responses can report `totalpages=7` for 218 rows at page size 30 while page 8 contains eight records. The provider derives `ceil(totalAnnouncement/pageSize)` and uses the maximum valid page estimate across reported and count-derived values. A short or empty page remains completion evidence.

Trusting `totalpages` alone was rejected because it demonstrably skips the final partial page. Always reading one extra page was rejected because record totals already provide a bounded, testable correction.

## Risks / Trade-offs

- [CNInfo category taxonomy changes] -> Keep provider mappings centralized and preserve SSE/SZSE fallback mappings plus local title validation.
- [Broad `披露` keyword returns unrelated records] -> Apply the strict anomaly selector before candidate construction; the stream remains bounded and independently checkpointed.
- [A relevant anomaly omits both discovery keywords] -> Weekly reconciliation remains the missing-period backstop; add a proven keyword only after observing a real miss.
- [First filtered run has no prior cursor] -> Accept one bounded lookback scan because category is intentionally part of the stable scope identity.
- [BSE advanced-filter endpoint changes] -> Keep the override local to financial discovery, retain provider diagnostics, and degrade without cursor advancement on malformed or incomplete responses.

## Migration Plan

1. Add category mappings and provider pagination tests.
2. Add main/anomaly scope construction and completeness propagation with financial-sync tests.
3. Run focused unit tests and a bounded live 14-day CNInfo comparison.
4. Deploy without database migration; new SSE/SZSE category scopes create new scan-state rows naturally.

Rollback removes the new scopes and mappings. Existing filtered cursor rows remain harmless because their scope keys no longer match the restored query.

## Open Questions

None.
