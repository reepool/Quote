## 1. Baseline And Ownership Map

- [ ] 1.1 Confirm W2 identity boundaries and record DataManager research methods, direct callers, existing services, databases, providers, and side effects.
- [ ] 1.2 Group methods into local read, industry, shareholder, valuation, financial, futures, FX, special commodity, and business-profile slices.
- [ ] 1.3 Define compatibility result snapshots for representative API, DCF, scheduler, and operator calls.
- [ ] 1.4 Add a dependency-direction test that rejects new research imports of the global DataManager from application/domain modules.

## 2. Local Read Slice

- [ ] 2.1 Make existing query/read services the direct owners of company, financial, industry, valuation, futures, FX, and commodity reads.
- [ ] 2.2 Migrate representative API routes to injected narrow read services while preserving local-only and response behavior.
- [ ] 2.3 Convert corresponding DataManager read methods to compatibility delegates and remove duplicate projections.

## 3. Research Sync Slices

- [ ] 3.1 Migrate the industry application slice, including command, current domain service, storage/provider dependencies, adapters, and tests.
- [ ] 3.2 Migrate the shareholder application slice with equivalent availability, incremental, and reconciliation semantics.
- [ ] 3.3 Migrate valuation and financial application slices without changing calculation, report-period, or available-date semantics.
- [ ] 3.4 Migrate futures, FX, and special-commodity facade orchestration to their existing domain services.
- [ ] 3.5 Defer business-profile/announcement slices that overlap an active change and record the exact dependency instead of partial migration.

## 4. Callers And Cleanup

- [ ] 4.1 Rebind scheduler, scripts, API, and DCF consumers to narrow services for completed slices.
- [ ] 4.2 Remove migrated DataManager business blocks and annotate remaining delegates with caller/removal metadata.
- [ ] 4.3 Split migrated research routes by domain when doing so removes direct facade dependencies rather than only moving code.

## 5. Acceptance

- [ ] 5.1 Run research query, domain sync, availability-date, DCF integration, API, and scheduler regression suites for migrated slices.
- [ ] 5.2 Compare representative result schemas, database writes, business keys, watermarks, and warnings against the baseline.
- [ ] 5.3 Verify no new production path introduces implicit network reads or duplicate writers.
- [ ] 5.4 Update current research architecture and mark completed/deferred W4 slices in the framework program.
