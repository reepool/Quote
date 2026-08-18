## Context

Research already has query, read, sync, and domain services, but many callers still depend on the global `DataManager`. The facade also contains orchestration for industry, shareholders, valuation, financials, futures, FX, commodities, and business-profile workflows. New code therefore tends to add another DataManager method instead of using a narrow capability.

## Goals / Non-Goals

**Goals:**

- Make existing research services the direct application owners of use cases.
- Migrate by vertical domain slices with stable read/write behavior.
- Keep DataManager compatibility while eliminating its business logic incrementally.
- Ensure API and scheduler code depend on narrow interfaces.

**Non-Goals:**

- Merging research databases or introducing a generic domain framework.
- Rewriting domain calculations, providers, or business-profile pipelines in one change.
- Removing DataManager before all callers and scripts migrate.

## Decisions

1. **Add a small research application boundary only where needed.** Existing `research/query_service.py` and domain services remain the first implementation owners; new application modules coordinate them rather than wrapping every method forever.
2. **Migrate read paths first.** Read services have lower write risk and can prove dependency direction before sync commands move.
3. **Use one domain per slice.** Industry, shareholders, valuation, financials, and market data are migrated independently with their own contracts and tests.
4. **Use narrow protocols/constructor dependencies.** Services depend on the repository/provider capabilities they need, not `DataManager` or the entire storage manager.
5. **Keep DataManager delegates explicit.** Every delegate records replacement service and caller migration status; no new domain method is added to the facade.

Alternatives rejected: a single `ResearchService` would recreate the same god object; a big-bang package move would make rollback difficult; changing all public APIs first would expand scope without reducing internal coupling.

## Risks / Trade-offs

- **[Hidden callers depend on DataManager side effects] ->** Trace calls and compare structured results/side effects per slice before rebinding.
- **[Research operations have different availability-date semantics] ->** Preserve domain-specific commands and test report/knowledge cutoffs.
- **[Import cycles appear during migration] ->** Keep adapters outward and services inward; inject dependencies instead of importing global facades.
- **[Partial migration leaves confusing paths] ->** Maintain the program matrix and remove each delegate once its caller count reaches zero.

## Migration Plan

1. Inventory DataManager methods, callers, storage dependencies, and existing research services.
2. Define read/query contracts and migrate representative API endpoints.
3. Migrate one write/sync domain at a time with command, service, repository, adapter, and tests.
4. Update scheduler/scripts to call the service directly.
5. Remove migrated business blocks and retain only documented delegates.
6. Rollback by restoring adapter bindings to the existing facade; no public or database contract changes are required.

## Open Questions

- Which domain should be the first non-read vertical slice after quote maintenance: industry or shareholders?
- Which current DataManager side effects are intentionally part of an API contract and need explicit result fields?
