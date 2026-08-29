## Context

The API is a public entry point, not an application-service owner. `api/routes.py` currently mixes HTTP concerns with domain dispatch and, for backtest reads, directly constructs `BacktestQuoteStore` and `FinancialVintageStore`. The route split must reduce coupling without hiding a second query implementation.

## Goals / Non-Goals

**Goals:**

- Establish one route owner per endpoint family.
- Preserve URL paths, methods, status codes, response fields, authentication, and error semantics.
- Make backtest reads explicit query-service dependencies even when they do not use `DataManager`.
- Keep router assembly small and dependency direction visible.

**Non-Goals:**

- No API version migration or URL redesign.
- No rewrite of response models or domain calculations.
- No universal controller base or generated registry.

## Decisions

1. **Split by business domain.** Initial modules are quotes, instruments/master, research, corporate-actions, backtest-data, and system/operations.
2. **Use application services as owners.** A route may normalize HTTP input and map a result, but it may not contain SQL, provider fallback, persistence loops, or domain policy.
3. **Make backtest ownership explicit.** Existing `BacktestQuoteStore` and `FinancialVintageStore` remain their current storage implementations initially, behind a named backtest query boundary; no data-store merge is implied.
4. **Keep compatibility at assembly level.** Existing imports of `api.routes.router` continue to work while endpoint functions move to domain modules.
5. **Migrate in vertical slices.** Each route family moves with dependency wiring, response snapshots, and a rollback binding before the next family.

## Migration Plan

1. Inventory all endpoints, direct facade/store dependencies, response/status contracts, and external references.
2. Move one low-risk read family and prove route resolution and response equivalence.
3. Move research, quote/master, corporate-action, backtest, and operations families in bounded slices.
4. Reduce `api/routes.py` to router assembly and documented compatibility imports.
5. Roll back a family by restoring its router binding; never register old and new endpoint implementations for the same method/path.

Before each production binding, run no-write route resolution and verify the first natural request against the response baseline.
