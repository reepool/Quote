## Why

Research query, synchronization, and market-data services already exist, but `DataManager` still owns or wraps large amounts of cross-domain orchestration and read projection. New API and operator features therefore continue to depend on a global facade instead of narrow business capabilities.

## What Changes

- Make existing query/read/sync services the direct owners of research use cases.
- Extract remaining DataManager logic as vertical slices, beginning with local read queries and then industry, shareholders, valuation, financials, futures, FX, and special commodities.
- Keep DataManager methods only as temporary compatibility delegates with recorded callers and removal conditions.
- Make new API and scheduler adapters depend on narrow application services.
- Preserve local-only read behavior, response schemas, error semantics, databases, and current synchronization results.
- Require every slice to remove real facade logic rather than add another wrapper layer.

## Capabilities

### New Capabilities

- `research-application-service-boundaries`: Defines ownership, dependency direction, vertical-slice migration, facade compatibility, and local read guarantees for research use cases.

### Modified Capabilities

None.

## Impact

- Affects `data_manager.py`, `research/query_service.py`, domain read/sync services, API research routes, scheduler callers, and tests.
- Depends on W2 identity boundaries; prepares W5 repository and W7 scheduler changes.
- Implements W4, FR-06, FR-09, and FR-11 without changing research business calculations.
