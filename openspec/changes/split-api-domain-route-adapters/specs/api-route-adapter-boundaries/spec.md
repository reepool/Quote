## ADDED Requirements

### Requirement: Every endpoint has one domain route owner
Each existing API endpoint SHALL be assigned to exactly one domain route module and one owning query or command service.

#### Scenario: Research endpoint is migrated
- **WHEN** the endpoint is moved from `api/routes.py`
- **THEN** the domain route module invokes the owning service and the assembly module registers no second implementation for the same method/path

### Requirement: Routes remain protocol adapters
Route modules MUST limit themselves to HTTP parsing, authentication/dependency resolution, service invocation, and response/error mapping; they MUST NOT own SQL, provider fallback, write loops, or domain decisions.

#### Scenario: Quote update request is handled
- **WHEN** an API client submits a maintenance command
- **THEN** the route constructs the application command and does not implement quote selection, calendar, persistence, or retry logic

### Requirement: Public API behavior remains compatible
The migration SHALL preserve existing URL paths, methods, status codes, response fields, authentication behavior, and error semantics unless a separate API requirement changes them.

#### Scenario: Existing DCF endpoint is called
- **WHEN** a client calls the endpoint before and after route migration
- **THEN** the normalized response and status/error contract remain equivalent

### Requirement: Backtest read ownership is explicit
Backtest route reads using `BacktestQuoteStore` or `FinancialVintageStore` SHALL be assigned to a named backtest query boundary and MUST NOT become an untracked parallel path merely because they bypass `DataManager`.

#### Scenario: Backtest data endpoint is migrated
- **WHEN** the route module requests vintage financial or quote data
- **THEN** it calls the backtest query boundary, which preserves current local-only and vintage semantics

### Requirement: Route migration is reversible and single-bound
Before a route family is rebound, the change MUST verify route resolution, run representative no-write requests, and ensure only one implementation is registered for each method/path.

#### Scenario: Domain route module is enabled
- **WHEN** the first natural request reaches the new module
- **THEN** response snapshots and errors are compared with the baseline and the old binding remains available only as a documented rollback choice

### Requirement: Core route file growth stops
After a route family is migrated, new endpoints for that family MUST be added to its domain route module and not to `api/routes.py` except for documented compatibility assembly.

#### Scenario: New corporate-action endpoint is added during migration
- **WHEN** an owning application service exists
- **THEN** the endpoint is implemented in the corporate-action route module without adding domain logic to the assembly file
