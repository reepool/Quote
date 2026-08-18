## ADDED Requirements

### Requirement: Research use cases have narrow application owners
Each migrated research query or synchronization use case SHALL have one application/read service owner with an explicit input and result contract.

#### Scenario: Industry readiness is requested
- **WHEN** an API or operator adapter requests industry readiness
- **THEN** it calls the industry read service directly and does not implement the projection in DataManager or the adapter

### Requirement: Services depend on narrow capabilities
Research application services SHALL depend only on the repository, provider, and domain capabilities required by their use case and MUST NOT import the global DataManager facade.

#### Scenario: Shareholder sync is constructed
- **WHEN** the shareholder application service is initialized
- **THEN** its dependencies expose shareholder storage/provider contracts rather than the entire application container

### Requirement: Migration proceeds by vertical domain slice
Each migration slice SHALL include the application owner, required storage/provider bindings, external adapters, compatibility delegate, and acceptance tests for one stable domain boundary.

#### Scenario: A domain slice is proposed
- **WHEN** only a facade wrapper is added and no caller or business logic is migrated
- **THEN** the slice is not considered complete

### Requirement: Local research reads remain local
Read services SHALL preserve existing local-only behavior and MUST NOT introduce implicit network access when serving API, DCF, or research queries.

#### Scenario: Local valuation input is missing
- **WHEN** a read service cannot find the requested local data
- **THEN** it returns the existing missing/not-ready result and does not call an upstream provider implicitly

### Requirement: DataManager compatibility is temporary and logic-free
Retained DataManager methods SHALL only normalize backward-compatible arguments and delegate to the owning service, with a recorded replacement and removal condition.

#### Scenario: Last caller migrates
- **WHEN** repository search and tests show no remaining caller of a compatibility method
- **THEN** the method is removed in the current or final cleanup workstream

### Requirement: Research behavior remains equivalent
Service extraction SHALL preserve response schemas, error semantics, availability-date rules, database writes, watermarks, and domain calculations unless a separate business requirement changes them.

#### Scenario: Financial query is migrated
- **WHEN** representative instruments and report periods are queried before and after migration
- **THEN** normalized results and availability semantics are equivalent

### Requirement: Core facade growth stops
New research capabilities MUST NOT add domain orchestration to `data_manager.py` or `api/routes.py` when a domain service or route module can own the behavior.

#### Scenario: New research endpoint is added during migration
- **WHEN** the endpoint requires an existing research use case
- **THEN** it depends on the owning read/application service and adds no duplicate DataManager implementation
