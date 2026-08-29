## Why

`api/routes.py` is a 4,800-line mixed router containing quote, master, research, corporate-action, backtest, and system endpoints. Some research endpoints already delegate to services, while other routes construct stores or reach through global facades. Without an explicit API boundary, the framework program can complete while the public entry layer remains a second place for business orchestration.

## What Changes

- Split route definitions by stable business domain while preserving the existing public URL surface.
- Make each route module an HTTP adapter that validates input, invokes an owning query/command service, and maps the result to the existing response model.
- Assign the backtest store read path and system/operations routes explicit owners.
- Keep `api/routes.py` as a temporary router assembly and compatibility surface only.
- Add route ownership and response-equivalence checks before changing the application binding.

## Capabilities

### New Capabilities

- `api-route-adapter-boundaries`: Defines route ownership, adapter responsibilities, compatibility, and backtest read-path assignment.

### Modified Capabilities

None.

## Impact

- Affects `api/routes.py`, new domain route modules, API dependency wiring, response-contract tests, and current API documentation.
- Depends on W2 identity boundaries, W3 quote commands, W4 research services, and W6 corporate-action services; it does not require a new web framework or API version.
- Implements W9, FR-09, FR-11, and FR-17 without changing URLs, authentication, database schemas, or financial semantics.
