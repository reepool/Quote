## 1. Baseline And Ownership

- [ ] 1.1 Inventory every route, method/path, response model, status/error contract, dependency, and direct facade/store access.
- [ ] 1.2 Assign each endpoint to quotes, instruments/master, research, corporate-actions, backtest-data, or system/operations and record its owning service.
- [ ] 1.3 Add route-resolution and response-contract fixtures for representative reads and commands, including DCF and backtest vintage reads.

## 2. Route Modules

- [ ] 2.1 Create domain route modules and move one low-risk read family with unchanged registration and response behavior.
- [ ] 2.2 Move research and DCF routes to narrow query/application services, preserving local-only and availability-date semantics.
- [ ] 2.3 Move quote, instrument/master, and corporate-action routes to W2/W3/W6 owners without duplicating command or state logic.
- [ ] 2.4 Move backtest-data and system/operations routes; assign `BacktestQuoteStore` and `FinancialVintageStore` behind the explicit backtest query boundary.

## 3. Assembly And Compatibility

- [ ] 3.1 Reduce `api/routes.py` to router assembly and documented compatibility imports; prohibit new domain logic there.
- [ ] 3.2 Verify authentication, dependency injection, method/path registration, response models, status codes, and error mappings.
- [ ] 3.3 Add a route test that rejects duplicate method/path registration and direct SQL/provider access from route modules.

## 4. Acceptance

- [ ] 4.1 Run API, research, quote, corporate-action, backtest, and system regression tests with representative response snapshots.
- [ ] 4.2 Perform no-write route resolution and compare the first natural request per migrated family before retiring the old binding.
- [ ] 4.3 Update API current documentation and record remaining compatibility imports and their deletion conditions.
- [ ] 4.4 Mark W9 complete in `framework_refactoring_program.md` only when every endpoint has one owner and `api/routes.py` contains no domain implementation.
