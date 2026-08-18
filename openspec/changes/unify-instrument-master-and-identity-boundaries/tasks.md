## 1. Baseline And Contracts

- [ ] 1.1 Confirm W1 documentation governance is complete and record the production scheduler, database-key, API-alias, and master-universe baseline.
- [ ] 1.2 Inventory instrument normalization functions and call sites across API, DataManager, providers, research, backtest, and scripts.
- [ ] 1.3 Build table-driven fixtures for SSE, SZSE, BSE, HKEX, US, index metadata-only, vendor aliases, and ambiguous bare symbols.
- [ ] 1.4 Inventory equity write/gap calendar decisions and classify authoritative versus heuristic call sites.

## 2. Canonical Identity

- [ ] 2.1 Implement the structured instrument identity and explicit storage/exchange/vendor renderers in the existing utility boundary.
- [ ] 2.2 Convert legacy conversion helpers into compatibility delegates and prove accepted valid inputs remain equivalent.
- [ ] 2.3 Migrate API and DataManager input boundaries to canonical parsing with explicit exchange context for bare symbols.
- [ ] 2.4 Migrate provider, research, announcement, and backtest call sites in bounded slices and remove duplicate suffix maps after each slice.

## 3. Master And Calendar Boundaries

- [ ] 3.1 Define the governed master-universe result consumed by equity maintenance commands and include snapshot/refresh diagnostics.
- [ ] 3.2 Route daily and historical quote commands through the governed master-universe boundary without changing lifecycle policy.
- [ ] 3.3 Implement an authoritative equity-calendar port backed by the current quote database/source boundary.
- [ ] 3.4 Replace heuristic equity write/gap decisions with fail-closed authoritative calendar reads while retaining non-writing diagnostics where useful.

## 4. Acceptance And Cutover

- [ ] 4.1 Compare canonical ids, universe membership, and calendar decisions across CLI/API/scheduler fixtures before rebinding production entry points.
- [ ] 4.2 Run instrument master, quote API, gap, backtest identity, and trading-calendar regression suites.
- [ ] 4.3 Remove zero-caller normalization/calendar duplicates and record any compatibility adapter that remains with its deletion condition.
- [ ] 4.4 Update current identity/master/calendar documentation and mark W2 complete in the framework program.
