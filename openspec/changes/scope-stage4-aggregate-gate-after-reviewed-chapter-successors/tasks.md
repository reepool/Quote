## 1. Scope review gate

- [ ] 1.1 Independent review accepts this aggregate scope: the judgment set contains only material-input `.2026-09-30.3` at 23/23 and 23/23, operating-quantity `.2026-10-01.4` at 38/38 and 38/38, segment-financial `.2026-10-01.4` at 137/137 and 144/144, and counterparties `.2026-09-29.1` at 45/45 and 45/45, each with critical numeric errors 0 inside its own plan; one shared four-report identity; per-chapter enqueue, run, result, and source-review hashes; and historical rows MI-1, MI-2, OQ-1, OQ-2, OQ-3, SF-1, SF-2, and SF-3 kept outside the set with their original metrics and critical errors. A later pass requires matching identities, four business passes, correct artifact bindings, and intact semantic and coverage boundaries. Any miss is hold. Do not apply that rule in this review. Do not check 2.1 or 3.1. Do not add chapter scores together, record aggregate `expansion_gates_met=true`, modify historical artifacts, change Python, or start restricted promotion, the six-chapter package, scale quality, or production

## 2. Apply the gate, not yet authorized

- [ ] 2.1 After 1.1, read the frozen rows and apply the pass／hold conditions. If every condition holds, record the research-scope aggregate pass for this contract only. If any condition fails, record hold. Do not add the chapter scores together, edit historical rows, or authorize production in that task

## 3. Archive, not yet authorized

- [ ] 3.1 After 2.1, keep that application result and archive this scope. Do not start a restricted-promotion implementation or a production admission in that task
