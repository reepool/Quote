## ADDED Requirements

### Requirement: Execution stability is proven before another cohort replay
Before another provider-bearing twenty-report shadow replay is admitted, the system MUST pass provider-free fixtures for all historically oversized segment scopes, gateway tests for provider-compatible repair and output-budget/truncation classification, and one bounded live repair integration probe on the frozen primary Gemini logical route. The evidence MUST bind the prior replay's five over-budget successes, maximum 40,673 output-token usage, five `provider_unavailable` calls, four terminal schema failures, and maximum 259,482 ms latency without modifying that replay.

#### Scenario: Stability fixtures and bounded probe pass
- **WHEN** partition, merge, budget, trace, repair-payload, valid-excess, and truncation checks all pass and the bounded Gemini repair probe returns schema-valid JSON
- **THEN** the stability change may close and authorize one later separate new-ID full-cohort replay
- **AND** it does not execute that replay or change production authorization itself

#### Scenario: The bounded probe has an infrastructure failure
- **WHEN** DNS, transport, deadline, or provider availability prevents the live repair probe from completing
- **THEN** the result is recorded with its typed diagnostic and this stability gate remains incomplete
- **AND** no model substitution, timeout increase, targeted cohort run, or historical-result splice is used to claim success

### Requirement: Stability validation remains research-only
All fixtures, traces, probe receipts, and future replay admission decisions produced by this change MUST retain `production_authorization=not_authorized`. They MUST NOT write approved company-profile data, enable Stage 6, scheduler/backfill, commodity exposure, value-chain publication, or DCF.

#### Scenario: All stability checks pass
- **WHEN** every execution-stability acceptance check is green
- **THEN** the result states only that a separate controlled shadow replay may proceed
- **AND** no research fact is promoted to production by this change
