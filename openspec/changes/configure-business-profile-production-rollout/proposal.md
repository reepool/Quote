## Why

The asynchronous business-profile pipeline is implemented, but production remains empty because the deployment configuration has no frozen runtime identities, no governed rollout phases, and no safe full-market bootstrap path. A date-only manual backfill currently expands every matching report instead of selecting one latest active annual report per issuer, which would create avoidable downloads and semantic work during a long-running market-wide rollout.

## What Changes

- Add a persistent production-rollout configuration with ordered field-family phases, bounded cohort sizes, historical discovery windows, retry budgets, readiness gates, and an explicit final daily transition.
- Add a manual-only full-market bootstrap task that first advances official announcement discovery and then enqueues only the latest eligible annual report per issuer.
- Bind semantic processing to real parser, selector, schema, catalog, model, verifier, rule, and policy identities instead of placeholder values.
- Keep automatic promotion disabled for initial shadow phases and require a complete, matching field-family promotion manifest before approved writes.
- Keep the daily scheduler disabled until the bootstrap is complete, required field families meet coverage and exception gates, and the operator explicitly changes rollout mode.
- Expose rollout progress and queue/coverage reconciliation without requiring routine per-record human approval.

## Capabilities

### New Capabilities

- `business-profile-production-rollout`: Governed, resumable all-market latest-annual bootstrap and staged transition to daily incremental production.

### Modified Capabilities


## Impact

- Configuration: `config/10_research.json`, `config/05_scheduler.json`, and a dedicated rollout configuration file.
- Runtime: business-profile async work selection, DataManager entry points, scheduler tasks, and task-manager parameter handling.
- Operations: rollout validation/reporting, documentation, and focused scheduler/queue tests.
- Data remains additive: no existing PDFs, manifests, candidates, approved facts, or audit history are deleted or rewritten.
