## 1. Rollout Configuration And Identity

- [x] 1.1 Add a validated staged business-profile production rollout configuration with structured shadow active and all promotion/daily phases closed
- [x] 1.2 Implement deterministic runtime identity derivation from code, catalogs, and the configured semantic LLM profile
- [x] 1.3 Reject explicit runtime identities and promotion phases that do not match the derived production identity or passed manifests

## 2. Latest-Annual Bootstrap

- [x] 2.1 Extend scoped backfill selection with an explicit `latest_annual_only` policy over bounded historical discovery
- [x] 2.2 Preserve existing `expanded` specialist/history behavior and idempotent queue, asset, correction, and checkpoint reuse
- [x] 2.3 Wire rollout phase, selection policy, typed list parameters, and bounded budgets through DataManager, scheduler, and task-manager execution

## 3. Safe Deployment Defaults And Operations

- [x] 3.1 Enable only business-profile discovery, reconciliation, semantic runtime, async production, and the manual backfill job; keep promotion and daily cron disabled
- [x] 3.2 Add rollout readiness reporting for discovery completeness, queue state, field-family status, manifest readiness, and daily transition eligibility
- [x] 3.3 Update the production runbook with the stable bootstrap command, repeated-run behavior, phase transitions, and final daily activation procedure

## 4. Verification And Delivery

- [x] 4.1 Add focused tests for rollout config validation, derived identities, latest-annual bootstrap selection, typed task parameters, and fail-closed promotion/daily gates
- [x] 4.2 Run focused business-profile and scheduler tests, isolated rollout-gate validation, strict OpenSpec validation, and static checks
- [x] 4.3 Review all uncommitted changes, fix confirmed findings without touching baseline changes, then commit and push only this change
