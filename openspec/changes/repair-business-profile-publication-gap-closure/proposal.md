## Why

The targeted `business_profile_backfill` path now completes extraction, verification, and publication, but still reports false publication gaps and leaves stale machine-rework exceptions after equivalent facts have been promoted. This prevents reliable unattended rollout even when the annual-report evidence and LLM verification succeeded.

## What Changes

- Bind newly derived value-chain and commodity-exposure records to the current runtime catalogs while preserving immutable source-evidence metadata.
- Treat only real operator decisions as human review blockers; allow automated contract-recovery decisions to be superseded by newly verified current evidence.
- Publish only the current effective commodity facts, close superseded or successfully replaced publication exceptions, and deduplicate repeated runtime exceptions.
- Define deterministic exposure direction for production activities and publish commodity identity independently from optional executable price-series selection.
- Make publication-gap counts reflect all and only unresolved current-run gaps.
- Repair the selected-artifact recovery query and add regression coverage for the observed recovery path.
- Reopen concentration facts rejected by the obsolete automated contract audit and force recovered `retry_due` work onto a fresh checkpoint.
- Send human-readable concentration scope and object labels to independent verification, reject internally inconsistent verifier responses, and require a valid current proof before promotion.
- Recompute local deterministic proofs on resume and never substitute an opaque concentration scope identity for missing business text.
- Require independent verification for LLM semantic-synthesis rows; only promoted deterministic parsers may bypass the semantic verifier.

## Capabilities

### New Capabilities

- `business-profile-publication-closure`: Defines current-catalog derivation, effective-fact selection, commodity-identity publication, automatic exception closure, and accurate publication reporting.

### Modified Capabilities

None.

## Impact

- Affected modules: `research/business_profile_semantic_extraction.py`, `research/business_profile_semantic_runtime.py`, `research/business_profile_async_production.py`, `research/business_profile_contract_recovery.py`, `research/business_profile_exposure_production.py`, `research/business_profile_review.py`, rollout verifier identities, and their focused tests.
- Existing Telegram command, scheduler job, shared annual-report asset path, database schema, and single-writer contract remain unchanged.
- No new service, queue, database table, external dependency, or alternate publication path is introduced.
