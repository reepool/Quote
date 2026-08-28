# Harden Business Profile End-to-End Integrity

## Why

The current business-profile pipeline can lose valid activity and relationship facts before persistence, admit temporal conflicts, discard disclosed counterparties that are not yet in the entity catalog, bypass commodity publication gates, expose diagnostic history without authentication, and misclassify uncertain units or periods. These are real end-to-end correctness and operational issues, not optional cleanup.

## What Changes

- Persist producer metadata through the repository-compatible payload shape while retaining identity inputs for deduplication.
- Make activity and relationship temporal conflict detection use their actual primary keys and reject empty supersession pointers.
- Preserve unresolved named counterparties as raw, reviewable relationship facts instead of dropping them.
- Require the runtime publication manifest to reach commodity publication; missing or failed gates remain held.
- Protect business-profile history as a diagnostic endpoint without changing approved public reads.
- Keep unknown dimensions out of physical-volume classification and preserve the source period basis during unit conversion.

## Scope

This change is limited to the company business-profile, commodity-exposure, supply-chain persistence/publication path and its directly relevant tests. It does not add a second LLM provider or alter the shared PDF asset module.

## Acceptance Criteria

1. Valid LLM activity and relationship results are persisted end to end without repository unknown-field errors.
2. Conflicting records sharing one activity/relationship temporal identity cannot both be approved, while distinct source rows remain queryable.
3. A named counterparty absent from the catalog is retained with a null entity id and an explicit unresolved/catalog-pending status.
4. Commodity exposure publication cannot self-authorize required gates; a matching, externally supplied manifest is required.
5. History/candidate diagnostics require the configured trusted diagnostic scope; approved public profile/exposure reads remain available under their existing policy.
6. Unknown activity dimensions are not published as physical volume, and conversion provenance uses the activity period basis or explicit `unknown`.
