## Why

The business-profile semantic pipeline has strong document and promotion primitives but is not production-complete: automatic scope discovery cannot find issuers with no existing manifest, production configuration is disabled and empty, named relationships have no production entity resolver, anonymous concentration disclosures are dropped, and commodity publications cannot resolve any current catalog mapping. These gaps leave all production profile, role, relationship, and commodity-exposure tables empty and make the automation-first workflow inoperable.

## What Changes

- Add a lightweight full-market announcement discovery frontier that scans official indexes, then downloads only the minimum selected annual, semiannual, correction, or specialist disclosure needed for a field-family gap.
- Define automated operating frequencies: filing-season daily discovery, weekly semantic processing and machine retry, monthly reconciliation, semiannual freshness evaluation, and annual coverage/backfill reconciliation.
- Preserve immutable, content-addressed PDF archives and manifest lineage; add evidence-aware duplicate auditing without deleting governed or unreferenced files automatically.
- Build the production counterparty resolver from governed local issuer/entity identities and approved aliases, while allowing anonymous concentration facts to bypass named-entity resolution.
- Tighten deterministic value-chain role derivation so processor roles require explicit governed transformation evidence rather than any standalone `processes` assertion.
- Make commodity mapping resolution publish an actual market commodity identity, persist publication input gaps as machine-rework exceptions, and provide a bounded path for promoted mappings instead of shipping an all-candidate catalog.
- Retire or isolate legacy v1 LLM compatibility surfaces only after dependency checks prove they are unused; clean only reproducible caches and generated test artifacts automatically.

## Capabilities

### New Capabilities

- `business-profile-production-operations`: Full-market discovery, minimum-document acquisition, refresh frequencies, manifest/archive governance, and unattended reconciliation.
- `business-profile-derived-output-correctness`: Correct automatic production of relationships, concentration facts, value-chain roles, and commodity exposure publications from governed evidence.

### Modified Capabilities

- `scheduler`: Add explicit business-profile discovery, semantic maintenance, reconciliation, and annual coverage job contracts with production-safe defaults.
- `research-data-engine`: Require business-profile production storage initialization, local governed identity resolution, persistent machine-rework gaps, and evidence-safe archive audit behavior.

## Impact

- Affected modules include business-profile semantic scope discovery, official disclosure acquisition, activity/relationship production, commodity exposure production, scheduler task registration/configuration, research storage initialization, and business-profile operational reporting.
- Existing public read contracts remain additive and fail closed. No historical PDF, approved fact, manifest, or database row is overwritten or deleted.
- Network and LLM cost are bounded by index-first discovery, hash/freshness checks, selected sections, deterministic extraction first, and semantic calls only for unresolved field-family gaps.
