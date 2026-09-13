## Why

The business-profile pipeline currently preserves some correct object-level disclosures but can derive semantically false value-chain roles and then fail publication because derived record identity, temporal identity, and historical reuse follow different rules. The existing contract-row correction also covers new extraction better than reused historical artifacts, so repeated backfills can still collide on valid multi-contract facts such as the two `多晶硅料` rows.

This must be corrected before broader backfill rollout: the defect is primarily in program-side semantic layering and lifecycle contracts, not a lack of LLM reasoning capacity.

## What Changes

- Define a strict three-layer contract: immutable evidence produces object/row-specific atomic activities and operating facts; approved atomic records may derive scoped company capabilities; only governed current capabilities and facts feed exposure/publication consumers.
- Preserve distinct inventory objects such as `成品酒` and `半成品酒` as separate atomic activities/facts. Generic internal inventory storage MUST NOT imply that the company is an external `storage_provider`.
- Derive `storage_provider` only from evidence-backed third-party warehousing, storage, logistics-storage, or equivalent service activity. Aggregate multiple qualifying supports into one deterministic scoped role without losing supporting activity/evidence lineage.
- Complete contract/table-row identity across extraction, persistence, temporal checks, reuse, promotion, and replay so repeated product labels from distinct contracts remain distinct source facts.
- Apply `result_policy=reuse` and `result_policy=replace` consistently through semantic artifact selection, atomic persistence, derived roles, exposure publication, and local historical repair.
- Isolate deterministic conflicts to the affected fact or role group so one invalid derived group does not abort unrelated facts from the same report, and report conflicts as data-contract outcomes rather than LLM provider congestion.
- Add a bounded local audit/apply repair path that removes or transitions invalid machine-derived roles, reconstructs row-aware facts when local evidence is sufficient, preserves approved history and source evidence, and is idempotent without network or LLM calls.
- Report whether each result was newly LLM-extracted, reused, locally replayed, or program-derived, including actual token use so zero-token reuse/replay is observable.

## Capabilities

### New Capabilities

- `business-profile-semantic-layering`: Separation of object-level disclosures from derived company capabilities, including evidence requirements and deterministic identity for value-chain roles.
- `business-profile-contract-fact-integrity`: End-to-end preservation of distinct contract/table-row facts, extending the prior new-extraction row-identity behavior to persistence, promotion, historical reuse, and repair.
- `business-profile-replay-integrity`: Consistent reuse/replace semantics, conflict isolation, token accountability, and bounded local repair of historical derived data.

### Modified Capabilities

None. No current top-level OpenSpec capability owns these combined business-profile contracts.

## Impact

- Affects the existing business-profile semantic runtime, activity/role producer, temporal policies, governed repository, exposure/publication orchestration, backfill reporting, and their focused tests.
- Uses the existing `/run business_profile_backfill` entry point, semantic artifacts, evidence records, repository, promotion owner, and configured single LLM route; it introduces no second write path or assumed stronger model.
- Existing API routes and source evidence remain compatible. Corrected current projections may stop exposing invalid inventory-derived `storage_provider` roles and may expose separate contract-level facts that were previously collapsed or held.
- Historical correction is local, dry-run by default, scoped explicitly, and non-destructive to immutable evidence, valid approved history, and review decisions.
- No PDF parser, OCR, shareholder acquisition, external entity directory, LLM gateway, database platform, or generalized validation framework is added.
