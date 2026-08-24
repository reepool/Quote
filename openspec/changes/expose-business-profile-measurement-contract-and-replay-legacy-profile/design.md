## Context

The resolver already emits `activities`, `operating_facts`, value-chain roles, named relationships, exposure facts, and exposures under `company_specific_profile`. The response model currently types that object as an unrestricted dictionary, so API consumers cannot discover the structure or the measurement authority rules from OpenAPI. The deterministic v8 transformation links newly produced operating facts to activities through `metadata.source_activity_id`, while approved rows created before v8 remain readable but may be unlinked.

The production owner is the existing `BusinessProfileAsyncProductionService` reached through `run_business_profile_backfill`. The API remains a local read projection and must not perform replay or writes.

## Goals / Non-Goals

**Goals:**

- Expose the existing company-specific field families as an explicit API model.
- State in every business-profile response that operating facts are authoritative measurements and activity values are compatibility projections.
- Compute a deterministic linkage status from approved operating facts without rejecting historical unlinked rows.
- Replay `601088.SH` through the authoritative queue after deployment and verify the new linkage contract end to end.
- Describe the actual annual automatic update and manual repair triggers.

**Non-Goals:**

- Change endpoint paths, remove fields, or introduce a breaking schema version.
- Add database columns or rewrite records directly with SQL.
- Change annual-report discovery, scheduler cadence, semantic extraction, verification, or publication behavior.
- Require market-price mapping for profile readiness.

## Decisions

### Add a root measurement contract without changing existing field semantics

`measurement_contract` is added to the response as a structured object containing the contract version, authoritative path, compatibility projection role, link field, activity-derived/standalone operating-fact counts, and linkage status. This is preferable to changing `schema_version` because all existing fields and meanings remain compatible and clients ignoring unknown fields continue to work.

### Type the company-specific container while preserving extension compatibility

`company_specific_profile` uses a dedicated Pydantic model with the currently emitted field families. The model permits extra fields so a historical or independently deployed resolver extension is not silently discarded. Individual governed rows remain dictionaries because their record families already have distinct versioned storage contracts and fully typing every historical metadata variant is outside this API increment.

### Derive linkage status from approved operating facts

The resolver identifies activity-derived operating facts by their deterministic measurement authority or an existing `source_activity_id`, and counts standalone facts separately. The status is `not_applicable` when no activity-derived facts exist, `linked` when all activity-derived facts are linked, `partially_linked` when only some are linked, and `unlinked` otherwise. Customer/supplier concentration and other independently sourced operating facts therefore remain valid without an activity link. Status is descriptive and does not control readiness, so old approved profiles remain queryable.

### Replay through the existing production command

After code tests pass, `601088.SH` is forced through the same expanded annual-report backfill entry point used by operators. The v8 persisted semantic artifact should be reused where identities match. Existing idempotency, supersession, review, and single-writer controls decide the approved rows; no special migration implementation is added.

### Annual updates continue to use effective shared annual-report assets

The shared annual-report asset job is enabled at 00:15 Asia/Shanghai every day. It discovers and downloads effective annual reports and corrections independently of profile processing. The business-profile incremental consumer is implemented for 06:20 every day with a 10-day lookback and 3-day overlap, but both its scheduler switch and `daily_incremental` rollout phase are currently disabled. Therefore production does not yet refresh profiles automatically; activation requires both switches after the targeted rollout is accepted.

Once enabled, the consumer reads changed effective assets and advances the durable queue through acquire, parse, semantic, verify, and single-writer publish. A corrected full annual report becomes the effective asset for its company/report period and replaces the earlier full report as the processing source. Failed or unfinished work remains in the durable queue for bounded retry and later scheduled continuation. Manual backfill remains the same-owner repair path for a selected company, historical scope, or urgent special disclosure.

## Risks / Trade-offs

- [Typed container could drop unknown resolver fields] -> Allow extra fields in the container model.
- [Historical rows report `unlinked`] -> Treat linkage as compatibility telemetry, not a readiness gate, and replay only the explicitly authorized representative company.
- [Forced replay could duplicate facts] -> Use the existing deterministic identities, supersession rules, and single writer, then verify counts and API output.
- [A production backfill may already be running] -> Check persistent control/queue state before replay and do not start a competing writer.

## Migration Plan

1. Deploy the additive resolver and response-model change.
2. Verify focused resolver, API, and OpenAPI tests.
3. Confirm no active business-profile backfill owns the queue.
4. Run the existing expanded forced backfill for `601088.SH` and both semantic field families.
5. Verify linked operating facts, activity compatibility projection, role/relationship/exposure preservation, logical activity de-duplication, and API readiness.

Rollback is a normal code revert. The additive response field can be removed without changing stored data; replayed rows remain valid under the prior reader.

## Open Questions

None.
