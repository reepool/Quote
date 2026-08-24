## Context

Atomic annual-report extraction returns semantic assertions that may describe the same business activity with different disclosed measurements. The normalized LLM assertion identity includes value, unit, and evidence, while `BusinessProfileActivityProducer` currently regenerates an activity identity without value or unit. For `300708.SZ`, sales volume and sales revenue therefore became conflicting payloads under one activity primary key.

The existing schema already has separate `company_business_activities` and `company_operating_facts` tables. Operating facts have no dedicated activity foreign-key column but provide `metadata_json`, so the behavior can be corrected without a database migration. Existing exposure production currently reads an activity's `value` and `unit`, which requires a compatibility projection until that downstream contract is deliberately changed.

## Goals / Non-Goals

**Goals:**

- Represent one semantic business action/object as one activity even when the filing discloses several measurements.
- Preserve every numeric LLM assertion as a separately identified operating fact linked through metadata to its source activity and semantic assertion.
- Keep deterministic, program-owned metric classification and unit conversion.
- Replay the persisted `300708.SZ` semantic response after a runtime identity change without consuming extraction LLM tokens.
- Preserve existing `601088.SH` activity, role, operating-fact, and exposure behavior.

**Non-Goals:**

- Migrate existing database tables or rewrite existing approved production rows.
- Replace activity-derived exposure production with an operating-fact-only implementation.
- Expand the LLM response schema or ask the LLM to standardize fields, calculate values, or convert units.
- Redesign semantic verification or business-profile field-family orchestration.

## Decisions

### Group activities before candidate construction

Atomic assertions are grouped by instrument, report period, subject scope, action, normalized object, resolved object ID, segment, and geography. Numeric value, unit, semantic assertion ID, and evidence are excluded from the activity grouping key. All source semantic assertion and evidence identities are retained in activity metadata.

This grouping is performed in the existing semantic runtime conversion point, where both the normalized assertions and product resolution are available. It avoids a second persistence owner and retains the bundle's existing conflict protection.

### Persist measurements as operating facts

Every assertion with both a raw value and raw unit produces an operating fact. The program classifies the metric from the original Chinese source label, with action and unit used only as deterministic fallback context. The minimum catalog expansion covers sales revenue, purchase amount, purchase volume, and an explicit other measurement type in addition to existing volume metrics.

The fact identity includes the source document, grouped activity identity, semantic assertion identity, fact type, raw value, raw unit, evidence identity, and the runtime processing contract. Metadata stores `source_activity_id`, `semantic_assertion_id`, `object_raw`, and `source_label_raw`.

Known units use the current unit catalog. An unresolved unit does not erase the LLM fact or terminate unrelated activity conversion: the raw fact remains a candidate with no normalized value and a unit-resolution-pending marker, so existing unit governance can resolve it later.

### Retain one marked compatibility projection on each activity

An activity keeps at most one `value` and `unit` pair for existing downstream consumers. Physical measurements such as sales, production, inventory, purchase volume, or reserves rank before currency measurements; stable source order breaks ties. Metadata declares the projection as legacy compatibility and identifies its source operating fact type and semantic assertion.

This preserves `601088.SH` and current exposure derivation while making operating facts authoritative for multiple measurements. Removing the projection is deferred until all downstream consumers read operating facts directly.

### Version deterministic transformation, not the LLM contract

The semantic runtime processing identity is advanced because persisted candidate output changes. The LLM prompt/schema version remains unchanged, allowing exact semantic artifacts to replay when document, selected evidence, prompt, and schema identities match. A new run identity prevents collision with the failed v7 bundle while preserving immutable prior audit history.

## Risks / Trade-offs

- [Compatibility projection can be mistaken for the complete measurement set] -> Mark it explicitly in metadata and retain all measurements as authoritative operating facts.
- [Grouping can merge repeated descriptions of the same action/object] -> Keep subject, report period, object resolution, segment, and geography in the key and preserve all evidence/assertion identities.
- [Unknown metric labels cannot be safely standardized] -> Persist them as `other_measurement` with the exact Chinese label rather than dropping or inventing a category.
- [Unknown units cannot be normalized immediately] -> Persist raw candidate facts and block only their normalized publication path, not the activity or other facts.
- [Catalog/runtime version changes can create new candidate identities] -> Do not rewrite existing approved rows; test existing-record reads and derived output idempotency using `601088.SH`-shaped fixtures.

## Migration Plan

1. Deploy the catalog and runtime identity updates with the grouped conversion logic.
2. Re-run the same forced `300708.SZ` command. The exact stored semantic artifact is replayed and converted under the new runtime identity.
3. Verify one sales activity and separate sales-volume/sales-revenue operating facts are published without a primary-key conflict.
4. Confirm `601088.SH` remains API-ready and its existing approved role/exposure counts are unchanged.

Rollback consists of reverting the code/config change. Existing approved rows are untouched; newly created candidates remain auditable and are not destructively removed.

## Open Questions

None for this change. Replacing the downstream exposure compatibility projection is a separate future change only after all current consumers migrate to linked operating facts.
