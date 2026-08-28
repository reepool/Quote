# Design

## Persistence Boundary

The activity and relationship producers continue to derive identity and content hashes from source-row and contract-reference values, but place those values under `metadata` before repository upsert. Repository payloads are validated at the boundary so unknown top-level keys fail during tests rather than at runtime.

## Temporal Identity

The governance conflict helper maps every record type to its persisted primary key (`activity_id`, `relationship_id`, or the existing fact key). Pointer comparisons are only meaningful when both pointers are non-empty. The same helper is used by write, approval, batch verification, and supersession checks.

## Unresolved Counterparties

Named relationships remain facts even when entity resolution misses the catalog. The relationship stores the raw name, direction, object, evidence, reporting period, `counterparty_entity_id=null`, and `resolution_status=unresolved`/`catalog_pending`. Anonymous aggregate disclosures retain their existing anonymous-fact path and do not create a fake entity edge.

## Publication and Diagnostics

Commodity publication receives the runtime's `FieldFamilyPromotionManifest` (or an equivalent immutable publication context). The publisher never constructs an enabled/benchmark-passed all-true manifest. Missing, stale, or failed gates produce a held/input-gap result. History is a diagnostic route and is guarded by trusted identity plus the dedicated business-profile diagnostic scope; approved read routes are unchanged.

## Units and Periods

Unknown or unresolved dimensions produce a pending/other measurement type rather than `production_volume`. Unit conversion reads `period_basis` from the activity or metadata, falling back to explicit `unknown`; it never assumes `full_year`.
## Compatibility

Existing approved records remain readable. New unresolved relationship status and metadata are additive. No data deletion or migration is required for this change.
