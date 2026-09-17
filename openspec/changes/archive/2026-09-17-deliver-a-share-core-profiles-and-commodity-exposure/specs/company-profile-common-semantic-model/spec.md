## MODIFIED Requirements

### Requirement: Subject period and business regime are explicit
Every governed fact MUST carry a supported subject scope and period semantics. Subject scope MUST be one of `consolidated_group`, `issuer`, `named_subsidiary`, `business_segment`, or `unclear`. When the source does not explicitly identify a narrower issuer, named subsidiary, or business segment and no unresolved subject conflict exists, the research adapter MUST use `subject_scope=consolidated_group` with `subject_basis=report_default_group_scope`. Explicit issuer, subsidiary, segment, and source-supported numeric-reconciliation scopes remain authoritative. Regime-sensitive facts MUST preserve report period, knowledge time, regime effective time, and comparison basis without rewriting historical knowledge. An explicitly restated comparative MUST carry `comparison_basis`; a missing basis is a blocker, not an inferred default.

#### Scenario: Ordinary report wording uses the default group scope
- **WHEN** a report fact is written with `公司` or otherwise has no explicit narrower subject and the Evidence supports the fact
- **THEN** the adapter records `subject_scope=consolidated_group` and `subject_basis=report_default_group_scope`
- **AND** it preserves the original wording and Evidence without claiming issuer-specific wording

#### Scenario: Explicit standalone wording remains issuer-scoped
- **WHEN** local Evidence explicitly identifies a parent-only statement, 母公司 or 本公司单体 scope rather than ordinary company wording
- **THEN** the fact retains `subject_scope=issuer` and its source-supported basis
- **AND** the default group rule does not widen the subject

#### Scenario: Named subsidiary or segment remains narrow
- **WHEN** Evidence identifies a named subsidiary or a disclosed segment row
- **THEN** the fact retains `named_subsidiary` or `business_segment`
- **AND** it is not promoted to consolidated group solely by the report-level default

#### Scenario: Same-control comparative is disclosed later
- **WHEN** a post-restructuring report restates an earlier period on a same-control basis
- **THEN** the restated fact coexists with the predecessor's original-as-published fact using separate knowledge times and comparison bases
- **AND** the later record does not overwrite the earlier known fact

## ADDED Requirements

### Requirement: Core assessment retains dimension-level accepted Evidence
The program-owned common-core projection MUST record mapping_version=company_profile_common_core_mapping.v1 and exactly the principal_business, products_services and revenue_model dimensions. Each dimension MUST retain answered, supporting_record_ids, evidence_ids, source excerpt or table anchor, and missing_reason when unanswered. References MUST resolve to accepted records in the same report and support that particular answer. All three substantive answers are required for core completeness; empty/legal-empty results and mere object existence MUST NOT count. These are planned M1 projection fields, not additions to extract/repair/verify payloads or the ChapterTask enum. Implementation MUST explicitly register the projection schema before writing it.

#### Scenario: One overview does not explain monetization
- **WHEN** an accepted overview describes products but does not explain revenue generation and no business-revenue Evidence is available
- **THEN** revenue_model remains unanswered while supported dimensions and other accepted facts remain available.

### Requirement: Commodity exposure has a versioned derived field contract
M3 MUST implement company_profile_commodity_exposure.v1 as a program-derived CommodityExposure projection. It MUST NOT add CommodityExposure to the Stage 5 extract object vocabulary or copy numeric values from legacy Activity fields. Before writer integration, the model and read/write projection schema MUST register the following contract:

| Field | Required behavior |
| --- | --- |
| exposure_id, schema_version | Program identity bound to source records, role, commodity or source name, period and policy |
| report, reported_period, period_type, knowledge_time | Preserve source identity and time; no invented dates |
| subject_scope, subject_basis, business_object | Preserve the source scope and business object; no implicit cross-segment aggregation |
| source_record_ids, evidence_ids | Nonempty references to accepted new-model facts and their Evidence |
| measurement_record_ids | Optional references to separate quantity/amount Measurements; no legacy Activity.value source |
| source_native_name, commodity_id, mapping_status | Name retained; status mapped/pending/ambiguous; unique commodity_id only for mapped |
| role | product_sales/raw_material_input/energy_consumption/hedge_underlying/unknown; non-unknown roles require source support |
| assertion_class, mapping_version, policy_version | deterministic_derivation with auditable mapping and policy versions |
| market_series_id, market_link_status | Nullable series; not_linked/linked/ambiguous; linked requires a series ID |
| uncertainty | Preserve explicit unresolved meaning without model-reported probabilities |

Identity/version/name fields MUST be nonempty strings; optional commodity/market identifiers MUST be strings or null. Record/Evidence references MUST be arrays of IDs validated against the same report; report MUST reuse ReportIdentity, and time/scope fields MUST reuse the common semantic types. The report-level assessment MUST separately retain assessment_status=not_assessed/assessed/extraction_failed, checked_evidence_ids and its association list. An assessed empty list MUST retain the nonempty checked source range and MUST NOT mean zero exposure. Unknown quantity, market link or net sensitivity MUST NOT suppress an established association. Role values are projection-only, never request field IDs; hedge_underlying requires a representable accepted hedge fact, otherwise it remains unresolved. No default net-profit direction, netting or elasticity field is permitted. Current ResearchBoundary remains a placeholder until task 3.1 is implemented; this specification is not a claim that runtime already supports the schema.

#### Scenario: Mapped copper input without a quantity
- **WHEN** accepted new-model records identify copper input but no quantity or market link
- **THEN** the derived object retains its source references, mapped commodity, supported role and empty optional measurement/market links.

#### Scenario: No commodity association was assessed
- **WHEN** the report has not been inspected for commodity associations
- **THEN** assessment_status is not_assessed even if the association list is empty.

#### Scenario: Role leaks into request vocabulary
- **WHEN** a projection role such as energy_consumption is used as a Stage 5 field_id
- **THEN** preparation rejects the unknown field rather than silently extending the extraction schema.
