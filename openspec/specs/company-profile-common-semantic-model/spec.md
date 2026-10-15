# company-profile-common-semantic-model Specification

## Purpose
TBD - created by archiving change implement-company-profile-common-semantic-model. Update Purpose after archive.
## Policy version
New requests follow the 2026-09-13 report-default subject policy: explicit narrower local Evidence wins; unresolved scope conflicts remain unclear. Current all-A-share delivery uses the common business core plus conditional enhancements. Stage-four fixture and non-production boundaries below describe that historical implementation, not permanent restrictions on future new-contract research release.

## Requirements
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

### Requirement: Common profile objects have versioned and non-overlapping responsibilities
The system MUST provide a versioned common semantic model for `BusinessOverview`, `Segment`, `Activity`, `Measurement`, `Relationship`, `BusinessEvent`, `BusinessRegime`, `IndustryPackageAssignment`, `Evidence`, and `CoverageResult`. Numeric operating values MUST be represented as Measurements and MUST NOT be embedded in Activities. Research text MUST NOT become a new fact source.

#### Scenario: Product row contains revenue cost and margin
- **WHEN** one source row reports operating revenue, operating cost, and gross margin for a product
- **THEN** the model represents one Segment and three independent Measurements
- **AND** it does not create a numeric sales Activity from that row

#### Scenario: Business overview feeds a research summary
- **WHEN** a research-facing summary is constructed from an approved BusinessOverview and structured records
- **THEN** every number, object, action, relationship, and event in the summary is traceable to an input record
- **AND** the summary cannot introduce a new fact, causal claim, role, or judgment

### Requirement: Evidence and occurrence identity preserve the physical source
Every source-backed object MUST reference versioned Evidence containing report identity, one-based PDF physical page, section, and a stable table cell or normalized bounded-text anchor. A table Measurement identity MUST include `logical_slot + physical_anchor` and, when the Evidence distinguishes source rows or measured objects, that source-row/measured-object identity; semantic interpretation, normalized names, evidence IDs, run IDs, and model artifacts MUST remain outside the physical occurrence identity. Equal numeric values MUST NOT merge distinct source rows or measured objects.

#### Scenario: One table row contains different metric cells
- **WHEN** revenue and cost appear on the same row but in different columns
- **THEN** their logical slots and cell anchors produce distinct occurrences
- **AND** neither candidate overwrites or merges with the other

#### Scenario: Equal values belong to different source rows
- **WHEN** one utilization table reports `铁 ... 96%` and `坯材 ... 96%` under the same header
- **THEN** the adapter retains two independently anchored Measurements
- **AND** it does not emit an occurrence conflict solely because the values are equal

#### Scenario: Printed page differs from physical page
- **WHEN** the PDF physical page is 59 and the printed label is 58
- **THEN** Evidence stores page 59 as the authoritative coordinate and label 58 separately
- **AND** verification does not substitute the printed label for the physical coordinate

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

### Requirement: Measurements preserve source-native meaning before canonical conversion
Each Measurement MUST contain exactly one metric, one logical slot, one subject, one measured object or segment, one period, and the source-native value, unit, header, qualifier, footnote references, and Evidence. Canonical conversion MUST be a separate program-owned result. Reported and derived values MUST have distinct provenance. A production-capacity Measurement MUST carry a source-supported `capacity_kind`; a processing-volume Measurement MUST carry `processing_direction=external_service_provided` and MUST NOT represent buyer-side outsourcing, an internal production step, or self-operated recycling. Consolidation rows MUST use `row_class=consolidation_adjustment` and keep revenue, cost, and margin as separate Measurements; they are not products or Activities.

#### Scenario: Capacity is observed
- **WHEN** a source reports production capacity
- **THEN** the Measurement preserves the source unit and a controlled `capacity_kind`
- **AND** unlike capacity kinds are not treated as equivalent

#### Scenario: Processing label includes a sales alias
- **WHEN** one physical fact is labelled `涂覆加工量（销量）` and represents an externally provided processing service
- **THEN** the model creates one `processing_volume` Measurement and preserves the complete source-native label
- **AND** it does not duplicate that occurrence as `sales_volume`

#### Scenario: Inventory has no source footnote
- **WHEN** inventory value, unit, object, header, and time are explicit but the source has no footnote
- **THEN** the Measurement may be observed with an empty footnote list
- **AND** the model does not invent an inventory scope

### Requirement: Activities and disclosed identities use frozen closed sets
The model MUST restrict v1 Activity actions to `develops`, `produces`, `processes`, `sells`, `purchases`, `provides_service`, and `operates`; an observed verb outside this set MUST remain an unresolved source candidate. Every Activity `activity_actor` MUST be supported by the source's direct grammatical or economic relationship, and a third-party trader's sale MUST NOT be rewritten as the issuer's `sells`. Counterparty identity MUST use only `named`, `report_local_anonymous`, or `report_local_aggregate`; an aggregate total without names remains `not_disclosed` for names and MUST NOT be backfilled from another chapter.

#### Scenario: Unsupported action and aggregate identity stay bounded
- **WHEN** a source uses an unreviewed action verb and a top-five section reports only an aggregate amount without names
- **THEN** the action remains unresolved and the top-five aggregate is represented only by its concentration Measurement
- **AND** counterparty-name coverage remains `not_disclosed` with no Relationship created from that aggregate

#### Scenario: Independently disclosed aggregate identity remains a relationship
- **WHEN** another section explicitly discloses a transaction counterparty identity such as `集团所属单位`
- **THEN** that source identity may use `identity_class=report_local_aggregate` in an independent Relationship
- **AND** it does not change top-five counterparty-name coverage to observed

### Requirement: Coverage is a first-class result rather than an empty collection
For every active package and chapter-task checklist item, the system MUST record its requirement level and one of `observed`, `not_disclosed`, `not_applicable`, `extraction_failed`, or `unclear`, with an applicable `reason_code` and evidence pages where applicable. For `not_disclosed`, the `reason_code` set is closed to `explicit_confidentiality`, `explicit_disclosure_exemption`, or `source_reason_unspecified`; the first two require explicit source wording. Coverage is evaluated only against the active package's chapter-task checklist, not an unbounded universe of metrics. An empty object list MUST NOT imply task completion.

#### Scenario: Required product table was not read
- **WHEN** a required product performance table is known to exist but its page was omitted or unreadable
- **THEN** the relevant CoverageResult is `extraction_failed`
- **AND** the profile cannot report that task as complete

#### Scenario: Counterparty totals contain no names
- **WHEN** the complete section reports top-five totals but no counterparty names
- **THEN** name coverage is `not_disclosed` while concentration Measurements may be observed
- **AND** no Relationship is invented from concentration alone

### Requirement: Researcher projection exposes facts uncertainty evidence and downstream boundaries
The system MUST provide a stable research-facing projection grouped into business overview, regime/package, segments, activities, operating measurements, disclosed inputs and counterparties, business events, coverage, and evidence references. The projection MUST distinguish reported facts, deterministic derivations, and research assumptions. It MUST derive discrete confidence as `high`, `medium`, `low`, or `rejected` from existing Evidence completeness, verification/disposition, subject basis, and contradiction state; the LLM MUST NOT provide a probability. A program-owned closed usage policy MUST restrict consolidated aggregation, ranking, and cross-company comparison according to `object_type`, metric family, measured object, and `subject_scope`/`subject_basis`. Commodity exposure and value-chain sections MUST explicitly report `not_authorized`, `not_assessed`, `insufficient_evidence`, or approved bounded results rather than infer direction or complete chain position.

#### Scenario: Manufacturing profile is rendered with subject caveats
- **WHEN** validated manufacturing/materials records for one report are projected and some accepted records have `subject_scope=unclear`
- **THEN** a researcher can read the source-native business overview, product revenue/cost/margin, applicable capacity/production/sales/inventory, disclosed inputs/relationships, regime, and coverage state
- **AND** each displayed fact links to source-native value and Evidence, while consolidated-sensitive uses are withheld according to the closed policy

#### Scenario: Unclear consolidated-sensitive fact is restricted
- **WHEN** an accepted revenue, margin, or concentration Measurement has `subject_scope=unclear`
- **THEN** the projection may retain it as a source fact with bounded confidence
- **AND** it MUST NOT expose the Measurement as a consolidated aggregation, ranking, or cross-company comparison input

#### Scenario: Business-segment fact remains distinct
- **WHEN** a Measurement has `subject_scope=business_segment`
- **THEN** the projection may display it as a segment fact with its derived confidence
- **AND** it MUST NOT reinterpret it as a group total without explicit or reconciled group basis

#### Scenario: Raw-material price risk is mentioned without an approved exposure contract
- **WHEN** the report mentions lithium, nickel, or cobalt price effects but no later commodity mapping and direction decision has been approved
- **THEN** the projection may show the disclosed material/risk fact and `commodity_exposure.status=not_assessed`
- **AND** it does not state that a price rise is positive or negative for profit

### Requirement: Stage-four implementation remains isolated from production state
The common model and projection MUST run without modifying production databases, existing approved facts, schedulers, Telegram, DCF, production prompts, or freeze switches. The model MAY be instantiated from fixtures and in-memory candidates, but production publication MUST remain unauthorized.

#### Scenario: Stage-four reference profile is generated
- **WHEN** the reference fixture is validated and rendered
- **THEN** the output is produced without network calls or production database writes
- **AND** `production_authorization` remains `not_authorized`

### Requirement: Period type governs the meaning of reported period
The common semantic model MUST keep duration, instant, event, and expected time semantics distinct by using the existing `period_type`. An annual duration fact MUST retain the disclosed annual period rather than a mechanically substituted report-end date. An instant fact MUST retain its evidenced as-of date. An event MUST keep its occurrence period separate from `event_date`, `regime_effective_at`, and `knowledge_time`. Normalization MUST NOT replace a source-supported narrower period or mutate historical records outside the current adapter output.

#### Scenario: Annual revenue uses a duration period
- **WHEN** an annual-report revenue Measurement has `period_type=duration` and its only date-shaped period is the same report-year end date
- **THEN** the bounded adapter records the annual `reported_period` as that year
- **AND** the original table header and Evidence remain source-native and unchanged

#### Scenario: Inventory remains an instant
- **WHEN** a report discloses inventory volume as of `2025-12-31` with `period_type=instant`
- **THEN** `reported_period` remains `2025-12-31`
- **AND** it is not reduced to an annual duration period

#### Scenario: Event time remains separated
- **WHEN** a restructuring event occurred on `2025-01-06` and was learned from a later annual report
- **THEN** the event keeps its occurrence period, `event_date`, `regime_effective_at`, and `knowledge_time` as separate fields
- **AND** none of those values overwrites another

### Requirement: Expected completion remains an Evidence-bound qualifier
When an approved Evidence bundle explicitly states an expected completion, completion, or commissioning period for a capacity project, the semantic record MUST preserve the exact source expression in `source_native.qualifier`. The expected period MUST NOT replace the record's report-period semantics, and it MUST NOT be inferred from Gold, project magnitude, industry custom, or a page outside the prepared Evidence scope.

#### Scenario: Continuation Evidence states expected completion
- **WHEN** a capacity row and its approved continuation page explicitly state an expected completion year
- **THEN** the capacity record retains the report period and copies the expected-completion wording into `source_native.qualifier`
- **AND** both physical Evidence anchors remain traceable

#### Scenario: Expected completion is absent
- **WHEN** the prepared Evidence reports new or under-construction capacity without an expected completion statement
- **THEN** the qualifier remains empty or preserves only other explicit source wording
- **AND** the system does not insert a completion year from Gold or convention

### Requirement: Substantive answers retain original control and operating periods
Three-dimensional business answers MUST preserve original subsidiary and investee relationships, control or exit boundaries, industry and product matrices, actual versus planned activity, transaction mechanisms and revenue confirmation. Evidence-based period restrictions MUST survive acceptance, query and export; answered status or keyword hits alone MUST NOT establish substantive completeness.

#### Scenario: A commodity business stops during the report year
- **WHEN** the original report states soybean operation stopped after September and separately describes current sales and service-charge mechanisms
- **THEN** final answers retain that period limitation and distinguish goods from supported service charges

#### Scenario: Related-party scope changes while current procurement continues
- **WHEN** source evidence distinguishes full-year procurement, January-February approved amounts and a later nonrelated-party period
- **THEN** the answer retains those native periods and actors without combining approved limits with actual income

### Requirement: Unexplained official business conflicts remain uncertain in answers
Official wording that conflicts with independently checked same-report business and revenue disclosures and cannot be explained MUST retain its native Evidence and source wording with existing unclear uncertainty semantics. It MUST NOT support deterministic principal-business, products-services or revenue-model answers or be reused as resolved core evidence. Source quote equality alone MUST NOT establish business accuracy. Correct same-report sources MUST continue to support substantive answers; ordinary source-supported software and integrated-circuit business MUST NOT be removed merely because of product words.

#### Scenario: A native segment description conflicts with the report's actual business
- **WHEN** a segment product description names software and integrated circuits while checked principal-business and revenue disclosures describe fishing, fish processing and transport and the contradiction remains unexplained
- **THEN** the original description remains available as unclear Evidence and exits deterministic three-dimensional answers and their support IDs while correct business content remains

#### Scenario: A report consistently describes a software business
- **WHEN** the source-supported business and income context consistently supports software or integrated circuits
- **THEN** normal evidence and substantive answers remain eligible without a word blacklist

### Requirement: Associated company metadata preserves native nonconsolidated scope
An independently frozen contract MAY name associated entities as group_associated_entity, but delivery MUST use the existing native business_segment scope with the complete named actor and source-native associated/nonconsolidated qualifier. It MUST NOT add a model enum solely for the contract label or promote the associated entity's business to consolidated group operations. Company-type or name continuations MUST preserve exact identity; original subsidiaries and associated entities MUST remain distinguishable through acceptance, query and export.

#### Scenario: A frozen actor label is corrected to match an associated company
- **WHEN** the source identifies 江苏联海动力机械有限公司 and 福马新能（北京）科技有限公司 as associated and not consolidated, and A approves correcting the successor contract metadata
- **THEN** their complete native actors and nonconsolidation qualifiers use business_segment scope without rewriting the retained original contract or inferring group operation


### Requirement: Explicit relation adjudication and raw actors preserve native scope
When official headings conflict with explicit same-report relationship disclosures, an A-adjudicated relationship MUST retain the original conflicting Evidence and column explanation. Independently disclosed associated-company income MUST NOT become consolidated group revenue. An informal original actor MUST NOT be replaced by an unproven legal-name alias. Substantive answers and named actions MUST preserve actual commissioning, trial, mass-delivery, disposal and control-period limitations, and distinguish procurement, production, sale and services in their original direction.

#### Scenario: A heading conflicts with an explicit associate disclosure
- **WHEN** Guangzhou Development p223–224 uses an important-joint-venture heading but p234 explicitly identifies the two investees as associates and A rules to follow p234
- **THEN** the associate relationship and separate income survive with the original heading conflict retained and without promotion to consolidated group income

#### Scenario: An overseas platform has only an original short name
- **WHEN** a report attributes imported-coal sales to 香港国际公司 without proving equivalence to a legal full name
- **THEN** the action retains that original actor rather than inventing an alias
