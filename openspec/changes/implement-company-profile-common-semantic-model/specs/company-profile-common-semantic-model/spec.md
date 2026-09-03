## ADDED Requirements

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
Every source-backed object MUST reference versioned Evidence containing report identity, one-based PDF physical page, section, and a stable table cell or normalized bounded-text anchor. A table Measurement identity MUST include `logical_slot + physical_anchor`; semantic interpretation, normalized names, evidence IDs, run IDs, and model artifacts MUST remain outside the physical occurrence identity.

#### Scenario: One table row contains different metric cells
- **WHEN** revenue and cost appear on the same row but in different columns
- **THEN** their logical slots and cell anchors produce distinct occurrences
- **AND** neither candidate overwrites or merges with the other

#### Scenario: Printed page differs from physical page
- **WHEN** the PDF physical page is 59 and the printed label is 58
- **THEN** Evidence stores page 59 as the authoritative coordinate and label 58 separately
- **AND** verification does not substitute the printed label for the physical coordinate

### Requirement: Subject period and business regime are explicit
Every governed fact MUST carry a supported subject scope and period semantics. Subject scope MUST be one of `consolidated_group`, `issuer`, `named_subsidiary`, `business_segment`, or `unclear`; unsupported scope MUST remain `unclear`. Regime-sensitive facts MUST preserve report period, knowledge time, regime effective time, and comparison basis without rewriting historical knowledge.

#### Scenario: Company wording lacks group evidence
- **WHEN** a management-discussion table only says `公司` and has neither explicit group wording nor documented reconciliation to the consolidated statement
- **THEN** its subject scope remains `unclear`
- **AND** the model does not default it to `consolidated_group`

#### Scenario: Same-control comparative is disclosed later
- **WHEN** a post-restructuring report restates an earlier period on a same-control basis
- **THEN** the restated fact coexists with the predecessor's original-as-published fact using separate knowledge times and comparison bases
- **AND** the later record does not overwrite the earlier known fact

### Requirement: Measurements preserve source-native meaning before canonical conversion
Each Measurement MUST contain exactly one metric, one logical slot, one subject, one measured object or segment, one period, and the source-native value, unit, header, qualifier, footnote references, and Evidence. Canonical conversion MUST be a separate program-owned result. Reported and derived values MUST have distinct provenance.

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

### Requirement: Coverage is a first-class result rather than an empty collection
For every active package and chapter-task checklist item, the system MUST record its requirement level and one of `observed`, `not_disclosed`, `not_applicable`, `extraction_failed`, or `unclear`, with a reason and evidence pages where applicable. An empty object list MUST NOT imply task completion.

#### Scenario: Required product table was not read
- **WHEN** a required product performance table is known to exist but its page was omitted or unreadable
- **THEN** the relevant CoverageResult is `extraction_failed`
- **AND** the profile cannot report that task as complete

#### Scenario: Counterparty totals contain no names
- **WHEN** the complete section reports top-five totals but no counterparty names
- **THEN** name coverage is `not_disclosed` while concentration Measurements may be observed
- **AND** no Relationship is invented from concentration alone

### Requirement: Researcher projection exposes facts uncertainty evidence and downstream boundaries
The system MUST provide a stable research-facing projection grouped into business overview, regime/package, segments, activities, operating measurements, disclosed inputs and counterparties, business events, coverage, and evidence references. The projection MUST distinguish reported facts, deterministic derivations, and research assumptions. Commodity exposure and value-chain sections MUST explicitly report `not_authorized`, `not_assessed`, `insufficient_evidence`, or approved bounded results rather than infer direction or complete chain position.

#### Scenario: Manufacturing profile is rendered
- **WHEN** validated manufacturing/materials records for one report are projected
- **THEN** a researcher can read the original business overview, product revenue/cost/margin, applicable capacity/production/sales/inventory, disclosed inputs/relationships, regime, and coverage state
- **AND** each displayed fact links to source-native value and Evidence

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
