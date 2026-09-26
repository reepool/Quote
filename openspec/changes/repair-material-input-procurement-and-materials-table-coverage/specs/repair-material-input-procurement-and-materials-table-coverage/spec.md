## ADDED Requirements

### Requirement: Company purchase rows can name a material input
A row MUST be eligible for `extract_material_inputs` only when its table heading, transaction direction, and row content together show that the report subject purchases a raw material. A named material stated in the raw-material cell or in parentheses MAY become a `material_input` relationship. The rule MUST NOT hard-code an instrument id, a page number, or a material name. A sales row, a service row, a generic purchase amount with no material name, or a row whose buyer cannot be determined MUST NOT create an input role. The Ningde Times page 73 related-party purchase table is an acceptance fixture for this rule, not part of the rule.

#### Scenario: A related-party purchase names the company's raw materials
- **WHEN** a table says the report subject purchases raw materials and the row names those materials in the cell or in parentheses
- **THEN** each named material may be delivered as a `material_input` relationship
- **AND** the delivery does not depend on one instrument id, page, or material name

#### Scenario: Sales, services, generic amounts, and unclear buyers stay refused
- **WHEN** the row records a sale, a provided service, only a generic purchase amount, or does not show that the report subject is the buyer
- **THEN** no `material_input` relationship is created from that row

### Requirement: A materials-and-energy table admits raw-material rows only
A formal “主要原材料及能源” table MUST contribute a material input only for a row that is explicitly a raw material. An independent company purchase or consumption row for a named material, including 丁酮肟, MAY create an input fact. Energy rows, including 蒸汽 and 电, MUST stay excluded. An outsourced-processing arrangement by itself MUST NOT create an input role, even when the processed object is the same source-native name.

#### Scenario: A consumed raw-material row is eligible
- **WHEN** the table lists a named raw material as purchased or consumed by the company
- **THEN** that row may support a `material_input` fact
- **AND** the fact does not require a quantity

#### Scenario: Energy rows are excluded
- **WHEN** the same table lists 蒸汽, 电, or another energy row
- **THEN** that row is not recorded as `raw_material_input`

#### Scenario: Toll processing remains a separate disclosure
- **WHEN** the report only says an outside party processes a named material supplied by the company
- **THEN** that arrangement does not create a `material_input` fact
- **AND** a separate purchase or consumption row for the same name may still be eligible

### Requirement: The repair reuses the isolated material-input path
The repair MUST reuse the existing `extract_material_inputs` chapter, Evidence preparation, Stage 5 extract, repair, and verify, and the research isolation bundle. Output disposition MUST remain `accepted_for_review`. Sales evidence alone, generic direct-material cost, and raw-material inventory amounts MUST keep their existing refusals. Legal-empty, unclear, and extraction failure MUST stay distinct and MUST NOT be rewritten as a zero, a guessed commodity id, or a successful fact. The same source-native name MAY keep both a sales role and an input role without netting. A pending or ambiguous catalog mapping MUST keep an established input relationship and MUST NOT carry a commodity id.

#### Scenario: A repaired run stays research-only
- **WHEN** the two disclosure forms are implemented
- **THEN** the candidates stay in an isolated research bundle with disposition `accepted_for_review`
- **AND** common-core identity, publication, closure, completed mode, and production authorization remain unchanged

### Requirement: The archived 19/23 observation stays untouched
This change MUST NOT overwrite or rewrite the 2026-09-26 archived dossiers, enqueue file, run file, bundle result, or source review. That observation remains source recall 19/23, source accuracy 19/19, critical numeric errors 0, and `expansion_gates_met=false`. A later recount MUST derive its own recall, accuracy, critical numeric errors, and gate from the new run. This change MUST NOT presume the result is 23/23 and MUST NOT presume the gate passes. It MUST NOT repair the bitumen catalog alias, enable the six-chapter manufacturing package, or open operating quantities, counterparties, DCF, trading, or the legacy writer.

#### Scenario: The historical observation remains the 19/23 record
- **WHEN** this change is reviewed or later implemented
- **THEN** the archived source review still records recall 19/23, accuracy 19/19, critical numeric errors 0, and `expansion_gates_met=false`
- **AND** no new recall or gate value is written before an independent recount

### Requirement: Scope review blocks implementation
No Python change, Evidence-plan change, enqueue, replay, or source-review write is authorized before independent scope review accepts the two disclosure forms, their refusals, and the archived-observation boundary. The change MUST NOT start another expansion or authorize production before that review.

#### Scenario: Scope review has not passed
- **WHEN** task 1.1 is still unchecked
- **THEN** implementation, enqueue, replay, and source review are refused
- **AND** no repaired recall or gate result is written in advance
