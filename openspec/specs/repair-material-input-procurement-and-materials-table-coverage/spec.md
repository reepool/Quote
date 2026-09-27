# repair-material-input-procurement-and-materials-table-coverage Specification

## Purpose

This capability admits two company-owned material disclosures into the existing `extract_material_inputs` chapter: a purchase row proved by its heading, transaction direction, and row content, and a raw-material row in a formal “主要原材料及能源” table. The run stays research-only at `accepted_for_review`. The closed observation for the three frozen 2025 reports, plan `manufacturing_materials_stage4_material_inputs.2026-09-26.2`, and this single chapter is source recall 23/23, source accuracy 23/23, critical numeric errors 0, and `expansion_gates_met=true`. That true gate covers only this slice. It does not overwrite the archived stage-4 19/23 observation, does not absorb the fixed two-company 9/9, and does not authorize scale quality or production.

## Requirements

### Requirement: Company purchase rows can name a material input

A row MUST be eligible for `extract_material_inputs` only when its table heading, transaction direction, and row content together show that the report subject purchases a raw material. When those three conditions hold, each named material stated in the raw-material cell or in parentheses MUST be delivered as a `material_input` relationship. The rule MUST NOT hard-code an instrument id, a page number, or a material name. A word such as 服务 in a compound table heading MUST NOT reject the whole table. Each row MUST be judged by its own transaction direction. A sales row, a true service row, a generic purchase amount with no material name, or a row whose buyer cannot be determined MUST NOT create an input role. The Ningde Times page 73 related-party purchase table is an acceptance fixture for this rule, not part of the rule.

#### Scenario: A related-party purchase names the company's raw materials

- **WHEN** the table heading, transaction direction, and row content together show that the report subject purchases raw materials and the row names those materials in the cell or in parentheses
- **THEN** each named material MUST be delivered as a `material_input` relationship
- **AND** the delivery does not depend on one instrument id, page, or material name

#### Scenario: A compound heading does not reject purchase rows

- **WHEN** the table heading also contains a word such as 服务 and a specific row shows that the report subject purchases a named raw material
- **THEN** that purchase row MUST still be delivered as a `material_input` relationship
- **AND** a different row that is only a provided service MUST NOT create an input role

#### Scenario: Sales, services, generic amounts, and unclear buyers stay refused

- **WHEN** the row records a sale, a provided service, only a generic purchase amount, or does not show that the report subject is the buyer
- **THEN** no `material_input` relationship is created from that row

### Requirement: A materials-and-energy table admits raw-material rows only

A formal “主要原材料及能源” table MUST contribute a material input only for a row that is explicitly a raw material. A named raw-material row that the company purchases or consumes MUST be delivered as a `material_input` fact, including 丁酮肟 when that name appears in such a row. Absence of a quantity MUST still allow that delivery. Energy rows, including 蒸汽 and 电, MUST stay excluded. An outsourced-processing arrangement by itself MUST NOT create an input role, even when the processed object is the same source-native name.

#### Scenario: A consumed raw-material row is delivered

- **WHEN** the table lists a named raw material as purchased or consumed by the company
- **THEN** that row MUST be delivered as a `material_input` fact
- **AND** the delivery remains allowed when the row states no quantity

#### Scenario: Energy rows are excluded

- **WHEN** the same table lists 蒸汽, 电, or another energy row
- **THEN** that row is not recorded as `raw_material_input`

#### Scenario: Toll processing remains a separate disclosure

- **WHEN** the report only says an outside party processes a named material supplied by the company
- **THEN** that arrangement does not create a `material_input` fact
- **AND** a separate purchase or consumption row for the same name MUST still be delivered when that row names the material as the company's own raw-material purchase or consumption

### Requirement: The repair reuses the isolated material-input path

The repair MUST reuse the existing `extract_material_inputs` chapter, Evidence preparation, Stage 5 extract, repair, and verify, and the research isolation bundle. Output disposition MUST remain `accepted_for_review`. Sales evidence alone, generic direct-material cost, and raw-material inventory amounts MUST keep their existing refusals. Legal-empty, unclear, and extraction failure MUST stay distinct and MUST NOT be rewritten as a zero, a guessed commodity id, or a successful fact. The same source-native name MAY keep both a sales role and an input role without netting. Pending and ambiguous catalog mappings affect only the catalog mapping. They MUST NOT block an established input relationship, and they MUST NOT carry a commodity id.

#### Scenario: A repaired run stays research-only

- **WHEN** the two disclosure forms are implemented
- **THEN** the candidates stay in an isolated research bundle with disposition `accepted_for_review`
- **AND** common-core identity, publication, closure, completed mode, and production authorization remain unchanged

#### Scenario: Pending or ambiguous mapping does not block the relationship

- **WHEN** an established input relationship has a pending or ambiguous catalog mapping
- **THEN** the relationship remains delivered
- **AND** its commodity id stays null

### Requirement: The archived 19/23 observation stays untouched

This capability MUST NOT overwrite or rewrite the 2026-09-26 archived dossiers, enqueue file, run file, bundle result, or source review. That observation remains source recall 19/23, source accuracy 19/19, critical numeric errors 0, and `expansion_gates_met=false`. The fixed two-company 9/9 observation is not part of this capability's denominator. This capability MUST NOT repair the bitumen catalog alias, enable the six-chapter manufacturing package, or open operating quantities, counterparties, DCF, trading, or the legacy writer.

#### Scenario: The historical observation remains the 19/23 record

- **WHEN** this capability is cited
- **THEN** the 2026-09-26 archived source review still records recall 19/23, accuracy 19/19, critical numeric errors 0, and `expansion_gates_met=false`
- **AND** the fixed two-company 9/9 observation stays outside this denominator

### Requirement: The closed repair observation is recall 23/23 for this slice only

The authoritative observation is the source review archived at `openspec/changes/archive/2026-09-27-repair-material-input-procurement-and-materials-table-coverage/replay/20260926.2/source_review.json`, SHA-256 `b6aec33bcf7402e4d880a19f232bc1f7001e2c90793b900136e6fcf3cab6f6f9`. It MUST record source recall 23/23, source accuracy 23/23, critical numeric errors 0, and `expansion_gates_met=true`. That true gate covers only the three frozen 2025 reports `300750.SZ`, `603659.SH`, and `920015.BJ`, plan `manufacturing_materials_stage4_material_inputs.2026-09-26.2`, and `extract_material_inputs`. It MUST NOT be read as a cross-industry scale-quality claim or a production authorization. `scale_quality_claim_allowed` MUST remain false and `production_authorization` MUST remain `not_authorized`.

#### Scenario: The recount stays bound to the .2 replay files

- **WHEN** the repair observation is cited
- **THEN** it stays bound to enqueue SHA-256 `4152290506bdb18f0aa6d48187c54243bca4ca7c9249fe927723636608995ea8`, run SHA-256 `dbaa0aef4edf1a5f516f55c7349b8cbb82be900f0d8df1a8a051d04bc3eadf87`, result SHA-256 `d4c81a5e554c01c619e89885f1fac304e09688c582a603919256cf0a7947ff51`, and source-review SHA-256 `b6aec33bcf7402e4d880a19f232bc1f7001e2c90793b900136e6fcf3cab6f6f9`
- **AND** recall remains 23/23, accuracy remains 23/23, critical numeric errors remain 0, and `expansion_gates_met` remains true only for this slice

#### Scenario: The slice gate does not authorize scale quality or production

- **WHEN** the three-report material-input recount meets recall 1, accuracy 1, and critical numeric errors 0
- **THEN** `expansion_gates_met` is true only for those three reports, the `.2026-09-26.2` plan, and `extract_material_inputs`
- **AND** the archived 19/23 observation and the fixed two-company 9/9 observation stay independent
- **AND** scale quality and production stay unauthorized

### Requirement: Scope review blocks implementation

No Python change, Evidence-plan change, enqueue, replay, or source-review write is authorized before independent scope review accepts the two disclosure forms, their refusals, and the archived-observation boundary. The change MUST NOT start another expansion or authorize production before that review.

#### Scenario: Scope review has not passed

- **WHEN** task 1.1 is still unchecked
- **THEN** implementation, enqueue, replay, and source review are refused
- **AND** no repaired recall or gate result is written in advance
