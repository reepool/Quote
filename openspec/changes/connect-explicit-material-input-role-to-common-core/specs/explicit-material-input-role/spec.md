## ADDED Requirements

### Requirement: Explicit named inputs use the existing material path
When an official report states that a named material is an input to the company's production or operations, the existing `company_profile_common_core` owner MUST activate `ChapterTask.EXTRACT_MATERIAL_INPUTS` and route that disclosure through the existing Stage 5 provider, acceptance, and writer. The result MUST be a `material_input` Relationship and a `CommodityExposure` with `role=raw_material_input` inside the existing `company_facts` and `commodity_associations` read scope. The delivery MUST keep the source name, subject, report identity, period, and Evidence. Absence of a quantity or a market series MUST NOT block the relationship. The rule MUST NOT hard-code an instrument id, a page number, or a material name, and MUST NOT invent a named input from industry knowledge.

#### Scenario: A named production input is delivered
- **WHEN** an official sentence names a material and states that it is an input to the company's production or operations
- **THEN** the existing material chapter emits a `material_input` relationship for that source name
- **AND** the commodity read scope shows `raw_material_input` for that relationship
- **AND** the source name, subject, report identity, period, and Evidence remain on the delivery

#### Scenario: No quantity still delivers the relationship
- **WHEN** the same explicit input sentence does not state a quantity or a market series
- **THEN** the relationship and `raw_material_input` role are still delivered

#### Scenario: A service report with no explicit commodity role stays empty
- **WHEN** an official report does not state a named material as a company input
- **THEN** this change does not emit a `raw_material_input` exposure for that report

#### Scenario: An existing non-Dongfeng manufacturing fixture follows the same rule
- **WHEN** an existing manufacturing fixture other than the frozen vehicle report states a named company input
- **THEN** the same activation and role path accepts that input
- **AND** the rule does not depend on one instrument id

### Requirement: Non-explicit material wording stays refused
The owner MUST NOT activate material input for a price-risk sentence that does not say the named material is a company input, for a generic label such as direct material cost or raw-material inventory, for a balance-sheet amount, or for a product the company sells. Energy input MUST keep the existing `energy_consumption` role. The projection MUST NOT derive a profit direction, price sensitivity, or net exposure from a price-risk sentence.

#### Scenario: Price risk without a company input is refused
- **WHEN** the text only says raw-material prices may rise and does not state that a named material is a company input
- **THEN** no `raw_material_input` relationship is emitted

#### Scenario: Generic cost or inventory amounts are refused
- **WHEN** the text only says direct material cost or raw-material inventory, or only gives a balance-sheet amount
- **THEN** no `raw_material_input` relationship is emitted

#### Scenario: A sold product is not a purchased input
- **WHEN** the text names a product the company sells
- **THEN** that name is not delivered as `raw_material_input`

#### Scenario: Energy keeps its existing role
- **WHEN** the explicit input is an energy consumption already recognized by the existing energy rule
- **THEN** the role remains `energy_consumption`
- **AND** it is not relabeled `raw_material_input`

#### Scenario: Price risk does not become sensitivity
- **WHEN** a sentence discusses raw-material price movement
- **THEN** the delivery does not add a profit direction, price sensitivity, or net exposure
