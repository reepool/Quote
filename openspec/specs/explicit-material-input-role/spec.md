# explicit-material-input-role Specification

## Purpose
When an official report names a material as the company's own production or operating input, common-core activates the existing material chapter and delivers a `material_input` relationship with `role=raw_material_input`. The current full processing identity keeps `owned_page_facts=v8` and adds `material_input_facts=v1`. Passing the fixed two-company gate does not authorize the next expansion, a scale-quality claim, or production.

## Requirements

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
The owner MUST NOT activate material input for a price-risk sentence that does not say the named material is a company input, for a generic label such as direct material cost or raw-material inventory, or for a balance-sheet amount. The named material and the company's own input MUST be bound in the same sentence or an explicitly owned upstream-material table column. A purchase, consumption, or input verb whose subject is a customer, supplier, or downstream party MUST NOT activate the chapter. A principal-raw-material list MUST NOT activate only because 制造 or 生产 appears later in a fixed window. Sales evidence alone MUST NOT support `raw_material_input`. Energy input MUST keep the existing `energy_consumption` role. The projection MUST NOT derive a profit direction, price sensitivity, or net exposure from a price-risk sentence.

#### Scenario: Price risk without a company input is refused
- **WHEN** the text only says raw-material prices may rise and does not state that a named material is a company input
- **THEN** no `raw_material_input` relationship is emitted

#### Scenario: Generic cost or inventory amounts are refused
- **WHEN** the text only says direct material cost or raw-material inventory, or only gives a balance-sheet amount
- **THEN** no `raw_material_input` relationship is emitted

#### Scenario: Sales evidence alone does not create an input role
- **WHEN** the only evidence is that the company sells a named product
- **THEN** that evidence does not emit `raw_material_input`

#### Scenario: A third party purchasing for production is not the company's input
- **WHEN** the sentence says a customer, supplier, or downstream party purchases a named material for production
- **THEN** the material chapter stays inactive
- **AND** no `raw_material_input` relationship is emitted

#### Scenario: Selling principal materials to manufacturers is not an input
- **WHEN** the company sells named principal raw materials and the customers are manufacturers
- **THEN** nearby manufacturing words do not emit `raw_material_input`

#### Scenario: A price move affecting downstream manufacturers is not an input
- **WHEN** a principal-raw-material price sentence says the move affects downstream manufacturers
- **THEN** no `raw_material_input` relationship is emitted

#### Scenario: Independent sales and input evidence keep both roles
- **WHEN** one accepted fact says the company sells a source-native commodity and a separate accepted fact says that same commodity is a production input
- **THEN** the delivery keeps both `product_sales` and `raw_material_input`
- **AND** neither role overwrites the other or is netted away

#### Scenario: Energy keeps its existing role
- **WHEN** the explicit input is an energy consumption already recognized by the existing energy rule
- **THEN** the role remains `energy_consumption`
- **AND** it is not relabeled `raw_material_input`

#### Scenario: Price risk does not become sensitivity
- **WHEN** a sentence discusses raw-material price movement
- **THEN** the delivery does not add a profit direction, price sensitivity, or net exposure

### Requirement: Upstream material use does not assert external purchase
An owned current upstream-material column MUST use the existing material chapter and emit `Relationship(relation_type=material_input)`, retaining source name, subject, period, column and Evidence. Input use alone MUST NOT emit `Activity(action=purchases)`. External purchase MUST require independently stated purchase wording or an external-purchase column. Energy MUST retain its existing consumption semantics. Independent sale, material-input and external-purchase facts for the same native name MUST coexist without overwriting or netting. Third-party and planned inputs MUST remain refused. Accuracy review MUST verify each underlying action or relation against its own source; matching a commodity role alone MUST NOT establish fact accuracy.

#### Scenario: An upstream item is used internally
- **WHEN** the owned current table names a material as an upstream input without external-purchase disclosure
- **THEN** the material chapter delivers its material_input Relationship and raw_material_input role without a purchases assertion from that column

#### Scenario: Another source explicitly states external purchase
- **WHEN** a separate owned table states external purchase of the same named material
- **THEN** its purchase fact is retained independently of the input relationship and sales facts, while energy retains consumption semantics

#### Scenario: A correct role has an unsupported purchase action
- **WHEN** an accepted input role derives from a purchases fact supported only by an upstream-use column
- **THEN** the underlying fact fails accuracy despite the role-name match

### Requirement: Unmapped named inputs still deliver the role
An explicit named input MUST still deliver the Relationship and CommodityExposure when the existing catalog has no match or more than one candidate. The exposure MUST keep `source_native_name`. No match MUST use `mapping_status=pending` and `commodity_id=null`. Multiple candidates that cannot be chosen uniquely MUST use `mapping_status=ambiguous` and `commodity_id=null`. A non-empty unique `commodity_id` MUST be allowed only when `mapping_status=mapped`. Mapping failure MUST NOT drop the established input role. This change MUST NOT build a new catalog or market-series link.

#### Scenario: No catalog match stays pending
- **WHEN** an explicit named input has no catalog match
- **THEN** the Relationship and CommodityExposure are still delivered
- **AND** `source_native_name` is preserved
- **AND** `mapping_status` is `pending`
- **AND** `commodity_id` is null

#### Scenario: Ambiguous catalog candidates are not guessed
- **WHEN** an explicit named input matches more than one catalog candidate and no unique choice is stated
- **THEN** the input role is still delivered
- **AND** `mapping_status` is `ambiguous`
- **AND** `commodity_id` is null

### Requirement: Energy read categories do not rewrite underlying transaction actions
Separate current purchase and actual-use columns MUST remain separate source-grounded facts. An explicitly disclosed energy purchase MUST retain Activity(action=purchases), its original actor, native column and Evidence even when the existing read projection uses energy_consumption. That read category MUST NOT establish consumption or material_input. Actual self-use MUST require its own source wording or use column and retain Relationship(relation_type=material_input) with the existing energy_consumption role. The same native good MAY retain affirmative sales, purchases and actual input independently; each underlying action or relationship MUST be verified against its own source. Negative or planned transaction branches MUST NOT create current facts while independently affirmative branches remain valid.

#### Scenario: Energy purchase and consumption columns coexist
- **WHEN** an official current table separately discloses energy 采购量 and 耗用量
- **THEN** acceptance, query and export preserve a purchases Activity and a separate material_input Relationship without converting either because they share an energy read category

### Requirement: Actual named application establishes input without inferring purchases
Explicit current application of named materials in production MUST use the existing material_input Relationship with original actor, native name, period and Evidence. A separate named purchases Activity MUST require its own purchase wording or current purchase column. Neither purchase nor production alone MUST establish actual use, and negated or planned application MUST NOT create current input relationships. Named subsidiary sales and independently disclosed input roles MAY coexist without duplicate role recall.

#### Scenario: Current material application and a separate box-board purchase coexist
- **WHEN** official source states actual application of 意杨 and 竹材 and separately discloses 箱板 purchases
- **THEN** acceptance, query and export preserve two material_input relationships and a purchases Activity without converting box-board purchase into input or actual application into external purchase

### Requirement: Named actors and affirmative directions govern current commodity actions
Named subsidiary actions MUST keep their native actor and affirmative current direction. Purchase and actual manufacturing or energy input MUST require separate source support; purchases MUST NOT imply material_input,and material_input MUST NOT imply external purchases. Product enumeration MUST end before downstream applications; supplier names containing sales MUST NOT become issuer sales. Known negative,planned,third-party and application-only wording MUST remain bounded while same-page valid actions remain.

#### Scenario: A supplier name contains sales
- **WHEN** a purchase table names an automobile sales company and fixed assets
- **THEN** the issuer purchase direction remains and no fragment of the supplier name becomes a sales Activity

#### Scenario: A product description continues into electronic applications
- **WHEN** the actual chemical product is followed by semiconductor or display application wording
- **THEN** the original product and subsidiary are retained without creating sales objects from the downstream applications
