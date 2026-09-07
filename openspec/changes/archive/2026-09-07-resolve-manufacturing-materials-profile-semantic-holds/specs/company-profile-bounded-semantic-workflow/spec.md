## ADDED Requirements

### Requirement: Verification preserves one primary metric for a composite source-native label
Verification MUST evaluate a candidate against its requested checklist field, physical Evidence anchor, source meaning, and complete source-native label. A parenthetical or secondary source word MUST NOT require a second metric or cause `evidence_field_mismatch` when the same physical fact has one governed primary metric. This rule MUST NOT permit a candidate whose metric conflicts with the economic direction or a different table header.

#### Scenario: External processing volume includes a sales alias
- **WHEN** one Evidence anchor says `涂覆加工量（销量）` and the source describes an external processing service provided by the reporting business
- **THEN** verify may pass one `processing_volume` preserving that complete label
- **AND** it does not require or synthesize `sales_volume` from the same anchor

### Requirement: No-change disclosures use coverage rather than fabricated events
For the business-regime chapter task, the workflow MUST keep consolidation-scope changes separate from principal-business, product, or service change status. When complete Evidence explicitly reports no applicable major business change, the workflow MUST allow `business_regime` coverage with status `not_applicable` and source-supported reason; it MUST NOT fabricate a BusinessEvent from a cross-reference or merge the no-change statement into a separate consolidation event.

#### Scenario: Business change field says not applicable
- **WHEN** the requested business-change field is complete and explicitly marked not applicable
- **THEN** the workflow may complete that checklist obligation with `not_applicable` coverage and Evidence
- **AND** no event is required merely to avoid an empty record array

### Requirement: Material-input verification preserves explicit energy inputs
For `extract_material_inputs`, the governed `material_input` Relationship MUST accept both explicitly named raw-material inputs and explicitly named energy inputs when the Evidence identifies them as procured or consumed inputs. Verification MUST preserve the source-native item name and MUST NOT return `object_not_allowed` solely because the report classifies an item as energy. This rule MUST NOT relabel energy as raw material or authorize CommodityExposure inference.

#### Scenario: A source table lists steam and electricity as energy inputs
- **WHEN** a complete “主要原材料及能源” table explicitly lists steam or electricity as an input
- **THEN** verify may pass the source-supported `material_input` Relationship
- **AND** the result remains a research fact without inferred price sensitivity or production publication

### Requirement: Product-extension events are not overridden by non-regime no-change wording
In the `business_mode_and_extension` scope, an evidenced `product_extension` BusinessEvent MUST complete the `business_regime` checklist field through accepted observed coverage. A separate statement that the operating mode did not materially change MUST NOT create a BusinessRegime or a legal-empty coverage result that overrides the accepted event.

#### Scenario: A new product is supplied while the operating mode remains unchanged
- **WHEN** the Evidence reports a new product or service supply during the period and separately says the operating mode did not materially change
- **THEN** the workflow retains the product-extension event and derives observed coverage from that accepted event
- **AND** it does not add a conflicting `not_applicable` result for the same checklist field

### Requirement: Same-control comparative verification is column and knowledge-time aware
Verification MUST require `comparison_basis` for each explicitly restated comparative and MUST preserve reported period, knowledge time, and subject evidence per column. It MUST NOT treat a current-period value as restated merely because prior-year columns exist, and MUST NOT reject a valid restated column solely because its value differs from the predecessor's original annual report.

#### Scenario: Adjusted and pre-adjustment columns coexist
- **WHEN** Evidence includes clearly labelled adjusted and pre-adjustment comparative columns
- **THEN** verify checks each candidate against its own column label and comparison basis
- **AND** it blocks any instruction to overwrite the predecessor original-as-published fact
