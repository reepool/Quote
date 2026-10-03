## ADDED Requirements

### Requirement: The chemical feedstock oil internal sale is delivered
The p26 business-composition sentence MUST deliver 化工原料油 as an explicit role with the refining-division subject, the 部分 qualifier, the internal-sales scope, and 化工事业部 as receiver. No disclosed quantity MUST stay an empty value, and a name absent from the catalog MUST stay pending. The record MUST pass acceptance and appear in query and export.

#### Scenario: The fifth internal sale reaches export
- **WHEN** the refining-division sentence states 部分化工原料油内部销售给化工事业部
- **THEN** query and export carry the record with 炼油事业部, the 部分 internal-sales scope, and 化工事业部
- **AND** the value stays empty and the exposure stays pending

### Requirement: The wrapped product row is restored without losing industry revenue
The in-table label join MUST reattach a wrapped label that contains in-column spaces, restoring the product/电力及热力 row with its original amount and 元 unit. The reconciliation slot MUST separate column contexts, so the industry revenue Measurement stays accepted and delivered while the product record is added, and no occurrence_semantic_conflict is raised for either.

#### Scenario: Both column views deliver the same row
- **WHEN** the Huaneng combined table wraps 电 力 及 热／力 in the product column
- **THEN** industry and product Segment and Measurement records for 电力及热力 are all accepted
- **AND** no disposition carries occurrence_semantic_conflict for those records

### Requirement: Unified-identity isolated review with explicit mapping
Before observation, the review MUST fix its rules: source rows in different columns or dimensions are judged separately; the same fact restated across pages counts once; a Measurement repeating a Segment's value adds no recall but is still checked; every six-dimension answer, role, and accepted fact MUST map to at least one finding. The rerun MUST use the new `revenue_sentence_repair=v10` identity, its own directory, and the same two frozen reports. Elapsed time MUST cover the whole round; thresholds stay recall 100%, accuracy 100%, zero critical numeric errors, 300 seconds, and the shared 50000-token budget. Scores MUST NOT be prefilled, and the v9 observation stays untouched.

#### Scenario: Every delivered record is mapped
- **WHEN** the v10 review is recorded
- **THEN** each dimension answer, role, Segment, Measurement, and ordinary Activity has its own finding
- **AND** the recall denominator follows the fixed rules from the source reading
