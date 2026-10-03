## ADDED Requirements

### Requirement: Scoring and column criteria are fixed before the rerun
The review MUST, before observation: judge each answer's own text independently; map every delivered fact to exactly one finding; check Segment and Measurement records alike for subject, column, label, amount, and unit; and keep an explicit reasoned gap without counting it as a recall hit. The 600031 cross-page 分产品/分行业 print conflict MUST keep the printed column and judge by it consistently for both record kinds. The rerun MUST use the full new `revenue_sentence_repair=v11` identity, its own directory, and the same two frozen reports, with gates at recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 shared tokens, and at most 300 measured seconds.

#### Scenario: An explicit gap is not a recall hit
- **WHEN** a revenue answer is an explicit reasoned gap while the source discloses the composition
- **THEN** the finding records the gap and does not count the answer as recalled

### Requirement: Wutong delivers the toll service, the toll revenue, and the table amounts
The products answer MUST state the toll-service substance (为过往车辆提供通行服务,收取车辆通行费), and the revenue answer MUST state the toll mechanism and revenue sources instead of stopping at an explicit gap. The construction-service row, the 205 国道天长段新线 row, and the 建造期 row MUST be delivered alongside the existing rows, and the revenue Measurements MUST carry the 本集团 direct-source-wording subject so they pass acceptance. Digit-leading road labels, decimals, slashes, and wrapped labels keep their boundaries; negated sentences, third-party tolls, and narrow-subject tables MUST NOT be promoted.

#### Scenario: The blocked measurements are accepted
- **WHEN** the segment excerpt states 本集团 and the rows are projected
- **THEN** the Segment and Measurement records carry the direct-source-wording subject and are accepted
- **AND** query and export show the toll-service answer, the revenue answer, and every table row with amount and unit

### Requirement: Sany delivers the steel input and the four hedge underlyings
The p9 主要原材料 sentence MUST bind 钢材 as a raw_material_input, and the p25 期货业务 sentence MUST bind 钢材, 铜, 铝, and 原油 as four independent hedge_underlying records. Quantities MUST stay empty, catalog mapping follows the existing rules, the futures aggregate amounts MUST NOT be split across the four commodities, and the hedge wording MUST NOT be inferred as a physical purchase. Each record MUST pass acceptance and appear in query and export.

#### Scenario: Hedge roles stay independent and empty
- **WHEN** the p25 sentence lists 钢材、铜、铝、原油 as hedge underlyings
- **THEN** four independent hedge records are delivered with empty values
- **AND** no futures aggregate amount is attached to any of them
