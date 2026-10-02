## ADDED Requirements

### Requirement: The repair stays closed until this change is accepted
Before implementation starts, the change MUST only contain its scope documents. It MUST NOT change Python. It MUST NOT run, query, or export a repair. It MUST NOT modify the 6/11 review or any older source review. It MUST NOT authorize production or scale quality.

#### Scenario: Writing this change does not start a repair run
- **WHEN** the change documents are created
- **THEN** no new repair snapshot exists
- **AND** the `600008.SH` and `600019.SH` review remains recall 6/11, accuracy 10/15, and critical numeric errors 0

### Requirement: A company business sentence is kept without requiring 主要从事
The overview projection MUST accept a sentence in the business section that states the reporting company's own business with “业务覆盖” or “专注于”. `600008.SH` page 10 and `600019.SH` page 9 MUST become sourced principal answers. A third party's business MUST NOT become the company's answer.

#### Scenario: Both principal sentences survive
- **WHEN** the two frozen reports are projected with the repair marker
- **THEN** each principal dimension quotes that company's own sentence
- **AND** the missing reason `no_accepted_evidence` is not copied onto the repair record

### Requirement: Sales mode, region, and department are not sales actions
A commodity sales role MUST NOT be created from “分销售模式”, “销售区域”, or “销售部”. The sentence “销售钢铁产品” and the sentence stating imported iron-ore procurement MUST remain. Scrap in the company's supply table and energy medium in the company's related-party sales or procurement row MUST be delivered under those source names. An unmapped name MUST stay pending.

#### Scenario: False steel sales stay out and the named gaps stay in
- **WHEN** page 15, 35, 42, 54, or 201 is projected
- **THEN** those pages do not add a steel sales role
- **AND** scrap procurement and energy-medium sales and procurement are queryable with their sources
