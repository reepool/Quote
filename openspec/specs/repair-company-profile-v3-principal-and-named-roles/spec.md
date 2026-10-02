# repair-company-profile-v3-principal-and-named-roles Specification

## Purpose

The v4 reread of frozen `600008.SH` and `600019.SH` is recall 12/12 and accuracy 14/15, with critical numeric errors 0. The measured run took 4.43 seconds and used 0 tokens. Accuracy counts each delivered record once. The inaccurate record is a steel-sales association on page 201, taken from an investee business-nature table. `expansion_gates_met` is false. Production stays not authorized and scale quality is not claimed. The prior 6/11 review and older observations stay unchanged.

## Requirements

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
- **WHEN** a sentence only contains “分销售模式”, “销售区域”, or “销售部”
- **THEN** that sentence does not add a steel sales role
- **AND** the page 15 production-and-sales row for “其他钢铁产品” remains a sales role
- **AND** scrap is taken from page 24 “国内采购”, not from internal self-supply
- **AND** page 69 energy medium keeps both sales and procurement under that source name, without splitting a mixed amount
