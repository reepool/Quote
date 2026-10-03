# repair-company-profile-service-operating-energy-roles Specification

## Purpose

The v6 reread of frozen `600008.SH` and `600019.SH` is recall 14/14 and accuracy 16/16, with critical numeric errors 0. Page 17 now delivers sewage electricity use by 运营端 and incineration diesel use by the company, both as energy consumption with no quantity. The principal sentences and the manufacturing roles from v5 remain. The measured run took 4.37 seconds and used 0 tokens. The expansion gate is true for this two-company review. Production stays not authorized and scale quality is not claimed. The v5 12/12 and 14/14 review and older observations stay unchanged. The next unseen batch was not frozen.

## Requirements

### Requirement: Service operating energy is delivered from the source clause
The repair MUST select the page that states 电耗 or 柴油单耗 and deliver each as `energy_consumption` through the existing acceptance, query, and export path. The source subject and business column MUST stay on the delivered fact. `600008.SH` page 17 binds 电耗 to 运营端 and 污水, and 柴油单耗 to 公司 and 焚烧业务. When the sentence gives no quantity, the measurement list MUST stay empty. 药耗, 上网电量, and 燃料成本 MUST NOT become energy consumption.

#### Scenario: Page 17 electricity and diesel are traceable
- **WHEN** page 17 states that sewage electricity use and incineration diesel use declined
- **THEN** query and export each show an energy-consumption role for 电耗 and 柴油单耗
- **AND** the evidence page is 17 and the quote contains the source clause
- **AND** neither role carries a quantity

### Requirement: Price risk, project capability, and subsidiaries stay unbound
A price-risk sentence MUST NOT become procurement. Project scale or operating capability MUST NOT become product sales. A subsidiary's diesel use MUST NOT become the parent company's role. The v5 principal sentence and the accepted manufacturing roles MUST remain.

#### Scenario: The same page does not invent sales or parent diesel use
- **WHEN** the page also states treatment scale, project operating capability, a price adjustment, an ECO disposal, a power price risk, and a subsidiary diesel figure
- **THEN** those statements do not add a product-sales role, a power procurement role, or a second diesel role
- **AND** the v5 identity does not adopt the new energy roles
