## ADDED Requirements

### Requirement: Core answers state the business, the product series, and the revenue structure
`600022.SH` page 9 MUST deliver the steel-business sentence through the five product series in the accepted overview, the query excerpt, and the export. A thickness or width specification MUST stay with its product line and MUST NOT replace that series. `600009.SH` MUST keep 航空性收入 and 非航空性收入 together with their child lines, including a label that wraps before 服务收入. The revenue answer MUST show that structure.

#### Scenario: The steel sentence and the airport revenue table are readable end to end
- **WHEN** page 9 states the five series and a later sentence states a thickness range
- **AND** the airport table wraps 旅客及货邮航空服务收入
- **THEN** the accepted record, query, and export contain the five series
- **AND** the revenue answer contains both parent lines and the wrapped child

### Requirement: Commodity roles follow the local clause or table row
Page 25 MUST bind iron-ore domestic procurement, iron-ore imports, and scrap procurement as separate rows. Sales MUST keep the page 61 revenue-source sentence and the page 182 production-and-sales sentence. A resume, an audit procedure, and a parent company's business nature MUST NOT become company sales. An airport associate fuel company MUST remain its own subject. Previously accepted named roles MUST remain.

#### Scenario: Valid sales stay and the excluded clauses do not
- **WHEN** the pages contain the revenue-source sentence, the production-and-sales sentence, an audit confirmation, a resume, and a parent business-nature row
- **THEN** only the two valid sales clauses become steel sales
- **AND** the supply table yields three separate procurement bindings
