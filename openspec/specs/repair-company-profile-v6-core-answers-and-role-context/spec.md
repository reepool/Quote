# repair-company-profile-v6-core-answers-and-role-context Specification

## Purpose

The v7 reread of frozen `600009.SH` and `600022.SH` is recall 10/10 and accuracy 11/11, with critical numeric errors 0. The steel answer now includes the two bases and the five product series. Airport revenue now lists aviation and non-aviation income with their child lines, including the wrapped passenger-and-cargo line. Iron-ore domestic procurement, iron-ore imports, and scrap are separate rows. Steel sales remain on pages 61 and 182; the repeated sale counts only in accuracy. Resumes, audit procedures, and the parent company's business nature are absent. `600009.SH` has a checked negative for company commodity role. The measured run took 2.44 seconds and used 0 tokens. The expansion gate is true for these two reports. Production stays not authorized and scale quality is not claimed. The prior 7/9 and 5/11 review was not rewritten, and the next unseen batch was not frozen.

## Requirements

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
