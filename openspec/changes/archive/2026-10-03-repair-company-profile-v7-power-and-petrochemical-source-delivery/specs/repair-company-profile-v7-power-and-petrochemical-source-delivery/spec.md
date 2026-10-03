## ADDED Requirements

### Requirement: Principal and product answers use the company source sentence
The repair MUST select and project Huaneng's full “公司的主要业务是” sentence and Sinopec's page-4 “主要从事” description into the accepted record, the query text, and the export. A subsidiary business column and an accounting-policy paragraph MUST NOT replace that source. The answer MUST NOT be a truncated clause or the bare word “销售”.

#### Scenario: Both company sentences survive export
- **WHEN** the Huaneng overview and the Sinopec company profile are in the automatically selected pages
- **THEN** query and export contain the full business sentences
- **AND** the subsidiary table and the accounting policy are not the principal answer

### Requirement: Petrochemical revenue keeps the current-period industry composition
The repair MUST recognize a wrapped in-column unit “人民币百万元” and read the current-period revenue column. The revenue answer MUST retain 勘探及开发, 炼油, 营销及分销, and 化工. An elimination row and the total MUST keep those meanings and MUST NOT become industry answers.

#### Scenario: The wrapped million-yuan table is the revenue answer
- **WHEN** the industry header wraps “人民币百万元” and the rows include an elimination and a total
- **THEN** the exported revenue answer lists the four businesses
- **AND** the first amount is the current-period column in 百万元

### Requirement: Commodity roles keep the stated subject, quantity, and sales scope
Coal procurement MUST keep 1.86 亿吨 as the purchased volume. Crude procurement MUST be delivered from the purchase sentence. Gasoline, diesel, and kerosene internal sales MUST keep the refining-division subject and the internal-sales scope. Huaneng's 96.37% MUST remain one combined power-and-heat sales share. Names absent from the catalog MUST stay pending. Previously accepted steel, airport-revenue, and energy roles MUST remain, and resume, audit-procedure, and third-party clauses MUST stay excluded.

#### Scenario: Volumes and internal sales are not rewritten
- **WHEN** the coal sentence states 1.86 亿吨 and the refining division internally sells gasoline, diesel, and kerosene
- **THEN** the coal fact uses 亿吨
- **AND** each internal sale names the refining division and the internal-sales scope
- **AND** power and heat share one 96.37% fact
