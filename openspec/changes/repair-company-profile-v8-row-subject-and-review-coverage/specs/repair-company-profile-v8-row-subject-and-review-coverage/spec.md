## ADDED Requirements

### Requirement: Decimal continuation lines never become segment rows
The revenue-table reader MUST NOT treat a decimal amount prefix as row numbering. A line whose leading number is followed by another digit after the enumeration mark MUST NOT be stripped into a row label, and a gross-margin or year-over-year continuation line MUST NOT produce a Segment. The “中国境内” row MUST keep its stored figures, and “中国境外” MUST regain its 元 unit, its segment record, and its operating-revenue Measurement.

#### Scenario: The wrapped margin line is not a row
- **WHEN** a region table wraps “18.40 -6.29 -10.66 增加 4.00” onto the next line after the domestic row
- **THEN** no Segment named “40” or “18” is projected
- **AND** the overseas row keeps revenue 18,523,422,143 in 元 in query and export

### Requirement: Legal numbered revenue rows survive the tightening
Legal enumerated revenue classes and numbered parent-child revenue tables MUST still parse. The airport aviation and non-aviation parent rows with their child rows MUST remain delivered as before.

#### Scenario: Airport parent-child revenue is unchanged
- **WHEN** a numbered revenue table lists aviation and non-aviation parents with child rows
- **THEN** the parent and child rows keep their dimensions, headers, and amounts
- **AND** the existing directed tests pass unchanged

### Requirement: A subsidiary sentence is not the company principal
The principal-business reader MUST NOT capture a “主要业务是” sentence whose subject is a subsidiary. A “子公司的主要业务是” sentence MUST NOT become the company principal answer, and the company's own sentence MUST still be selected when present.

#### Scenario: Subsidiary wording is rejected
- **WHEN** the selected excerpt states “子公司的主要业务是……”
- **THEN** the principal answer is not that subsidiary sentence
- **AND** a company-own sentence in the same excerpt is still delivered in full

### Requirement: The million-yuan unit stays inside its own table
A wrapped “人民币百万元” column unit MUST apply to the table that states it. A later independent table in the same excerpt that declares no unit MUST NOT inherit the million-yuan unit, and rows in that table MUST NOT gain revenue Measurements from an inherited unit. A combined-table group that declares one unit MUST still share it across its subtables.

#### Scenario: The unitless second table stays unitless
- **WHEN** an excerpt holds a million-yuan first table and an independent second table without a unit declaration
- **THEN** first-table rows keep 百万元 and their Measurements
- **AND** second-table rows carry no inherited unit and no invented Measurement

### Requirement: Isolated redelivery with full-coverage review
The rerun MUST use the new `revenue_sentence_repair=v9` identity and its own directory, and MUST consume the same two frozen report references. Query and export MUST select the same identity. The review MUST rebuild recall from the source reports and MUST score every actually delivered answer, role, Segment, and Measurement; duplicate disclosures enter accuracy only. Elapsed time MUST be measured, thresholds MUST stay recall 100%, accuracy 100%, zero critical numeric errors, and at most 300 seconds. Scores MUST NOT be prefilled, and v7 or v8 observations MUST NOT be rewritten.

#### Scenario: Every delivered record is judged
- **WHEN** the v9 round is reviewed
- **THEN** the recall denominator is rebuilt from the frozen reports
- **AND** the accuracy denominator covers all delivered records including Segments and Measurements
- **AND** the fake segment absent from delivery is consistent with a miss-free result
