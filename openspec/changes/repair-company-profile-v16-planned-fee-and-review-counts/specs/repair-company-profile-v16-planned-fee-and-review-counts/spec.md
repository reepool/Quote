## ADDED Requirements

### Requirement: Planned collection vetoes the whole sentence
A sentence in which any clause pairs 尚未/拟/计划/预期/将 with 收取 or 形成 MUST NOT answer company revenue, even when an earlier clause states the service with the company subject. The free add-on clause keeps its clause-level scope, and the five existing counterexamples and both positives keep their current outcomes.

#### Scenario: The service clause cannot borrow a planned collection
- **WHEN** the Rizhao-shaped overview states 公司的经营模式主要为：为客户提供……服务，计划收取货物堆存费……
- **THEN** the revenue gate refuses the sentence
- **AND** the free add-on positive and the other counterexamples keep their outcomes

### Requirement: The fee mechanism maps to the unique overview record and scoring is one-to-one
The review MUST map the fee-mechanism finding to the single Rizhao Overview record and verify its complete text, MUST NOT carry a separate carrier finding for that record, and MUST score accuracy over the six dimension answers plus each accepted fact exactly once (28 objects). The rerun MUST use the new `revenue_sentence_repair=v17` identity, its own directory, and the same frozen plan, with the gates at recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 shared tokens, and at most 300 measured seconds. The main spec MUST be updated with the final planned-collection behavior and the limited-acceptance conclusion, and the v16 snapshot is kept byte-for-byte.

#### Scenario: The Rizhao overview is counted once
- **WHEN** the v17 review is recorded
- **THEN** the accuracy denominator is 28 with no duplicate carrier finding for the Rizhao overview
- **AND** the main spec carries the planned-collection behavior and the limited-acceptance conclusion
