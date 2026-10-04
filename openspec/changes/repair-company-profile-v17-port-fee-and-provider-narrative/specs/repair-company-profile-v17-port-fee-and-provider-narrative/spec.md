## ADDED Requirements

### Requirement: The complete port fee sentence is delivered
The candidate extraction MUST keep 公司主语、服务与收费动作的 same-sentence continuity for the SIPG operating-model sentence, so the accepted record text, the query answer, and the export answer all contain 港口服务 plus 包干费、库场使用费 and 港口其他收费. The planned, negated, and third-party counterexamples and the free add-on positive keep their outcomes, and the PDF wraps of the real p12 stay covered.

#### Scenario: The fees reach the answers
- **WHEN** the SIPG overview states 公司的经营模式主要为：为客户提供港口及相关服务，收取港口作业包干费、库场使用费和港口其他收费
- **THEN** the accepted record text, the query answer, and the export answer all contain the service and the three fees
- **AND** the counterexamples and the free add-on positive keep their outcomes

### Requirement: The provider narrative delivers the principal and products substance
The projection MUST accept 作为……核心提供商，公司…… as a current-business narrative. The Wan Dong principal answer MUST carry the complete source sentence, and the principal and products substance MUST present 医学影像装备、智慧医疗解决方案 or clearly-sourced product categories. Vision, plans, and third-party positioning MUST NOT be promoted, and the existing segment rows stay correct. The card completes directed end-to-end validation without starting a formal round.

#### Scenario: The provider narrative answers
- **WHEN** the Wan Dong overview states 作为国产医学影像装备与智慧医疗解决方案的核心提供商，公司……
- **THEN** the principal answer carries the complete source sentence
- **AND** the products substance names the imaging equipment and solutions substance without promoting the vision or plans

### Requirement: The v18 round is reviewed one-to-one with complete negatives
The rerun MUST use the new `revenue_sentence_repair=v18` identity, its own directory, and the same two frozen reports, with the first genuine execution as the formal round and whole-round timing including retries. The review MUST map every dimension answer and every accepted fact — including Activities — to exactly one finding, MUST rebuild the denominator from the actual source and delivery, and MUST give each no-role negative its actual checked pages. Gates stay recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 shared tokens, and at most 300 measured seconds. The v17 unseen observation is kept byte-for-byte.

#### Scenario: Every object is mapped once
- **WHEN** the v18 review is recorded
- **THEN** the accuracy denominator equals the six answers plus all accepted facts with no omissions
- **AND** each no-role negative names the pages that were actually scanned
