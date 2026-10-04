## ADDED Requirements

### Requirement: Application enumerations never become company operations
A "主要产品为 X,用于 A、B、C" sentence MUST project X as the product and MUST NOT project A, B, or C as company activity records. The rotary-drill record and its application text stay in the evidence, and the three dimension answers, the valid construction-machinery business, the five roles, and all numeric rows remain queryable and exportable.

#### Scenario: The seven application records disappear
- **WHEN** the Sany overview states 主要产品为旋挖钻机，用于市政建设、公路桥梁……等基础施工
- **THEN** no Activity record names an application area with a company action
- **AND** the rotary-drill product and every numeric row still export

### Requirement: Bindings require the same sentence and an affirmative action
The raw-material, hedge, and toll bindings MUST find the commodity name inside the sentence that states the declaration, and that sentence MUST carry an affirmative action: no 未/拟/计划 prefix on the hedge or toll verb, no 客户 or 第三方 subject on the raw-material or toll sentence, and no third-party collection wording in the toll-service capture. Table records MUST keep a narrower local scope when the table excerpt shows 母公司 wording instead of reading the whole-passage group subject. Real positives keep the steel dual roles, empty quantities, printed columns, and original amounts.

#### Scenario: The reproduced counterexamples are refused
- **WHEN** an excerpt states customer steel, a not-yet or planned futures business, a commodity name in another sentence, or a third-party or unformed toll
- **THEN** no company role, revenue, or service answer is bound from it
- **AND** a parent-company table under group narrative is not lifted to the group subject

### Requirement: The v12 round is reviewed fact by fact
The rerun MUST use the new `revenue_sentence_repair=v12` identity, its own directory, and the same two frozen reports, executing normally through query and export. The review MUST check every answer and accepted fact for subject, action, object, column, value, and unit, rebuild the denominator, and not prefill scores. Gates stay recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 shared tokens, and at most 300 measured seconds. The v11 observation is untouched and cannot justify expansion.

#### Scenario: Every record carries local support
- **WHEN** the v12 review is recorded
- **THEN** each accepted record names the sentence that supports its subject, action, and object
- **AND** the gates are computed from the honest counts
