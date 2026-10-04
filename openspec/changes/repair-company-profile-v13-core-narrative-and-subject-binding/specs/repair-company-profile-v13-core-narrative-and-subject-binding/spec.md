## ADDED Requirements

### Requirement: Business narratives deliver substantive principal and product answers
The projection MUST accept a 主要经营 sentence and a 作为……上市公司/工业企业……研发制造 narrative as overview sources. The Rizhao products answer MUST state 装卸、堆存、中转、运输代理、仓储, and the Zhongzhi products answer MUST state 直升机、运 12/运 12F 及相关制造服务; the principal answers carry the full source sentences. A heading or an industry label MUST NOT substitute for the answer text, and the complete text with its evidence MUST pass acceptance, query, and export.

#### Scenario: Both narratives answer
- **WHEN** the two overviews state 主要经营……装卸、堆存及中转业务 and 中直股份作为……研发制造……直升机……运 12 和运 12F
- **THEN** both principal answers carry the full source sentences
- **AND** both products answers carry the service and product substance with evidence

### Requirement: The port fee mechanism and the default group subject are wired
The Rizhao revenue answer MUST keep the same complete sentence relationship: the company provides the service and collects the 包干费, 堆存费, and other logistics fees. A 为客户提供…服务 positive passes, while customer or third-party collection and negated or planned sentences are refused. Numeric records without a narrower or conflicting local subject MUST adopt the report default group scope through the existing acceptance rules, and a narrower scope such as 母公司 still wins. Handled cargo types and the template hedge sentence MUST NOT generate commodity roles.

#### Scenario: The mechanism and the subjects are wired
- **WHEN** the Rizhao excerpt states 公司…为客户提供…服务，收取…包干费…费用 and the numeric rows carry no narrower local subject
- **THEN** the revenue answer keeps the provide-and-collect relationship
- **AND** the numeric records carry the report default group scope with a null-free basis
- **AND** no commodity role is generated from cargo types or the template sentence

### Requirement: The v14 round is reviewed with asset-read freshness
The rerun MUST use the new `revenue_sentence_repair=v14` identity, its own directory, and the same two frozen reports and plan, executing normally with the whole-round time measured from the first genuine execute to the second export return; failures and retries are retained. The review MUST re-judge the six dimension texts and every delivered fact for subject, column, amount, and unit, and MUST read freshness from the frozen assets. A company without commodity roles keeps a negative with the actual check scope. Passing requires recall 100%, accuracy 100%, zero critical numeric errors, at most 50000 tokens, and at most 300 seconds. Scores are not prefilled and the v13 observation is untouched.

#### Scenario: The formal round decides
- **WHEN** the v14 round completes
- **THEN** the freshness values come from the frozen report assets
- **AND** the gates are computed from the honest counts of the formal round
